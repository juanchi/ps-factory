import os
import time
import json
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone

from dotenv import load_dotenv

from radar.x_client import XClient
from radar.x_radar import fetch_list_tweets  # (tweets, next_token)
from radar.scoring import score_tweet

from db.sqlite_store import (
    kv_get,
    kv_set,
    upsert_radar_candidate,
    get_radar_candidate,
    list_radar_candidates_by_run,
)

load_dotenv("/opt/ps_factory/config/.env", override=True)


def _now_ts() -> int:
    return int(time.time())


def _run_id() -> str:
    return f"run-{_now_ts()}"


def _short_title_from_tweet(text: str, max_len: int = 110) -> str:
    t = (text or "").strip().replace("\n", " ")
    if len(t) <= max_len:
        return t
    return t[: max_len - 1].rstrip() + "…"


def _parse_created_at_to_ts(created_at: Optional[str]) -> Optional[int]:
    if not created_at:
        return None
    try:
        s = created_at.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp())
    except Exception:
        return None


async def run_radar_x() -> Tuple[str, Dict, List[Dict]]:
    """
    Radar con 2 listas de X (Camino B real para Lists):
    - List endpoint no soporta start_time ni since_id
    - Traemos N tweets recientes y filtramos localmente por created_at
    """

    bearer = os.getenv("X_BEARER_TOKEN", "").strip()
    list_global = os.getenv("X_LIST_GLOBAL_ID", "").strip()
    list_panama = os.getenv("X_LIST_PANAMA_ID", "").strip()

    if not bearer:
        raise RuntimeError("Missing X_BEARER_TOKEN")
    if not list_global or not list_panama:
        raise RuntimeError("Missing X_LIST_GLOBAL_ID / X_LIST_PANAMA_ID")

    client = XClient(bearer_token=bearer)

    # Último timestamp procesado por lista
    fallback_hours = int(os.getenv("RADAR_FALLBACK_HOURS", "6"))
    default_start_ts = _now_ts() - (fallback_hours * 3600)

    last_seen_global = await kv_get("x_last_seen_ts_global")
    last_seen_panama = await kv_get("x_last_seen_ts_panama")

    start_ts_global = int(last_seen_global) if last_seen_global else default_start_ts
    start_ts_panama = int(last_seen_panama) if last_seen_panama else default_start_ts

    # Traer tweets recientes (2 llamadas total)
    global_page, _ = fetch_list_tweets(
        client,
        list_global,
        max_results=40,
        source="x_list_global",
    )
    panama_page, _ = fetch_list_tweets(
        client,
        list_panama,
        max_results=10,
        source="x_list_panama",
    )

    # Filtrar localmente por ventana de tiempo
    tweets: List[Dict] = []
    for tw in global_page:
        ts = _parse_created_at_to_ts(tw.get("created_at"))
        if ts and ts > start_ts_global:
            tweets.append(tw)

    for tw in panama_page:
        ts = _parse_created_at_to_ts(tw.get("created_at"))
        if ts and ts > start_ts_panama:
            tweets.append(tw)

    # Actualizar last_seen por lista con el created_at más nuevo visto (aunque lo filtres o no)
    max_ts_global = start_ts_global
    for tw in global_page:
        ts = _parse_created_at_to_ts(tw.get("created_at"))
        if ts and ts > max_ts_global:
            max_ts_global = ts

    max_ts_panama = start_ts_panama
    for tw in panama_page:
        ts = _parse_created_at_to_ts(tw.get("created_at"))
        if ts and ts > max_ts_panama:
            max_ts_panama = ts

    if max_ts_global > start_ts_global:
        await kv_set("x_last_seen_ts_global", str(max_ts_global))
    if max_ts_panama > start_ts_panama:
        await kv_set("x_last_seen_ts_panama", str(max_ts_panama))

    run_id = _run_id()
    created_at = _now_ts()

    scored: List[Dict] = []
    for tw in tweets:
        if not tw.get("id"):
            continue

        total, breakdown = score_tweet(tw, source=tw.get("source", "x"))
        candidate_id = f"x:{tw['id']}"

        evidence = {
            "tweet": tw,
            "note": "X es sensor, no fuente final. Confirmar con medios si aplica.",
        }

        candidate = {
            "candidate_id": candidate_id,
            "run_id": run_id,
            "source": tw.get("source", "x"),
            "title": _short_title_from_tweet(tw.get("text", "")),
            "summary": None,
            "evidence_json": json.dumps(evidence, ensure_ascii=False),
            "scores_json": json.dumps(breakdown, ensure_ascii=False),
            "total_score": float(total),
            "created_at": created_at,
        }

        scored.append(candidate)
        await upsert_radar_candidate(candidate)

    scored.sort(key=lambda c: c["total_score"], reverse=True)

    # Tie-break Panamá: si top2 están muy cerca, prioriza mayor pa_hits
    tie_delta = float(os.getenv("RADAR_TIE_DELTA", "0.25"))
    tie_min_pa_adv = int(os.getenv("RADAR_TIE_MIN_PA_ADV", "1"))
    if len(scored) >= 2:
        a = scored[0]
        b = scored[1]
        gap = float(a.get("total_score") or 0.0) - float(b.get("total_score") or 0.0)
        if gap <= tie_delta:
            try:
                sa = json.loads(a.get("scores_json") or "{}")
            except Exception:
                sa = {}
            try:
                sb = json.loads(b.get("scores_json") or "{}")
            except Exception:
                sb = {}

            pa_a = int(sa.get("pa_hits") or 0)
            pa_b = int(sb.get("pa_hits") or 0)
            if (pa_b - pa_a) >= tie_min_pa_adv:
                scored[0], scored[1] = scored[1], scored[0]

    # top 4 (1 winner + 3 alternos)
    top4 = scored[:4]

    # Fallback/fill desde el último run para siempre tener hasta 4 opciones
    # (sin costo extra de API X). Si hay pocos nuevos, completamos con previos.
    last_run = await kv_get("radar_last_run_id")
    if (not top4 or len(top4) < 4) and last_run:
        prev = await list_radar_candidates_by_run(last_run)
        prev.sort(key=lambda c: c["total_score"], reverse=True)

        seen = {c.get("candidate_id") for c in top4}
        for p in prev:
            cid = p.get("candidate_id")
            if cid in seen:
                continue
            top4.append(p)
            seen.add(cid)
            if len(top4) >= 4:
                break

    if not top4:
        raise RuntimeError("Radar no encontró candidatos (ni nuevos ni previos).")

    await kv_set("radar_last_run_id", run_id)

    winner = top4[0]
    alternates = top4[1:4]
    return run_id, winner, alternates


async def get_candidate(candidate_id: str) -> Dict | None:
    return await get_radar_candidate(candidate_id)
