import os
import re
import time
import json
import asyncio
from difflib import SequenceMatcher
from datetime import datetime, timezone

from dotenv import load_dotenv
from telegram import Bot
from telegram.constants import ParseMode

from radar.engine import run_radar_x
from gen.openclaw_gen import openclaw_chat
from tg.renderers import render_post_html
from tg.callbacks import build_post_keyboard
from tg.nova_bot import _extract_json, _prompt_from_candidate, _candidate_preview
from db.sqlite_store import (
    create_post,
    add_version,
    set_draft_message_ref,
    log_event,
    kv_get,
    kv_set,
    get_post,
    list_recent_latest_posts,
)


def _now_ts() -> int:
    return int(time.time())


async def _notify(bot: Bot, chat_id: int | None, text: str) -> None:
    if not chat_id:
        return
    await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def _kv_incr(key: str, step: int = 1) -> int:
    cur = await kv_get(key)
    try:
        n = int(cur) if cur is not None else 0
    except Exception:
        n = 0
    n += step
    await kv_set(key, str(n))
    return n


async def _mark_observability(*, result: str, winner_score: float | None = None, detail: str = "") -> None:
    await _kv_incr(f"obs:daily_radar_runs_total:{result}")
    await kv_set("obs:daily_radar_last_result", result)
    await kv_set("obs:daily_radar_last_run_ts", str(_now_ts()))
    if winner_score is not None:
        await kv_set("obs:daily_radar_last_winner_score", f"{winner_score:.3f}")
    if detail:
        await kv_set("obs:daily_radar_last_detail", detail[:500])


def _ops_message(*, level: str, title: str, run_id: str | None = None, post_id: str | None = None, winner_score: float | None = None, min_score: float | None = None, reason: str | None = None, detail: str | None = None) -> str:
    icon = {"ok": "✅", "skip": "🟡", "error": "🔴", "info": "ℹ️"}.get(level, "ℹ️")
    lines = [f"{icon} <b>{title}</b>"]
    if run_id:
        lines.append(f"<b>run_id:</b> <code>{run_id}</code>")
    if post_id:
        lines.append(f"<b>post_id:</b> <code>{post_id}</code>")
    if winner_score is not None:
        lines.append(f"<b>winner_score:</b> <code>{winner_score:.3f}</code>")
    if min_score is not None:
        lines.append(f"<b>min_score:</b> <code>{min_score:.3f}</code>")
    if reason:
        lines.append(f"<b>reason:</b> <code>{reason}</code>")
    if detail:
        lines.append(f"<b>detail:</b> <code>{detail[:180]}</code>")
    return "\n".join(lines)


async def _notify_once_per_day(*, bot: Bot, ops_chat_id: int, day: str, reason: str, text: str) -> None:
    if not ops_chat_id:
        return
    once_key = f"ops:daily_radar_notice:{reason}:{day}"
    already = await kv_get(once_key)
    if already:
        return
    await _notify(bot, ops_chat_id, text)
    await kv_set(once_key, str(_now_ts()))


def _norm_text(s: str) -> str:
    t = (s or "").lower()
    t = re.sub(r"https?://\S+|www\.\S+", " ", t)
    t = re.sub(r"[^a-z0-9áéíóúñ\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _content_signature(content: dict) -> str:
    topic = content.get("topic") or ""
    hook = content.get("hook") or ""
    caption = content.get("caption") or ""
    insight = content.get("insight") or ""
    return _norm_text(f"{topic} || {hook} || {caption} || {insight}")


async def _is_duplicate_candidate_or_semantic(post: dict, winner_id: str) -> tuple[bool, str, str | None]:
    recent_limit = int(os.getenv("DAILY_DUP_RECENT_LIMIT", "30"))
    sim_threshold = float(os.getenv("DAILY_DUP_SIM_THRESHOLD", "0.90"))
    recent = await list_recent_latest_posts(limit=recent_limit)

    # 1) duplicate by winner candidate id
    for r in recent:
        c = r.get("content") or {}
        if (c.get("radar_selected_candidate_id") or c.get("radar_winner_candidate_id")) == winner_id:
            return True, "duplicate_candidate", r.get("post_id")

    # 2) semantic near-duplicate by signature
    sig_new = _content_signature(post)
    if len(sig_new) < 30:
        return False, "", None

    for r in recent:
        c = r.get("content") or {}
        sig_old = _content_signature(c)
        if len(sig_old) < 30:
            continue
        sim = SequenceMatcher(None, sig_new, sig_old).ratio()
        if sim >= sim_threshold:
            return True, f"duplicate_semantic:{sim:.3f}", r.get("post_id")

    return False, "", None


async def run_daily() -> int:
    load_dotenv('/opt/ps_factory/config/.env', override=True)

    token = os.environ['TG_BOT_TOKEN']
    drafts_chat_id = int(os.environ['TG_DRAFTS_CHAT_ID'])
    ops_chat_id = int(os.getenv('TG_OPS_CHAT_ID', '0') or 0)

    bot = Bot(token=token)

    today = datetime.now(timezone.utc).date().isoformat()
    today_key = f"scheduler:daily_radar_run:{today}"
    lock_key = f"scheduler:daily_radar_lock:{today}"
    post_id = f"daily-radar-{today.replace('-', '')}"

    existing = await get_post(post_id)
    if existing and existing.get('draft_chat_id') and existing.get('draft_message_id'):
        await _notify_once_per_day(
            bot=bot,
            ops_chat_id=ops_chat_id,
            day=today,
            reason="already_published",
            text=_ops_message(level="info", title="Daily radar ya publicado hoy", post_id=post_id, reason="already_published"),
        )
        await kv_set(today_key, str(existing.get('draft_message_id')))
        await _mark_observability(result="skip", detail="already_published")
        return 0

    lock_acquired = False
    lock_val = await kv_get(lock_key)
    now_ts = _now_ts()
    if lock_val:
        try:
            lock_ts = int(lock_val)
        except Exception:
            lock_ts = now_ts
        if now_ts - lock_ts < 900:
            await _notify_once_per_day(
                bot=bot,
                ops_chat_id=ops_chat_id,
                day=today,
                reason="lock_recent",
                text=_ops_message(level="info", title="Daily radar en ejecución o recién corrido", reason="lock_recent", detail=lock_key),
            )
            await _mark_observability(result="skip", detail="lock_recent")
            return 0
    await kv_set(lock_key, str(now_ts))
    lock_acquired = True

    try:
        already = await kv_get(today_key)
        if already:
            await _notify_once_per_day(
                bot=bot,
                ops_chat_id=ops_chat_id,
                day=today,
                reason="already_marked",
                text=_ops_message(level="info", title="Daily radar ya ejecutado hoy", reason="already_marked", detail=today_key),
            )
            await _mark_observability(result="skip", detail="already_marked")
            return 0

        run_id, winner, alternates = await run_radar_x()
        winner_id = winner['candidate_id']
        winner_score = float(winner.get('total_score') or 0.0)
        min_score = float(os.getenv('RADAR_MIN_SCORE', '0.55'))

        if winner_score < min_score:
            await kv_set(today_key, f"skip:{winner_score:.3f}")
            await _mark_observability(result="skip", winner_score=winner_score, detail="below_threshold")
            await _notify(
                bot,
                ops_chat_id,
                _ops_message(
                    level="skip",
                    title="Daily radar skip por umbral editorial",
                    run_id=run_id,
                    winner_score=winner_score,
                    min_score=min_score,
                    reason="below_threshold",
                ),
            )
            print(json.dumps({
                "component": "daily_radar",
                "result": "skip",
                "run_id": run_id,
                "winner_score": round(winner_score, 3),
                "min_score": round(min_score, 3),
            }, ensure_ascii=False))
            return 0

        prompt = await _prompt_from_candidate(winner)
        raw = openclaw_chat(prompt)
        post = json.loads(_extract_json(raw))

        post['post_id'] = post_id
        post['topic'] = str(post.get('topic') or 'Radar X (Top)')
        post['radar_winner_candidate_id'] = winner_id
        post['radar_selected_candidate_id'] = winner_id

        alt_ids = [a['candidate_id'] for a in alternates]
        post['radar_alternate_candidate_ids'] = alt_ids
        post['radar_winner_preview'] = _candidate_preview(winner)
        post['radar_alternate_previews'] = [_candidate_preview(a) for a in alternates]

        is_dup, dup_reason, dup_of_post = await _is_duplicate_candidate_or_semantic(post, winner_id)
        if is_dup:
            await kv_set(today_key, f"skip:{dup_reason}:{dup_of_post or ''}")
            await _mark_observability(result="skip", winner_score=winner_score, detail=dup_reason)
            await _notify(
                bot,
                ops_chat_id,
                _ops_message(
                    level="skip",
                    title="Daily radar skip por duplicado",
                    run_id=run_id,
                    winner_score=winner_score,
                    reason=dup_reason,
                    detail=f"dup_of={dup_of_post or 'n/a'}",
                ),
            )
            print(json.dumps({
                "component": "daily_radar",
                "result": "skip",
                "reason": dup_reason,
                "run_id": run_id,
                "winner_score": round(winner_score, 3),
                "dup_of_post": dup_of_post,
            }, ensure_ascii=False))
            return 0

        bitcoin_anchor = str(post.get('bitcoin_anchor') or '')
        await create_post(post_id=post_id, topic=post['topic'], bitcoin_anchor=bitcoin_anchor)
        await add_version(post_id=post_id, version=1, content=post)
        await log_event(post_id, 'RADAR_GEN_DAILY', {
            'run_id': run_id,
            'winner': winner_id,
            'winner_score': winner_score,
            'min_score': min_score,
            'alts': alt_ids,
        })

        msg = await bot.send_message(
            chat_id=drafts_chat_id,
            text=render_post_html(post_id, 1, post),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=build_post_keyboard(post_id, candidate_ids=alt_ids),
        )
        await set_draft_message_ref(post_id, drafts_chat_id, msg.message_id)

        await kv_set(today_key, f"ok:{post_id}:{msg.message_id}")
        await _mark_observability(result="ok", winner_score=winner_score, detail=post_id)

        await _notify(
            bot,
            ops_chat_id,
            _ops_message(
                level="ok",
                title="Daily radar enviado a Drafts",
                run_id=run_id,
                post_id=post_id,
                winner_score=winner_score,
            ),
        )
        print(json.dumps({
            "component": "daily_radar",
            "result": "ok",
            "run_id": run_id,
            "post_id": post_id,
            "winner_score": round(winner_score, 3),
            "draft_message_id": msg.message_id,
        }, ensure_ascii=False))
        return 0

    except Exception as e:
        await _mark_observability(result="error", detail=str(e))
        await _notify(
            bot,
            ops_chat_id,
            _ops_message(level="error", title="Daily radar error", reason="exception", detail=str(e)),
        )
        print(json.dumps({
            "component": "daily_radar",
            "result": "error",
            "error": str(e)[:300],
        }, ensure_ascii=False))
        raise

    finally:
        if lock_acquired:
            await kv_set(lock_key, "0")


if __name__ == '__main__':
    raise SystemExit(asyncio.run(run_daily()))
