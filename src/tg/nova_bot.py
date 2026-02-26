import os
import time
import json
import html
import sqlite3
import asyncio
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes
from telegram.error import BadRequest, NetworkError, TimedOut
from telegram.request import HTTPXRequest

from tg.callbacks import build_post_keyboard
from gen.openclaw_gen import openclaw_chat
from gen.image_gen import generate_image, validate_4_5, build_image_prompt_en as _build_image_prompt_en, ImageGenError

from db.sqlite_store import (
    DB_PATH,
    create_post,
    add_version,
    get_latest_version_number,
    get_latest_version,
    get_version,
    list_versions,
    set_draft_message_ref,
    approve_post,
    get_post,
    log_event,
    get_last_post_id,
    get_radar_candidate,
    kv_get,
    kv_set,
)

from tg.renderers import render_post_html
from radar.engine import run_radar_x

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _now_ts() -> int:
    return int(time.time())


def _e(s: str) -> str:
    return html.escape(str(s or ""), quote=False)


def _utc_day_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _compose_publish_pack(post_id: str, ver: int, content: dict) -> str:
    topic = str(content.get("topic") or "POST")
    hook = str(content.get("hook") or "").strip()
    explain = str(content.get("explain_simple") or "").strip()
    caption = str(content.get("caption") or "").strip()

    use_emojis = os.getenv("SOCIAL_USE_EMOJIS", "1").strip().lower() in {"1", "true", "yes", "on"}
    brand_tag = os.getenv("SOCIAL_BRAND_HASHTAG", "#PanamáSoberano").strip() or "#PanamáSoberano"
    second_tag = os.getenv("SOCIAL_SECOND_HASHTAG", "").strip()
    tags = " ".join([t for t in [brand_tag, second_tag] if t]).strip()

    e1 = "⚡ " if use_emojis else ""
    e2 = "🎯 " if use_emojis else ""

    def _fit_x_limit(s: str, limit: int = 280) -> str:
        s = (s or "").strip()
        if len(s) <= limit:
            return s
        return s[: max(1, limit - 1)].rstrip() + "…"

    x_base = (
        f"{e1}{hook}\n\n"
        f"{caption[:220]}\n\n"
        f"{tags}"
    ).strip()
    x_copy = _fit_x_limit(x_base, 280)

    ig_copy = (
        f"{e1}{hook}\n\n"
        f"{explain[:320]}\n\n"
        f"{e2}{caption[:180]}\n\n"
        f"{tags}"
    ).strip()

    tiktok_copy = (
        f"{e1}{hook}\n"
        f"{explain[:180]}\n\n"
        f"{e2}{caption[:140]}\n"
        f"{tags}"
    ).strip()

    return (
        f"🟠 <b>{_e(topic)}</b> — <code>{_e(post_id)}</code> · v{ver}\n\n"
        f"<b>X</b>\n<code>{_e(x_copy)}</code>\n\n"
        f"<b>Instagram</b>\n<code>{_e(ig_copy)}</code>\n\n"
        f"<b>TikTok</b>\n<code>{_e(tiktok_copy)}</code>"
    )


def _extract_json(s: str) -> str:
    s = (s or "").strip()

    if "```" in s:
        parts = s.split("```")
        candidates = [p for p in parts if "{" in p and "}" in p]
        if candidates:
            s = max(candidates, key=len).strip()
            if s.lower().startswith("json"):
                s = s[4:].strip()

    if "{" in s and "}" in s:
        s = s[s.find("{") : s.rfind("}") + 1]
    return s


def _versions_keyboard(post_id: str, versions: list[int], limit: int = 8) -> InlineKeyboardMarkup:
    rows = []
    for v in versions[:limit]:
        rows.append(
            [
                InlineKeyboardButton(text=f"👁 Ver v{v}", callback_data=f"VIEW:{post_id}:{v}"),
                InlineKeyboardButton(text=f"↩️ Revert v{v}", callback_data=f"REVERT:{post_id}:{v}"),
            ]
        )
    rows.append([InlineKeyboardButton(text="Cerrar", callback_data="CLOSE")])
    return InlineKeyboardMarkup(rows)


async def _safe_edit_message_text(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup,
) -> bool:
    try:
        await context.bot.edit_message_text(
            chat_id=int(chat_id),
            message_id=int(message_id),
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
        return True
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return False
        raise


async def _safe_reply(update: Update, text: str, *, tries: int = 3) -> None:
    """
    reply_text con retry para errores de red (httpx.ReadError, timeouts).
    """
    last = None
    for _ in range(tries):
        try:
            if update.message:
                await update.message.reply_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            return
        except (NetworkError, TimedOut) as e:
            last = e
            await asyncio.sleep(1.2)
    if last:
        raise last


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    logger.exception("Unhandled exception", exc_info=err)

    try:
        if isinstance(update, Update) and update.effective_message:
            msg = (
                "⚠️ Ocurrió un error.\n\n"
                f"<code>{type(err).__name__}: {str(err)[:300]}</code>\n\n"
                "Si fue un error de red (Telegram), reintenta en 10–30 segundos."
            )
            await update.effective_message.reply_text(msg, parse_mode=ParseMode.HTML)
    except Exception:
        pass


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Nova lista ✅\n\n"
        "<b>Comandos</b>:\n"
        "/demo\n"
        "/gen &lt;tema&gt;\n"
        "/radar (corre Radar X y manda el ganador a Drafts)\n"
        "/health\n"
        "/last\n"
        "/post &lt;id&gt;\n"
        "/versions &lt;id&gt;\n\n"
        "<b>Botones</b>:\n"
        "✅ APROBAR\n"
        "♻️ REGENERAR\n"
        "⚡️ Generar alterno (Top 3 del Radar)\n"
        "📚 Versions\n",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lines = []
    ok = True

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("SELECT 1;")
        conn.close()
        lines.append("🟢 SQLite OK")
    except Exception as e:
        ok = False
        lines.append(f"🔴 SQLite ERROR: {e}")

    missing = []
    for k in ["TG_DRAFTS_CHAT_ID", "TG_APPROVED_CHAT_ID", "TG_BOT_TOKEN"]:
        if not os.getenv(k):
            missing.append(k)

    for k in ["X_BEARER_TOKEN", "X_LIST_GLOBAL_ID", "X_LIST_PANAMA_ID"]:
        if not os.getenv(k):
            missing.append(k)

    if not missing:
        lines.append("🟢 Env vars OK")
    else:
        ok = False
        lines.append("🔴 Env vars faltantes: " + ", ".join(missing))

    try:
        test = openclaw_chat("Responde SOLO 'ok'.")
        if "ok" in (test or "").lower():
            lines.append("🟢 Fábrica de Contenidos OK")
        else:
            lines.append("🟡 Fábrica de Contenidos respondió distinto a 'ok'")
    except Exception as e:
        ok = False
        lines.append(f"🔴 Fábrica de Contenidos ERROR: {e}")

    status = "🟢 SISTEMA SALUDABLE" if ok else "🔴 PROBLEMA DETECTADO"
    await update.message.reply_text(
        f"<b>{status}</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_last(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    post_id = await get_last_post_id()
    if not post_id:
        await update.message.reply_text("ℹ️ No hay posts todavía en SQLite.", parse_mode=ParseMode.HTML)
        return

    latest = await get_latest_version(post_id)
    post_db = await get_post(post_id)

    if not latest:
        status = (post_db or {}).get("status", "unknown")
        await update.message.reply_text(
            f"ℹ️ Post encontrado pero sin versiones.\n\n<b>ID:</b> <code>{post_id}</code>\n<b>Status:</b> <code>{status}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    ver, content = latest
    status = (post_db or {}).get("status", "unknown")
    header = (
        f"🧾 <b>Último post</b>\n"
        f"<b>ID:</b> <code>{post_id}</code>\n"
        f"<b>Status:</b> <code>{status}</code>\n"
        f"<b>Versión:</b> <code>v{ver}</code>\n\n"
    )
    await update.message.reply_text(
        header + render_post_html(post_id, ver, content),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    post_id = " ".join(context.args).strip()
    if not post_id:
        await update.message.reply_text("Uso: <code>/post &lt;id&gt;</code>", parse_mode=ParseMode.HTML)
        return

    post_db = await get_post(post_id)
    if not post_db:
        await update.message.reply_text(f"❌ No existe en SQLite: <code>{post_id}</code>", parse_mode=ParseMode.HTML)
        return

    latest = await get_latest_version(post_id)
    if not latest:
        status = post_db.get("status", "unknown")
        await update.message.reply_text(
            f"ℹ️ Encontré el post pero no hay versiones.\n\n<b>ID:</b> <code>{post_id}</code>\n<b>Status:</b> <code>{status}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    ver, content = latest
    status = post_db.get("status", "unknown")
    header = (
        f"🧾 <b>Post</b>\n"
        f"<b>ID:</b> <code>{post_id}</code>\n"
        f"<b>Status:</b> <code>{status}</code>\n"
        f"<b>Versión:</b> <code>v{ver}</code>\n\n"
    )
    await update.message.reply_text(
        header + render_post_html(post_id, ver, content),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_versions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    post_id = " ".join(context.args).strip()
    if not post_id:
        await update.message.reply_text("Uso: <code>/versions &lt;id&gt;</code>", parse_mode=ParseMode.HTML)
        return

    post_db = await get_post(post_id)
    if not post_db:
        await update.message.reply_text(f"❌ No existe en SQLite: <code>{post_id}</code>", parse_mode=ParseMode.HTML)
        return

    vers = await list_versions(post_id)
    if not vers:
        await update.message.reply_text(f"ℹ️ No hay versiones para: <code>{post_id}</code>", parse_mode=ParseMode.HTML)
        return

    status = post_db.get("status", "unknown")
    text = (
        f"📚 <b>Versiones</b>\n"
        f"<b>ID:</b> <code>{post_id}</code>\n"
        f"<b>Status:</b> <code>{status}</code>\n\n"
        f"Disponibles: " + ", ".join([f"v{v}" for v in vers[:20]])
    )

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=_versions_keyboard(post_id, vers),
        disable_web_page_preview=True,
    )


async def cmd_demo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    post_id = f"demo-{_now_ts()}"
    post = {
        "post_id": post_id,
        "topic": "DEMO",
        "hook": "Lo que parece ‘noticia financiera’ en realidad es un cambio de poder.",
        "explain_simple": "Cuando cambian las reglas del dinero, cambian los incentivos: quién gana, quién pierde y quién puede censurar.",
        "bitcoin_anchor": "Bitcoin es la salida porque no depende de permisos: es dinero abierto, verificable y resistente a censura.",
        "insight": "El verdadero ‘producto’ de los sistemas cerrados es el control. Bitcoin compite quitándoles ese control.",
        "risk": "Evitar conclusiones absolutas: confirmar datos y separar hecho vs opinión.",
        "caption": "Panamá no necesita más ruido: necesita claridad.\n\nCuando el dinero tiene dueño, tu vida también.\n\n#Bitcoin #Panamá #EducaciónFinanciera",
        "visual_prompt": "Arte 4:5 tipo editorial moderno, símbolo abstracto de libertad monetaria, estilo limpio, alto contraste, sin logos de terceros, texto en español: 'Bitcoin = Dinero sin permiso'.",
        "qa": [
            "¿Está anclado explícitamente a Bitcoin?",
            "¿No es partidista ni ataque personal?",
            "¿Tiene valor educativo real?",
            "¿Riesgos/limitaciones mencionados?",
        ],
    }

    drafts = os.getenv("TG_DRAFTS_CHAT_ID")
    if not drafts:
        await update.message.reply_text("❌ Falta TG_DRAFTS_CHAT_ID en .env", parse_mode=ParseMode.HTML)
        return
    drafts_chat_id = int(drafts)

    bitcoin_anchor = str(post.get("bitcoin_anchor") or "")
    await create_post(post_id=post_id, topic=post["topic"], bitcoin_anchor=bitcoin_anchor)
    await add_version(post_id=post_id, version=1, content=post)
    await log_event(post_id, "DEMO", {"source": "telegram"})

    msg = await context.bot.send_message(
        chat_id=drafts_chat_id,
        text=render_post_html(post_id, 1, post),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=build_post_keyboard(post_id, candidate_ids=[]),
    )
    await set_draft_message_ref(post_id, drafts_chat_id, msg.message_id)
    await update.message.reply_text("Listo ✅ Mandé un post demo a PS | Drafts.", parse_mode=ParseMode.HTML)


def _candidate_preview(candidate: dict) -> dict:
    """Resumen corto legible para mostrar alternos en Draft."""
    try:
        evidence = json.loads(candidate.get("evidence_json") or "{}")
    except Exception:
        evidence = {}

    tw = evidence.get("tweet", {}) or {}
    author = (tw.get("author") or {}).get("username") or "unknown"
    text = (tw.get("text") or candidate.get("title") or "").strip().replace("\n", " ")
    if len(text) > 110:
        text = text[:109].rstrip() + "…"

    return {
        "candidate_id": candidate.get("candidate_id"),
        "author": author,
        "title": text,
        "score": round(float(candidate.get("total_score") or 0), 3),
    }


def _quality_gate_reason(candidate: dict) -> str | None:
    """
    Hard floor de calidad para Radar:
    - score mínimo (ya existe)
    - relevancia mínima
    - riesgo máximo
    - link requerido (opcional por env)
    """
    try:
        scores = json.loads(candidate.get("scores_json") or "{}")
    except Exception:
        scores = {}

    relevance = float(scores.get("relevance") or 0.0)
    risk = float(scores.get("risk") or 0.0)
    has_url = bool(scores.get("has_url"))

    min_rel = float(os.getenv("RADAR_MIN_RELEVANCE", "4.0"))
    max_risk = float(os.getenv("RADAR_MAX_RISK", "4.0"))
    require_link = os.getenv("RADAR_REQUIRE_LINK", "1").strip().lower() in {"1", "true", "yes", "on"}

    if relevance < min_rel:
        return f"relevance_below:{relevance:.2f}<{min_rel:.2f}"
    if risk > max_risk:
        return f"risk_above:{risk:.2f}>{max_risk:.2f}"
    if require_link and not has_url:
        return "missing_link"
    return None


def build_image_prompt_en(visual_prompt: str) -> str:
    """Provider-ready EN prompt policy for image generation."""
    return _build_image_prompt_en(visual_prompt)


async def _prompt_from_candidate(candidate: dict) -> str:
    evidence = json.loads(candidate["evidence_json"])
    tw = evidence.get("tweet", {})
    author = tw.get("author", {}) or {}
    metrics = tw.get("metrics", {}) or {}

    title = candidate.get("title") or ""
    text = tw.get("text") or ""
    username = author.get("username") or ""
    source = candidate.get("source") or "x"

    return f"""
Eres Panamá Soberano (todo en español).
Tono: profesor que abre los ojos + estratega de incentivos. No partidista. No ataques personales.
Objetivo: 1 post de redes basado en el tema, SIEMPRE con Bitcoin Anchor explícito.

INSUMO DEL RADAR (X es sensor, no fuente final):
- source: {source}
- autor: @{username}
- texto: {text}
- métricas: likes={metrics.get("like_count",0)}, rts={metrics.get("retweet_count",0)}, replies={metrics.get("reply_count",0)}, quotes={metrics.get("quote_count",0)}

TÍTULO/SEÑAL:
{title}

Reglas:
- No afirmes como hecho algo que no esté claro; usa lenguaje responsable (“según”, “parece”, “se reporta”).
- Incluye un “risk” indicando límites y necesidad de confirmación si aplica.
- Siempre ancla explícitamente a Bitcoin.

Devuelve JSON válido con estas llaves exactas:
- post_id (string)
- topic (string)
- hook (string)
- explain_simple (string)
- bitcoin_anchor (string)
- insight (string)
- risk (string)
- caption (string)
- visual_prompt (string)
- qa (array de strings)

Responde SOLO el JSON.
""".strip()


async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _safe_reply(update, "🛰️ Corriendo Radar X…")

    try:
        run_id, winner, alternates = await run_radar_x()
    except Exception as e:
        await update.message.reply_text(f"❌ Radar falló: <code>{str(e)[:500]}</code>", parse_mode=ParseMode.HTML)
        return

    winner_id = winner["candidate_id"]
    winner_score = float(winner.get("total_score") or 0.0)
    min_score = float(os.getenv("RADAR_MIN_SCORE", "0.55"))
    if winner_score < min_score:
        await update.message.reply_text(
            "🟡 Radar ejecutado, pero no se generó Draft por umbral editorial.\n\n"
            f"<b>run_id:</b> <code>{run_id}</code>\n"
            f"<b>winner_score:</b> <code>{winner_score:.3f}</code>\n"
            f"<b>min_score:</b> <code>{min_score:.3f}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    gate_reason = _quality_gate_reason(winner)
    if gate_reason:
        await update.message.reply_text(
            "🟡 Radar ejecutado, pero no se generó Draft por quality gate.\n\n"
            f"<b>run_id:</b> <code>{_e(run_id)}</code>\n"
            f"<b>reason:</b> <code>{_e(gate_reason)}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    prompt = await _prompt_from_candidate(winner)
    raw = openclaw_chat(prompt)

    try:
        post = json.loads(_extract_json(raw))
    except Exception:
        await update.message.reply_text(
            "❌ La Fábrica de Contenidos respondió pero no vino JSON válido.\n\n"
            + "<code>" + (raw[:1800] if raw else "<vacío>") + "</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    # post_id canónico y corto (evita Button_data_invalid por callback_data > 64 bytes)
    post_id = f"radar-{_now_ts()}"
    post["post_id"] = post_id
    post["topic"] = str(post.get("topic") or "Radar X (Top)")
    post["radar_winner_candidate_id"] = winner_id
    post["radar_selected_candidate_id"] = winner_id
    post["visual_prompt_en"] = build_image_prompt_en(str(post.get("visual_prompt") or ""))

    alt_ids = [a["candidate_id"] for a in alternates]
    post["radar_alternate_candidate_ids"] = alt_ids
    post["radar_winner_preview"] = _candidate_preview(winner)
    post["radar_alternate_previews"] = [_candidate_preview(a) for a in alternates]

    drafts_chat_id = int(os.environ["TG_DRAFTS_CHAT_ID"])

    bitcoin_anchor = str(post.get("bitcoin_anchor") or "")
    await create_post(post_id=post_id, topic=post["topic"], bitcoin_anchor=bitcoin_anchor)
    await add_version(post_id=post_id, version=1, content=post)
    await log_event(post_id, "RADAR_GEN", {"run_id": run_id, "winner": winner_id, "alts": alt_ids})

    # IMPORTANT: candidate_ids aquí son candidate_id reales ("x:....")
    msg = await context.bot.send_message(
        chat_id=drafts_chat_id,
        text=render_post_html(post_id, 1, post),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=build_post_keyboard(post_id, candidate_ids=alt_ids),
    )
    await set_draft_message_ref(post_id, drafts_chat_id, msg.message_id)

    await update.message.reply_text(
        f"✅ Radar listo. Ganador enviado a Drafts.\n\n<b>run_id:</b> <code>{run_id}</code>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_intraday_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ops_chat_id = int(os.getenv("TG_OPS_CHAT_ID", "0") or 0)
    effective_chat = update.effective_chat.id if update.effective_chat else 0

    # Seguridad básica: comando manual solo permitido desde chat OPS.
    if ops_chat_id and int(effective_chat) != ops_chat_id:
        await update.message.reply_text("❌ Este comando solo está permitido en PS | OPS.")
        return

    await _safe_reply(update, "🚨 Ejecutando intraday monitor manual...")

    try:
        from scheduler.intraday_monitor_run import run_intraday_monitor
        await run_intraday_monitor()
    except Exception as e:
        await update.message.reply_text(
            f"❌ Intraday monitor falló: <code>{str(e)[:400]}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    last_result = await kv_get("intraday:last_result")
    last_ts = await kv_get("intraday:last_run_ts")
    last_detail_raw = await kv_get("intraday:last_detail")

    detail = {}
    try:
        detail = json.loads(last_detail_raw or "{}")
    except Exception:
        detail = {}

    reason_map = {
        "skip_low_score": "score por debajo del umbral",
        "skip_low_relevance": "relevancia baja",
        "skip_high_risk": "riesgo alto",
        "skip_missing_link": "sin enlace de soporte",
        "skip_cooldown": "en cooldown",
        "skip_stale": "candidato desactualizado",
        "skip_cap_reached": "límite diario de alertas alcanzado",
        "alerted": "alerta enviada a OPS",
    }
    human_reason = reason_map.get((last_result or "").strip(), (last_result or "unknown"))

    score = detail.get("score")
    relevance = detail.get("relevance")
    risk = detail.get("risk")
    has_url = detail.get("has_url")
    title = (detail.get("title") or "")[:140]
    min_score = detail.get("min_score")
    min_relevance = detail.get("min_relevance")
    max_risk = detail.get("max_risk")
    require_link = detail.get("require_link")
    delta_score = detail.get("delta_score")
    delta_relevance = detail.get("delta_relevance")
    delta_risk = detail.get("delta_risk")

    extra = ""
    if title:
        extra += f"\n<b>title:</b> {_e(title)}"
    if score is not None:
        extra += f"\n<b>score:</b> <code>{score}</code>"
    if relevance is not None:
        extra += f"\n<b>relevance:</b> <code>{relevance}</code>"
    if risk is not None:
        extra += f"\n<b>risk:</b> <code>{risk}</code>"
    if has_url is not None:
        extra += f"\n<b>has_link:</b> <code>{'yes' if has_url else 'no'}</code>"

    if min_score is not None:
        extra += f"\n<b>min_score:</b> <code>{min_score}</code>"
    if min_relevance is not None:
        extra += f"\n<b>min_relevance:</b> <code>{min_relevance}</code>"
    if max_risk is not None:
        extra += f"\n<b>max_risk:</b> <code>{max_risk}</code>"
    if require_link is not None:
        extra += f"\n<b>require_link:</b> <code>{'yes' if bool(require_link) else 'no'}</code>"

    if delta_score is not None:
        extra += f"\n<b>delta_score:</b> <code>{delta_score}</code>"
    if delta_relevance is not None:
        extra += f"\n<b>delta_relevance:</b> <code>{delta_relevance}</code>"
    if delta_risk is not None:
        extra += f"\n<b>delta_risk:</b> <code>{delta_risk}</code>"

    extra += "\n\n<i>Si igual lo quieres, usa:</i> <code>/intraday_force_draft</code>"

    await update.message.reply_text(
        "✅ Intraday monitor ejecutado.\n\n"
        f"<b>resultado:</b> <code>{_e(human_reason)}</code>\n"
        f"<b>ts:</b> <code>{(last_ts or 'n/a')[:40]}</code>"
        f"{extra}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_intraday_force_draft(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ops_chat_id = int(os.getenv("TG_OPS_CHAT_ID", "0") or 0)
    drafts_chat_id = int(os.getenv("TG_DRAFTS_CHAT_ID", "0") or 0)
    effective_chat = update.effective_chat.id if update.effective_chat else 0

    if ops_chat_id and int(effective_chat) != ops_chat_id:
        await update.message.reply_text("❌ Este comando solo está permitido en PS | OPS.")
        return

    candidate_id = None
    if context.args:
        candidate_id = (context.args[0] or "").strip()

    if not candidate_id:
        raw = await kv_get("intraday:last_detail")
        try:
            d = json.loads(raw or "{}")
        except Exception:
            d = {}
        candidate_id = (d.get("candidate_id") or "").strip()

    if not candidate_id:
        await update.message.reply_text("❌ No tengo candidate_id reciente. Ejecuta /intraday_now primero.")
        return

    cand = await get_radar_candidate(candidate_id)
    if not cand:
        await update.message.reply_text(f"❌ candidate_id no encontrado: <code>{_e(candidate_id)}</code>", parse_mode=ParseMode.HTML)
        return

    await update.message.reply_text(
        f"🛠 Forzando Draft desde intraday candidate <code>{_e(candidate_id)}</code>...",
        parse_mode=ParseMode.HTML,
    )

    prompt = await _prompt_from_candidate(cand)
    raw = openclaw_chat(prompt)

    try:
        post = json.loads(_extract_json(raw))
    except Exception:
        await update.message.reply_text("❌ El modelo no devolvió JSON válido.")
        return

    post_id = f"intraday-{_now_ts()}"
    post["post_id"] = post_id
    post["topic"] = str(post.get("topic") or "Intraday manual override")
    post["radar_selected_candidate_id"] = candidate_id
    post["radar_winner_candidate_id"] = candidate_id
    post["radar_winner_preview"] = _candidate_preview(cand)
    post["radar_alternate_candidate_ids"] = []
    post["radar_alternate_previews"] = []

    await create_post(post_id, post["topic"], post.get("bitcoin_anchor") or "")
    await add_version(post_id, 1, post)
    await log_event(post_id, "INTRADAY_FORCE_DRAFT", {"candidate_id": candidate_id})

    msg = await context.bot.send_message(
        chat_id=drafts_chat_id,
        text=render_post_html(post_id, 1, post),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=build_post_keyboard(post_id, candidate_ids=[]),
    )
    await set_draft_message_ref(post_id, drafts_chat_id, msg.message_id)

    await update.message.reply_text(
        "✅ Draft forzado enviado a Drafts.\n\n"
        f"<b>post_id:</b> <code>{_e(post_id)}</code>\n"
        f"<b>candidate_id:</b> <code>{_e(candidate_id)}</code>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_intraday_force_draft(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ops_chat_id = int(os.getenv("TG_OPS_CHAT_ID", "0") or 0)
    effective_chat = update.effective_chat.id if update.effective_chat else 0

    if ops_chat_id and int(effective_chat) != ops_chat_id:
        await update.message.reply_text("❌ Este comando solo está permitido en PS | OPS.")
        return

    await _safe_reply(update, "🛠 Ejecutando intraday FORCE -> Draft...")

    try:
        run_id, winner, alternates = await run_radar_x()
    except Exception as e:
        await update.message.reply_text(f"❌ Radar falló: <code>{str(e)[:500]}</code>", parse_mode=ParseMode.HTML)
        return

    winner_id = winner["candidate_id"]
    winner_score = float(winner.get("total_score") or 0.0)

    prompt = await _prompt_from_candidate(winner)
    raw = openclaw_chat(prompt)
    try:
        post = json.loads(_extract_json(raw))
    except Exception:
        await update.message.reply_text("❌ El modelo no devolvió JSON válido.", parse_mode=ParseMode.HTML)
        return

    post_id = f"intraday-{_now_ts()}"
    post["post_id"] = post_id
    post["topic"] = str(post.get("topic") or "Intraday FORCE")
    post["radar_winner_candidate_id"] = winner_id
    post["radar_selected_candidate_id"] = winner_id
    post["visual_prompt_en"] = build_image_prompt_en(str(post.get("visual_prompt") or ""))

    alt_ids = [a["candidate_id"] for a in alternates]
    post["radar_alternate_candidate_ids"] = alt_ids
    post["radar_winner_preview"] = _candidate_preview(winner)
    post["radar_alternate_previews"] = [_candidate_preview(a) for a in alternates]

    bitcoin_anchor = str(post.get("bitcoin_anchor") or "")
    await create_post(post_id=post_id, topic=post["topic"], bitcoin_anchor=bitcoin_anchor)
    await add_version(post_id=post_id, version=1, content=post)
    await log_event(post_id, "INTRADAY_FORCE_DRAFT", {
        "run_id": run_id,
        "winner": winner_id,
        "winner_score": winner_score,
        "alts": alt_ids,
    })

    drafts_chat_id = int(os.getenv("TG_DRAFTS_CHAT_ID", "0") or 0)
    if not drafts_chat_id:
        await update.message.reply_text("❌ TG_DRAFTS_CHAT_ID no está configurado.")
        return

    msg = await context.bot.send_message(
        chat_id=drafts_chat_id,
        text=render_post_html(post_id, 1, post),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=build_post_keyboard(post_id, candidate_ids=alt_ids),
    )
    await set_draft_message_ref(post_id, drafts_chat_id, msg.message_id)

    await update.message.reply_text(
        "✅ FORCE enviado a Drafts.\n\n"
        f"<b>run_id:</b> <code>{run_id}</code>\n"
        f"<b>post_id:</b> <code>{post_id}</code>\n"
        f"<b>winner_score:</b> <code>{winner_score:.3f}</code>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_gen(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    topic = " ".join(context.args).strip()
    if not topic:
        topic = "Un tema actual de economía o noticias que afecte a Panamá y que se pueda anclar a Bitcoin."

    canonical_post_id = f"gen-{_now_ts()}"

    prompt = f"""
Eres Panamá Soberano (todo en español).
Tono: profesor que abre los ojos + estratega de incentivos. No partidista. No ataques personales.
Objetivo: 1 post de redes basado en el tema, SIEMPRE con Bitcoin Anchor explícito.

Tema: {topic}

Devuelve JSON válido con estas llaves exactas:
- post_id (string)
- topic (string)
- hook (string)
- explain_simple (string)
- bitcoin_anchor (string)
- insight (string)
- risk (string)
- caption (string)
- visual_prompt (string)
- qa (array de strings)

Responde SOLO el JSON.
""".strip()

    raw = openclaw_chat(prompt)

    try:
        post = json.loads(_extract_json(raw))
    except Exception:
        await update.message.reply_text(
            "❌ La Fábrica de Contenidos respondió pero no vino en JSON válido.",
            parse_mode=ParseMode.HTML,
        )
        return

    # post_id canónico y corto (ignora post_id del modelo)
    post_id = canonical_post_id
    post["post_id"] = post_id
    post["topic"] = str(post.get("topic") or topic)

    bitcoin_anchor = str(post.get("bitcoin_anchor") or "")
    await create_post(post_id=post_id, topic=post["topic"], bitcoin_anchor=bitcoin_anchor)
    await add_version(post_id=post_id, version=1, content=post)
    await log_event(post_id, "GEN", {"source": "telegram"})

    drafts_chat_id = int(os.environ["TG_DRAFTS_CHAT_ID"])
    msg = await context.bot.send_message(
        chat_id=drafts_chat_id,
        text=render_post_html(post_id, 1, post),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=build_post_keyboard(post_id, candidate_ids=[]),
    )

    await set_draft_message_ref(post_id, drafts_chat_id, msg.message_id)
    await update.message.reply_text("✅ Listo. Enviado a Drafts.", parse_mode=ParseMode.HTML)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data or ""

    if data.startswith("VERSIONS:"):
        post_id = data.split(":", 1)[1]
        post_db = await get_post(post_id)
        if not post_db:
            await query.message.reply_text(f"❌ No existe en SQLite: <code>{post_id}</code>", parse_mode=ParseMode.HTML)
            return

        vers = await list_versions(post_id)
        if not vers:
            await query.message.reply_text(f"ℹ️ No hay versiones para: <code>{post_id}</code>", parse_mode=ParseMode.HTML)
            return

        status = post_db.get("status", "unknown")
        text = (
            f"📚 <b>Versiones</b>\n"
            f"<b>ID:</b> <code>{post_id}</code>\n"
            f"<b>Status:</b> <code>{status}</code>\n\n"
            f"Disponibles: " + ", ".join([f"v{v}" for v in vers[:20]])
        )
        await query.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=_versions_keyboard(post_id, vers),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("VIEW:"):
        try:
            _, post_id, v_str = data.split(":", 2)
            v = int(v_str)
        except Exception:
            await query.message.reply_text("❌ VIEW malformado.", parse_mode=ParseMode.HTML)
            return

        got = await get_version(post_id, v)
        if not got:
            await query.message.reply_text(
                f"❌ No encontré <code>{post_id}</code> v<code>{v}</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        ver, content = got
        post_db = await get_post(post_id)
        status = (post_db or {}).get("status", "unknown")

        header = (
            f"🧾 <b>Vista versión</b>\n"
            f"<b>ID:</b> <code>{post_id}</code>\n"
            f"<b>Status:</b> <code>{status}</code>\n"
            f"<b>Versión:</b> <code>v{ver}</code>\n\n"
        )
        await query.message.reply_text(
            header + render_post_html(post_id, ver, content),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        return

    if data.startswith("REVERT:"):
        try:
            _, post_id, v_str = data.split(":", 2)
            target_v = int(v_str)
        except Exception:
            await query.message.reply_text("❌ REVERT malformado.", parse_mode=ParseMode.HTML)
            return

        post_db = await get_post(post_id)
        if not post_db:
            await query.message.reply_text(
                f"❌ No existe en SQLite: <code>{post_id}</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        draft_chat_id = post_db.get("draft_chat_id")
        draft_message_id = post_db.get("draft_message_id")
        if not draft_chat_id or not draft_message_id:
            await query.message.reply_text(
                "❌ Este post no tiene referencia al Draft (draft_chat_id/draft_message_id).",
                parse_mode=ParseMode.HTML,
            )
            return

        got = await get_version(post_id, target_v)
        if not got:
            await query.message.reply_text(
                f"❌ No encontré <code>{post_id}</code> v<code>{target_v}</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        ver, content = got
        edited = await _safe_edit_message_text(
            context,
            chat_id=int(draft_chat_id),
            message_id=int(draft_message_id),
            text=render_post_html(post_id, ver, content),
            reply_markup=build_post_keyboard(post_id, candidate_ids=[]),
        )

        who = query.from_user.username or query.from_user.full_name
        await log_event(post_id, "REVERT_DRAFT", {"to_version": ver, "by": who})

        if not edited:
            await query.message.reply_text(
                f"ℹ️ Ya estabas en <code>v{ver}</code> (no hubo cambios).",
                parse_mode=ParseMode.HTML,
            )
        else:
            await query.message.reply_text(
                f"↩️ Draft revertido a <code>v{ver}</code> (por <code>{who}</code>).",
                parse_mode=ParseMode.HTML,
            )
        return

    if data == "CLOSE":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if data.startswith("APPROVE:"):
        post_id = data.split(":", 1)[1]
        approver = query.from_user.username or query.from_user.full_name
        approved_chat_id = int(os.environ["TG_APPROVED_CHAT_ID"])

        image_only_on_approve = os.getenv("IMAGE_ONLY_ON_APPROVE", "1").strip().lower() in {"1", "true", "yes", "on"}
        image_timeout_s = int(os.getenv("IMAGE_TIMEOUT_SECONDS", "90"))
        image_max_attempts = int(os.getenv("IMAGE_45_MAX_ATTEMPTS", "3"))
        max_images_per_day = int(os.getenv("IMAGE_MAX_APPROVE_PER_DAY", "20"))

        post_db = await get_post(post_id)
        if post_db and str(post_db.get("status") or "").lower() == "approved":
            await query.message.reply_text("ℹ️ Este post ya fue aprobado. No se regenera imagen.", parse_mode=ParseMode.HTML)
            return

        lock_key = f"approve:lock:{post_id}"
        lock_val = await kv_get(lock_key)
        if lock_val:
            try:
                lock_age = _now_ts() - int(lock_val)
            except Exception:
                lock_age = 9999
            if lock_age < 120:
                await query.message.reply_text("⏳ APPROVE en progreso para este post. Espera unos segundos.", parse_mode=ParseMode.HTML)
                return
        await kv_set(lock_key, str(_now_ts()))

        latest = await get_latest_version(post_id)
        if latest:
            ver, content = latest

            if image_only_on_approve:
                day_key = _utc_day_key()
                img_day_counter_key = f"image:approve:day:{day_key}:count"
                raw_day_count = await kv_get(img_day_counter_key)
                try:
                    day_count = int(raw_day_count or "0")
                except Exception:
                    day_count = 0
                if day_count >= max_images_per_day:
                    await query.message.reply_text(
                        "🟠 APPROVE bloqueado: se alcanzó el límite diario de generación de imágenes.",
                        parse_mode=ParseMode.HTML,
                    )
                    await log_event(post_id, "APPROVE_BLOCKED_IMAGE_BUDGET", {"by": approver, "day": day_key, "count": day_count})
                    return

                visual_prompt = str((content or {}).get("visual_prompt") or "").strip()
                if not visual_prompt:
                    await query.message.reply_text("❌ APPROVE bloqueado: falta <code>visual_prompt</code> en el draft.", parse_mode=ParseMode.HTML)
                    await log_event(post_id, "APPROVE_BLOCKED_IMAGE", {"by": approver, "reason": "missing_visual_prompt", "version": ver})
                    return

                await query.message.reply_text("🖼 Generando imagen 4:5 para aprobar…", parse_mode=ParseMode.HTML)

                img_bytes = None
                img_mime = None
                provider_prompt = str((content or {}).get("visual_prompt_en") or build_image_prompt_en(visual_prompt))
                last_reason = "unknown"

                for i in range(1, image_max_attempts + 1):
                    try:
                        bts, mime, prompt_en = await asyncio.to_thread(
                            generate_image,
                            visual_prompt=visual_prompt,
                            timeout_s=image_timeout_s,
                        )
                        ok45, w, h = validate_4_5(bts, mime)
                        if ok45:
                            img_bytes, img_mime = bts, mime
                            provider_prompt = prompt_en or provider_prompt
                            break
                        last_reason = f"bad_aspect:{w}x{h}"
                        await log_event(post_id, "IMAGE_45_RETRY", {"attempt": i, "w": w, "h": h, "mime": mime})
                    except ImageGenError as e:
                        last_reason = f"image_gen_error:{str(e)[:180]}"
                        await log_event(post_id, "IMAGE_45_RETRY", {"attempt": i, "error": str(e)[:300]})
                    except Exception as e:
                        last_reason = f"unexpected:{str(e)[:180]}"
                        await log_event(post_id, "IMAGE_45_RETRY", {"attempt": i, "error": str(e)[:300]})

                if not img_bytes or not img_mime:
                    await query.message.reply_text(
                        "❌ APPROVE bloqueado: no se pudo generar imagen válida 4:5.\n"
                        f"<b>reason:</b> <code>{_e(last_reason)}</code>",
                        parse_mode=ParseMode.HTML,
                    )
                    await log_event(
                        post_id,
                        "APPROVE_BLOCKED_IMAGE",
                        {"by": approver, "version": ver, "reason": last_reason, "attempts": image_max_attempts},
                    )
                    return

                photo_msg = await context.bot.send_photo(
                    chat_id=approved_chat_id,
                    photo=img_bytes,
                    caption=f"✅ <b>APROBADO</b> por: <code>{_e(approver)}</code>",
                    parse_mode=ParseMode.HTML,
                )
                sent = await context.bot.send_message(
                    chat_id=approved_chat_id,
                    text=_compose_publish_pack(post_id, ver, content),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                await kv_set(img_day_counter_key, str(day_count + 1))
                await log_event(
                    post_id,
                    "APPROVE_IMAGE_OK",
                    {"by": approver, "version": ver, "mime": img_mime, "provider_prompt": provider_prompt[:500], "photo_message_id": photo_msg.message_id, "day_count": day_count + 1},
                )
            else:
                sent = await context.bot.send_message(
                    chat_id=approved_chat_id,
                    text=f"✅ <b>APROBADO</b> por: <code>{_e(approver)}</code>\n\n" + _compose_publish_pack(post_id, ver, content),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )

            await approve_post(
                post_id=post_id,
                approver=approver,
                approved_chat_id=approved_chat_id,
                approved_message_id=sent.message_id,
                approved_at=_now_ts(),
            )
            await log_event(post_id, "APPROVE", {"by": approver, "version": ver})
        else:
            await context.bot.send_message(
                chat_id=approved_chat_id,
                text=f"✅ <b>APROBADO</b> por: <code>{_e(approver)}</code>\n\n" + (query.message.text or ""),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await log_event(post_id, "APPROVE_FALLBACK", {"by": approver})

        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"✅ Aprobado por {approver} y reenviado a PS | Aprobados.")
        await kv_set(lock_key, "0")
        return

    if data.startswith("REGEN:"):
        post_id = data.split(":", 1)[1]

        post_db = await get_post(post_id)
        if not post_db:
            await query.message.reply_text("❌ No encuentro ese post en SQLite.", parse_mode=ParseMode.HTML)
            return

        topic = post_db["topic"]
        draft_chat_id = post_db.get("draft_chat_id")
        draft_message_id = post_db.get("draft_message_id")

        if not draft_chat_id or not draft_message_id:
            await query.message.reply_text(
                "❌ No tengo referencia al mensaje draft (draft_chat_id/draft_message_id).",
                parse_mode=ParseMode.HTML,
            )
            return

        await query.message.reply_text(
            "♻️ Regenerando con <b>Fábrica de Contenidos</b>…",
            parse_mode=ParseMode.HTML,
        )

        latest = await get_latest_version(post_id)
        latest_content = (latest[1] if latest else {}) or {}

        latest_num = await get_latest_version_number(post_id)
        new_version = latest_num + 1

        regen_prompt = f"""
Eres Panamá Soberano (todo en español).
Tono: profesor que abre los ojos + estratega de incentivos. No partidista. No ataques personales.
Objetivo: 1 post de redes basado en el tema, SIEMPRE con Bitcoin Anchor explícito.

Tema: {topic}

Devuelve JSON válido con estas llaves exactas:
- post_id (string)
- topic (string)
- hook (string)
- explain_simple (string)
- bitcoin_anchor (string)
- insight (string)
- risk (string)
- caption (string)
- visual_prompt (string)
- qa (array de strings)

Responde SOLO el JSON.
""".strip()

        raw = openclaw_chat(regen_prompt)
        try:
            new_post = json.loads(_extract_json(raw))
        except Exception:
            await query.message.reply_text(
                "❌ La Fábrica de Contenidos respondió pero REGEN no vino en JSON válido.",
                parse_mode=ParseMode.HTML,
            )
            return

        new_post["post_id"] = post_id
        new_post["topic"] = str(new_post.get("topic") or topic)

        # Mantener contexto Radar para UX consistente en Draft
        for k in [
            "radar_selected_candidate_id",
            "radar_winner_candidate_id",
            "radar_alternate_candidate_ids",
            "radar_winner_preview",
            "radar_alternate_previews",
        ]:
            if k in latest_content:
                new_post[k] = latest_content.get(k)

        await add_version(post_id, new_version, new_post)
        await log_event(post_id, "REGEN", {"version": new_version})

        edited = await _safe_edit_message_text(
            context,
            chat_id=int(draft_chat_id),
            message_id=int(draft_message_id),
            text=render_post_html(post_id, new_version, new_post),
            reply_markup=build_post_keyboard(post_id, candidate_ids=[]),
        )

        if not edited:
            await query.message.reply_text("ℹ️ REGEN no cambió el contenido (quedó igual).", parse_mode=ParseMode.HTML)
        else:
            await query.message.reply_text(
                f"✅ Regeneración lista. Ahora estás en <code>v{new_version}</code>.",
                parse_mode=ParseMode.HTML,
            )
        return

    # GEN alterno desde Radar por índice corto: GEN:<post_id>:<1|2|3>
    if data.startswith("GEN:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            await query.message.reply_text("❌ GEN malformado.", parse_mode=ParseMode.HTML)
            return

        _, post_id, pick = parts

        post_db = await get_post(post_id)
        if not post_db:
            await query.message.reply_text("❌ No encuentro ese post en SQLite.", parse_mode=ParseMode.HTML)
            return

        draft_chat_id = post_db.get("draft_chat_id")
        draft_message_id = post_db.get("draft_message_id")
        if not draft_chat_id or not draft_message_id:
            await query.message.reply_text("❌ No tengo referencia al Draft.", parse_mode=ParseMode.HTML)
            return

        latest = await get_latest_version(post_id)
        if not latest:
            await query.message.reply_text("❌ No hay versiones para ese post.", parse_mode=ParseMode.HTML)
            return

        _ver, content = latest
        alt_ids = (content or {}).get("radar_alternate_candidate_ids") or []

        try:
            idx = int(pick) - 1
            candidate_id = alt_ids[idx]
        except Exception:
            await query.message.reply_text("❌ Índice de alterno inválido.", parse_mode=ParseMode.HTML)
            return

        cand = await get_radar_candidate(candidate_id)
        if not cand:
            await query.message.reply_text(
                f"❌ No encuentro ese candidate_id en radar_candidates: <code>{candidate_id}</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        await query.message.reply_text(
            "⚡️ Generando alternativa desde Radar con <b>Fábrica de Contenidos</b>…",
            parse_mode=ParseMode.HTML,
        )

        latest_num = await get_latest_version_number(post_id)
        new_version = latest_num + 1

        prompt = await _prompt_from_candidate(cand)
        raw = openclaw_chat(prompt)

        try:
            alt_post = json.loads(_extract_json(raw))
        except Exception:
            await query.message.reply_text("❌ Alternativa no vino en JSON válido.", parse_mode=ParseMode.HTML)
            return

        alt_post["post_id"] = post_id
        alt_post["topic"] = str(alt_post.get("topic") or post_db.get("topic") or "Radar alt")
        alt_post["radar_selected_candidate_id"] = candidate_id
        alt_post["radar_alternate_candidate_ids"] = alt_ids

        # Conserva y mejora previews para que el hint sea legible
        prev_alts = (content or {}).get("radar_alternate_previews") or []
        alt_post["radar_alternate_previews"] = prev_alts
        alt_post["radar_winner_candidate_id"] = (content or {}).get("radar_winner_candidate_id")
        alt_post["radar_winner_preview"] = (content or {}).get("radar_winner_preview")
        alt_post["radar_selected_preview"] = _candidate_preview(cand)

        await add_version(post_id, new_version, alt_post)
        await log_event(post_id, "ALT_GEN", {"version": new_version, "candidate_id": candidate_id, "pick": pick})

        edited = await _safe_edit_message_text(
            context,
            chat_id=int(draft_chat_id),
            message_id=int(draft_message_id),
            text=render_post_html(post_id, new_version, alt_post),
            reply_markup=build_post_keyboard(post_id, candidate_ids=alt_ids),
        )

        if not edited:
            await query.message.reply_text("ℹ️ La alternativa quedó igual (sin cambios).", parse_mode=ParseMode.HTML)
        else:
            await query.message.reply_text(
                f"✅ Alternativa aplicada. Ahora estás en <code>v{new_version}</code>.",
                parse_mode=ParseMode.HTML,
            )
        return

    await query.message.reply_text("ℹ️ Acción no reconocida.", parse_mode=ParseMode.HTML)


def main() -> None:
    load_dotenv("/opt/ps_factory/config/.env")

    token = os.environ["TG_BOT_TOKEN"]

    # Timeouts más altos para evitar httpx.ReadError
    request = HTTPXRequest(
        connection_pool_size=8,
        read_timeout=30.0,
        write_timeout=30.0,
        connect_timeout=30.0,
        pool_timeout=30.0,
    )

    app = Application.builder().token(token).request(request).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("demo", cmd_demo))
    app.add_handler(CommandHandler("gen", cmd_gen))
    app.add_handler(CommandHandler("radar", cmd_radar))
    app.add_handler(CommandHandler("intraday_now", cmd_intraday_now))
    app.add_handler(CommandHandler("intraday_force_draft", cmd_intraday_force_draft))
    app.add_handler(CommandHandler("intraday_force_draft", cmd_intraday_force_draft))

    app.add_handler(CommandHandler("health", cmd_health))
    app.add_handler(CommandHandler("last", cmd_last))
    app.add_handler(CommandHandler("post", cmd_post))
    app.add_handler(CommandHandler("versions", cmd_versions))

    app.add_handler(CallbackQueryHandler(on_callback))

    # Error handler global
    app.add_error_handler(on_error)

    print("Nova corriendo ✅")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
