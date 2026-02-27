import os
import time
import json
import html
import sqlite3
import asyncio
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes
from telegram.error import BadRequest, NetworkError, TimedOut
from telegram.request import HTTPXRequest

from tg.callbacks import build_post_keyboard
from gen.openclaw_gen import openclaw_chat
from gen.image_gen import generate_image, validate_4_5, build_image_prompt_en as _build_image_prompt_en, ImageGenError, apply_carousel_index_badge

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


def _compose_publish_blocks(content: dict) -> dict:
    hook = str(content.get("hook") or "").strip()
    explain = str(content.get("explain_simple") or "").strip()
    caption = str(content.get("caption") or "").strip()

    use_emojis = os.getenv("SOCIAL_USE_EMOJIS", "1").strip().lower() in {"1", "true", "yes", "on"}
    brand_tag = os.getenv("SOCIAL_BRAND_HASHTAG", "#PanamáSoberano").strip() or "#PanamáSoberano"
    second_tag = os.getenv("SOCIAL_SECOND_HASHTAG", "").strip()
    tags = " ".join([t for t in [brand_tag, second_tag] if t]).strip()

    e1 = "⚡ " if use_emojis else ""
    e2 = "🎯 " if use_emojis else ""

    def _close_copy_text(t: str) -> str:
        txt = (t or "").strip()
        if not txt:
            return txt
        # evitar cierres abiertos tipo "con", "de", "para" al final
        bad_endings = {
            "de", "del", "la", "el", "los", "las", "un", "una", "y", "o",
            "con", "sin", "para", "por", "en", "a", "que", "como", "sobre"
        }
        words = txt.split()
        while words and words[-1].strip(".,;:!?").lower() in bad_endings:
            words.pop()
        txt = " ".join(words).rstrip(" ,;:-")
        if txt and not txt.endswith((".", "!", "?")):
            txt += "."
        return txt

    def _word_clip(s: str, limit: int) -> str:
        t = " ".join((s or "").replace("\n", " ").split())
        if len(t) <= limit:
            return _close_copy_text(t)
        cut = t[:limit].rstrip()
        sp = cut.rfind(" ")
        if sp > int(limit * 0.6):
            cut = cut[:sp]
        # sin elipsis para evitar sensación de frase incompleta
        return _close_copy_text(cut)

    def _sentences(s: str) -> list[str]:
        txt = " ".join((s or "").replace("\n", " ").split())
        if not txt:
            return []
        protected = {"EE. UU.": "EEUU", "p. ej.": "pej", "etc.": "etc"}
        for k, v in protected.items():
            txt = txt.replace(k, v)
        import re
        parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", txt) if p.strip()]
        out: list[str] = []
        for p in parts:
            for k, v in protected.items():
                p = p.replace(v, k)
            if not p.endswith((".", "!", "?")):
                p += "."
            out.append(p)
        return out

    def _summarize_sentences(s: str, limit: int, max_sentences: int = 2) -> str:
        selected: list[str] = []
        for snt in _sentences(s):
            trial = " ".join(selected + [snt]).strip()
            if len(trial) <= limit and len(selected) < max_sentences:
                selected.append(snt)
            else:
                break
        return " ".join(selected).strip() if selected else _word_clip(s, limit)

    def _fit_x_summarized(hook_text: str, explain_text: str, caption_text: str, tags_text: str, limit: int = 280) -> str:
        lines: list[str] = []
        if hook_text:
            lines.append(f"{e1}{hook_text}".strip())
        body_budget = max(70, limit - len("\n\n".join(lines + [tags_text])) - (2 if lines else 0))
        body = _summarize_sentences(f"{explain_text} {caption_text}".strip(), body_budget, max_sentences=2)
        if body:
            lines.append(body)
        if tags_text:
            lines.append(tags_text)
        text = "\n\n".join([x for x in lines if x]).strip()
        return _word_clip(text, limit) if len(text) > limit else text

    return {
        "x": _fit_x_summarized(hook, explain, caption, tags, 280),
        "instagram": (
            f"{e1}{hook}\n\n"
            f"{_summarize_sentences(explain, 360, max_sentences=3)}\n\n"
            f"{e2}{_summarize_sentences(caption, 200, max_sentences=2)}\n\n"
            f"{tags}"
        ).strip(),
        "tiktok": (
            f"{e1}{hook}\n"
            f"{_summarize_sentences(explain, 190, max_sentences=2)}\n\n"
            f"{e2}{_summarize_sentences(caption, 150, max_sentences=1)}\n"
            f"{tags}"
        ).strip(),
    }


def _compose_publish_pack(post_id: str, ver: int, content: dict) -> str:
    topic = str(content.get("topic") or "POST")
    b = _compose_publish_blocks(content)
    return (
        f"🟠 <b>{_e(topic)}</b> — <code>{_e(post_id)}</code> · v{ver}\n\n"
        f"<b>X</b>\n<code>{_e(b['x'])}</code>\n\n"
        f"<b>Instagram</b>\n<code>{_e(b['instagram'])}</code>\n\n"
        f"<b>TikTok</b>\n<code>{_e(b['tiktok'])}</code>"
    )


def _build_carousel_keyboard(post_id: str) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("✅ APROBAR CARRUSEL", callback_data=f"APPROVE:{post_id}")]]
    rows.append([InlineKeyboardButton("📚 Versions", callback_data=f"VERSIONS:{post_id}")])
    return InlineKeyboardMarkup(rows)


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


def _split_telegram_chunks(text: str, max_len: int = 1800) -> list[str]:
    t = str(text or "")
    if len(t) <= max_len:
        return [t]
    parts = t.split("\n\n")
    out: list[str] = []
    cur = ""
    for p in parts:
        cand = (cur + "\n\n" + p).strip() if cur else p
        if len(cand) <= max_len:
            cur = cand
        else:
            if cur:
                out.append(cur)
            if len(p) <= max_len:
                cur = p
            else:
                i = 0
                while i < len(p):
                    out.append(p[i:i+max_len])
                    i += max_len
                cur = ""
    if cur:
        out.append(cur)
    return out or [t[:max_len]]


async def _send_draft_payload(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    post_id: str,
    version: int,
    post: dict,
    candidate_ids: list[str],
):
    split_on = os.getenv("TG_SPLIT_DRAFT_SECTIONS", "1").strip().lower() in {"1", "true", "yes", "on"}
    full = render_post_html(post_id, version, post)
    if not split_on:
        return await context.bot.send_message(
            chat_id=chat_id,
            text=full,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=build_post_keyboard(post_id, candidate_ids=candidate_ids),
        )

    chunks = _split_telegram_chunks(full, max_len=1700)
    last_msg = None
    for i, ch in enumerate(chunks):
        is_last = (i == len(chunks) - 1)
        last_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=ch,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=(build_post_keyboard(post_id, candidate_ids=candidate_ids) if is_last else None),
        )
    return last_msg


async def _send_approved_payload(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    post_id: str,
    version: int,
    post: dict,
):
    split_on = os.getenv("TG_SPLIT_APPROVED_BY_NETWORK", "1").strip().lower() in {"1", "true", "yes", "on"}
    if not split_on:
        return await context.bot.send_message(
            chat_id=chat_id,
            text=_compose_publish_pack(post_id, version, post),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    topic = str(post.get("topic") or "POST")
    blocks = _compose_publish_blocks(post)
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🟠 <b>{_e(topic)}</b> — <code>{_e(post_id)}</code> · v{version}",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    await context.bot.send_message(chat_id=chat_id, text="<b>X</b>", parse_mode=ParseMode.HTML)
    await context.bot.send_message(chat_id=chat_id, text=f"<code>{_e(blocks['x'])}</code>", parse_mode=ParseMode.HTML)
    await context.bot.send_message(chat_id=chat_id, text="<b>Instagram</b>", parse_mode=ParseMode.HTML)
    await context.bot.send_message(chat_id=chat_id, text=f"<code>{_e(blocks['instagram'])}</code>", parse_mode=ParseMode.HTML)
    await context.bot.send_message(chat_id=chat_id, text="<b>TikTok</b>", parse_mode=ParseMode.HTML)
    return await context.bot.send_message(chat_id=chat_id, text=f"<code>{_e(blocks['tiktok'])}</code>", parse_mode=ParseMode.HTML)


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

    msg = await _send_draft_payload(
        context,
        chat_id=drafts_chat_id,
        post_id=post_id,
        version=1,
        post=post,
        candidate_ids=[],
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
    msg = await _send_draft_payload(
        context,
        chat_id=drafts_chat_id,
        post_id=post_id,
        version=1,
        post=post,
        candidate_ids=alt_ids,
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
    alternates = detail.get("alternates") or []

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

    if alternates:
        extra += "\n\n<b>Alternativas (Top no ganador)</b>"
        for i, a in enumerate(alternates[:3], start=1):
            atitle = _e(str(a.get('title') or '')[:110])
            extra += (
                f"\n{i}) {atitle}"
                f"\n   <code>{_e(a.get('candidate_id') or '')}</code>"
                f" · score <code>{_e(a.get('total_score'))}</code>"
                f" · rel <code>{_e(a.get('relevance'))}</code>"
            )

    extra += "\n\n<i>Si igual lo quieres, usa:</i> <code>/intraday_force_draft</code>"
    extra += "\n<i>Alterno específico:</i> <code>/intraday_force_draft 1|2|3</code> o <code>/intraday_force_draft &lt;candidate_id&gt;</code>"

    kb_rows = [[InlineKeyboardButton("✅ Draft ganador", callback_data="IDF:W")]]
    if alternates:
        r = []
        for i in range(1, min(3, len(alternates)) + 1):
            r.append(InlineKeyboardButton(f"⚡ Alt {i}", callback_data=f"IDF:{i}"))
        if r:
            kb_rows.append(r)

    await update.message.reply_text(
        "✅ Intraday monitor ejecutado.\n\n"
        f"<b>resultado:</b> <code>{_e(human_reason)}</code>\n"
        f"<b>ts:</b> <code>{(last_ts or 'n/a')[:40]}</code>"
        f"{extra}",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb_rows),
    )


async def _intraday_force_candidate_to_draft(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    candidate_id: str,
    reply_fn,
) -> None:
    drafts_chat_id = int(os.getenv("TG_DRAFTS_CHAT_ID", "0") or 0)
    if not drafts_chat_id:
        await reply_fn("❌ TG_DRAFTS_CHAT_ID no está configurado.")
        return

    cand = await get_radar_candidate(candidate_id)
    if not cand:
        await reply_fn(f"❌ candidate_id no encontrado: <code>{_e(candidate_id)}</code>", parse_mode=ParseMode.HTML)
        return

    await reply_fn(
        f"🛠 Forzando Draft desde intraday candidate <code>{_e(candidate_id)}</code>...",
        parse_mode=ParseMode.HTML,
    )

    prompt = await _prompt_from_candidate(cand)
    raw = openclaw_chat(prompt)

    try:
        post = json.loads(_extract_json(raw))
    except Exception:
        await reply_fn("❌ El modelo no devolvió JSON válido.")
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

    msg = await _send_draft_payload(
        context,
        chat_id=drafts_chat_id,
        post_id=post_id,
        version=1,
        post=post,
        candidate_ids=[],
    )
    await set_draft_message_ref(post_id, drafts_chat_id, msg.message_id)

    await reply_fn(
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

    candidate_id = None
    raw = await kv_get("intraday:last_detail")
    try:
        d = json.loads(raw or "{}")
    except Exception:
        d = {}

    if context.args:
        arg = (context.args[0] or "").strip()
        if arg in {"1", "2", "3"}:
            alts = d.get("alternates") or []
            try:
                candidate_id = str((alts[int(arg)-1] or {}).get("candidate_id") or "").strip()
            except Exception:
                candidate_id = ""
        else:
            candidate_id = arg

    if not candidate_id:
        candidate_id = (d.get("candidate_id") or "").strip()

    if not candidate_id:
        await update.message.reply_text("❌ No tengo candidate_id reciente. Ejecuta /intraday_now primero.")
        return

    await _intraday_force_candidate_to_draft(context, candidate_id=candidate_id, reply_fn=update.message.reply_text)


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
    msg = await _send_draft_payload(
        context,
        chat_id=drafts_chat_id,
        post_id=post_id,
        version=1,
        post=post,
        candidate_ids=[],
    )

    await set_draft_message_ref(post_id, drafts_chat_id, msg.message_id)
    await update.message.reply_text("✅ Listo. Enviado a Drafts.", parse_mode=ParseMode.HTML)


async def cmd_carousel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    topic = " ".join(context.args).strip()
    if not topic:
        await update.message.reply_text("Uso: <code>/carousel &lt;tema&gt;</code> o <code>/carrusel &lt;tema&gt;</code>", parse_mode=ParseMode.HTML)
        return

    await update.message.reply_text("🧩 Generando carrusel (fase 3: continuidad narrativa)...", parse_mode=ParseMode.HTML)

    prompt = f"""
Eres editor senior de storytelling para carruseles de Instagram. Responde SOLO JSON válido.
Objetivo: crear un carrusel de 6 slides en español con continuidad narrativa total y emoción humana.
Tema: {topic}

Devuelve este JSON exacto:
{{
  "topic": "string",
  "storyline": {{
    "hook": "string",
    "conflict": "string",
    "turn": "string",
    "resolution": "string"
  }},
  "visual_bible": "string",
  "protagonist": "string",
  "slides": [
    {{"n":1,"title":"string","body":"string","bridge":"string","emotion":"string","subject":"string","visual_prompt":"string"}},
    {{"n":2,"title":"string","body":"string","bridge":"string","emotion":"string","subject":"string","visual_prompt":"string"}},
    {{"n":3,"title":"string","body":"string","bridge":"string","emotion":"string","subject":"string","visual_prompt":"string"}},
    {{"n":4,"title":"string","body":"string","bridge":"string","emotion":"string","subject":"string","visual_prompt":"string"}},
    {{"n":5,"title":"string","body":"string","bridge":"string","emotion":"string","subject":"string","visual_prompt":"string"}},
    {{"n":6,"title":"string","body":"string","bridge":"string","emotion":"string","subject":"string","visual_prompt":"string"}}
  ],
  "caption": "string"
}}

Reglas:
- títulos de 4-8 palabras
- body corto (máx ~70 palabras), cerrar con sentido
- cada slide debe conectar explícitamente con el siguiente (bridge)
- arco emocional obligatorio: curiosidad -> tensión -> conflicto -> giro -> claridad -> resolución
- mantener mismos elementos visuales base en todo el carrusel (visual_bible)
- protagonista consistente en al menos 4 de 6 slides
- evitar sesgo de casting: no repetir género sin motivo narrativo; variar planos/personajes
- evitar look stock genérico; priorizar estilo editorial cinematográfico/documental
- tono educativo/estratégico, no partidista
""".strip()

    raw = openclaw_chat(prompt)
    try:
        car = json.loads(_extract_json(raw))
    except Exception:
        await update.message.reply_text("❌ No vino JSON válido para carrusel.", parse_mode=ParseMode.HTML)
        return

    slides = car.get("slides") or []
    if not isinstance(slides, list) or not slides:
        await update.message.reply_text("❌ Carrusel inválido: faltan slides.", parse_mode=ParseMode.HTML)
        return

    slides = slides[:6]
    post_id = f"car-{_now_ts()}"
    topic_out = str(car.get("topic") or topic)
    caption = str(car.get("caption") or "")
    storyline = car.get("storyline") or {}
    visual_bible = str(car.get("visual_bible") or "").strip()
    protagonist = str(car.get("protagonist") or "").strip()

    content = {
        "post_id": post_id,
        "topic": topic_out,
        "carousel": slides,
        "caption": caption,
        "carousel_storyline": storyline,
        "carousel_visual_bible": visual_bible,
        "carousel_protagonist": protagonist,
    }

    await create_post(post_id=post_id, topic=topic_out, bitcoin_anchor="")
    await add_version(post_id=post_id, version=1, content=content)
    await log_event(post_id, "CAROUSEL_GEN", {"source": "telegram", "slides": len(slides)})

    drafts_chat_id = int(os.environ["TG_DRAFTS_CHAT_ID"])
    await context.bot.send_message(
        chat_id=drafts_chat_id,
        text=(
            f"🧩 <b>CARRUSEL v1</b> — <code>{_e(post_id)}</code>\n"
            f"<b>Tema:</b> {_e(topic_out)}\n"
            f"<b>Slides:</b> <code>{len(slides)}</code>\n\n"
            f"<b>Storyline hook:</b> {_e(storyline.get('hook') or '')}\n"
            f"<b>Protagonista:</b> {_e(protagonist or 'n/a')}\n"
            f"<b>Visual bible:</b> {_e(visual_bible[:220])}\n\n"
            "<i>Fase 3:</i> continuidad narrativa + numeración visual 1/6..6/6 al aprobar."
        ),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )

    for i, s in enumerate(slides, start=1):
        title = _e(str(s.get("title") or f"Slide {i}"))
        body = _e(str(s.get("body") or ""))
        bridge = _e(str(s.get("bridge") or ""))
        emotion = _e(str(s.get("emotion") or ""))
        subject = _e(str(s.get("subject") or ""))
        vp = _e(str(s.get("visual_prompt") or ""))
        await context.bot.send_message(
            chat_id=drafts_chat_id,
            text=(
                f"<b>Slide {i}: {title}</b>\n\n"
                f"{body}\n\n"
                f"🎭 <b>Emoción</b> {emotion} · <b>Sujeto</b> {subject}\n"
                f"🔗 <b>Puente al siguiente</b>\n{bridge}\n\n"
                f"🖼 <b>Prompt visual</b>\n{vp}"
            ),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    if caption:
        await context.bot.send_message(
            chat_id=drafts_chat_id,
            text=f"📝 <b>Caption carrusel sugerido</b>\n{_e(caption)}",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    ctrl_msg = await context.bot.send_message(
        chat_id=drafts_chat_id,
        text="✅ <b>Control de carrusel</b> (usar al final)",
        parse_mode=ParseMode.HTML,
        reply_markup=_build_carousel_keyboard(post_id),
    )
    await set_draft_message_ref(post_id, drafts_chat_id, ctrl_msg.message_id)

    await update.message.reply_text("✅ Carrusel enviado a Drafts (fase 2).", parse_mode=ParseMode.HTML)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data or ""

    if data.startswith("IDF:"):
        sel = data.split(":", 1)[1].strip().upper()
        raw = await kv_get("intraday:last_detail")
        try:
            d = json.loads(raw or "{}")
        except Exception:
            d = {}

        candidate_id = ""
        if sel == "W":
            candidate_id = str(d.get("candidate_id") or "").strip()
        elif sel in {"1", "2", "3"}:
            alts = d.get("alternates") or []
            try:
                candidate_id = str((alts[int(sel)-1] or {}).get("candidate_id") or "").strip()
            except Exception:
                candidate_id = ""

        if not candidate_id:
            await query.message.reply_text("❌ No hay candidate_id disponible para esa opción.", parse_mode=ParseMode.HTML)
            return

        await _intraday_force_candidate_to_draft(context, candidate_id=candidate_id, reply_fn=query.message.reply_text)
        return

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

            carousel_slides = (content or {}).get("carousel") or []
            if isinstance(carousel_slides, list) and carousel_slides:
                day_key = _utc_day_key()
                img_day_counter_key = f"image:approve:day:{day_key}:count"
                carousel_day_limit = int(os.getenv("IMAGE_MAX_APPROVE_PER_DAY_CAROUSEL", str(max_images_per_day)))
                raw_day_count = await kv_get(img_day_counter_key)
                try:
                    day_count = int(raw_day_count or "0")
                except Exception:
                    day_count = 0

                needed = min(6, len(carousel_slides)) if image_only_on_approve else 0
                if image_only_on_approve and (day_count + needed) > carousel_day_limit:
                    await query.message.reply_text(
                        "🟠 APPROVE CARRUSEL bloqueado: límite diario de imágenes alcanzado.\n"
                        f"<b>limit:</b> <code>{carousel_day_limit}</code> · <b>used:</b> <code>{day_count}</code> · <b>needed:</b> <code>{needed}</code>",
                        parse_mode=ParseMode.HTML,
                    )
                    await log_event(post_id, "APPROVE_BLOCKED_IMAGE_BUDGET", {"by": approver, "day": day_key, "count": day_count, "needed": needed, "limit": carousel_day_limit})
                    return

                await context.bot.send_message(
                    chat_id=approved_chat_id,
                    text=f"✅ <b>CARRUSEL APROBADO</b> por: <code>{_e(approver)}</code>",
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )

                sent = None
                generated = 0
                if image_only_on_approve:
                    await query.message.reply_text("🧩 Generando 6 imágenes del carrusel…", parse_mode=ParseMode.HTML)

                    media_items = []
                    slide_order = []

                    visual_bible = str((content or {}).get("carousel_visual_bible") or "").strip()
                    protagonist = str((content or {}).get("carousel_protagonist") or "").strip()

                    for idx, s in enumerate(carousel_slides[:6], start=1):
                        title = str(s.get("title") or f"Slide {idx}").strip()
                        body = str(s.get("body") or "").strip()
                        emotion = str(s.get("emotion") or "").strip()
                        subject = str(s.get("subject") or "").strip()
                        bridge = str(s.get("bridge") or "").strip()
                        visual_prompt = str(s.get("visual_prompt") or "").strip()
                        prompt_src = (
                            f"Carousel slide {idx}/6. "
                            f"Protagonist: {protagonist}. "
                            f"Visual continuity bible: {visual_bible}. "
                            f"Emotion target: {emotion}. Subject focus: {subject}. "
                            f"Slide title: {title}. Narrative text context: {body}. "
                            f"Transition bridge to next slide: {bridge}. "
                            f"Scene direction: {visual_prompt or (title + '. ' + body)}"
                        ).strip()

                        img_bytes = None
                        img_mime = None
                        last_reason = "unknown"

                        for attempt in range(1, image_max_attempts + 1):
                            try:
                                bts, mime, _ = await asyncio.to_thread(
                                    generate_image,
                                    visual_prompt=prompt_src,
                                    timeout_s=image_timeout_s,
                                )
                                ok45, w, h = validate_4_5(bts, mime)
                                if ok45:
                                    img_bytes, img_mime = bts, mime
                                    break
                                last_reason = f"bad_aspect:{w}x{h}"
                            except Exception as e:
                                last_reason = str(e)[:180]

                        if not img_bytes:
                            await query.message.reply_text(
                                f"❌ Carrusel bloqueado en slide {idx}: no se pudo generar imagen 4:5.\n"
                                f"<code>{_e(last_reason)}</code>",
                                parse_mode=ParseMode.HTML,
                            )
                            await log_event(post_id, "APPROVE_BLOCKED_CAROUSEL", {"by": approver, "version": ver, "slide": idx, "reason": last_reason})
                            return

                        img_bytes, img_mime = apply_carousel_index_badge(img_bytes, img_mime, idx=idx, total=min(6, len(carousel_slides)))
                        media_items.append(InputMediaPhoto(media=img_bytes))
                        slide_order.append(idx)
                        generated += 1

                    sent = None
                    if media_items:
                        mg = await context.bot.send_media_group(chat_id=approved_chat_id, media=media_items)
                        if mg:
                            sent = mg[0]

                    cap = str((content or {}).get("caption") or "").strip()
                    if cap:
                        sent = await context.bot.send_message(
                            chat_id=approved_chat_id,
                            text=f"📝 <b>Caption carrusel</b>\n{_e(cap)}\n\n<i>Secuencia visual: 1/6 → 6/6 (badge en cada slide)</i>",
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )

                    await kv_set(img_day_counter_key, str(day_count + generated))
                    await log_event(
                        post_id,
                        "APPROVE_CAROUSEL_OK",
                        {"by": approver, "version": ver, "slides": generated, "day_count": day_count + generated, "carousel_order": slide_order},
                    )
                else:
                    # fallback textual only (rare)
                    for idx, s in enumerate(carousel_slides[:6], start=1):
                        title = str(s.get("title") or f"Slide {idx}").strip()
                        body = str(s.get("body") or "").strip()
                        sent = await context.bot.send_message(
                            chat_id=approved_chat_id,
                            text=f"<b>Slide {idx}: {_e(title)}</b>\n\n{_e(body)}",
                            parse_mode=ParseMode.HTML,
                        )

                await approve_post(
                    post_id=post_id,
                    approver=approver,
                    approved_chat_id=approved_chat_id,
                    approved_message_id=(sent.message_id if sent else 0),
                    approved_at=_now_ts(),
                )
                await log_event(post_id, "APPROVE", {"by": approver, "version": ver, "kind": "carousel"})

                await query.edit_message_reply_markup(reply_markup=None)
                await query.message.reply_text(f"✅ Carrusel aprobado por {approver} y enviado a PS | Aprobados.")
                await kv_set(lock_key, "0")
                return

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
                sent = await _send_approved_payload(
                    context,
                    chat_id=approved_chat_id,
                    post_id=post_id,
                    version=ver,
                    post=content,
                )
                await kv_set(img_day_counter_key, str(day_count + 1))
                await log_event(
                    post_id,
                    "APPROVE_IMAGE_OK",
                    {"by": approver, "version": ver, "mime": img_mime, "provider_prompt": provider_prompt[:500], "photo_message_id": photo_msg.message_id, "day_count": day_count + 1},
                )
            else:
                await context.bot.send_message(
                    chat_id=approved_chat_id,
                    text=f"✅ <b>APROBADO</b> por: <code>{_e(approver)}</code>",
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                sent = await _send_approved_payload(
                    context,
                    chat_id=approved_chat_id,
                    post_id=post_id,
                    version=ver,
                    post=content,
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
    app.add_handler(CommandHandler("carousel", cmd_carousel))
    app.add_handler(CommandHandler("carrusel", cmd_carousel))
    app.add_handler(CommandHandler("radar", cmd_radar))
    app.add_handler(CommandHandler("intraday_now", cmd_intraday_now))
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
