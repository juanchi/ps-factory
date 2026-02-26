import html
from typing import Any, Dict


def _e(s: Any) -> str:
    return html.escape(str(s or ""), quote=False)


def _clip(s: Any, n: int) -> str:
    t = str(s or "").strip()
    if len(t) <= n:
        return t
    return t[: max(1, n - 1)].rstrip() + "…"


def render_post_html(post_id: str, version: int, post: Dict[str, Any]) -> str:
    # Título / encabezado
    title = post.get("title") or post.get("topic") or post_id

    hook = _clip(post.get("hook", ""), 280)
    explain = _clip(post.get("explain_simple", ""), 700)
    btc = _clip(post.get("bitcoin_anchor", ""), 360)
    insight = _clip(post.get("insight", ""), 360)
    risk = _clip(post.get("risk", ""), 260)
    caption = _clip(post.get("caption", ""), 420)
    visual = _clip(post.get("visual_prompt", ""), 320)
    visual_en = _clip(post.get("visual_prompt_en", ""), 360)

    qa = (post.get("qa") or [])[:5]
    qa_lines = "\n".join([f"• {_e(_clip(x, 140))}" for x in qa]) if qa else "• (sin checklist)"

    alerts = post.get("daily_editorial_alerts") or []
    alerts_block = ""
    if alerts:
        lines = "\n".join([f"• {_e(_clip(a, 120))}" for a in alerts[:4]])
        alerts_block = f"\n\n🟡 <b>Alertas editoriales (forzado)</b>\n{lines}"

    selected = post.get("radar_selected_candidate_id") or post.get("radar_winner_candidate_id")
    selected_preview = post.get("radar_selected_preview") or post.get("radar_winner_preview") or {}

    alternates = (post.get("radar_alternate_candidate_ids") or [])[:3]
    alt_previews = (post.get("radar_alternate_previews") or [])[:3]

    if alt_previews:
        lines = []
        for i, p in enumerate(alt_previews, start=1):
            title = _e(p.get("title") or p.get("candidate_id") or "(sin título)")
            author = _e(p.get("author") or "unknown")
            score = p.get("score")
            score_txt = f" · score {_e(score)}" if score is not None else ""
            lines.append(f"{i}. @{author}: {title}{score_txt}")
        alt_lines = "\n".join(lines)
    else:
        alt_lines = "\n".join([f"{i}. <code>{_e(cid)}</code>" for i, cid in enumerate(alternates, start=1)])

    selected_line = "N/A"
    if selected_preview:
        t = _e(selected_preview.get("title") or selected_preview.get("candidate_id") or selected)
        a = _e(selected_preview.get("author") or "unknown")
        selected_line = f"@{a}: {t}"
    elif selected:
        selected_line = f"<code>{_e(selected)}</code>"

    radar_block = ""
    if selected or alternates or alt_previews:
        radar_block = (
            f"\n\n🛰 <b>Radar (Top no ganador)</b>\n"
            f"Seleccionado: {selected_line}\n"
            f"Alternos:\n{alt_lines or '1. (sin alternos)'}\n"
            f"<i>Nota: los botones de alternos regeneran una nueva versión desde ese candidato.</i>"
        )

    # Estilo: emojis + labels en negrita, con saltos claros (como antes)
    body = (
        f"🟠 <b>POST DEL DÍA</b> — <code>{_e(post_id)}</code> · v{version}\n\n"
        f"🎯 <b>Hook</b>\n{_e(hook)}\n\n"
        f"🧠 <b>Explicación simple</b>\n{_e(explain)}\n\n"
        f"⚡️ <b>Bitcoin Anchor</b>\n{_e(btc)}\n\n"
        f"📈 <b>Insight</b>\n{_e(insight)}\n\n"
        f"⚠️ <b>Riesgos / matices</b>\n{_e(risk)}\n\n"
        f"📝 <b>Caption sugerido (IG/TikTok/X)</b>\n{_e(caption)}\n\n"
        f"🖼 <b>Prompt visual (4:5)</b>\n{_e(visual)}\n\n"
        f"🧾 <b>Image prompt EN (provider-ready)</b>\n{_e(visual_en)}\n\n"
        f"✅ <b>Checklist QA</b>\n{qa_lines}"
        f"{alerts_block}"
        f"{radar_block}"
    )

    if len(body) > 3900:
        # recorte de seguridad para Telegram (4096 chars hard limit)
        body = body[:3890].rstrip() + "\n\n<i>(resumen truncado por longitud)</i>"
    return body
