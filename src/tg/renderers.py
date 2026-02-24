import html
from typing import Any, Dict


def _e(s: Any) -> str:
    return html.escape(str(s or ""), quote=False)


def render_post_html(post_id: str, version: int, post: Dict[str, Any]) -> str:
    # Título / encabezado
    title = post.get("title") or post.get("topic") or post_id

    hook = post.get("hook", "")
    explain = post.get("explain_simple", "")
    btc = post.get("bitcoin_anchor", "")
    insight = post.get("insight", "")
    risk = post.get("risk", "")
    caption = post.get("caption", "")
    visual = post.get("visual_prompt", "")

    qa = post.get("qa") or []
    qa_lines = "\n".join([f"• {_e(x)}" for x in qa]) if qa else "• (sin checklist)"

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
    return (
        f"🟠 <b>POST DEL DÍA</b> — <code>{_e(post_id)}</code> · v{version}\n\n"
        f"🎯 <b>Hook</b>\n{_e(hook)}\n\n"
        f"🧠 <b>Explicación simple</b>\n{_e(explain)}\n\n"
        f"⚡️ <b>Bitcoin Anchor</b>\n{_e(btc)}\n\n"
        f"📈 <b>Insight</b>\n{_e(insight)}\n\n"
        f"⚠️ <b>Riesgos / matices</b>\n{_e(risk)}\n\n"
        f"📝 <b>Caption sugerido (IG/TikTok/X)</b>\n{_e(caption)}\n\n"
        f"🖼 <b>Prompt visual (4:5)</b>\n{_e(visual)}\n\n"
        f"✅ <b>Checklist QA</b>\n{qa_lines}"
        f"{radar_block}"
    )
