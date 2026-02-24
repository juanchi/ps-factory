from typing import List, Optional
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def _fits_callback_data(s: str) -> bool:
    return len((s or "").encode("utf-8")) <= 64


def build_post_keyboard(post_id: str, candidate_ids: Optional[List[str]] = None) -> InlineKeyboardMarkup:
    """
    candidate_ids = candidate_id reales de radar_candidates (ej: "x:202599...")
    En callback_data usamos índice corto para evitar límite de 64 bytes:
    GEN:<post_id>:1 | GEN:<post_id>:2 | GEN:<post_id>:3
    """
    candidate_ids = candidate_ids or []
    rows = []

    approve_cb = f"APPROVE:{post_id}"
    regen_cb = f"REGEN:{post_id}"
    if _fits_callback_data(approve_cb) and _fits_callback_data(regen_cb):
        rows.append(
            [
                InlineKeyboardButton("✅ APROBAR", callback_data=approve_cb),
                InlineKeyboardButton("♻️ REGENERAR", callback_data=regen_cb),
            ]
        )

    # Alternos (Top 3 del Radar), por índice
    for i, _cid in enumerate(candidate_ids[:3], start=1):
        cb = f"GEN:{post_id}:{i}"
        if _fits_callback_data(cb):
            rows.append([InlineKeyboardButton(f"⚡️ Regenerar desde alterno #{i}", callback_data=cb)])

    versions_cb = f"VERSIONS:{post_id}"
    if _fits_callback_data(versions_cb):
        rows.append([InlineKeyboardButton("📚 Versions", callback_data=versions_cb)])

    # Fallback defensivo si no se pudo construir ningún botón válido
    if not rows:
        rows = [[InlineKeyboardButton("📚 Versions", callback_data="CLOSE")]]

    return InlineKeyboardMarkup(rows)
