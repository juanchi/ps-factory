import os
import time
import json
import asyncio
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
)


def _now_ts() -> int:
    return int(time.time())


async def _notify(bot: Bot, chat_id: int | None, text: str) -> None:
    if not chat_id:
        return
    await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def run_daily() -> int:
    load_dotenv('/opt/ps_factory/config/.env', override=True)

    token = os.environ['TG_BOT_TOKEN']
    drafts_chat_id = int(os.environ['TG_DRAFTS_CHAT_ID'])
    ops_chat_id = int(os.getenv('TG_OPS_CHAT_ID', '0') or 0)

    bot = Bot(token=token)

    today_key = f"scheduler:daily_radar_run:{datetime.now(timezone.utc).date().isoformat()}"
    already = await kv_get(today_key)
    if already:
        await _notify(bot, ops_chat_id, f"ℹ️ Daily radar ya ejecutado hoy. key=<code>{today_key}</code>")
        return 0

    run_id, winner, alternates = await run_radar_x()
    winner_id = winner['candidate_id']
    winner_score = float(winner.get('total_score') or 0.0)
    min_score = float(os.getenv('RADAR_MIN_SCORE', '0.55'))

    # idempotencia diaria: se marca aunque haga skip por umbral
    await kv_set(today_key, str(_now_ts()))

    if winner_score < min_score:
        await _notify(
            bot,
            ops_chat_id,
            "🟡 Daily radar ejecutado, skip por umbral editorial.\n\n"
            f"<b>run_id:</b> <code>{run_id}</code>\n"
            f"<b>winner_score:</b> <code>{winner_score:.3f}</code>\n"
            f"<b>min_score:</b> <code>{min_score:.3f}</code>",
        )
        return 0

    prompt = await _prompt_from_candidate(winner)
    raw = openclaw_chat(prompt)
    post = json.loads(_extract_json(raw))

    post_id = f"radar-{_now_ts()}"
    post['post_id'] = post_id
    post['topic'] = str(post.get('topic') or 'Radar X (Top)')
    post['radar_winner_candidate_id'] = winner_id
    post['radar_selected_candidate_id'] = winner_id

    alt_ids = [a['candidate_id'] for a in alternates]
    post['radar_alternate_candidate_ids'] = alt_ids
    post['radar_winner_preview'] = _candidate_preview(winner)
    post['radar_alternate_previews'] = [_candidate_preview(a) for a in alternates]

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

    await _notify(
        bot,
        ops_chat_id,
        "✅ Daily radar enviado a Drafts.\n\n"
        f"<b>run_id:</b> <code>{run_id}</code>\n"
        f"<b>post_id:</b> <code>{post_id}</code>\n"
        f"<b>winner_score:</b> <code>{winner_score:.3f}</code>",
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(run_daily()))
