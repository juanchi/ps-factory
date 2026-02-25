import os
import json
import time
import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram import Bot
from telegram.constants import ParseMode

from radar.engine import run_radar_x
from db.sqlite_store import kv_get, kv_set


def _now_ts() -> int:
    return int(time.time())


def _ops_message(title: str, *, run_id: str, candidate_id: str, score: float, relevance: float, risk: float, has_url: bool, reason: str = "impact_candidate") -> str:
    return (
        "🚨 <b>Intraday Impact Candidate</b>\n"
        f"<b>title:</b> {title}\n"
        f"<b>run_id:</b> <code>{run_id}</code>\n"
        f"<b>candidate_id:</b> <code>{candidate_id}</code>\n"
        f"<b>score:</b> <code>{score:.3f}</code>\n"
        f"<b>relevance:</b> <code>{relevance:.2f}</code>\n"
        f"<b>risk:</b> <code>{risk:.2f}</code>\n"
        f"<b>has_link:</b> <code>{'yes' if has_url else 'no'}</code>\n"
        f"<b>reason:</b> <code>{reason}</code>"
    )


def _parse_ts_iso(s: str | None) -> int | None:
    if not s:
        return None
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


async def _notify(bot: Bot, chat_id: int | None, text: str) -> None:
    if not chat_id:
        return
    await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def _set_last(result: str, detail: dict) -> None:
    await kv_set('intraday:last_run_ts', str(_now_ts()))
    await kv_set('intraday:last_result', result)
    await kv_set('intraday:last_detail', json.dumps(detail, ensure_ascii=False)[:2000])


async def run_intraday_monitor() -> int:
    load_dotenv('/opt/ps_factory/config/.env', override=True)

    token = os.environ['TG_BOT_TOKEN']
    ops_chat_id = int(os.getenv('TG_OPS_CHAT_ID', '0') or 0)
    bot = Bot(token=token)

    run_tz = os.getenv('INTRADAY_RUN_TZ', 'America/Panama')
    day = datetime.now(ZoneInfo(run_tz)).date().isoformat()

    # Configs
    min_score = float(os.getenv('INTRADAY_MIN_SCORE', '7.0'))
    min_rel = float(os.getenv('INTRADAY_MIN_RELEVANCE', '5.0'))
    max_risk = float(os.getenv('INTRADAY_MAX_RISK', '3.5'))
    require_link = os.getenv('INTRADAY_REQUIRE_LINK', '1').strip().lower() in {'1', 'true', 'yes', 'on'}
    cooldown_hours = float(os.getenv('INTRADAY_COOLDOWN_HOURS', '6'))
    max_alerts_day = int(os.getenv('INTRADAY_MAX_ALERTS_PER_DAY', '3'))
    max_age_minutes = int(os.getenv('INTRADAY_MAX_AGE_MINUTES', '240'))

    # daily cap
    cap_key = f"intraday:alerts_count:{day}"
    cur_count_raw = await kv_get(cap_key)
    cur_count = int(cur_count_raw or '0')
    if cur_count >= max_alerts_day:
        await _set_last('skip_cap_reached', {'reason': 'cap_reached', 'max_alerts_day': max_alerts_day, 'day': day})
        return 0

    run_id, winner, _alts = await run_radar_x()

    candidate_id = str(winner.get('candidate_id') or '')
    title = str(winner.get('title') or '').strip()[:180]
    total_score = float(winner.get('total_score') or 0.0)

    try:
        scores = json.loads(winner.get('scores_json') or '{}')
    except Exception:
        scores = {}

    relevance = float(scores.get('relevance') or 0.0)
    risk = float(scores.get('risk') or 0.0)
    has_url = bool(scores.get('has_url'))

    # freshness check (avoid stale fallback candidates)
    tweet_created_ts = None
    try:
        evidence = json.loads(winner.get('evidence_json') or '{}')
        tweet_created_ts = _parse_ts_iso(((evidence.get('tweet') or {}).get('created_at')))
    except Exception:
        tweet_created_ts = None

    if tweet_created_ts:
        age_min = (_now_ts() - tweet_created_ts) / 60.0
        if age_min > max_age_minutes:
            await _set_last('skip_stale', {
                'reason': 'stale_candidate', 'age_min': round(age_min, 1), 'max_age_minutes': max_age_minutes,
                'candidate_id': candidate_id, 'title': title,
            })
            return 0

    # impact gate
    base_detail = {
        'candidate_id': candidate_id,
        'title': title,
        'score': round(total_score, 3),
        'relevance': round(relevance, 2),
        'risk': round(risk, 2),
        'has_url': has_url,
    }

    if total_score < min_score:
        await _set_last('skip_low_score', {**base_detail, 'reason': 'low_score', 'min_score': min_score})
        return 0
    if relevance < min_rel:
        await _set_last('skip_low_relevance', {**base_detail, 'reason': 'low_relevance', 'min_relevance': min_rel})
        return 0
    if risk > max_risk:
        await _set_last('skip_high_risk', {**base_detail, 'reason': 'high_risk', 'max_risk': max_risk})
        return 0
    if require_link and not has_url:
        await _set_last('skip_missing_link', {
            **base_detail,
            'reason': 'missing_link',
            'require_link': require_link,
        })
        return 0

    # cooldown by candidate
    cd_key = f"intraday:last_alert:{candidate_id}"
    last_alert_raw = await kv_get(cd_key)
    if last_alert_raw:
        try:
            last_alert_ts = int(last_alert_raw)
            if _now_ts() - last_alert_ts < int(cooldown_hours * 3600):
                await _set_last('skip_cooldown', {
                    **base_detail,
                    'reason': 'cooldown',
                    'cooldown_hours': cooldown_hours,
                })
                return 0
        except Exception:
            pass

    await _notify(
        bot,
        ops_chat_id,
        _ops_message(
            title=title or '(sin título)',
            run_id=run_id,
            candidate_id=candidate_id,
            score=total_score,
            relevance=relevance,
            risk=risk,
            has_url=has_url,
        ),
    )

    await kv_set(cd_key, str(_now_ts()))
    await kv_set(cap_key, str(cur_count + 1))
    await _set_last('alerted', {
        **base_detail,
        'reason': 'impact_candidate',
        'run_id': run_id,
    })

    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(run_intraday_monitor()))
