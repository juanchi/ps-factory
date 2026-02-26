import os
import json
import time
import html
import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from radar.engine import run_radar_x
from db.sqlite_store import kv_get, kv_set


def _now_ts() -> int:
    return int(time.time())


def _escape_html(s: str) -> str:
    return html.escape(str(s or ""), quote=False)


def _ops_message(title: str, *, run_id: str, candidate_id: str, score: float, relevance: float, risk: float, has_url: bool, alternates: list[dict] | None = None, reason: str = "impact_candidate") -> str:
    alt_lines = ""
    if alternates:
        rows = []
        for i, a in enumerate(alternates[:3], start=1):
            t = str(a.get('title') or '').strip()[:110]
            rows.append(
                f"{i}) {_escape_html(t)}\n"
                f"   <code>{a.get('candidate_id','')}</code> · score <code>{float(a.get('total_score') or 0):.3f}</code> · rel <code>{float(a.get('relevance') or 0):.2f}</code>"
            )
        alt_lines = "\n<b>alternates:</b>\n" + "\n".join(rows)

    hint = (
        "\n\n<i>Para draft manual:</i> <code>/intraday_force_draft</code> (ganador)"
        "\n<i>o:</i> <code>/intraday_force_draft &lt;candidate_id&gt;</code>"
        "\n<i>o:</i> <code>/intraday_force_draft 1|2|3</code> (alternos)"
    )

    return (
        "🚨 <b>Intraday Impact Candidate</b>\n"
        f"<b>title:</b> {title}\n"
        f"<b>run_id:</b> <code>{run_id}</code>\n"
        f"<b>winner:</b> <code>{candidate_id}</code>\n"
        f"<b>score:</b> <code>{score:.3f}</code>\n"
        f"<b>relevance:</b> <code>{relevance:.2f}</code>\n"
        f"<b>risk:</b> <code>{risk:.2f}</code>\n"
        f"<b>has_link:</b> <code>{'yes' if has_url else 'no'}</code>\n"
        f"<b>reason:</b> <code>{reason}</code>"
        f"{alt_lines}"
        f"{hint}"
    )


def _parse_ts_iso(s: str | None) -> int | None:
    if not s:
        return None
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def _intraday_ops_keyboard(alternates: list[dict] | None = None) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("✅ Draft ganador", callback_data="IDF:W")]]
    alts = alternates or []
    if alts:
        alt_row = []
        for i in range(1, min(3, len(alts)) + 1):
            alt_row.append(InlineKeyboardButton(f"⚡ Alt {i}", callback_data=f"IDF:{i}"))
        if alt_row:
            rows.append(alt_row)
    return InlineKeyboardMarkup(rows)


async def _notify(bot: Bot, chat_id: int | None, text: str, *, alternates: list[dict] | None = None) -> None:
    if not chat_id:
        return
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=_intraday_ops_keyboard(alternates),
    )


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
    notify_on_skip = os.getenv('INTRADAY_NOTIFY_ON_SKIP', '1').strip().lower() in {'1', 'true', 'yes', 'on'}

    # daily cap (aplica solo al modo alerted estricto)
    cap_key = f"intraday:alerts_count:{day}"
    cur_count_raw = await kv_get(cap_key)
    cur_count = int(cur_count_raw or '0')
    if (not notify_on_skip) and cur_count >= max_alerts_day:
        await _set_last('skip_cap_reached', {'reason': 'cap_reached', 'max_alerts_day': max_alerts_day, 'day': day})
        return 0

    run_id, winner, alts = await run_radar_x()

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
            detail = {
                'reason': 'stale_candidate',
                'age_min': round(age_min, 1),
                'max_age_minutes': max_age_minutes,
                'candidate_id': candidate_id,
                'title': title,
                'run_id': run_id,
            }
            await _set_last('skip_stale', detail)
            if notify_on_skip:
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
                        reason='stale_candidate',
                    ),
                    alternates=[],
                )
            return 0

    # impact gate
    alt_preview = []
    for a in (alts or [])[:3]:
        try:
            sc = json.loads(a.get('scores_json') or '{}')
        except Exception:
            sc = {}
        alt_preview.append({
            'candidate_id': a.get('candidate_id'),
            'title': str(a.get('title') or '')[:120],
            'total_score': round(float(a.get('total_score') or 0.0), 3),
            'relevance': round(float(sc.get('relevance') or 0.0), 2),
        })

    base_detail = {
        'candidate_id': candidate_id,
        'title': title,
        'score': round(total_score, 3),
        'relevance': round(relevance, 2),
        'risk': round(risk, 2),
        'has_url': has_url,
        'alternates': alt_preview,
        'delta_score': round(total_score - min_score, 3),
        'delta_relevance': round(relevance - min_rel, 3),
        'delta_risk': round(max_risk - risk, 3),
    }

    async def _notify_editor_review(reason: str) -> None:
        if not notify_on_skip:
            return
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
                alternates=alt_preview,
                reason=reason,
            ),
            alternates=alt_preview,
        )

    if total_score < min_score:
        await _set_last('skip_low_score', {**base_detail, 'reason': 'low_score', 'min_score': min_score, 'run_id': run_id})
        await _notify_editor_review('low_score')
        return 0
    if relevance < min_rel:
        await _set_last('skip_low_relevance', {**base_detail, 'reason': 'low_relevance', 'min_relevance': min_rel, 'run_id': run_id})
        await _notify_editor_review('low_relevance')
        return 0
    if risk > max_risk:
        await _set_last('skip_high_risk', {**base_detail, 'reason': 'high_risk', 'max_risk': max_risk, 'run_id': run_id})
        await _notify_editor_review('high_risk')
        return 0
    if require_link and not has_url:
        await _set_last('skip_missing_link', {
            **base_detail,
            'reason': 'missing_link',
            'require_link': require_link,
            'run_id': run_id,
        })
        await _notify_editor_review('missing_link')
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
                    'run_id': run_id,
                })
                await _notify_editor_review('cooldown')
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
            alternates=alt_preview,
        ),
        alternates=alt_preview,
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
