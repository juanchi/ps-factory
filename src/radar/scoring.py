import math
import re
from typing import Dict, Tuple, List, Any
from urllib.parse import urlparse

# ---------------------------
# Keywords
# ---------------------------

BTC_KW: List[str] = [
    "bitcoin", "btc", "lightning", "ln", "sats", "satoshi", "stablecoin",
    "cbdc", "custody", "self-custody", "fees", "hashrate", "mining",
    "taproot", "etf", "halving", "onchain", "on-chain", "utxo", "mempool"
]

PANAMA_KW: List[str] = [
    "panamá", "panama", "balboa", "asamblea", "banco", "dólar", "dolar",
    "inflación", "inflacion", "canal", "zona libre", "colón", "colon",
    "tocumen", "metro", "css", "contraloría", "contraloria"
]

RISK_KW: List[str] = [
    "traidor", "corrupto", "ladrón", "ladron", "vendido", "dictador",
    "partido", "presidente", "diputado", "campaña", "campana",
    "imbecil", "estupido", "estúpido", "basura", "hp"
]

# Domain trust scoring (v1)
HIGH_TRUST_DOMAINS: List[str] = [
    "reuters.com",
    "bloomberg.com",
    "wsj.com",
    "ft.com",
    "apnews.com",
    "coindesk.com",
    "theblock.co",
]

LOW_TRUST_DOMAINS: List[str] = [
    "blogspot.com",
    "medium.com",
    "substack.com",
]

# ---------------------------
# Helpers
# ---------------------------

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)

def _kw_hits(text: str, kws: List[str]) -> int:
    t = (text or "").lower()
    return sum(1 for k in kws if k in t)

def _has_url(text: str) -> bool:
    return bool(_URL_RE.search(text or ""))

def _count_urls(text: str) -> int:
    return len(_URL_RE.findall(text or ""))

def _is_rt(text: str) -> bool:
    t = (text or "").lstrip()
    return t.startswith("RT @") or t.startswith("rt @")

def _extract_domains(text: str) -> List[str]:
    domains: List[str] = []
    for u in _URL_RE.findall(text or ""):
        raw = u.strip().rstrip(').,;!?')
        if raw.lower().startswith("www."):
            raw = "http://" + raw
        try:
            host = (urlparse(raw).netloc or "").lower()
            if host.startswith("www."):
                host = host[4:]
            if host:
                domains.append(host)
        except Exception:
            continue
    return domains

def _domain_trust_score(domains: List[str]) -> Tuple[float, str]:
    if not domains:
        return 0.0, "none"

    def _match(host: str, base: str) -> bool:
        return host == base or host.endswith("." + base)

    if any(_match(h, d) for h in domains for d in HIGH_TRUST_DOMAINS):
        return 0.9, "high"

    if any(_match(h, d) for h in domains for d in LOW_TRUST_DOMAINS):
        return -0.35, "low"

    return 0.0, "unknown"

def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default

# ---------------------------
# Main scoring
# ---------------------------

def score_tweet(tw: Dict, *, source: str) -> Tuple[float, Dict]:
    """
    PS Factory – Noticia Dura Mode (v4: domain trust)

    Prioriza:
      - Relevancia BTC/Panamá
      - Presencia de link (señal de noticia)
      - Engagement real

    Penaliza:
      - Pelea política / insultos
      - RT sin link y con baja relevancia

    Retorna: (total_score 0-10, breakdown dict)
    """

    text_raw = (tw.get("text") or "")
    text = text_raw.lower()
    m = tw.get("metrics") or {}

    likes = _safe_int(m.get("like_count", 0))
    rts = _safe_int(m.get("retweet_count", 0))
    replies = _safe_int(m.get("reply_count", 0))
    quotes = _safe_int(m.get("quote_count", 0))

    # ---------------------------
    # 1️⃣ Viralidad (log scale)
    # ---------------------------
    vir_raw = likes + (2 * rts) + replies + (2 * quotes)
    viral = min(10.0, math.log10(vir_raw + 1) * 3.5)

    # ---------------------------
    # 2️⃣ Relevancia editorial
    # ---------------------------
    btc_hits = _kw_hits(text, BTC_KW)
    pa_hits = _kw_hits(text, PANAMA_KW)

    if "panama" in (source or "").lower():
        relevance = min(10.0, 2.0 + btc_hits * 1.3 + pa_hits * 1.2)
    else:
        relevance = min(10.0, 2.0 + btc_hits * 1.3 + pa_hits * 0.6)

    # ---------------------------
    # 3️⃣ Valor educativo
    # ---------------------------
    edu = 0.0
    if any(k in text for k in [
        "why", "porque", "cómo", "como",
        "explica", "incentivo", "censura",
        "custody", "permissionless"
    ]):
        edu += 1.5

    if btc_hits >= 2:
        edu += 1.5

    if len(text) > 200:
        edu += 0.5

    edu = min(10.0, edu)

    # ---------------------------
    # 4️⃣ Riesgo editorial
    # ---------------------------
    risk_hits = _kw_hits(text, RISK_KW)
    risk = min(10.0, risk_hits * 2.5)

    # ---------------------------
    # 5️⃣ Señales de noticia dura
    # ---------------------------
    has_url = _has_url(text_raw)
    url_count = _count_urls(text_raw)
    is_rt = _is_rt(text_raw)
    domains = _extract_domains(text_raw)

    link_boost = 0.0
    if has_url:
        link_boost += 2.2
        if url_count >= 2:
            link_boost += 0.3

    domain_boost, domain_trust = _domain_trust_score(domains)

    # 🔵 RT inteligente:
    # Solo penaliza si:
    # - Es RT
    # - No tiene link
    # - Relevancia baja (<5)
    rt_penalty = 0.0
    if is_rt and not has_url and relevance < 5:
        rt_penalty = 0.8

    # ---------------------------
    # 6️⃣ Score final
    # ---------------------------
    total = (
        (0.45 * relevance) +
        (0.25 * viral) +
        (0.15 * edu) +
        link_boost +
        domain_boost -
        (0.35 * risk) -
        rt_penalty
    )

    total = max(0.0, min(10.0, total))

    breakdown = {
        "viral": round(viral, 2),
        "relevance": round(relevance, 2),
        "edu": round(edu, 2),
        "risk": round(risk, 2),
        "link_boost": round(link_boost, 2),
        "domain_boost": round(domain_boost, 2),
        "domain_trust": domain_trust,
        "domains": domains,
        "rt_penalty": round(rt_penalty, 2),
        "has_url": has_url,
        "url_count": url_count,
        "is_rt": is_rt,
        "btc_hits": btc_hits,
        "pa_hits": pa_hits,
        "vir_raw": vir_raw,
        "likes": likes,
        "rts": rts,
        "replies": replies,
        "quotes": quotes,
    }

    return total, breakdown
