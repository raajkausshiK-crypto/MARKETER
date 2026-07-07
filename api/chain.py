"""
api/chain.py
============
Vercel Python serverless function that returns a live NSE option chain (or a
synthetic intrinsic-value grid for cash-only stocks) using the Upstox REST API.

Endpoint:
    GET /api/chain?symbol=NIFTY[&expiry=2026-07-07]

Response JSON mirrors the old WebSocket bridge payload:
    {
      "type": "chain", "symbol": "NIFTY", "spot": 24005.85,
      "expiry": "2026-07-07", "expiries": [...],
      "rows": [{"strike": .., "call": {ltp,oi,iv,..}, "put": {...}}, ...]
    }

Environment:
    UPSTOX_ACCESS_TOKEN  - a valid daily Upstox access token (set in Vercel).

NOTE: Upstox may enforce a static-IP allowlist on your API app. Vercel functions
use dynamic IPs, so that restriction must be DISABLED for this to work.
"""

import datetime
import json
import os
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler

UPSTOX_HOST = "https://api.upstox.com"
# Real chains return the FULL strike list; this only bounds the synthetic grid
# used for cash-only stocks that have no listed options.
STRIKES_EACH_SIDE = 20

# Short-lived in-memory cache. Vercel keeps a warm Lambda between invocations,
# so this dict persists across requests on the same instance. It collapses the
# rapid 2s polling (and the same symbol requested by several panes/tabs) into
# far fewer Upstox calls, which is what avoids hitting Upstox rate limits.
_CACHE = {}
_CACHE_TTL = 8.0        # seconds a cached chain is considered fresh
_CACHE_MAX = 200        # guard against unbounded growth

# Last known-good chain per symbol, served if a refresh fails/returns empty.
_GOOD = {}
_GOOD_TTL = 90.0        # serve stale-but-good data up to this long on failure

# Instrument keys and expiry lists don't change intraday, so cache them for a
# long time. This keeps the flaky /instruments/search and /option/contract
# endpoints off the hot path (they were the ones intermittently rate-limiting
# and forcing the synthetic cash-only fallback).
_META_CACHE = {}
_META_TTL = 1800.0      # 30 min

# Price candles for the stock graph; one Upstox call per symbol per minute.
_CANDLE_CACHE = {}
_CANDLE_TTL = 60.0

# Fast-path map: symbol -> Upstox instrument_key. Indices AND common F&O stocks
# are hardcoded so we skip the /instruments/search endpoint entirely — that
# endpoint is aggressively rate-limited (HTTP 429), and since a failed search
# never caches, stocks would otherwise re-search every poll and stay broken.
# Equity keys are "NSE_EQ|<ISIN>".
SYMBOL_TO_INSTRUMENT = {
    # Indices
    "NIFTY": "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
    "NIFTYNXT50": "NSE_INDEX|Nifty Next 50",
    # F&O stocks (NSE_EQ|ISIN)
    "RELIANCE": "NSE_EQ|INE002A01018",
    "TCS": "NSE_EQ|INE467B01029",
    "HDFCBANK": "NSE_EQ|INE040A01034",
    "INFY": "NSE_EQ|INE009A01021",
    "ICICIBANK": "NSE_EQ|INE090A01021",
    "SBIN": "NSE_EQ|INE062A01020",
    "ITC": "NSE_EQ|INE154A01025",
    "AXISBANK": "NSE_EQ|INE238A01034",
    "KOTAKBANK": "NSE_EQ|INE237A01028",
    "LT": "NSE_EQ|INE018A01030",
    "HINDUNILVR": "NSE_EQ|INE030A01027",
    "BHARTIARTL": "NSE_EQ|INE397D01024",
    "MARUTI": "NSE_EQ|INE585B01010",
    "SUNPHARMA": "NSE_EQ|INE044A01036",
    "TATAMOTORS": "NSE_EQ|INE155A01022",
    "TATASTEEL": "NSE_EQ|INE081A01020",
    "WIPRO": "NSE_EQ|INE075A01022",
    "HCLTECH": "NSE_EQ|INE860A01027",
    "BAJFINANCE": "NSE_EQ|INE296A01024",
    "CIPLA": "NSE_EQ|INE059A01026",
    "ZOMATO": "NSE_EQ|INE758T01015",
    "ETERNAL": "NSE_EQ|INE758T01015",
}


class UpstoxError(Exception):
    def __init__(self, status, message):
        self.status = status
        self.message = message
        super().__init__(message)


def _token():
    tok = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip().strip('"').strip("'")
    if not tok:
        on_vercel = "yes" if os.environ.get("VERCEL") else "no"
        raise UpstoxError(
            500,
            "UPSTOX_ACCESS_TOKEN is not set for this deployment. "
            f"(on_vercel={on_vercel}) Add it in Vercel > Settings > "
            "Environment Variables for the Production environment, then REDEPLOY.",
        )
    return tok


def _get(path, params):
    url = f"{UPSTOX_HOST}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {_token()}",
        "Accept": "application/json",
        # Upstox sits behind Cloudflare which blocks the default urllib UA.
        "User-Agent": "Mozilla/5.0 (compatible; OptionsScreener/1.0)",
    })
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        try:
            msg = json.loads(body)["errors"][0]["message"]
        except Exception:
            msg = body[:200] or e.reason
        raise UpstoxError(e.status, msg)
    except Exception as e:  # network / timeout
        raise UpstoxError(502, f"Upstream request failed: {e}")


# ------------------------------------------------------------------ helpers
def _meta_get(key):
    hit = _META_CACHE.get(key)
    if hit and (time.time() - hit[0]) < _META_TTL:
        return hit[1]
    return None


def _meta_put(key, val):
    if val:                       # never cache an empty/failed result
        _META_CACHE[key] = (time.time(), val)


def resolve_instrument(symbol):
    symbol = symbol.upper().strip()
    if symbol in SYMBOL_TO_INSTRUMENT:
        return SYMBOL_TO_INSTRUMENT[symbol]

    cached = _meta_get(("inst", symbol))
    if cached:
        return cached

    data = _get("/v2/instruments/search", {"query": symbol}).get("data", []) or []

    def seg(d):
        return (d.get("segment") or "").upper()

    def tsym(d):
        return (d.get("trading_symbol") or "").upper()

    ordered, seen = [], set()

    def add(items):
        for d in items:
            key = d.get("instrument_key")
            if key and key not in seen and seg(d) in ("NSE_EQ", "NSE_INDEX"):
                seen.add(key)
                ordered.append(key)

    fo_underlyings = {d.get("underlying_key") for d in data
                      if seg(d) == "NSE_FO" and d.get("underlying_key")}
    add([d for d in data if d.get("instrument_key") in fo_underlyings])
    exact = [d for d in data if tsym(d) == symbol]
    add([d for d in exact if seg(d) == "NSE_INDEX"])
    add([d for d in exact if seg(d) == "NSE_EQ"])
    add([d for d in data if seg(d) == "NSE_INDEX"])
    add([d for d in data if seg(d) == "NSE_EQ"])

    if not ordered:
        raise UpstoxError(404, f"No NSE stock/index found for '{symbol}'")
    _meta_put(("inst", symbol), ordered[0])
    return ordered[0]


def get_expiries(instrument_key):
    cached = _meta_get(("exp", instrument_key))
    if cached:
        return cached
    try:
        data = _get("/v2/option/contract", {"instrument_key": instrument_key}).get("data", []) or []
    except UpstoxError:
        return []                 # transient failure — don't cache, retry next time
    exps = sorted({(d.get("expiry") or "")[:10] for d in data if d.get("expiry")})
    _meta_put(("exp", instrument_key), exps)
    return exps


def get_ltp(instrument_key):
    data = _get("/v3/market-quote/ltp", {"instrument_key": instrument_key}).get("data", {}) or {}
    for v in data.values():
        if isinstance(v, dict) and v.get("last_price") is not None:
            return float(v["last_price"])
    return 0.0


def _leg(opt):
    opt = opt or {}
    md = opt.get("market_data") or {}
    gk = opt.get("option_greeks") or {}

    def num(v):
        try:
            return round(float(v), 2)
        except (TypeError, ValueError):
            return 0.0

    return {
        "ltp": num(md.get("ltp")),
        "oi": num(md.get("oi")),
        "bid": num(md.get("bid_price")),
        "ask": num(md.get("ask_price")),
        "iv": num(gk.get("iv")),
    }


def build_option_chain(symbol, instrument_key, expiry, expiries):
    if expiry not in expiries:
        expiry = expiries[0]

    data = _get("/v2/option/chain", {
        "instrument_key": instrument_key, "expiry_date": expiry,
    }).get("data", []) or []

    spot = data[0].get("underlying_spot_price", 0.0) if data else 0.0

    # Full chain: return every strike the exchange lists for this expiry,
    # sorted low -> high. (PCR / Max Pain / OI totals then cover the whole
    # chain, and the frontend highlights ATM + windows as it sees fit.)
    data.sort(key=lambda r: r.get("strike_price", 0))

    rows = [{
        "strike": r.get("strike_price"),
        "call": _leg(r.get("call_options")),
        "put": _leg(r.get("put_options")),
    } for r in data]

    # Trim the dead tail: drop strikes with no activity at all (0 OI and 0 LTP
    # on both call and put). These far-OTM strikes just render as rows of zeros.
    live = [r for r in rows
            if r["call"]["oi"] or r["put"]["oi"] or r["call"]["ltp"] or r["put"]["ltp"]]
    rows = live or rows

    return {
        "type": "chain", "symbol": symbol, "spot": round(spot, 2),
        "expiry": expiry, "expiries": expiries, "rows": rows,
    }


def build_synthetic_chain(symbol, instrument_key):
    spot = get_ltp(instrument_key)
    if spot >= 20000:
        step = 100
    elif spot >= 5000:
        step = 50
    elif spot >= 1000:
        step = 20
    elif spot >= 250:
        step = 5
    elif spot >= 50:
        step = 2.5
    else:
        step = 1
    atm = round(spot / step) * step

    rows = []
    for i in range(-STRIKES_EACH_SIDE, STRIKES_EACH_SIDE + 1):
        strike = round(atm + i * step, 2)
        if strike <= 0:
            continue
        rows.append({
            "strike": strike,
            "call": {"ltp": round(max(0.0, spot - strike), 2), "oi": 0, "bid": 0, "ask": 0, "iv": 0},
            "put": {"ltp": round(max(0.0, strike - spot), 2), "oi": 0, "bid": 0, "ask": 0, "iv": 0},
        })

    return {
        "type": "chain", "symbol": symbol, "spot": round(spot, 2),
        "expiry": "CASH (no options)", "expiries": ["CASH (no options)"],
        "rows": rows, "cash_only": True,
    }


def get_chain(symbol, expiry):
    instrument_key = resolve_instrument(symbol)
    expiries = get_expiries(instrument_key)
    if not expiries:
        return build_synthetic_chain(symbol, instrument_key)
    return build_option_chain(symbol, instrument_key, expiry, expiries)


def _is_good(payload):
    """A real, non-empty option chain (not an error / synthetic-empty fallback)."""
    return bool(payload and payload.get("rows") and not payload.get("cash_only"))


def get_chain_cached(symbol, expiry):
    """get_chain() with a short TTL cache + stale-while-error fallback.

    - Fresh good chains are cached for _CACHE_TTL seconds.
    - If a refresh fails or returns an empty/synthetic chain, we serve the last
      known-good chain for up to _GOOD_TTL seconds so a stock never flips to
      "Error" or a wall of zeros just because Upstox rate-limited one request.
    """
    key = (symbol.upper(), expiry or "")
    now = time.time()

    hit = _CACHE.get(key)
    if hit and (now - hit[0]) < _CACHE_TTL:
        return hit[1]

    try:
        payload = get_chain(symbol, expiry)
        err = None
    except UpstoxError as e:
        payload, err = None, e

    if _is_good(payload):
        if len(_CACHE) >= _CACHE_MAX:      # simple size guard: drop oldest entry
            _CACHE.pop(min(_CACHE, key=lambda k: _CACHE[k][0]), None)
        _CACHE[key] = (now, payload)
        _GOOD[key] = (now, payload)
        return payload

    # Refresh failed / empty / synthetic — prefer a recent last-good chain.
    stale = _GOOD.get(key)
    if stale and (now - stale[0]) < _GOOD_TTL:
        return stale[1]

    if payload is not None:                # genuinely cash-only or empty
        return payload
    raise err or UpstoxError(502, "Upstream unavailable and no cached chain yet.")


# ------------------------------------------------------------------ candles
def get_candles(symbol):
    """Intraday 1-minute price candles for the stock graph; falls back to
    daily candles (last ~45 days) when the market is closed / intraday empty.

    Returns {"type": "candles", "symbol", "interval", "candles": [[ts,o,h,l,c], ...]}
    in chronological order.
    """
    instrument_key = resolve_instrument(symbol)
    enc = urllib.parse.quote(instrument_key, safe="")

    interval = "1minute"
    try:
        data = _get(f"/v2/historical-candle/intraday/{enc}/1minute", {}).get("data", {}) or {}
        candles = data.get("candles") or []
    except UpstoxError:
        candles = []

    if not candles:
        interval = "day"
        to = datetime.date.today()
        frm = to - datetime.timedelta(days=45)
        data = _get(f"/v2/historical-candle/{enc}/day/{to}/{frm}", {}).get("data", {}) or {}
        candles = data.get("candles") or []

    # Upstox returns newest-first: [ts, open, high, low, close, volume, oi]
    candles = [[c[0], c[1], c[2], c[3], c[4]] for c in reversed(candles)]
    return {"type": "candles", "symbol": symbol, "interval": interval, "candles": candles}


def get_candles_cached(symbol):
    now = time.time()
    hit = _CANDLE_CACHE.get(symbol)
    if hit and (now - hit[0]) < _CANDLE_TTL:
        return hit[1]
    payload = get_candles(symbol)
    if payload.get("candles"):             # don't cache empty results
        _CANDLE_CACHE[symbol] = (now, payload)
    return payload


# ------------------------------------------------------------------ handler
class handler(BaseHTTPRequestHandler):
    """Classic Vercel Python serverless function entrypoint."""

    def do_GET(self):
        qs = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(qs)
        symbol = (params.get("symbol", ["NIFTY"])[0] or "NIFTY").upper().strip()
        expiry = params.get("expiry", [None])[0] or None
        want_candles = bool(params.get("candles"))

        try:
            payload = get_candles_cached(symbol) if want_candles else get_chain_cached(symbol, expiry)
        except UpstoxError as e:
            payload = {"type": "error", "message": e.message, "status": e.status}
        except Exception as e:  # pragma: no cover
            payload = {"type": "error", "message": f"Server error: {e}"}

        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


# Local test: `python api/chain.py NIFTY`
if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "NIFTY"
    print(json.dumps(get_chain(sym, None), indent=2)[:600])

