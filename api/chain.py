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

import json
import os
import time
import urllib.parse
import urllib.request
from collections import Counter
from http.server import BaseHTTPRequestHandler

UPSTOX_HOST = "https://api.upstox.com"
STRIKES_EACH_SIDE = 20   # strikes above AND below ATM -> ~41-row chain

# Short-lived in-memory cache. Vercel keeps a warm Lambda between invocations,
# so this dict persists across requests on the same instance. It collapses the
# rapid 2s polling (and the same symbol requested by several panes/tabs) into
# far fewer Upstox calls, which is what avoids hitting Upstox rate limits.
_CACHE = {}
_CACHE_TTL = 3.0        # seconds a cached chain is considered fresh
_CACHE_MAX = 200        # guard against unbounded growth

# Fast-path map for common indices (search also works, this just saves a call).
SYMBOL_TO_INSTRUMENT = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
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
def resolve_instrument(symbol):
    symbol = symbol.upper().strip()
    if symbol in SYMBOL_TO_INSTRUMENT:
        return SYMBOL_TO_INSTRUMENT[symbol]

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
    return ordered[0]


def get_expiries(instrument_key):
    try:
        data = _get("/v2/option/contract", {"instrument_key": instrument_key}).get("data", []) or []
    except UpstoxError:
        return []
    exps = sorted({(d.get("expiry") or "")[:10] for d in data if d.get("expiry")})
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
    data.sort(key=lambda r: r.get("strike_price", 0))

    if data:
        strikes = [r.get("strike_price", 0) for r in data]
        diffs = [round(strikes[i + 1] - strikes[i], 2) for i in range(len(strikes) - 1)]
        diffs = [d for d in diffs if d > 0]
        step = Counter(diffs).most_common(1)[0][0] if diffs else 0

        atm_idx = min(range(len(data)),
                      key=lambda i: abs(data[i].get("strike_price", 0) - spot))
        atm = data[atm_idx].get("strike_price", 0)

        if step > 0:
            by_strike = {round(r.get("strike_price", 0), 2): r for r in data}

            def find(t):
                for s, r in by_strike.items():
                    if abs(s - t) < 0.01:
                        return r
                return None

            selected = [by_strike[round(atm, 2)]] if round(atm, 2) in by_strike else []
            for i in range(1, STRIKES_EACH_SIDE + 1):
                up = find(atm + i * step)
                if up is None:
                    break
                selected.append(up)
            for i in range(1, STRIKES_EACH_SIDE + 1):
                dn = find(atm - i * step)
                if dn is None:
                    break
                selected.append(dn)
            data = sorted(selected, key=lambda r: r.get("strike_price", 0)) or data
        else:
            lo = max(0, atm_idx - STRIKES_EACH_SIDE)
            hi = min(len(data), atm_idx + STRIKES_EACH_SIDE + 1)
            data = data[lo:hi]

    rows = [{
        "strike": r.get("strike_price"),
        "call": _leg(r.get("call_options")),
        "put": _leg(r.get("put_options")),
    } for r in data]

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


def get_chain_cached(symbol, expiry):
    """get_chain() with a short TTL cache to throttle upstream Upstox calls.

    On error we still raise (so errors are never cached); only successful
    payloads are stored, and only for _CACHE_TTL seconds.
    """
    key = (symbol.upper(), expiry or "")
    now = time.time()
    hit = _CACHE.get(key)
    if hit and (now - hit[0]) < _CACHE_TTL:
        return hit[1]

    payload = get_chain(symbol, expiry)

    if len(_CACHE) >= _CACHE_MAX:          # simple size guard: drop oldest entry
        oldest = min(_CACHE, key=lambda k: _CACHE[k][0])
        _CACHE.pop(oldest, None)
    _CACHE[key] = (now, payload)
    return payload


# ------------------------------------------------------------------ handler
class handler(BaseHTTPRequestHandler):
    """Classic Vercel Python serverless function entrypoint."""

    def do_GET(self):
        qs = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(qs)
        symbol = (params.get("symbol", ["NIFTY"])[0] or "NIFTY").upper().strip()
        expiry = params.get("expiry", [None])[0] or None

        try:
            payload = get_chain_cached(symbol, expiry)
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

