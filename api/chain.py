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
    # ── Indices ──
    "NIFTY":       "NSE_INDEX|Nifty 50",
    "BANKNIFTY":   "NSE_INDEX|Nifty Bank",
    "FINNIFTY":    "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY":  "NSE_INDEX|NIFTY MID SELECT",
    "NIFTYNXT50":  "NSE_INDEX|Nifty Next 50",
    "SENSEX":      "BSE_INDEX|SENSEX",

    # ── Banking & Finance ──
    "HDFCBANK":    "NSE_EQ|INE040A01034",
    "ICICIBANK":   "NSE_EQ|INE090A01021",
    "SBIN":        "NSE_EQ|INE062A01020",
    "AXISBANK":    "NSE_EQ|INE238A01034",
    "KOTAKBANK":   "NSE_EQ|INE237A01028",
    "BANKBARODA":  "NSE_EQ|INE028A01039",
    "PNB":         "NSE_EQ|INE160A01022",
    "INDUSINDBK":  "NSE_EQ|INE095A01012",
    "FEDERALBNK":  "NSE_EQ|INE171A01029",
    "IDFCFIRSTB":  "NSE_EQ|INE092T01019",
    "BANDHANBNK":  "NSE_EQ|INE545U01014",
    "AUBANK":      "NSE_EQ|INE949L01017",
    "CANBK":       "NSE_EQ|INE476A01022",
    "UNIONBANK":   "NSE_EQ|INE692A01016",
    "INDIANB":     "NSE_EQ|INE562A01011",
    "BAJFINANCE":  "NSE_EQ|INE296A01024",
    "BAJAJFINSV":  "NSE_EQ|INE918I01026",
    "HDFCLIFE":    "NSE_EQ|INE795G01014",
    "SBILIFE":     "NSE_EQ|INE123W01016",
    "ICICIPRULI":  "NSE_EQ|INE726G01019",
    "MUTHOOTFIN":  "NSE_EQ|INE414G01012",
    "MANAPPURAM":  "NSE_EQ|INE522D01027",
    "CHOLAFIN":    "NSE_EQ|INE121A01024",
    "M&MFIN":      "NSE_EQ|INE774D01024",
    "LICHSGFIN":   "NSE_EQ|INE115A01026",
    "RECLTD":      "NSE_EQ|INE020B01018",
    "PFC":         "NSE_EQ|INE134E01011",
    "SHRIRAMFIN":  "NSE_EQ|INE721A01013",

    # ── IT / Technology ──
    "TCS":         "NSE_EQ|INE467B01029",
    "INFY":        "NSE_EQ|INE009A01021",
    "WIPRO":       "NSE_EQ|INE075A01022",
    "HCLTECH":     "NSE_EQ|INE860A01027",
    "TECHM":       "NSE_EQ|INE669C01036",
    "LTIM":        "NSE_EQ|INE214T01019",
    "MPHASIS":     "NSE_EQ|INE356A01018",
    "COFORGE":     "NSE_EQ|INE591G01017",
    "PERSISTENT":  "NSE_EQ|INE262H01013",
    "LTTS":        "NSE_EQ|INE010V01017",
    "CYIENT":      "NSE_EQ|INE136B01020",
    "TATAELXSI":   "NSE_EQ|INE670A01012",
    "ZOMATO":      "NSE_EQ|INE758T01015",
    "ETERNAL":     "NSE_EQ|INE758T01015",
    "PAYTM":       "NSE_EQ|INE982J01020",
    "NAUKRI":      "NSE_EQ|INE663F01024",
    "POLICYBZR":   "NSE_EQ|INE417T01026",
    "ROUTE":       "NSE_EQ|INE450U01017",
    "BIRLASOFT":   "NSE_EQ|INE836A01035",
    "OFSS":        "NSE_EQ|INE881D01027",
    "SONATASOFT":  "NSE_EQ|INE269A01021",
    "INTELLECT":   "NSE_EQ|INE306R01017",
    "TANLA":       "NSE_EQ|INE483C01032",
    "MASTEK":      "NSE_EQ|INE759A01021",
    "ZENSAR":      "NSE_EQ|INE520A01027",
    "HAPPSTMNDS": "NSE_EQ|INE419U01012",

    # ── Pharma & Healthcare ──
    "SUNPHARMA":   "NSE_EQ|INE044A01036",
    "CIPLA":       "NSE_EQ|INE059A01026",
    "DRREDDY":     "NSE_EQ|INE089A01023",
    "DIVISLAB":    "NSE_EQ|INE361B01024",
    "AUROPHARMA":  "NSE_EQ|INE406A01037",
    "BIOCON":      "NSE_EQ|INE376G01013",
    "LUPIN":       "NSE_EQ|INE326A01037",
    "APOLLOHOSP":  "NSE_EQ|INE437A01024",
    "MAXHEALTH":   "NSE_EQ|INE027H01010",
    "TORNTPHARM":  "NSE_EQ|INE685A01028",
    "ZYDUSLIFE":   "NSE_EQ|INE010B01027",
    "LALPATHLAB":  "NSE_EQ|INE600L01024",
    "ALKEM":       "NSE_EQ|INE540L01014",
    "IPCALAB":     "NSE_EQ|INE571A01020",
    "NATCOPHARMA": "NSE_EQ|INE987B01026",
    "LAURUSLABS":  "NSE_EQ|INE947Q01028",
    "GLENMARK":    "NSE_EQ|INE935A01035",
    "METROPOLIS":  "NSE_EQ|INE112L01020",
    "ABBOTINDIA":  "NSE_EQ|INE358A01014",
    "FORTIS":      "NSE_EQ|INE061F01013",
    "GRANULES":    "NSE_EQ|INE101D01020",
    "SYNGENE":     "NSE_EQ|INE398R01022",
    "AJANTPHARM":  "NSE_EQ|INE031B01049",
    "ASTRAZEN":    "NSE_EQ|INE203A01020",
    "PFIZER":      "NSE_EQ|INE182A01018",
    "GLAND":       "NSE_EQ|INE068V01023",

    # ── Automobile & Auto Ancillary ──
    "TATAMOTORS":  "NSE_EQ|INE155A01022",
    "MARUTI":      "NSE_EQ|INE585B01010",
    "M&M":         "NSE_EQ|INE101A01026",
    "BAJAJ-AUTO":  "NSE_EQ|INE917I01010",
    "EICHERMOT":   "NSE_EQ|INE066A01021",
    "HEROMOTOCO":  "NSE_EQ|INE158A01026",
    "ASHOKLEY":    "NSE_EQ|INE208A01029",
    "TVSMOTOR":    "NSE_EQ|INE494B01023",
    "BALKRISIND":  "NSE_EQ|INE787D01026",
    "MRF":         "NSE_EQ|INE883A01011",
    "APOLLOTYRE":  "NSE_EQ|INE438A01022",
    "MOTHERSON":   "NSE_EQ|INE775A01035",
    "BOSCHLTD":    "NSE_EQ|INE323A01026",
    "BHARATFORG":  "NSE_EQ|INE465A01025",
    "EXIDEIND":    "NSE_EQ|INE302A01020",
    "TIINDIA":     "NSE_EQ|INE592A01026",
    "ENDURANCE":   "NSE_EQ|INE913H01037",
    "SUNDRMFAST":  "NSE_EQ|INE387A01021",
    "ESCORTS":     "NSE_EQ|INE042A01014",
    "FORCEMOT":    "NSE_EQ|INE451A01017",
    "AMARAJABAT":  "NSE_EQ|INE885A01032",
    "SONACOMS":    "NSE_EQ|INE073K01018",
    "SWARAJENG":   "NSE_EQ|INE277A01014",
    "CEATLTD":     "NSE_EQ|INE482A01036",
    "TATAMTRDVR":  "NSE_EQ|INE155A01030",
    "OLECTRA":     "NSE_EQ|INE260D01016",

    # ── Energy, Oil & Gas ──
    "RELIANCE":    "NSE_EQ|INE002A01018",
    "ONGC":        "NSE_EQ|INE213A01029",
    "IOC":         "NSE_EQ|INE242A01010",
    "BPCL":        "NSE_EQ|INE029A01011",
    "GAIL":        "NSE_EQ|INE129A01019",
    "NTPC":        "NSE_EQ|INE733E01010",
    "POWERGRID":   "NSE_EQ|INE752E01010",
    "ADANIGREEN":  "NSE_EQ|INE364U01010",
    "ADANIENT":    "NSE_EQ|INE423A01024",
    "ADANIPORTS":  "NSE_EQ|INE742F01042",
    "TATAPOWER":   "NSE_EQ|INE245A01021",
    "NHPC":        "NSE_EQ|INE848E01016",
    "COALINDIA":   "NSE_EQ|INE522F01014",
    "PETRONET":    "NSE_EQ|INE347G01014",
    "HINDPETRO":   "NSE_EQ|INE094A01015",
    "IGL":         "NSE_EQ|INE203G01027",
    "MGL":         "NSE_EQ|INE002S01010",
    "GUJGASLTD":   "NSE_EQ|INE844O01030",
    "SJVN":        "NSE_EQ|INE002L01015",
    "TORNTPOWER":  "NSE_EQ|INE813H01021",
    "CESC":        "NSE_EQ|INE486A01021",
    "JSWENERGY":   "NSE_EQ|INE121E01018",
    "IREDA":       "NSE_EQ|INE202E01016",
    "TATACONSUM":  "NSE_EQ|INE192A01025",
    "GSPL":        "NSE_EQ|INE246F01010",
    "AEGISCHEM":   "NSE_EQ|INE208C01025",

    # ── Metals & Mining ──
    "TATASTEEL":   "NSE_EQ|INE081A01020",
    "JSWSTEEL":    "NSE_EQ|INE019A01038",
    "HINDALCO":    "NSE_EQ|INE038A01020",
    "VEDL":        "NSE_EQ|INE205A01025",
    "SAIL":        "NSE_EQ|INE114A01011",
    "NMDC":        "NSE_EQ|INE584A01023",
    "NATIONALUM":  "NSE_EQ|INE139A01034",
    "JINDALSTEL":  "NSE_EQ|INE220G01021",
    "APLAPOLLO":   "NSE_EQ|INE702C01027",
    "RATNAMANI":   "NSE_EQ|INE703B01027",
    "WELCORP":     "NSE_EQ|INE191B01025",
    "MOIL":        "NSE_EQ|INE490G01020",
    "HINDZINC":    "NSE_EQ|INE267A01025",

    # ── FMCG & Consumer ──
    "ITC":         "NSE_EQ|INE154A01025",
    "HINDUNILVR":  "NSE_EQ|INE030A01027",
    "NESTLEIND":   "NSE_EQ|INE239A01016",
    "BRITANNIA":   "NSE_EQ|INE216A01030",
    "GODREJCP":    "NSE_EQ|INE102D01028",
    "DABUR":       "NSE_EQ|INE016A01026",
    "MARICO":      "NSE_EQ|INE196A01026",
    "COLPAL":      "NSE_EQ|INE259A01022",
    "TATACONSUM":  "NSE_EQ|INE192A01025",
    "EMAMILTD":    "NSE_EQ|INE548C01032",
    "PIDILITIND":  "NSE_EQ|INE318A01026",
    "UNITDSPR":    "NSE_EQ|INE854D01024",
    "VGUARD":      "NSE_EQ|INE951I01027",
    "PGHH":        "NSE_EQ|INE179A01014",
    "BATAINDIA":   "NSE_EQ|INE176A01028",
    "JUBLFOOD":    "NSE_EQ|INE797F01020",
    "DMART":       "NSE_EQ|INE883S01010",
    "TRENT":       "NSE_EQ|INE849A01020",
    "TITAN":       "NSE_EQ|INE280A01028",
    "PAGEIND":     "NSE_EQ|INE761H01022",
    "MANYAVAR":    "NSE_EQ|INE0CGZ01013",
    "RELAXO":      "NSE_EQ|INE131B01039",
    "VBL":         "NSE_EQ|INE200M01013",
    "JYOTHYLAB":   "NSE_EQ|INE668F01031",
    "RADICO":      "NSE_EQ|INE944F01028",
    "UBL":         "NSE_EQ|INE686F01025",

    # ── Infrastructure & Construction ──
    "LT":          "NSE_EQ|INE018A01030",
    "ULTRACEMCO":  "NSE_EQ|INE481G01011",
    "SHREECEM":    "NSE_EQ|INE070A01015",
    "AMBUJACEM":   "NSE_EQ|INE079A01024",
    "ACC":         "NSE_EQ|INE012A01025",
    "DALBHARAT":   "NSE_EQ|INE050A01025",
    "RAMCOCEM":    "NSE_EQ|INE331A01037",
    "JKCEMENT":    "NSE_EQ|INE823G01014",
    "IRCON":       "NSE_EQ|INE962Y01021",
    "NBCC":        "NSE_EQ|INE095N01031",
    "NCC":         "NSE_EQ|INE868B01028",
    "BEL":         "NSE_EQ|INE263A01024",
    "HAL":         "NSE_EQ|INE066F01020",
    "BHARTIARTL":  "NSE_EQ|INE397D01024",
    "GRASIM":      "NSE_EQ|INE047A01021",
    "SIEMENS":     "NSE_EQ|INE003A01024",
    "ABB":         "NSE_EQ|INE117A01022",
    "CUMMINSIND":  "NSE_EQ|INE298A01020",
    "THERMAX":     "NSE_EQ|INE152A01029",
    "HAVELLS":     "NSE_EQ|INE176B01034",
    "VOLTAS":      "NSE_EQ|INE226A01021",
    "BLUESTARLT":  "NSE_EQ|INE472A01039",
    "CROMPTON":    "NSE_EQ|INE299U01018",
    "DIXON":       "NSE_EQ|INE935N01020",
    "KAYNES":      "NSE_EQ|INE918Z01012",
    "AFFLE":       "NSE_EQ|INE00WK01017",

    # ── Telecom & Media ──
    "BHARTIARTL":  "NSE_EQ|INE397D01024",
    "IDEA":        "NSE_EQ|INE669E01016",
    "TATACOMM":    "NSE_EQ|INE151A01013",
    "HATHWAY":     "NSE_EQ|INE982F01036",
    "NAZARA":      "NSE_EQ|INE418L01014",
    "NETWEB":      "NSE_EQ|INE0N1Y01019",
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


_QUOTE_CACHE = {}
_QUOTE_TTL = 300.0  # 5 min — prev close doesn't change intraday


def get_quote(instrument_key):
    """Fetch full quote including previous day's close price."""
    now = time.time()
    hit = _QUOTE_CACHE.get(instrument_key)
    if hit and (now - hit[0]) < _QUOTE_TTL:
        return hit[1]
    result = {"last_price": 0.0, "close_price": 0.0, "open": 0.0, "high": 0.0, "low": 0.0, "volume": 0}
    try:
        data = _get("/v2/market-quote/quotes", {"instrument_key": instrument_key}).get("data", {}) or {}
        for v in data.values():
            if isinstance(v, dict):
                result["last_price"] = float(v.get("last_price") or 0)
                result["close_price"] = float(v.get("close_price") or 0)
                ohlc = v.get("ohlc") or {}
                result["open"] = float(ohlc.get("open") or 0)
                result["high"] = float(ohlc.get("high") or 0)
                result["low"] = float(ohlc.get("low") or 0)
                result["volume"] = int(v.get("volume") or 0)
                break
    except Exception:
        pass
    _QUOTE_CACHE[instrument_key] = (now, result)
    return result


def _leg(opt):
    opt = opt or {}
    md = opt.get("market_data") or {}
    gk = opt.get("option_greeks") or {}

    def num(v, nd=2):
        try:
            return round(float(v), nd)
        except (TypeError, ValueError):
            return 0.0

    oi = num(md.get("oi"))
    prev_oi = num(md.get("prev_oi"))
    return {
        "ltp": num(md.get("ltp")),
        "oi": oi,
        "prev_oi": prev_oi,
        "oi_chg": round(oi - prev_oi, 2),
        "volume": num(md.get("volume")),
        "bid": num(md.get("bid_price")),
        "ask": num(md.get("ask_price")),
        "iv": num(gk.get("iv")),
        "delta": num(gk.get("delta"), 4),
        "gamma": num(gk.get("gamma"), 4),
        "theta": num(gk.get("theta"), 2),
        "vega": num(gk.get("vega"), 2),
        "pop": num(gk.get("pop")),
    }


def _compute_analytics(rows, spot):
    """Put-Call Ratio, Max Pain, OI totals, and ATM strike for a chain."""
    total_call_oi = sum(r["call"]["oi"] for r in rows)
    total_put_oi = sum(r["put"]["oi"] for r in rows)
    pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else 0.0

    # Max pain: the expiry strike (among listed strikes) at which the total
    # intrinsic value payable to option holders is minimized (i.e. writers'
    # pain is least). Uses open interest as the weight.
    strikes = [r["strike"] for r in rows if r.get("strike") is not None]
    max_pain = None
    if strikes:
        best_loss = None
        for expiry_price in strikes:
            loss = 0.0
            for r in rows:
                k = r["strike"]
                if k is None:
                    continue
                loss += r["call"]["oi"] * max(0.0, expiry_price - k)
                loss += r["put"]["oi"] * max(0.0, k - expiry_price)
            if best_loss is None or loss < best_loss:
                best_loss = loss
                max_pain = expiry_price

    atm = min(strikes, key=lambda k: abs(k - spot)) if strikes else None

    return {
        "pcr": pcr,
        "max_pain": max_pain,
        "total_call_oi": round(total_call_oi, 2),
        "total_put_oi": round(total_put_oi, 2),
        "atm": atm,
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

    quote = get_quote(instrument_key)
    prev_close = quote["close_price"] or spot

    analytics = _compute_analytics(rows, spot)
    return {
        "type": "chain", "symbol": symbol, "spot": round(spot, 2),
        "prev_close": round(prev_close, 2),
        "open": quote.get("open", 0), "high": quote.get("high", 0),
        "low": quote.get("low", 0), "volume": quote.get("volume", 0),
        "expiry": expiry, "expiries": expiries, "rows": rows,
        **analytics,
    }


def build_synthetic_chain(symbol, instrument_key):
    quote = get_quote(instrument_key)
    spot = quote["last_price"] or get_ltp(instrument_key)
    prev_close = quote["close_price"] or spot
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
            "call": {"ltp": round(max(0.0, spot - strike), 2), "oi": 0, "prev_oi": 0, "oi_chg": 0,
                     "volume": 0, "bid": 0, "ask": 0, "iv": 0, "delta": 0, "gamma": 0, "theta": 0, "vega": 0, "pop": 0},
            "put": {"ltp": round(max(0.0, strike - spot), 2), "oi": 0, "prev_oi": 0, "oi_chg": 0,
                    "volume": 0, "bid": 0, "ask": 0, "iv": 0, "delta": 0, "gamma": 0, "theta": 0, "vega": 0, "pop": 0},
        })

    return {
        "type": "chain", "symbol": symbol, "spot": round(spot, 2),
        "prev_close": round(prev_close, 2),
        "open": quote.get("open", 0), "high": quote.get("high", 0),
        "low": quote.get("low", 0), "volume": quote.get("volume", 0),
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
    candles = [[c[0], c[1], c[2], c[3], c[4], c[5] if len(c) > 5 else 0] for c in reversed(candles)]
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

