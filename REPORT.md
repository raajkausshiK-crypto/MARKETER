# Project Report — Real-Time Options Screener (Upstox)

**Date:** 2026-07-03
**Repository:** https://github.com/fluffywebtech-oss/market-options-screener
**Local path:** `/Users/prashantyadav/market`

---

## 1. Objective

Build a Real-Time Options Screener for Indian stocks and indices (NIFTY, CIPLA,
etc.) that displays a live option chain — Call/Put LTP, OI, IV, and moneyness
(ITM/OTM) — driven by live data from the Upstox API, and deploy it publicly on
Vercel.

---

## 2. What Was Built

### 2.1 Frontend — `index.html`
- Single self-contained HTML file styled with Tailwind CSS (CDN).
- Header with underlying symbol + live spot price (flashes green up / red down).
- Controls: symbol search, custom strike input, reset, expiry dropdown,
  connect/disconnect button, connection status indicator.
- Options chain table (9 columns): Call OI / IV / LTP · **STRIKE** · Put LTP /
  IV / OI · Status, with ITM row highlighting (green calls, red puts).
- **Dual data source (auto-detected by hostname):**
  - **Local** (`localhost`) → connects to the Python WebSocket bridge.
  - **Deployed** (any real domain) → polls the `/api/chain` HTTPS endpoint
    every 1.5s.
- Local random-walk simulator retained as a fallback when no backend is reachable.

### 2.2 Local backend — `upstox_bridge.py`
- Python WebSocket server on `ws://localhost:8765` (`asyncio` + `websockets`).
- Uses the official `upstox-python-sdk`.
- Resolves ANY NSE stock/index via the instrument-search API (not a fixed list).
- Returns real option chains for F&O symbols; for cash-only stocks it fetches
  live LTP and builds a synthetic intrinsic-value grid.
- Produces a **uniform strike grid** centered on ATM (consistent spacing).
- Falls back to a fully simulated chain when no token/SDK is present.

### 2.3 Cloud backend — `api/chain.py` (Vercel serverless)
- Python serverless function: `GET /api/chain?symbol=NIFTY[&expiry=YYYY-MM-DD]`.
- **Standard library only** (`urllib`) — calls Upstox REST directly, no SDK to
  bundle. Sends a browser User-Agent to pass Upstox's Cloudflare layer.
- Mirrors the bridge logic: dynamic symbol resolution, real option chains,
  uniform strike grid, and synthetic chains for cash-only stocks.
- Reads the token from the `UPSTOX_ACCESS_TOKEN` environment variable.

### 2.4 Screener + resilience upgrades (later iterations)
Subsequent commits evolved the app from a single chain view into a full,
rate-limit-resilient screener:

- **Grid screener UI** — `index.html` now renders a grid of live chain panes
  (quad-style) so multiple stocks' full chains are visible at once, with
  internal scroll, ATM-centered rows, and a sticky header.
- **Full option chain** — returns all listed strikes per expiry (not just a
  ±10 window), with dead/zero strikes trimmed.
- **Caching** — `api/chain.py` caches chains for ~8s (`_CACHE_TTL`) and serves
  last-known-good data for up to 90s (`_GOOD_TTL`) on failure, so a stock never
  flips to "Error" or a wall of zeros from a single rate-limited request.
  Metadata (expiries/instrument keys) cached ~30 min (`_META_TTL`).
- **Rate-limit handling** — hard-throttled polling, gentler background refresh,
  and adaptive backoff on HTTP 429 so the app self-heals.
- **Hardcoded F&O instrument keys** — ~26 common symbols (indices + large-cap
  equities) are hardcoded to skip the aggressively rate-limited
  `/instruments/search` endpoint entirely.
- **Better errors** — surfaces the real upstream Upstox error instead of a
  generic 502.

### 2.5 Supporting files
- `vercel.json` — `{ "cleanUrls": true }`.
- `README.md`, `.gitignore`.
- `options_screener.html` — the original standalone prototype (kept for reference).

---

## 3. Architecture

| Environment       | Data path                                                        |
|-------------------|------------------------------------------------------------------|
| Deployed (Vercel) | Browser → `/api/chain` (Python serverless) → Upstox REST API     |
| Local development | Browser → `ws://localhost:8765` (`upstox_bridge.py`) → Upstox    |

Data contract (both paths): `{ type, symbol, spot, expiry, expiries[], rows[] }`
where each row is `{ strike, call:{ltp,oi,iv,bid,ask}, put:{...} }`.

---

## 4. Verification (local testing)

All confirmed working against the live Upstox API from the local machine:

| Symbol      | Type       | Result                                   |
|-------------|------------|------------------------------------------|
| NIFTY       | F&O        | Real chain, spot 24005.85, uniform 50-pt |
| RELIANCE    | F&O        | Real chain, spot 1308.00, uniform 10-pt  |
| ZOMATO      | F&O        | Real chain, spot 279.70                  |
| DMART/TRENT | F&O        | Real chain                               |
| ITC / WIPRO | F&O        | Uniform 2.5-pt grid                      |
| IRCTC       | Cash-only  | Spot 502.70 + intrinsic grid             |
| RANDOMXYZ   | Invalid    | Clear "not found" error                  |

---

## 5. Issues Encountered & Resolutions

| # | Issue | Resolution |
|---|-------|------------|
| 1 | Frontend showed only computed intrinsic values, not real premiums | Switched backend to the Upstox option-chain API (real Call/Put LTP/OI/IV) |
| 2 | Only 9 hardcoded symbols searchable | Dynamic resolution via `search_instrument`; validated F&O availability |
| 3 | NSE ticker renames (e.g. TATAMOTORS → TMPV/TMCV) | Pick the F&O-enabled underlying; skip optionless duplicates |
| 4 | Uneven strike spacing on far expiries | Detect standard step, walk outward from ATM on an exact grid |
| 5 | Non-F&O stocks not searchable | Added synthetic chain from live LTP (intrinsic values) |
| 6 | Vercel site couldn't reach `ws://localhost` (mixed-content / no cloud bridge) | Added `/api/chain` serverless function; frontend auto-detects remote vs local |
| 7 | Upstox behind Cloudflare returned 403 (Error 1010) | Added a browser User-Agent header |
| 8 | Vercel "No python entrypoint found" | Removed root `requirements.txt`; used classic `handler` (BaseHTTPRequestHandler) |
| 9 | Push blocked on `prashantyyadav/market` (read-only) | Created new repo `fluffywebtech-oss/market-options-screener` |

---

## 6. Outstanding Items / Blockers

1. **`UPSTOX_ACCESS_TOKEN` not set on Vercel** — the deployed function reports
   the token is missing. Must be added under Project → Settings → Environment
   Variables (Production) and then **redeployed** (env vars apply only to new
   deployments). A diagnostic (`on_vercel=yes/no`) was added to pinpoint this.
2. **Upstox static-IP allowlist (`UDAPI1221`)** — the account restricts API
   access to a fixed IP. Vercel uses dynamic IPs, so this allowlist **must be
   disabled** or the cloud function will get 403s.
3. **Daily token expiry** — Upstox access tokens expire daily. The env var must
   be refreshed (and redeployed) each day, or an OAuth refresh flow added.
4. **Deployment target** — the old Vercel project `market-eta-puce` is linked to
   `prashantyyadav/market` (which has no `/api/chain`). A new Vercel project must
   be created from `fluffywebtech-oss/market-options-screener`.
5. **Market hours** — prices tick live only during NSE hours (~09:15–15:30 IST);
   otherwise the last traded price is shown.

---

## 7. Recommended Next Steps

1. Deploy the new repo as a fresh Vercel project (Framework Preset: **Other**).
2. Set `UPSTOX_ACCESS_TOKEN` (Production) and redeploy.
3. Disable the Upstox static-IP allowlist.
4. Verify `https://<app>.vercel.app/api/chain?symbol=NIFTY` returns JSON.
5. (Optional) Add a token-refresh mechanism so the site stays live without daily
   manual updates.

---

## 8. Commit History

```
023d26e Add env diagnostic and tolerate quoted token value
d3c316d Fix Vercel entrypoint: remove root requirements.txt, use classic handler serverless function
d513c7e Use WSGI app entrypoint for Vercel Python runtime
2410473 Add Vercel serverless API for live option chain; auto-detect remote vs local data source
```
