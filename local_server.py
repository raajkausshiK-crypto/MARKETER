"""
local_server.py
===============
All-in-one LOCAL dev server for the screener: serves the static frontend AND
the /api/chain backend on the same port, so localhost stops depending on the
remote (Vercel) API.

Run:
    export UPSTOX_ACCESS_TOKEN="your_daily_token"
    python3 local_server.py            # serves on http://localhost:8080

The /api/chain route reuses the exact same logic as the deployed function
(imported from api/chain.py), so previous-close / OHLC / volume behave locally
identically to production.
"""
import json
import os
import sys
import urllib.parse
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

SETUP_PAGE = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Screener setup</title>
<style>
 body{background:#0d0d12;color:#e8e8f0;font-family:-apple-system,Segoe UI,Roboto,sans-serif;
      display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}
 .card{background:#16161f;border:1px solid #2a2a38;border-radius:16px;padding:32px;max-width:520px;width:90%}
 h1{font-size:20px;margin:0 0 6px} p{color:#9898a8;font-size:14px;line-height:1.5;margin:0 0 18px}
 textarea{width:100%;box-sizing:border-box;height:120px;background:#0d0d12;color:#7dd3fc;border:1px solid #2a2a38;
      border-radius:10px;padding:12px;font-family:ui-monospace,monospace;font-size:12px;resize:vertical}
 button{margin-top:14px;width:100%;padding:12px;border:0;border-radius:10px;background:#22d3ee;color:#003;
      font-weight:700;font-size:15px;cursor:pointer}
 .msg{margin-top:14px;font-size:14px;min-height:20px}
 .ok{color:#4ade80} .err{color:#f87171}
</style></head><body>
<div class=card>
 <h1>Paste your Upstox access token</h1>
 <p>This stays on your machine (this local server only). After saving, your screener at
    <b>/?api=local</b> will show the real previous-close and change %.</p>
 <textarea id=t placeholder="eyJ0eXAiOiJKV1Qi..."></textarea>
 <button onclick="save()">Save token &amp; start</button>
 <div class=msg id=m></div>
</div>
<script>
async function save(){
  const t=document.getElementById('t').value.trim(); const m=document.getElementById('m');
  if(!t){m.className='msg err';m.textContent='Paste a token first.';return;}
  m.className='msg';m.textContent='Checking with Upstox…';
  const r=await fetch('/api/token',{method:'POST',body:t});
  const j=await r.json();
  if(j.ok){m.className='msg ok';m.textContent='✓ Token works — spot '+j.spot+'. Opening screener…';
    setTimeout(()=>location.href='/?api=local',900);}
  else{m.className='msg err';m.textContent='✗ '+(j.message||'Token rejected. Get a fresh one and retry.');}
}
</script></body></html>"""

ROOT = os.path.dirname(os.path.abspath(__file__))

# Token: prefer the environment; otherwise read a local, gitignored file so the
# server can be started by tooling that can't set env vars. The user writes the
# token into this file themselves — it is never committed.
_TOKEN_FILE = os.path.join(ROOT, ".upstox_token")
if not os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip() and os.path.exists(_TOKEN_FILE):
    with open(_TOKEN_FILE) as _f:
        os.environ["UPSTOX_ACCESS_TOKEN"] = _f.read().strip()

sys.path.insert(0, os.path.join(ROOT, "api"))
import chain as backend  # noqa: E402  (the deployed serverless module)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=ROOT, **kw)

    def _send_json(self, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html):
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/setup":
            self._send_html(SETUP_PAGE)
            return
        if self.path.startswith("/api/chain"):
            params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            symbol = (params.get("symbol", ["NIFTY"])[0] or "NIFTY").upper().strip()
            expiry = params.get("expiry", [None])[0] or None
            want_candles = bool(params.get("candles"))
            try:
                payload = (backend.get_candles_cached(symbol) if want_candles
                           else backend.get_chain_cached(symbol, expiry))
            except backend.UpstoxError as e:
                payload = {"type": "error", "message": e.message, "status": e.status}
            except Exception as e:  # pragma: no cover
                payload = {"type": "error", "message": f"Server error: {e}"}
            self._send_json(payload)
            return
        super().do_GET()  # static files

    def do_POST(self):
        if urllib.parse.urlparse(self.path).path == "/api/token":
            length = int(self.headers.get("Content-Length", "0") or "0")
            token = self.rfile.read(length).decode("utf-8").strip()
            if not token:
                self._send_json({"ok": False, "message": "Empty token."})
                return
            # Store the user-supplied token for this process only, then verify it
            # with a live quote so we can report success/failure immediately.
            os.environ["UPSTOX_ACCESS_TOKEN"] = token
            backend._QUOTE_CACHE.clear()
            try:
                data = backend.get_chain_cached("NIFTY", None)
                if data.get("type") == "error":
                    self._send_json({"ok": False, "message": data.get("message", "rejected")})
                else:
                    # Persist so restarts keep working (gitignored, local only).
                    try:
                        with open(_TOKEN_FILE, "w") as f:
                            f.write(token)
                    except OSError:
                        pass
                    self._send_json({"ok": True, "spot": data.get("spot")})
            except backend.UpstoxError as e:
                self._send_json({"ok": False, "message": e.message})
            except Exception as e:
                self._send_json({"ok": False, "message": str(e)})
            return
        self.send_response(404)
        self.end_headers()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    if not os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip():
        print("!! UPSTOX_ACCESS_TOKEN is not set — /api/chain will return an "
              "auth error and prices will not load. Set it and restart.")
    print(f">> Local screener (static + API) on http://localhost:{port}")
    ThreadingHTTPServer(("", port), Handler).serve_forever()
