from fastapi import FastAPI, Body, Request, Depends, Header, HTTPException ,status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from typing import Any, Dict, List, AsyncGenerator, Optional
# from ollama import Client
from pathlib import Path
from collections import defaultdict
from threading import Lock
import os, json, time, httpx
from dotenv import load_dotenv
import time, logging
import asyncio
import threading
import anyio
from functools import lru_cache
from ollama import Client as OllamaClient


logger = logging.getLogger("uvicorn.access")
BASE_DIR = Path(__file__).parent
ROOT_DIR = BASE_DIR.parent

# Optioneel: kies expliciet een env-file (vb. via Start-NotificaAIChat.ps1)
explicit = os.getenv("ENV_FILE")

candidates = [
    Path(explicit) if explicit else None,
    BASE_DIR / ".env.local",
    BASE_DIR / ".env",
    ROOT_DIR / ".env.local",
    ROOT_DIR / ".env",
]
dotenv_file = next((p for p in candidates if p and p.exists()), None)

# In Docker/prod zijn env-vars leidend; overschrijf die niet
if dotenv_file and not os.getenv("DISABLE_DOTENV"):
    load_dotenv(dotenv_path=dotenv_file, override=False)
    print(f"[dotenv] loaded: {dotenv_file}")



# === Vars met nette fallbacks/cleanup ===
OLLAMA_HOST   = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434").strip()
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "mistral:instruct").strip()

# Support zowel API_AUTH_TOKEN (chat) als AUTH_TOKEN (oude naam in root .env)
_api = os.getenv("API_AUTH_TOKEN", "").strip()
_alt = os.getenv("AUTH_TOKEN", "").strip()
API_AUTH_TOKEN = _api or _alt

raw_cors = os.getenv("CORS_ORIGINS", "*").strip()
CORS_ORIGINS = ["*"] if raw_cors == "*" else [x.strip() for x in raw_cors.split(",") if x.strip()]
CORS_ALLOW_CREDENTIALS = os.getenv("CORS_ALLOW_CREDENTIALS", "false").lower() == "true"

# Config
CATALOG_BASE_URL = os.getenv("CATALOG_BASE_URL", "http://catalog-api:7000")

print(f"[auth] token active? {'yes' if API_AUTH_TOKEN else 'no'}")

# In-memory sessies (server-side)
_sessions: Dict[str, List[dict]] = defaultdict(list)
_sessions_lock = Lock()

app = FastAPI(title="Local Mistral via Ollama (Proxy)")


# CORS (beperk origins in productie)
origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
allow_credentials = os.getenv("CORS_ALLOW_CREDENTIALS", "true").lower() == "true"

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


def require_auth(authorization: Optional[str] = Header(default=None)) -> None:
    if not API_AUTH_TOKEN:
        return
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing Bearer token")
    if authorization[7:].strip() != API_AUTH_TOKEN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token")

def _normalize_options(options: dict) -> dict:
    """Zorg dat types kloppen voor Ollama (bv. num_ctx als int)."""
    if not isinstance(options, dict):
        return {}
    out = {}
    if "num_ctx" in options:
        try:
            out["num_ctx"] = int(options["num_ctx"])
        except Exception:
            pass
    # optioneel: deze gewoon doorzetten als aanwezig
    for k in ("temperature", "top_p", "seed", "num_predict", "stop"):
        if k in options:
            out[k] = options[k]
    return out

def _validate_messages(messages):
    if not isinstance(messages, list) or not messages:
        raise HTTPException(status_code=400, detail="`messages` must be a non-empty list")
    for m in messages:
        if not isinstance(m, dict) or "role" not in m or "content" not in m:
            raise HTTPException(status_code=400, detail="Each message must be {role, content}")
        if m["role"] not in ("system", "user", "assistant"):
            raise HTTPException(status_code=400, detail="role must be one of system|user|assistant")

@lru_cache(maxsize=1)
def get_ollama() -> OllamaClient:
    if not OLLAMA_HOST.startswith("http"):
        raise RuntimeError(f"Invalid OLLAMA_HOST: {OLLAMA_HOST!r}")
    return OllamaClient(host=OLLAMA_HOST)

# --- API routes ---
@app.get("/api/health")
def health():
    return {"status": "ok", "ollama_host": OLLAMA_HOST}

@app.get("/api/models")
def list_models(_: None = Depends(require_auth)):
    try:
        return get_ollama().list()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

def _merge_with_server_session(
    incoming_msgs: List[Dict[str, str]],
    session_id: str,
    reset_session: bool,
) -> List[Dict[str, str]]:
    """Neemt (optionele) system + laatste user uit incoming en combineert met server-historie."""
    with _sessions_lock:
        if reset_session:
            _sessions[session_id] = []
        hist = _sessions[session_id]

        # Neem system messages mee als ze nog niet in hist staan
        for m in incoming_msgs:
            if m.get("role") == "system" and m not in hist:
                hist.append(m)

        # Neem de laatste user message uit incoming mee
        last_user = None
        for m in reversed(incoming_msgs):
            if m.get("role") == "user":
                last_user = m
                break
        if last_user:
            hist.append(last_user)

        # Geef een kopie terug (huidige context)
        return list(hist)

def _append_assistant_to_session(session_id: str, text: str):
    with _sessions_lock:
        _sessions[session_id].append({"role": "assistant", "content": text})

# ---------- NON-STREAM ----------
@app.post("/api/chat")
def chat(
    body: Dict[str, Any] = Body(...),
    _: None = Depends(require_auth),
):
    model          = body.get("model") or DEFAULT_MODEL
    messages       = body.get("messages") or []
    options        = _normalize_options(body.get("options") or {})
    session_id     = body.get("session_id") or ""
    server_session = bool(body.get("server_session"))
    reset_session  = bool(body.get("reset_session"))

    # input check
    _validate_messages(messages)

    try:
        if server_session and session_id:
            eff_messages = _merge_with_server_session(messages, session_id, reset_session)
        else:
            eff_messages = messages

        resp = get_ollama().chat(model=model, messages=eff_messages, options=options)
        assistant_text = ((resp or {}).get("message") or {}).get("content", "")
        if server_session and session_id and assistant_text:
            _append_assistant_to_session(session_id, assistant_text)

        # (optioneel) meta info meegeven zonder UI te breken
        resp.setdefault("meta", {})
        resp["meta"].update({
            "server_session": server_session,
            "session_id": session_id or None,
            "options_used": options
        })
        return resp
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ---------- STREAM (SSE) ----------
@app.post("/api/chat/stream")
def chat_stream(
    body: Dict[str, Any] = Body(...),
    _: None = Depends(require_auth),
):
    model          = body.get("model") or DEFAULT_MODEL
    messages       = body.get("messages") or []
    options        = _normalize_options(body.get("options") or {})
    session_id     = body.get("session_id") or ""
    server_session = bool(body.get("server_session"))
    reset_session  = bool(body.get("reset_session"))

    # input check
    _validate_messages(messages)

    # maak effectieve messages
    if server_session and session_id:
        eff_messages = _merge_with_server_session(messages, session_id, reset_session)
    else:
        eff_messages = messages

    # serializer één keer definiëren
    def to_jsonable(o):
        if o is None or isinstance(o, (str, int, float, bool)):
            return o
        if isinstance(o, dict):
            return {k: to_jsonable(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)):
            return [to_jsonable(x) for x in o]
        d = getattr(o, "__dict__", None)
        return to_jsonable(d) if d is not None else str(o)

    async def sse() -> AsyncGenerator[bytes, None]:
        """
        Async generator die chunks via een queue ontvangt van een achtergrond-thread.
        """
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        acc_parts: List[str] = []

        def worker():
            # Draait in thread → blocking call hier is oké
            try:
                for chunk in get_ollama().chat(model=model, messages=eff_messages, options=options, stream=True):
                    # serialiseer in de thread (minder werk op event loop)
                    msg = (chunk.get("message") or {})
                    delta = msg.get("content") or ""
                    json_str = json.dumps(to_jsonable(chunk), ensure_ascii=False)

                    # stuur als "data" item
                    loop.call_soon_threadsafe(queue.put_nowait, {"t": "data", "json": json_str, "delta": delta})
            except Exception as e:
                err = json.dumps({"error": str(e)}, ensure_ascii=False)
                loop.call_soon_threadsafe(queue.put_nowait, {"t": "error", "json": err})
            finally:
                loop.call_soon_threadsafe(queue.put_nowait, {"t": "end"})

        t = threading.Thread(target=worker, daemon=True)
        t.start()

        try:
            while True:
                item = await queue.get()
                tpe = item.get("t")
                if tpe == "data":
                    delta = item.get("delta") or ""
                    if delta:
                        acc_parts.append(delta)
                    yield f"data: {item['json']}\n\n".encode("utf-8")
                elif tpe == "error":
                    yield f"event: error\ndata: {item['json']}\n\n".encode("utf-8")
                elif tpe == "end":
                    break
        finally:
            # einde -> schrijf assistent-bericht naar sessie
            if server_session and session_id and acc_parts:
                _append_assistant_to_session(session_id, "".join(acc_parts))

    return StreamingResponse(
        sse(),
        media_type="text/event-stream; charset=utf-8",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            # handig achter nginx om buffering uit te zetten:
            "X-Accel-Buffering": "no",
        },
    )

# ---------- STREAM (SSE) via AnyIO ----------
@app.post("/api/chat/stream_async")
def chat_stream_async(
    body: Dict[str, Any] = Body(...),
    _: None = Depends(require_auth),
):
    model          = body.get("model") or DEFAULT_MODEL
    messages       = body.get("messages") or []
    options        = _normalize_options(body.get("options") or {})
    session_id     = body.get("session_id") or ""
    server_session = bool(body.get("server_session"))
    reset_session  = bool(body.get("reset_session"))

    # input check
    _validate_messages(messages)

    # effectieve messages
    if server_session and session_id:
        eff_messages = _merge_with_server_session(messages, session_id, reset_session)
    else:
        eff_messages = messages

    def to_jsonable(o):
        if o is None or isinstance(o, (str, int, float, bool)):
            return o
        if isinstance(o, dict):
            return {k: to_jsonable(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)):
            return [to_jsonable(x) for x in o]
        d = getattr(o, "__dict__", None)
        return to_jsonable(d) if d is not None else str(o)

    async def sse() -> AsyncGenerator[bytes, None]:
        """
        Async generator: ontvangt chunks via asyncio.Queue die gevuld wordt
        door een worker die in een AnyIO thread draait.
        """
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        acc_parts: List[str] = []

        def worker(loop_: asyncio.AbstractEventLoop, q: asyncio.Queue):
            try:
                for chunk in get_ollama().chat(model=model, messages=eff_messages, options=options, stream=True):
                    msg = (chunk.get("message") or {})
                    delta = msg.get("content") or ""
                    json_str = json.dumps(to_jsonable(chunk), ensure_ascii=False)
                    # thread-safe enqueue
                    loop_.call_soon_threadsafe(q.put_nowait, {"t": "data", "json": json_str, "delta": delta})
            except Exception as e:
                err = json.dumps({"error": str(e)}, ensure_ascii=False)
                loop_.call_soon_threadsafe(q.put_nowait, {"t": "error", "json": err})
            finally:
                loop_.call_soon_threadsafe(q.put_nowait, {"t": "end"})

        async def run_worker():
            # start blocking werk in thread; keert pas terug als klaar
            await anyio.to_thread.run_sync(worker, loop, queue, cancellable=True)

        # run de worker parallel aan onze consume-loop
        worker_task = asyncio.create_task(run_worker())

        try:
            while True:
                item = await queue.get()
                tpe = item.get("t")
                if tpe == "data":
                    delta = item.get("delta") or ""
                    if delta:
                        acc_parts.append(delta)
                    yield f"data: {item['json']}\n\n".encode("utf-8")
                elif tpe == "error":
                    yield f"event: error\ndata: {item['json']}\n\n".encode("utf-8")
                elif tpe == "end":
                    break
        finally:
            # netjes afsluiten / annuleren
            if not worker_task.done():
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
            # server-sessie bijwerken
            if server_session and session_id and acc_parts:
                _append_assistant_to_session(session_id, "".join(acc_parts))

    return StreamingResponse(
        sse(),
        media_type="text/event-stream; charset=utf-8",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # voorkom buffering achter proxy
        },
    )

@app.post("/api/session/reset")
def session_reset(body: Dict[str, Any] = Body(...), _: None = Depends(require_auth)):
    sid = (body or {}).get("session_id")
    if not sid:
        return JSONResponse(status_code=400, content={"error": "session_id required"})
    with _sessions_lock:
        _sessions.pop(sid, None)
    return {"ok": True, "session_id": sid}

@app.get("/api/whereami")
def whereami(request: Request, _: None = Depends(require_auth)):
    return {
        "api_origin_hint": str(request.base_url).rstrip("/"),
        "client_host": request.client.host,
        "ollama_host": OLLAMA_HOST,
        "default_model": DEFAULT_MODEL,
    }


@app.get("/api/catalog/search")
async def proxy_catalog_search(q: str, _: None = Depends(require_auth)):
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{CATALOG_BASE_URL}/api/catalog/search", params={"q": q})
        return JSONResponse(status_code=r.status_code, content=r.json() if r.headers.get("content-type","").startswith("application/json") else {"raw": r.text})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# --- Static & root met absolute paden ---
BASE_DIR = Path(__file__).resolve().parent
# Absolute pad naar ./static naast app.py
STATIC_DIR = (Path(__file__).parent / "static").resolve()

# Mount alle assets onder /static
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Root serve: / -> index.html
@app.get("/", include_in_schema=False)
def root():
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        return PlainTextResponse("static/index.html not found", status_code=404)
    return FileResponse(str(index_file))

def _to_jsonable(o):
    if o is None or isinstance(o, (str, int, float, bool)):
        return o
    if isinstance(o, dict):
        return {k: _to_jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_to_jsonable(x) for x in o]
    d = getattr(o, "__dict__", None)
    return _to_jsonable(d) if d is not None else str(o)

def _normalize_options(opts: Dict[str, Any]) -> Dict[str, Any]:
    # voorbeeld: nummerieke strings → int
    out = {}
    for k, v in opts.items():
        if isinstance(v, str) and v.isdigit():
            out[k] = int(v)
        else:
            out[k] = v
    return out