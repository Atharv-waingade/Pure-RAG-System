import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response as StarletteResponse

# Import your ERP AI Agent logic
from app.routes import chat

# Import the engine's startup/shutdown hooks and security config
from app.engine import db, SECURITY_HEADERS, WARMUP_MODULES, log, _startup_ready
import asyncio

# ── Lifespan: warm up all 17 modules on startup, clean up on shutdown ─────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _startup_ready
    import app.engine as _eng
    _eng._startup_ready = asyncio.Event()

    await db.open()
    log.info("HTTP client opened")

    async def _warmup():
        log.info("Warmup: pre-fetching all %d modules...", len(WARMUP_MODULES))
        results = await db.sync_modules(WARMUP_MODULES)
        ok_count = sum(1 for v in results.values() if v in ("ok", "disk"))
        log.info("Warmup complete: %d/%d modules ready", ok_count, len(WARMUP_MODULES))
        _eng._startup_ready.set()

    asyncio.create_task(_warmup())
    yield
    await db.close()
    log.info("HTTP client closed")


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Umbrella AI", lifespan=lifespan)


# ── Security headers middleware ───────────────────────────────────────────────
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest,
                       call_next) -> StarletteResponse:
        import secrets
        req_id   = request.headers.get("X-Request-ID") or secrets.token_hex(8)
        response = await call_next(request)
        for k, v in SECURITY_HEADERS.items():
            response.headers[k] = v
        response.headers["X-Request-ID"] = req_id
        return response

app.add_middleware(SecurityHeadersMiddleware)


# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Connect the AI Agent route ────────────────────────────────────────────────
app.include_router(chat.router)


# ── Static frontend (bulletproof) ─────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(BASE_DIR, "static")

if os.path.exists(STATIC_DIR):
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
else:
    print(f"⚠️ WARNING: No 'static' folder found at {STATIC_DIR}. "
          f"API is running, but frontend is not mounted.")