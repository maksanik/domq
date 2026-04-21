from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI

from api.routers import listings, h3_stats, predict
from config import DATABASE_DSN
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: создаём пул соединений с PostgreSQL
    app.state.pool = await asyncpg.create_pool(DATABASE_DSN, min_size=2, max_size=20)
    yield
    # Shutdown: закрываем пул
    await app.state.pool.close()


app = FastAPI(
    title="DomQ — Аналитика недвижимости Москвы",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(listings.router)
app.include_router(h3_stats.router)
app.include_router(predict.router)


@app.get("/health")
async def health():
    return {"status": "ok"}


class NoCacheStaticMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        path = request.url.path
        if path.endswith((".js", ".html", ".css")):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
        return response


app.add_middleware(NoCacheStaticMiddleware)
app.mount("/", StaticFiles(directory="frontend/static", html=True), name="static")
