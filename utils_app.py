from typing import List
import logging
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


def _configure_logging() -> logging.Logger:
    """
    Configure un logger idempotent (pas de doublons sous Uvicorn/Gunicorn).
    Retourne le logger applicatif.
    """
    logger = logging.getLogger("ia-parse-excel")

    # Si aucun handler n'est présent, on configure (évite les logs dupliqués).
    if not logging.getLogger().handlers and not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )
    logger.setLevel(logging.INFO)

    # Aligne (optionnel) le logger d'accès Uvicorn si présent
    try:
        uvicorn_access = logging.getLogger("uvicorn.access")
        if uvicorn_access and not uvicorn_access.handlers:
            uvicorn_access.setLevel(logging.INFO)
    except Exception:
        pass

    return logger


def _get_cors_origins() -> List[str]:
    """
    Lit CORS_ORIGINS depuis l'env (séparées par des virgules).
    Retourne ["*"] si non défini (comportement actuel conservé).
    """
    raw = os.getenv("CORS_ORIGINS", "").strip()
    if not raw:
        return ["*"]
    return [o.strip() for o in raw.split(",") if o.strip()]


def create_app() -> FastAPI:
    """
    Crée et configure l'application FastAPI principale.
    """
    app = FastAPI(title="IA Parse Excel", version="1.0.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=_get_cors_origins(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/")
    async def root():
        return {"status": "ok", "message": "Service is running", "docs": "/docs"}

    @app.get("/ping")
    async def ping():
        return {"ping": "pong"}

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    return app

