from typing import List
import logging
import os
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
            # Laisse Uvicorn gérer ses handlers si déjà configuré
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
