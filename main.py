# main.py
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Any, Dict
import json
from openpyxl import load_workbook
from io import BytesIO
from datetime import datetime

app = FastAPI(  # docs accessibles par défaut: /docs et /redoc
    title="IA Parse Excel",
    version="1.0.0"
)

# CORS: autorise Bubble (élargis si besoin)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Endpoints de santé/diagnostic ------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "message": "Service is running", "docs": "/docs"}

@app.get("/ping")
def ping():
    return {"ping": "pong"}

@app.get("/healthz")
def healthz():
    return {"ok": True}

# --- Helper ------------------------------------------------------------------
def _parse_rules(rules_raw: Optional[str]) -> Dict[str, Any]:
    if not rules_raw:
        # Valeurs par défaut si rien n’est fourni depuis Bubble
        return {
            "full_day_threshold": 6,
            "half_day_min": 0.01,
            "half_day_max": 5.99,
        }
    try:
        return json.loads(rules_raw)
    except Exception:
        # Si Bubble envoie du texte non-JSON, on renvoie quand même des defaults
        return {
            "full_day_threshold": 6,
            "half_day_min": 0.01,
            "half_day_max": 5.99,
        }

def _parse_holidays(holiday_raw: Optional[str]) -> List[str]:
    if not holiday_raw:
        return []
    try:
        data = json.loads(holiday_raw)
        if isinstance(data, list):
            return [str(x) for x in data]
        return []
    except Exception:
        return []

def _coerce_number(x):
    if x is None:
        return None
    try:
        # Excel peut renvoyer int/float/str
        return float(x)
    except Exception:
        return None

def _coerce_bool(x):
    # On accepte True/False, "yes"/"no", "1"/"0"
    if isinstance(x, bool):
        return x
    if x is None:
        return None
    s = str(x).strip().lower()
    if s in ("yes", "true", "1"):
        return True
    if s in ("no", "false", "0"):
        return False
    return None

def _coerce_date(x):
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.date().isoformat()
    # Tentatives de parsing
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(x), fmt).date().isoformat()
        except Exception:
            pass
    # Dernière chance : string brute
    return str(x)

# --- Endpoint principal ------------------------------------------------------
@app.post("/parse-excel-upload")
async def parse_excel_upload(
    file: UploadFile = File(...),
    holiday_dates: Optional[str] = Form(default=None),  # ex: '["2025-01-01","2025-05-01"]'
    rules: Optional[str] = Form(default=None)          # ex: '{"full_day_threshold":6,...}'
):
    # Sécurité de base sur le mimetype/extension
    filename = (file.filename or "").lower()
    if not (filename.endswith(".xlsx") or filename.endswith(".xlsm")):
        raise HTTPException(status_code=400, detail="Only .xlsx/.xlsm are supported")

    # Charge le classeur en mémoire
    try:
        content = await file.read()
        wb = load_workbook(filename=BytesIO(content), data_only=True)
        ws = wb.active  # première feuille
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read Excel: {e}")

    # Paramètres optionnels
    rules_dict = _parse_rules(rules)
    holidays = _parse_holidays(holiday_dates)

    # On s’attend à une ligne d’en-têtes en 1ère ligne
    headers = []
    for cell in ws[1]:
        headers.append((cell.value or "").strip() if isinstance(cell.value, str) else str(cell.value or ""))

    # On construit les rows en essayant de mapper les noms que tu utilises dans Bubble :
    # matricule, nom, prenom, cin, date, heures_travaillees_decimal, hs_normales, hs_ferie, demi_journee, raw_body_text
    rows = []
    for r in ws.iter_rows(min_row=2, values_only=True):
        row = dict(zip(headers, r))

        # essaie de lire avec plusieurs libellés possibles
        def g(*keys):
            for k in keys:
                if k in row and row[k] is not None:
                    return row[k]
            return None

        item = {
            "matricule": g("matricule", "Matricule", "MATRICULE"),
            "nom": g("nom", "Nom", "NOM"),
            "prenom": g("prenom", "Prénom", "Prenom", "PRENOM"),
            "cin": g("CIN", "cin", "Cin"),
            "date": _coerce_date(g("date", "Date", "DATE")),
            "heures_travaillees_decimal": _coerce_number(g("heures_travaillees_decimal", "heures_travaillees", "Heures_travaillees")),
            "hs_normales": _coerce_number(g("hs_normales", "HS_normales")),
            "hs_ferie": _coerce_number(g("hs_ferie", "HS_ferie")),
            "demi_journee": _coerce_bool(g("demi_journee", "Demi_journee", "demi_journee?")),
            "raw_body_text": " | ".join([str(x) for x in r if x is not None])[:1000],
        }

        rows.append(item)

    # Réponse que Bubble consomme comme “Result of step 3’s body rows”
    return {
        "rules_used": rules_dict,
        "holiday_dates": holidays,
        "rows": rows,
        "rows_count": len(rows),
    }
