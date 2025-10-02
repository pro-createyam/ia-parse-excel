# main.py
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Any, Dict
from pydantic import BaseModel
from uuid import uuid4
from openpyxl import load_workbook
from io import BytesIO
from datetime import datetime
import unicodedata
import json

# ---------------------------------------------------------------------
# Initialisation FastAPI
# ---------------------------------------------------------------------
app = FastAPI(
    title="IA Parse Excel",
    version="1.0.0"
)

# Autoriser CORS pour Bubble (tu peux restreindre si besoin)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------
# Endpoints de santé / diagnostic
# ---------------------------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "message": "Service is running", "docs": "/docs"}

@app.get("/ping")
def ping():
    return {"ping": "pong"}

@app.get("/healthz")
def healthz():
    return {"ok": True}

# ---------------------------------------------------------------------
# Helpers généraux
# ---------------------------------------------------------------------
def _parse_rules(rules_raw: Optional[str]) -> Dict[str, Any]:
    if not rules_raw:
        return {"full_day_threshold": 6, "half_day_min": 0.01, "half_day_max": 5.99}
    try:
        return json.loads(rules_raw)
    except Exception:
        return {"full_day_threshold": 6, "half_day_min": 0.01, "half_day_max": 5.99}

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
        return float(x)
    except Exception:
        return None

def _coerce_bool(x):
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
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(x), fmt).date().isoformat()
        except Exception:
            pass
    return str(x)

# ---------------------------------------------------------------------
# Endpoint existant : parse-excel-upload
# ---------------------------------------------------------------------
@app.post("/parse-excel-upload")
async def parse_excel_upload(
    file: UploadFile = File(...),
    holiday_dates: Optional[str] = Form(default=None),
    rules: Optional[str] = Form(default=None)
):
    filename = (file.filename or "").lower()
    if not (filename.endswith(".xlsx") or filename.endswith(".xlsm")):
        raise HTTPException(status_code=400, detail="Only .xlsx/.xlsm are supported")

    try:
        content = await file.read()
        wb = load_workbook(filename=BytesIO(content), data_only=True)
        ws = wb.active
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read Excel: {e}")

    rules_dict = _parse_rules(rules)
    holidays = _parse_holidays(holiday_dates)

    headers = []
    for cell in ws[1]:
        headers.append((cell.value or "").strip() if isinstance(cell.value, str) else str(cell.value or ""))

    rows = []
    for r in ws.iter_rows(min_row=2, values_only=True):
        row = dict(zip(headers, r))

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

    return {
        "rules_used": rules_dict,
        "holiday_dates": holidays,
        "rows": rows,
        "rows_count": len(rows),
    }

# ---------------------------------------------------------------------
# Nouvel endpoint : template-intake (Upload 1 - Fichier injectible paie)
# ---------------------------------------------------------------------
# Mémoire (temporaire)
TEMPLATE_STORE: Dict[str, dict] = {}

def _norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    for ch in ["\n", "\r", "\t"]:
        s = s.replace(ch, " ")
    for ch in [",", ";", ":", ".", "(", ")", "[", "]"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    return s

def _col_letter(idx: int) -> str:
    s = ""
    while idx:
        idx, rem = divmod(idx-1, 26)
        s = chr(65+rem) + s
    return s

HEADER_ALIASES: Dict[str, List[str]] = {
    "matricule_salarie": ["matricule salarie", "matricule salarié"],
    "matricule_client": ["matricule client"],
    "nombre": ["nombre"],
    "nom": ["nom"],
    "prenom": ["prenom", "prénom"],
    "num_contrat": ["n° contrat", "no contrat", "numero contrat"],
    "num_avenant": ["n° avenant", "no avenant", "numero avenant"],
    "date_debut": ["date debut", "debut"],
    "date_fin": ["date fin", "fin"],
    "service": ["service"],
    "nb_jt": ["nb jt","nb jours travailles","nb jours travaillés"],
    "nb_ji": ["nb ji","nb jours injustifies","nb jr injustifie"],
    "nb_cp_280": ["280 - nb cp","nb cp"],
    "nb_sans_solde": ["nb sans solde","sans solde"],
    "nb_jf": ["nb jf","nb jours feries","nb jours fériés"],
    "tx_sal": ["tx sal","tx salaire"],
    "hrs_norm_010": ["010 - hrs norm","hrs normales","heures normales"],
    "rappel_hrs_norm_140": ["140 - rappel hrs norm","rappel heures normales"],
    "hs_25_020": ["020 - hs 25%","hs 25%"],
    "hs_50_030": ["030 - hs 50%","hs 50%"],
    "hs_100_050": ["050 - hs 100%","hs 100%"],
    "hrs_feries_091": ["091 - hrs feries","heures feries"],
    "prime_astreinte_462": ["462 - prime d'astreinte","prime astreinte"],
    "ind_panier_771": ["771 - indemn. panier/mois","indemnite panier"],
    "ind_transport_777": ["777 - ind.transport/mois","indemnite transport"],
    "ind_deplacement_780": ["780 - indemnite deplacement","indemnite deplacement"],
    "heures_jour_ferie_chome_090": ["090 - heures jour ferie chome","heures jour ferie chome"],
    "observations": ["observations","commentaires"],
    "fin_mission": ["fin mission (oui/non)","fin mission"],
}
EXPECTED_KEYS = list(HEADER_ALIASES.keys())

def _match_header_key(normalized_header: str) -> Optional[str]:
    for canonical, variants in HEADER_ALIASES.items():
        for v in variants:
            if _norm(v) == normalized_header:
                return canonical
    return None

class RosterItem(BaseModel):
    row_index_excel: int
    matricule_salarie: Optional[str] = None
    matricule_client: Optional[str] = None
    nom: Optional[str] = None
    prenom: Optional[str] = None
    service: Optional[str] = None

class TemplateIntakeResponse(BaseModel):
    template_id: str
    sheet_name: str
    header_row_index: int
    column_map: Dict[str, str]
    roster: List[RosterItem]
    missing_columns: List[str]

@app.post("/template-intake", response_model=TemplateIntakeResponse)
async def template_intake(
    file_template: UploadFile = File(...),
    client_id: Optional[str] = Form(None),
):
    filename = (file_template.filename or "").lower()
    if not (filename.endswith(".xlsx") or filename.endswith(".xlsm")):
        raise HTTPException(status_code=400, detail="Only .xlsx/.xlsm are supported for template")

    try:
        content = await file_template.read()
        wb = load_workbook(filename=BytesIO(content), data_only=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read Excel template: {e}")

    # Trouver la feuille et ligne d’entêtes
    best = {"ws": None, "row": None, "hits": -1, "colmap": {}}
    for ws in wb.worksheets:
        for r in range(1, min(ws.max_row, 10) + 1):
            hits = 0
            colmap = {}
            for c in range(1, ws.max_column + 1):
                val = ws.cell(row=r, column=c).value
                if val is None:
                    continue
                key = _match_header_key(_norm(str(val)))
                if key and key not in colmap:
                    colmap[key] = _col_letter(c)
                    hits += 1
            if hits > best["hits"]:
                best = {"ws": ws, "row": r, "hits": hits, "colmap": colmap}

    if best["ws"] is None:
        raise HTTPException(status_code=422, detail="No recognizable headers found in template")

    ws = best["ws"]
    header_row = best["row"]
    column_map = best["colmap"]

    missing_columns = [k for k in EXPECTED_KEYS if k not in column_map]

    def col_index(letter: Optional[str]) -> Optional[int]:
        if not letter:
            return None
        total = 0
        for ch in letter:
            total = total * 26 + (ord(ch) - 64)
        return total

    i_nom = col_index(column_map.get("nom"))
    i_pre = col_index(column_map.get("prenom"))
    i_msa = col_index(column_map.get("matricule_salarie"))
    i_mcl = col_index(column_map.get("matricule_client"))
    i_srv = col_index(column_map.get("service"))

    roster: List[RosterItem] = []
    for r in range(header_row + 1, ws.max_row + 1):
        v_nom = ws.cell(row=r, column=i_nom).value if i_nom else None
        v_pre = ws.cell(row=r, column=i_pre).value if i_pre else None
        v_msa = ws.cell(row=r, column=i_msa).value if i_msa else None
        v_mcl = ws.cell(row=r, column=i_mcl).value if i_mcl else None
        v_srv = ws.cell(row=r, column=i_srv).value if i_srv else None

        if not any([v_nom, v_pre, v_msa, v_mcl, v_srv]):
            continue

        roster.append(RosterItem(
            row_index_excel=r,
            matricule_salarie=str(v_msa).strip() if v_msa else None,
            matricule_client=str(v_mcl).strip() if v_mcl else None,
            nom=str(v_nom).strip() if v_nom else None,
            prenom=str(v_pre).strip() if v_pre else None,
            service=str(v_srv).strip() if v_srv else None,
        ))

    template_id = f"tpl_{uuid4().hex[:10]}"
    TEMPLATE_STORE[template_id] = {
        "binary": content,
        "sheet_name": ws.title,
        "header_row": header_row,
        "column_map": column_map,
        "roster": [ri.model_dump() for ri in roster],
        "client_id": client_id,
    }

    return TemplateIntakeResponse(
        template_id=template_id,
        sheet_name=ws.title,
        header_row_index=header_row,
        column_map=column_map,
        roster=roster,
        missing_columns=missing_columns,
    )

