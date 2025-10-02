# main.py
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Any, Dict, Tuple
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from io import BytesIO
from datetime import datetime
import json

# Fuzzy
from rapidfuzz import fuzz

app = FastAPI(title="IA Parse Excel", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ─────────────────────────── Health
@app.get("/")
def root():
    return {"status": "ok", "message": "Service is running", "docs": "/docs"}

@app.get("/ping")
def ping():
    return {"ping": "pong"}

@app.get("/healthz")
def healthz():
    return {"ok": True}

# ─────────────────────────── Utils
def _parse_rules(rules_raw: Optional[str]) -> Dict[str, Any]:
    defaults = {"full_day_threshold": 8, "half_day_min": 3.5, "half_day_max": 4.5}
    if not rules_raw:
        return defaults
    try:
        d = json.loads(rules_raw)
        return {**defaults, **d} if isinstance(d, dict) else defaults
    except Exception:
        return defaults

def _parse_holidays(holiday_raw: Optional[str]) -> List[str]:
    if not holiday_raw:
        return []
    try:
        data = json.loads(holiday_raw)
        return [str(x) for x in data] if isinstance(data, list) else []
    except Exception:
        return []

def _coerce_bool(x):
    if isinstance(x, bool): return x
    if x is None: return None
    s = str(x).strip().lower()
    if s in ("yes", "true", "1", "oui"): return True
    if s in ("no", "false", "0", "non"): return False
    return None

def _coerce_date(x):
    if x is None: return None
    if isinstance(x, datetime): return x.date().isoformat()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(x), fmt).date().isoformat()
        except Exception:
            pass
    return str(x)

def _parse_hours_to_decimal(s: Any) -> Optional[float]:
    if s is None: return None
    s = str(s).strip().lower().replace(" ", "")
    if not s: return None
    s = s.replace("h", ":").replace(",", ".")
    if ":" in s:
        try:
            hh, mm = s.split(":", 1)
            hh = int(hh or 0)
            mm = "".join(c for c in mm if c.isdigit())
            mm = int(mm or 0)
            return round(hh + mm/60.0, 2)
        except Exception:
            pass
    try:
        return round(float(s), 2)
    except Exception:
        return None

# --- Upload guard ------------------------------------------------------------
MAX_UPLOAD_BYTES = 15 * 1024 * 1024  # 15 MB

# --- Helpers entêtes/feuilles -----------------------------------------------
def _row_values(ws, row_index: int):
    return [cell.value for cell in ws[row_index]]

def _count_nonempty(vals):
    n = 0
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        n += 1
    return n

def _best_header_row(ws, max_scan: int = 15) -> int:
    """
    Scanne les 15 premières lignes et choisit celle qui ressemble le plus
    à une ligne d'en-têtes (max de cellules non vides).
    """
    best_row, best_score = 1, -1
    for r in range(1, min(ws.max_row, max_scan) + 1):
        vals = _row_values(ws, r)
        score = _count_nonempty(vals)
        if score > best_score:
            best_row, best_score = r, score
    return best_row

def _headers_at(ws, header_row_index: int) -> Dict[str, str]:
    """
    Retourne un dict { 'A': 'Nom de colonne', 'B': '...' } sur la ligne d’entête.
    Tolère cellules vides; ignore celles qui le sont.
    """
    headers: Dict[str, str] = {}
    for idx, cell in enumerate(ws[header_row_index], start=1):
        val = cell.value
        if val is None:
            continue
        txt = str(val).strip()
        if not txt:
            continue
        headers[get_column_letter(idx)] = txt
    return headers

def _pick_best_sheet(wb):
    """
    Parcourt toutes les feuilles et choisit celle dont la 'meilleure ligne d'entêtes'
    contient le plus de colonnes non vides.
    Retourne (worksheet, header_row_index).
    """
    best_ws = wb.active
    best_row_idx = _best_header_row(best_ws)
    best_score = _count_nonempty(_row_values(best_ws, best_row_idx)) if best_ws.max_row >= best_row_idx else 0

    for ws in wb.worksheets:
        r = _best_header_row(ws)
        if ws.max_row < r:
            continue
        vals = _row_values(ws, r)
        sc = _count_nonempty(vals)
        if sc > best_score:
            best_ws, best_row_idx, best_score = ws, r, sc

    return best_ws, best_row_idx

def _normalize(s: str) -> str:
    return " ".join(str(s or "").strip().lower().replace("_", " ").split())

# --- Synonymes d'en-têtes attendues (FR/EN/var.) -----------------------------
TARGET_SYNONYMS = {
    # Identités / clés
    "matricule_salarie": ["matricule salarie", "matricule salarié", "matricule", "employee id", "id salarie"],
    "matricule_client": ["matricule client", "client id", "code client"],
    "matricule": ["matricule", "employee id", "id", "code salarie", "code salarié"],
    "cin": ["cin", "c.i.n", "id card", "identity", "numero cin", "num cin"],

    # Référentiel template
    "nombre": ["nombre", "nb", "qty", "quantité"],
    "nom": ["nom", "last name", "surname", "family name"],
    "prenom": ["prenom", "prénom", "first name", "given name"],
    "num_contrat": ["n° contrat", "num contrat", "numero contrat", "contract no", "contract number"],
    "num_avenant": ["n° avenant", "num avenant", "avenant", "amendment no"],
    "date_debut": ["date debut", "date début", "start date", "date debut contrat"],
    "date_fin": ["date fin", "end date", "date fin contrat"],
    "service": ["service", "departement", "département", "department", "site", "unité"],

    "nb_jt": ["nb jt", "jours travailles", "jours travaillés", "jours", "nb jours"],
    "nb_ji": ["nb ji", "jours injustifies", "jours injustifiés", "ji"],
    "nb_cp_280": ["280 - nb cp", "cp", "conges payes", "congés payés", "paid leave days"],
    "nb_sans_solde": ["sans solde", "conge sans solde", "css", "unpaid leave"],
    "nb_jf": ["nb jf", "jours feries", "jours fériés", "public holidays"],
    "tx_sal": ["tx sal", "taux sal", "taux salarié", "salary rate"],

    "hrs_norm_010": ["010 - hrs norm", "heures normales", "hrs normales", "heure normal", "h. normal", "nb heures", "heures"],
    "rappel_hrs_norm_140": ["140 - rappel hrs norm", "rappel heures normales", "rappel 140"],
    "hs_25_020": ["020 - hs 25%", "heures sup 25", "hs 25", "maj 25"],
    "hs_50_030": ["030 - hs 50%", "heures sup 50", "hs 50", "maj 50"],
    "hs_100_050": ["050 - hs 100%", "heures sup 100", "hs 100", "maj 100"],
    "hrs_feries_091": ["091 - hrs feries", "heures feries", "heures fériées", "ferie", "férié", "jour férié"],

    "prime_astreinte_462": ["462 - prime astreinte", "astreinte", "prime astreinte"],
    "ind_panier_771": ["771 - indemn. panier/mois", "panier", "indemnite panier"],
    "ind_transport_777": ["777 - ind.transport/mois", "transport", "indemnite transport"],
    "ind_deplacement_780": ["780 - indemnité deplacement", "deplacement", "indemnite deplacement"],
    "heures_jour_ferie_chome_090": ["090 - heures jour ferie chome", "jour ferie chome", "ferie chome"],

    "observations": ["observations", "commentaire", "comments", "notes"],
    "fin_mission": ["fin mission", "fin de mission", "end of assignment"],

    # Timesheet génériques
    "date": ["date", "jour", "day", "date jour"],
    "absence": ["absence", "motif", "type jour", "statut jour", "am/pm"],
    "heures_norm": ["heures", "heures travaillees", "heures travaillées", "nbr heures", "hours worked", "h. normal"],
    "hs_25": ["hs 25", "heures sup 25", "maj 25"],
    "hs_50": ["hs 50", "heures sup 50", "maj 50"],
    "hs_100": ["hs 100", "heures sup 100", "maj 100"],
    "hs_feries": ["heures feries", "férié", "ferie", "public holiday hours"],
}

def _detect_columns(headers: Dict[str, str]) -> Dict[str, str]:
    """
    headers: dict { 'A': 'Intitulé', ... }
    Retourne un mapping { target_key -> column_letter }
    """
    detected: Dict[str, str] = {}
    # Normalise une fois
    header_items: List[Tuple[str, str]] = [(col, _normalize(txt)) for col, txt in headers.items()]

    for target, syns in TARGET_SYNONYMS.items():
        best = None
        best_score = 0
        for col, htxt in header_items:
            # score = meilleur score vs tous les synonymes connus pour ce target
            score = max(fuzz.token_set_ratio(htxt, _normalize(s)) for s in syns)
            if score > best_score:
                best_score, best = score, col
        if best is not None and best_score >= 80:
            detected[target] = best
    return detected

# ─────────────────────────── Legacy endpoint (utilisé dans Bubble actuellement)
@app.post("/parse-excel-upload")
async def parse_excel_upload(
    file: UploadFile = File(...),
    holiday_dates: Optional[str] = Form(default=None),
    rules: Optional[str] = Form(default=None),
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

    # Ligne 1 = entêtes
    headers = [(c.value or "") if not isinstance(c.value, str) else c.value.strip() for c in ws[1]]
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
            "heures_travaillees_decimal": _parse_hours_to_decimal(g("heures_travaillees_decimal", "heures", "Heures")),
            "hs_normales": _parse_hours_to_decimal(g("hs_normales", "HS_normales")),
            "hs_ferie": _parse_hours_to_decimal(g("hs_ferie", "HS_ferie")),
            "demi_journee": _coerce_bool(g("demi_journee", "Demi_journee", "demi_journee?")),
        }
        rows.append(item)

    return {
        "rules_used": rules_dict,
        "holiday_dates": holidays,
        "rows": rows,
        "rows_count": len(rows),
    }

# ─────────────────────────── NEW: Template Intake
@app.post("/template-intake")
async def template_intake(
    file_template: UploadFile = File(...),
    client_id: Optional[str] = Form(default=None),
):
    name = (file_template.filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xlsm")):
        raise HTTPException(status_code=400, detail="Only .xlsx/.xlsm are supported")

    try:
        content = await file_template.read()
        if len(content) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="File too large")
        wb = load_workbook(filename=BytesIO(content), data_only=True)
        # Choisir la meilleure feuille + ligne d'entêtes
        ws, header_row_index = _pick_best_sheet(wb)
        headers_dict = _headers_at(ws, header_row_index)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read Excel: {e}")

    # Colonnes attendues côté template paie
    expected = [
        "matricule_salarie","matricule_client","nombre","nom","prenom","num_contrat","num_avenant",
        "date_debut","date_fin","service","nb_jt","nb_ji","nb_cp_280","nb_sans_solde","nb_jf","tx_sal",
        "hrs_norm_010","rappel_hrs_norm_140","hs_25_020","hs_50_030","hs_100_050","hrs_feries_091",
        "prime_astreinte_462","ind_panier_771","ind_transport_777","ind_deplacement_780",
        "heures_jour_ferie_chome_090","observations","fin_mission"
    ]

    # Mapping exact sur normalisé
    header_norm = {col: _normalize(txt) for col, txt in headers_dict.items()}
    column_map: Dict[str, str] = {}
    for key in expected:
        key_norm = _normalize(key)
        found = None
        for col, htxt in header_norm.items():
            if htxt == key_norm:
                found = col
                break
        if found:
            column_map[key] = found

    # roster léger (quelques lignes sous l’entête)
    roster = []
    svc_col = column_map.get("service")
    for r in range(header_row_index + 1, min(ws.max_row, header_row_index + 1 + 100) + 1):
        item = {
            "row_index_excel": r,
            "matricule_salarie": None,
            "matricule_client": None,
            "nom": None,
            "prenom": None,
            "service": ws[f"{svc_col}{r}"].value if svc_col else None
        }
        roster.append(item)

    missing = [k for k in expected if k not in column_map]

    return {
        "template_id": f"tpl_{hex(abs(hash(name)))[2:12]}",
        "sheet_name": ws.title,
        "header_row_index": header_row_index,
        "column_map": column_map,
        "roster": roster,
        "missing_columns": missing,
    }

# ─────────────────────────── NEW: Timesheet Intake (pointage client)
@app.post("/timesheet-intake")
async def timesheet_intake(
    file_timesheet: UploadFile = File(...),
    holiday_dates: Optional[str] = Form(default=None),
    rules: Optional[str] = Form(default=None),
):
    fname = (file_timesheet.filename or "").lower()
    if not (fname.endswith(".xlsx") or fname.endswith(".xlsm")):
        raise HTTPException(status_code=400, detail="Only .xlsx/.xlsm are supported")

    try:
        content = await file_timesheet.read()
        if len(content) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="File too large")
        wb = load_workbook(filename=BytesIO(content), data_only=True)
        # Choisir la meilleure feuille + ligne d'entêtes
        ws, header_row_index = _pick_best_sheet(wb)
        headers_dict = _headers_at(ws, header_row_index)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read Excel: {e}")

    rules_dict = _parse_rules(rules)
    holidays = set(_parse_holidays(holiday_dates))

    # Détection automatique des colonnes
    detected = _detect_columns(headers_dict)

    # Preview de 5 lignes normalisées
    def val_at(r: int, col_key: str):
        col_letter = detected.get(col_key)
        if not col_letter:
            return None
        return ws[f"{col_letter}{r}"].value

    preview_rows = []
    start = header_row_index + 1
    end = min(ws.max_row, start + 4)
    for r in range(start, end + 1):
        row_date = _coerce_date(val_at(r, "date"))
        h_norm = _parse_hours_to_decimal(val_at(r, "heures_norm"))
        hs25 = _parse_hours_to_decimal(val_at(r, "hs_25"))
        hs50 = _parse_hours_to_decimal(val_at(r, "hs_50"))
        hs100 = _parse_hours_to_decimal(val_at(r, "hs_100"))
        hsfer = _parse_hours_to_decimal(val_at(r, "hs_feries"))
        absence_raw = val_at(r, "absence")
        demi_j = None
        if isinstance(absence_raw, str) and "demi" in absence_raw.lower():
            demi_j = True

        is_holiday = row_date in holidays if row_date else False

        preview_rows.append({
            "row_index_excel": r,
            "matricule": val_at(r, "matricule"),
            "cin": val_at(r, "cin"),
            "nom": val_at(r, "nom"),
            "prenom": val_at(r, "prenom"),
            "service": val_at(r, "service"),
            "date": row_date,
            "heures_norm_dec": h_norm,
            "hs_25_dec": hs25,
            "hs_50_dec": hs50,
            "hs_100_dec": hs100,
            "hs_feries_dec": hsfer,
            "demi_journee": demi_j,
            "is_holiday": is_holiday,
            "observations": val_at(r, "observations"),
        })

    warnings = []
    if not detected:
        warnings.append("Aucune colonne n’a été reconnue automatiquement (en-têtes trop atypiques).")
    for k in ("date", "heures_norm"):
        if k not in detected:
            warnings.append(f"Colonne importante non détectée: {k}")

    return {
        "sheet_name": ws.title,
        "header_row_index": header_row_index,
        "detected_columns": detected,     # ex: {"date": "C", "heures_norm": "H", ...}
        "preview_rows": preview_rows,     # 3–5 lignes pour vérifier
        "warnings": warnings,
        "rules_used": rules_dict,
        "holiday_dates": list(holidays),
    }
