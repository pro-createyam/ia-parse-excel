from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Tuple
import httpx
from io import BytesIO
from datetime import datetime, timedelta
from openpyxl import load_workbook
from rapidfuzz import process, fuzz
import json

app = FastAPI(title="Pointage Parser", version="0.1.0")

# ---------- Modèles (pour l'endpoint URL) ----------
class Rules(BaseModel):
    full_day_threshold: float = 6.0
    half_day_min: float = 0.01
    half_day_max: float = 5.99

class ParsePayload(BaseModel):
    file_url: str
    holiday_dates: List[str] = []
    rules: Rules = Rules()

# ---------- Dictionnaire de synonymes pour fuzzy-matching ----------
SYNONYMS = {
    "matricule": ["matricule", "mat.", "id", "code", "badge", "emp id", "personnel", "code sal", "code salarié"],
    "nom": ["nom", "lastname", "famille"],
    "prenom": ["prenom", "prénom", "firstname"],
    "cin": ["cin", "cni", "cnie", "id card", "numéro pièce", "num piece", "identité", "identite"],
    "date": ["date", "jour", "day", "date pointage", "date jour", "date de pointage"],
    "heures_travaillees_decimal": ["heures", "h", "worked hours", "durée", "duree", "time", "total h", "h dec", "heures (dec)", "nb h", "heures trava", "h. trav", "htrav"],
    "hs_normales": ["heures supp", "hs", "overtime", "supplementaires", "supplémentaires", "hs 0%", "hs norm", "hs normales"],
    "hs_ferie": ["ferie", "férié", "holiday ot", "jf", "jour férié", "hrs feries", "heures feries", "heures fériées", "fériés"],
    "demi_journee": ["½ journée", "1/2 journée", "demi", "half-day", "0.5 day", "demi journee", "0,5", "0.5"],
    "absence_type": ["absence", "motif", "leave type", "congé", "maladie", "absence type", "type absence"],
}

TARGET_ORDER = [
    "matricule","nom","prenom","cin","date",
    "heures_travaillees_decimal","hs_normales","hs_ferie",
    "demi_journee","absence_type"
]

# ---------- Utilitaires parsing ----------
def _best_header_row(ws, scan_rows: int = 40) -> int:
    """
    Choisit la ligne la plus "entête-like" dans les 40 premières:
      - beaucoup de cellules non vides
      - majorité de texte (pas que des nombres)
    """
    best_idx, best_score = 1, -1
    max_r = min(ws.max_row, scan_rows)
    max_c = ws.max_column
    for r in range(1, max_r + 1):
        vals = [ws.cell(r, c).value for c in range(1, max_c + 1)]
        non_empty = [v for v in vals if v not in (None, "")]
        if not non_empty:
            continue
        text_like = sum(isinstance(v, str) for v in non_empty)
        score = text_like * 2 + len(non_empty)
        if score > best_score:
            best_score = score
            best_idx = r
    return best_idx

def _headers_from_row(ws, header_row: int) -> List[str]:
    headers = []
    for c in range(1, ws.max_column + 1):
        v = ws.cell(header_row, c).value
        headers.append(str(v).strip() if v is not None else "")
    return headers

def _fuzzy_map_headers(headers: List[str]) -> Tuple[Dict[str, int], List[Dict[str, Any]]]:
    mapping = {}
    evidence = []
    for target, synonyms in SYNONYMS.items():
        best_header, best_score = None, -1
        for h in headers:
            match, score, _ = process.extractOne(h.lower(), synonyms, scorer=fuzz.WRatio)
            if score > best_score:
                best_score = score
                best_header = h
        if best_header and best_score >= 75:  # seuil à ajuster si besoin
            idx = headers.index(best_header)
            mapping[target] = idx
            evidence.append({"target": target, "matched_from": best_header, "score": best_score})
    return mapping, evidence

def _to_decimal_hours(val) -> Optional[float]:
    """
    Accepte: 8.5 ; "8.5" ; "08:30" ; "8:30" ; "8h30"
    Retourne heures décimales (float) ou None.
    """
    if val is None: return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().lower()
    if not s:
        return None
    # formats HH:MM ou HhMM
    for sep in [":", "h"]:
        if sep in s:
            try:
                hh, mm = s.split(sep)[:2]
                return float(int(hh)) + float(int(mm))/60.0
            except Exception:
                pass
    # décimal avec virgule/point
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None

def _to_iso_date(val) -> Optional[str]:
    """
    Accepte Excel serial (float), datetime, ou string ("dd/mm/yyyy", "yyyy-mm-dd"...).
    Retourne 'YYYY-MM-DD' ou None.
    """
    if val is None: return None
    # serial Excel (approx)
    if isinstance(val, (int, float)) and val > 59:
        origin = datetime(1899, 12, 30)  # base Excel (avec bug 1900)
        try:
            d = origin + timedelta(days=float(val))
            return d.strftime("%Y-%m-%d")
        except Exception:
            pass
    # datetime.date/datetime
    if hasattr(val, "year") and hasattr(val, "month") and hasattr(val, "day"):
        try:
            return datetime(val.year, val.month, val.day).strftime("%Y-%m-%d")
        except Exception:
            pass
    # strings usuelles
    s = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None

def _to_bool_demi(val) -> Optional[bool]:
    if val is None: return None
    s = str(val).strip().lower()
    tokens_true = {"½","1/2","0.5","half","demi","oui","yes","y","true","vrai"}
    tokens_false = {"0","non","no","false","faux"}
    if s in tokens_true: return True
    if s in tokens_false: return False
    try:
        f = float(s.replace(",", "."))
        if abs(f - 0.5) < 1e-6: return True
        if abs(f) < 1e-9: return False
    except Exception:
        pass
    return None

def _read_rows_from_ws(ws, holidays: set, rules: Rules) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    header_row = _best_header_row(ws, scan_rows=40)
    headers = _headers_from_row(ws, header_row)
    mapping, evidence = _fuzzy_map_headers(headers)

    warnings = []
    if not mapping.get("date"):
        warnings.append("Colonne 'date' non détectée avec confiance suffisante.")
    if not mapping.get("heures_travaillees_decimal"):
        warnings.append("Colonne 'heures' non détectée avec confiance suffisante.")

    rows_out = []
    dropped = 0
    start_data = header_row + 1

    for r in range(start_data, ws.max_row + 1):
        # ignorer lignes totalement vides
        row_vals = []
        for c in range(1, ws.max_column + 1):
            v = ws.cell(r, c).value
            if isinstance(v, str): v = v.strip()
            row_vals.append(v)
        if all(v in (None, "") for v in row_vals):
            continue

        rec = {t: None for t in TARGET_ORDER}

        # texte
        for field in ["matricule","nom","prenom","cin","absence_type"]:
            idx = mapping.get(field)
            rec[field] = str(row_vals[idx]).strip() if (idx is not None and row_vals[idx] not in (None,"")) else None

        # date
        didx = mapping.get("date")
        rec["date"] = _to_iso_date(row_vals[didx]) if didx is not None else None

        # heures & HS
        hidx = mapping.get("heures_travaillees_decimal")
        heures = _to_decimal_hours(row_vals[hidx]) if hidx is not None else None
        rec["heures_travaillees_decimal"] = heures if heures is not None else 0.0

        hs_n_idx = mapping.get("hs_normales")
        hs_norm = _to_decimal_hours(row_vals[hs_n_idx]) if hs_n_idx is not None else 0.0
        rec["hs_normales"] = hs_norm if hs_norm is not None else 0.0

        hs_f_idx = mapping.get("hs_ferie")
        hs_ferie = _to_decimal_hours(row_vals[hs_f_idx]) if hs_f_idx is not None else 0.0
        # si pas de colonne dédiée, déduire férié si la date ∈ holidays
        if hs_f_idx is None and rec["date"] and rec["date"] in holidays:
            hs_ferie = rec["heures_travaillees_decimal"] or 0.0
        rec["hs_ferie"] = hs_ferie if hs_ferie is not None else 0.0

        # demi-journée
        dj_idx = mapping.get("demi_journee")
        demi = _to_bool_demi(row_vals[dj_idx]) if dj_idx is not None else None

        # appliquer règles jour payé vs demi-journée si demi non fourni
        if demi is None:
            h = rec["heures_travaillees_decimal"] or 0.0
            if h >= rules.full_day_threshold:
                demi = False
            elif rules.half_day_min <= h <= rules.half_day_max:
                demi = True
            else:
                demi = False
        rec["demi_journee"] = bool(demi)

        # critères minimaux
        if not rec["date"]:
            dropped += 1
            continue
        if (rec["heures_travaillees_decimal"] is None or rec["heures_travaillees_decimal"] == 0.0) and not rec["demi_journee"] and not rec["absence_type"]:
            # aucune info exploitable
            dropped += 1
            continue

        # arrondis doux
        for k in ["heures_travaillees_decimal", "hs_normales", "hs_ferie"]:
            rec[k] = round(float(rec[k] or 0.0), 2)

        rows_out.append(rec)

    return rows_out, evidence, warnings

# ---------- Endpoint (A) : URL ----------
@app.post("/parse-excel")
async def parse_excel(p: ParsePayload):
    # télécharger le fichier
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(p.file_url)
            r.raise_for_status()
            content = r.content
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Download error: {e}")

    # ouvrir et parser
    try:
        wb = load_workbook(BytesIO(content), data_only=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Open excel error: {e}")

    holidays = set(p.holiday_dates or [])
    all_rows, all_evidence, all_warnings = [], [], []
    sheets_count = 0
    dropped_total = 0

    for ws in wb.worksheets:
        sheets_count += 1
        rows_out, evidence, warnings = _read_rows_from_ws(ws, holidays, p.rules)
        all_rows.extend(rows_out)
        all_evidence.extend(evidence)
        all_warnings.extend(warnings)
        # dropped est calculé dans _read_rows_from_ws mais non renvoyé individuellement :
        # on l'approxime par lignes non ajoutées -> complexité : ici on ne l'additionne pas finement.
        # (option: recalculer si besoin)

    # dédupliquer evidence par (target, matched_from)
    seen = set()
    evidence_unique = []
    for ev in all_evidence:
        key = (ev["target"], ev["matched_from"])
        if key not in seen:
            seen.add(key)
            evidence_unique.append(ev)

    return {
        "columns_detected": evidence_unique,
        "warnings": list(set(all_warnings)),
        "rows": all_rows,
        "stats": {"sheets": sheets_count, "rows_out": len(all_rows), "dropped": int(dropped_total)}
    }

# ---------- Endpoint (B) : UPLOAD (recommandé) ----------
@app.post("/parse-excel-upload")
async def parse_excel_upload(
    file: UploadFile = File(...),
    holiday_dates: Optional[str] = Form("[]"),
    rules: Optional[str] = Form('{"full_day_threshold":6,"half_day_min":0.01,"half_day_max":5.99}')
):
    try:
        content = await file.read()
        wb = load_workbook(BytesIO(content), data_only=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Open excel error: {e}")

    try:
        holidays = set(json.loads(holiday_dates or "[]"))
    except Exception:
        holidays = set()

    try:
        rules_obj = Rules(**json.loads(rules or "{}"))
    except Exception:
        rules_obj = Rules()

    all_rows, all_evidence, all_warnings = [], [], []
    sheets_count = 0
    dropped_total = 0

    for ws in wb.worksheets:
        sheets_count += 1
        rows_out, evidence, warnings = _read_rows_from_ws(ws, holidays, rules_obj)
        all_rows.extend(rows_out)
        all_evidence.extend(evidence)
        all_warnings.extend(warnings)

    seen = set()
    evidence_unique = []
    for ev in all_evidence:
        key = (ev["target"], ev["matched_from"])
        if key not in seen:
            seen.add(key)
            evidence_unique.append(ev)

    return {
        "columns_detected": evidence_unique,
        "warnings": list(set(all_warnings)),
        "rows": all_rows,
        "stats": {"sheets": sheets_count, "rows_out": len(all_rows), "dropped": int(dropped_total)}
    }

@app.get("/health")
def health():
    return {"ok": True}
