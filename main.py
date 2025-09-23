from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import httpx, pandas as pd
from io import BytesIO
from datetime import datetime

app = FastAPI(title="Pointage Parser")

class Rules(BaseModel):
    full_day_threshold: float = 6.0
    half_day_min: float = 0.01
    half_day_max: float = 5.99

class ParsePayload(BaseModel):
    file_url: str
    holiday_dates: List[str] = []
    rules: Rules = Rules()

def hhmm_to_decimal(x):
    if pd.isna(x): return 0.0
    if isinstance(x, (pd.Timestamp, datetime)): return 0.0
    s = str(x).strip()
    if not s: return 0.0
    if ":" in s:
        try:
            h, m = s.split(":")[:2]
            return float(int(h) + int(m)/60)
        except: pass
    s = s.replace(",", ".")
    try: return float(s)
    except: return 0.0

def guess_col(df, candidates):
    low = {c.lower(): c for c in df.columns}
    for want in candidates:
        for col_l in low:
            if want in col_l:
                return low[col_l]
    return None

@app.post("/parse-excel")
async def parse_excel(p: ParsePayload):
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(p.file_url)
            r.raise_for_status()
            content = r.content
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Download error: {e}")

    try:
        xls = pd.ExcelFile(BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Open excel error: {e}")

    rows_out, dropped, detected_cols = [], 0, set()
    feries = set(p.holiday_dates)

    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
        except Exception:
            continue
        if df.empty: continue

        date_col = guess_col(df, ["date", "jour"])
        if date_col is None:
            dropped += len(df); continue

        df["_date"] = pd.to_datetime(df[date_col], errors="coerce")
        df = df[df["_date"].notna()].copy()
        if df.empty: continue
        df["date_iso"] = df["_date"].dt.strftime("%Y-%m-%d")

        htrav_col = guess_col(df, ["heures trava", "h. trav", "htrav", "nb h", "heures"])
        hs_col    = guess_col(df, ["sup", "hs", "overtime"])
        abs_col   = guess_col(df, ["absenc", "absence", "motif"])
        matricule_col = guess_col(df, ["matricule", "matricul"])
        nom_col       = guess_col(df, ["nom"])
        prenom_col    = guess_col(df, ["prenom", "prénom"])
        cin_col       = guess_col(df, ["cin", "c.i.n", "ident", "cnie", "cni"])

        df["heures_trav"] = df[htrav_col].apply(hhmm_to_decimal) if htrav_col in df else 0.0
        df["hs_normales"] = df[hs_col].apply(hhmm_to_decimal) if hs_col in df else 0.0
        df["hs_ferie"] = df.apply(lambda r: float(r["heures_trav"]) if r["date_iso"] in feries else 0.0, axis=1)
        df["absence_type"] = df[abs_col].astype(str) if abs_col in df else ""

        def day_flag(h):
            if h >= p.rules.full_day_threshold: return (1.0, False)
            if p.rules.half_day_min <= h <= p.rules.half_day_max: return (0.5, True)
            return (0.0, False)
        df["jour_paye"], df["demi_journee"] = zip(*df["heures_trav"].map(day_flag))

        detected_cols.update([c for c in [date_col, htrav_col, hs_col, abs_col, matricule_col, nom_col, prenom_col, cin_col] if c])

        for _, r in df.iterrows():
            rows_out.append({
                "matricule": str(r.get(matricule_col, "")).strip() if matricule_col else "",
                "nom": str(r.get(nom_col, "")).strip() if nom_col else "",
                "prenom": str(r.get(prenom_col, "")).strip() if prenom_col else "",
                "cin": str(r.get(cin_col, "")).strip() if cin_col else "",
                "date": r["date_iso"],
                "heures_travaillees_decimal": round(float(r["heures_trav"]), 2),
                "hs_normales": round(float(r["hs_normales"]), 2) if hs_col else 0.0,
                "hs_ferie": round(float(r["hs_ferie"]), 2),
                "absence_type": "" if r["absence_type"] == "nan" else r["absence_type"],
                "demi_journee": bool(r["demi_journee"])
            })

    return {
        "columns_detected": sorted(list(detected_cols)),
        "rows": rows_out,
        "stats": {"sheets": len(xls.sheet_names), "rows_out": len(rows_out), "dropped": int(dropped)}
    }

@app.get("/health")
def health():
    return {"ok": True}
