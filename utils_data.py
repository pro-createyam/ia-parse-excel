from datetime import datetime, date, timedelta
from typing import Any, Optional, Dict, List
import json
import re

# ─────────────────────────── Regex numérique Excel
_RE_NUMERIC_SERIAL = re.compile(r"^\d+(\.\d+)?$")

# ─────────────────────────── Helpers généraux
def _coerce_bool(x: Any) -> Optional[bool]:
    """
    True si: true/yes/1/oui/o/y/vrai/si/on
    False si: false/no/0/non/n/faux/off
    """
    if isinstance(x, bool):
        return x
    if x is None:
        return None
    s = str(x).strip().lower()
    true_set = {"yes", "true", "1", "oui", "o", "y", "vrai", "si", "on"}
    false_set = {"no", "false", "0", "non", "n", "faux", "off"}
    if s in true_set:
        return True
    if s in false_set:
        return False
    return None


def _excel_serial_to_date(n: float) -> Optional[str]:
    """
    Convertit un nombre Excel (système 1900) en 'YYYY-MM-DD'.
    Excel 'jour 1' = 1899-12-31, mais la conversion usuelle est base 1899-12-30.
    Les fractions de jour sont ignorées.
    """
    try:
        n = float(n)
    except Exception:
        return None
    # bornes raisonnables (dates > 1900)
    if n < 1 or n > 60000:  # ~ 2064
        return None
    base = date(1899, 12, 30)
    try:
        d = base + timedelta(days=int(n))
        return d.isoformat()
    except Exception:
        return None


def _coerce_date(x: Any) -> Optional[str]:
    """
    Retourne une date ISO 'YYYY-MM-DD' si possible, sinon None.
    Gère: datetime/date, formats FR/EN/variations, nombres Excel sérialisés
    (y compris texte '45000.0' ou '45000,5').
    """
    if x is None:
        return None
    if isinstance(x, (datetime, date)):
        d = x.date() if isinstance(x, datetime) else x
        return d.isoformat()
    # Nombres Excel
    if isinstance(x, (int, float)):
        iso = _excel_serial_to_date(x)
        if iso:
            return iso

    s = str(x).strip()
    if not s:
        return None

    # Essai conversion numérique (texte => nombre Excel)
    s_num = s.replace(",", ".")
    if _RE_NUMERIC_SERIAL.fullmatch(s_num):
        try:
            iso = _excel_serial_to_date(float(s_num))
            if iso:
                return iso
        except Exception:
            pass

    # Essais de formats usuels
    candidates = (
        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y",
        "%d.%m.%Y", "%Y/%m/%d", "%Y.%m.%d", "%d/%m/%y", "%m/%d/%y", "%d.%m.%y",
    )
    for fmt in candidates:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass

    return None


def _parse_rules(rules_raw: Optional[str]) -> Dict[str, Any]:
    """
    Parse des règles JSON avec valeurs par défaut + validation.
    """
    defaults = {"full_day_threshold": 8.0, "half_day_min": 3.5, "half_day_max": 4.5}
    if not rules_raw:
        return defaults
    try:
        data = json.loads(rules_raw)
        if not isinstance(data, dict):
            return defaults
        out = {**defaults, **data}
        # Normalisation/validation
        for k in ("full_day_threshold", "half_day_min", "half_day_max"):
            try:
                out[k] = float(out[k])
            except Exception:
                out[k] = defaults[k]
        # bornes simples
        if out["full_day_threshold"] <= 0:
            out["full_day_threshold"] = defaults["full_day_threshold"]
        if out["half_day_min"] <= 0 or out["half_day_min"] >= out["full_day_threshold"]:
            out["half_day_min"] = defaults["half_day_min"]
        if out["half_day_max"] <= out["half_day_min"] or out["half_day_max"] >= out["full_day_threshold"]:
            out["half_day_max"] = defaults["half_day_max"]
        return out
    except Exception:
        return defaults


def _parse_holidays(holiday_raw: Optional[str]) -> List[str]:
    """
    Accepte JSON, CSV ou nombres Excel → renvoie une liste ISO 'YYYY-MM-DD'.
    """
    if not holiday_raw:
        return []
    holidays: List[str] = []
    try:
        data = json.loads(holiday_raw)
        if isinstance(data, list):
            items = data
        else:
            raise ValueError("Not a JSON list")
        for x in items:
            iso = _coerce_date(x)
            if iso:
                holidays.append(iso)
    except Exception:
        txt = str(holiday_raw).replace("\n", ",").replace(";", ",")
        parts = [p.strip() for p in txt.split(",") if p.strip()]
        for p in parts:
            iso = _coerce_date(p)
            if iso:
                holidays.append(iso)

    # Déduplication en conservant l'ordre
    seen = set()
    out_list: List[str] = []
    for d in holidays:
        if d not in seen:
            out_list.append(d)
            seen.add(d)
    return out_list
