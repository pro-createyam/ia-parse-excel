import re
from typing import Any, Optional, Dict

# ─────────────────────────── Regex liées aux heures/jours
_RE_MIN = re.compile(r"^\s*(\d+(?:[.,]\d+)?)\s*(mn|min|minutes?)?\s*$", re.I)
_RE_H = re.compile(r"^\s*(\d+(?:[.,]\d+)?)\s*(h|hr|heure?s?)?\s*$", re.I)
_RE_DAYS_DEC = re.compile(r"^\s*(\d+(?:[.,]\d+)?)\s*(j|jr|jour|jours?)?\s*$", re.I)
_RE_HALF_TOK = re.compile(r"(demi|1/2|½|half)", re.I)

# ─────────────────────────── Conversion heures/jours

def _parse_hours_to_decimal(v: Any) -> Optional[float]:
    """
    Convertit une valeur texte ou numérique en heures décimales.
    Exemples:
      "7h30" → 7.5
      "07:15" → 7.25
      "8,0" → 8.0
      "480 mn" → 8.0
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        if v < 0 or v > 24 * 2:  # bornes raisonnables (jusqu’à 48h)
            return None
        return round(float(v), 2)
    s = str(v).strip().lower()
    if not s:
        return None

    # Format HH:MM
    if ":" in s:
        try:
            h, m = s.split(":")
            return round(float(h) + float(m) / 60.0, 2)
        except Exception:
            pass

    # Ex: 7h30 → sépare avant/après h
    if "h" in s and any(c.isdigit() for c in s):
        s = s.replace(" ", "")
        try:
            if "h" in s:
                h, rest = s.split("h", 1)
                minutes = 0.0
                if rest:
                    minutes = float(rest.replace(",", "."))
                    if minutes > 59:  # sécurité
                        minutes = 0.0
                    minutes = minutes / 60.0
                return round(float(h.replace(",", ".")) + minutes, 2)
        except Exception:
            pass

    # Regex minutes (ex: "480 mn")
    m = _RE_MIN.match(s)
    if m:
        val = float(m.group(1).replace(",", "."))
        return round(val / 60.0, 2)

    # Regex heures (ex: "8h" ou "8 heures")
    m = _RE_H.match(s)
    if m:
        val = float(m.group(1).replace(",", "."))
        return round(val, 2)

    # Format direct décimal
    try:
        val = float(s.replace(",", "."))
        if 0 < val <= 48:
            return round(val, 2)
    except Exception:
        pass

    return None


def _parse_days(v: Any) -> Optional[float]:
    """
    Convertit une valeur texte ou numérique en jours (décimaux).
    Exemples:
      "2 jours" → 2.0
      "0.5" → 0.5
      "demi-journée" → 0.5
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        if v < 0 or v > 31:
            return None
        return round(float(v), 2)

    s = str(v).strip().lower()
    if not s:
        return None

    # Mots-clés “demi”
    if _RE_HALF_TOK.search(s):
        return 0.5

    # Regex jours
    m = _RE_DAYS_DEC.match(s)
    if m:
        try:
            val = float(m.group(1).replace(",", "."))
            if 0 <= val <= 31:
                return round(val, 2)
        except Exception:
            pass

    # Tentative directe décimale
    try:
        val = float(s.replace(",", "."))
        if 0 <= val <= 31:
            return round(val, 2)
    except Exception:
        pass

    return None


def _hours_to_days(hours: Optional[float], rules: Optional[Dict[str, Any]] = None) -> Optional[float]:
    """
    Convertit des heures en jours selon les règles.
    Par défaut: 1 jour = 8h, demi-journée = 3.5–4.5h.
    """
    if hours is None:
        return None
    try:
        r = rules or {}
        full_day = float(r.get("full_day_threshold", 8.0))
        half_min = float(r.get("half_day_min", 3.5))
        half_max = float(r.get("half_day_max", 4.5))

        if hours >= full_day:
            return 1.0
        if half_min <= hours <= half_max:
            return 0.5
        return round(hours / full_day, 2)
    except Exception:
        return None


def _days_to_hours(days: Optional[float], rules: Optional[Dict[str, Any]] = None) -> Optional[float]:
    """
    Convertit des jours en heures selon les règles.
    Par défaut: 1 jour = 8h.
    """
    if days is None:
        return None
    try:
        r = rules or {}
        full_day = float(r.get("full_day_threshold", 8.0))
        return round(days * full_day, 2)
    except Exception:
        return None
