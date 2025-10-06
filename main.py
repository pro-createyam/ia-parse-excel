# main.py
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Any, Dict, Tuple
from openpyxl import load_workbook
from io import BytesIO
from datetime import datetime, date, timedelta
import json
import re
import logging
import os
import math 


# Fuzzy
from rapidfuzz import fuzz


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


logger = _configure_logging()


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
    app = FastAPI(title="IA Parse Excel", version="1.0.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=_get_cors_origins(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ─────────────────────────── Health
    @app.get("/")
    def root() -> Dict[str, Any]:
        """Health root + lien doc."""
        return {"status": "ok", "message": "Service is running", "docs": "/docs"}

    @app.get("/ping")
    def ping() -> Dict[str, str]:
        """Ping liveness simple."""
        return {"ping": "pong"}

    @app.get("/healthz")
    def healthz() -> Dict[str, bool]:
        """Probes k8s/infra."""
        return {"ok": True}

    return app


# Instance globale (comportement inchangé)
app = create_app()

# ─────────────────────────── Utils

# Regex pré-compilées (perf & lisibilité)
_RE_NUMERIC_SERIAL = re.compile(r"^\d{1,5}(?:[.,]\d+)?$")  # ex: "45000", "45000.0", "45000,5"
_RE_MIN = re.compile(r"^\s*(\d+)\s*(?:m|min|mn)\s*$", re.IGNORECASE)
_RE_H = re.compile(r"^\s*(\d+)\s*h(?:\s*(\d{1,2}))?(?:m|mn)?\s*$", re.IGNORECASE)
# Jours (nouvelles regex)
_RE_DAYS_DEC = re.compile(r"^\s*([+-]?\d+(?:[.,]\d+)?)\s*(?:j|jour|jours|d|day|days)?\s*$", re.IGNORECASE)
_RE_HALF_TOK = re.compile(r"^\s*(?:1/2|0[.,]?5|demi|half|mi[-\s]?journ[eé]e|am|pm)\s*$", re.IGNORECASE)

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
    Accepte:
      - JSON: '["2025-01-01","2025-05-01"]'
      - CSV/texte: '2025-01-01, 2025-05-01' ou retours à la ligne
      - Nombres Excel (sérialisés) si présents, y compris en texte "45000.0"
    Retourne une liste ISO 'YYYY-MM-DD' (dédupliquée, ordre préservé).
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
    Les fractions de jour sont ignorées (pertes d'heures volontaire).
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

    # Essai conversion numérique (texte => nombre Excel), autorise décimales
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

    # Pas compris: on retourne None (plus propre que renvoyer la string brute)
    return None


def _parse_hours_to_decimal(s: Any) -> Optional[float]:
    """
    Convertit une heure exprimée en:
      - décimal: '7.5', '7,5'
      - H:M: '7:30', '07:5'
      - texte: '7h30', '7h', '1h30m', '90m', '90min', '1h30mn'
      - '08h' -> 8.0
    Retourne un float en heures (ex: 7.5), sinon None.
    (Les validations métier sur bornes/négatifs sont gérées en aval.)
    """
    if s is None:
        return None
    if isinstance(s, (int, float)):
        # Déjà numérique → on suppose des heures décimales
        try:
            return round(float(s), 2)
        except Exception:
            return None

    txt = str(s).strip().lower()
    if not txt:
        return None
    # nettoyages de base
    txt = txt.replace(" ", "")
    txt = txt.replace(",", ".")  # 7,5 -> 7.5

    # cas "90m" / "90min" / "90mn"
    m_match = _RE_MIN.fullmatch(txt)
    if m_match:
        minutes = int(m_match.group(1))
        return round(minutes / 60.0, 2)

    # cas "1h30" / "1h30m" / "1h30mn" / "1h"
    h_match = _RE_H.fullmatch(txt)
    if h_match:
        hh = int(h_match.group(1))
        mm = int(h_match.group(2) or 0)
        if mm >= 60:  # garde-fou
            mm = 59
        return round(hh + mm / 60.0, 2)

    # Autorise "h:" en séparateur (ex: "1h30" déjà géré; ici on gère "1:30")
    txt2 = txt.replace("h", ":")
    if ":" in txt2:
        try:
            hh_str, mm_str = txt2.split(":", 1)
            hh = int(hh_str or 0)
            mm_digits = re.sub(r"\D", "", mm_str)
            mm = int(mm_digits or 0)
            if mm >= 60:
                mm = 59
            return round(hh + mm / 60.0, 2)
        except Exception:
            pass

    # Sinon essaye décimal direct
    try:
        return round(float(txt), 2)
    except Exception:
        return None


# ─────────────── Nouvelles fonctions: jours ↔ heures ───────────────

def _parse_days(x: Any) -> Optional[float]:
    """
    Parse un nombre de jours à partir de :
      - décimal : '1.5', '1,5', '2', '0,25'
      - suffixes : '1.5j', '2 jours', '3 day(s)', '2d'
      - tokens demi-journée : 'demi', '1/2', '0.5', 'AM', 'PM', 'half', 'mi-journée'
    Retourne un float (jours) ou None.
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        try:
            return round(float(x), 3)
        except Exception:
            return None

    s = str(x).strip()
    if not s:
        return None

    # demi-journée explicite
    if _RE_HALF_TOK.fullmatch(s):
        return 0.5

    # décimal + unités facultatives (j, jour, days…)
    m = _RE_DAYS_DEC.fullmatch(s.replace(",", "."))
    if m:
        try:
            return round(float(m.group(1)), 3)
        except Exception:
            return None

    return None


def _hours_to_days(hours: Optional[float], rules: Optional[Dict[str, Any]] = None) -> Optional[float]:
    """
    Convertit des heures → jours selon la règle métier:
      - Utilise rules['full_day_threshold'] (par défaut 8.0)
      - Si half_day_min <= h <= half_day_max → 0.5
      - Sinon h / full_day_threshold
    Retourne un float en jours (arrondi 3 déc.) ou None si input invalide.
    """
    if hours is None:
        return None
    try:
        h = float(hours)
    except Exception:
        return None
    if h < 0:
        return None  # on laisse les validations aval traiter si besoin

    r = rules or {"full_day_threshold": 8.0, "half_day_min": 3.5, "half_day_max": 4.5}
    fdt = float(r.get("full_day_threshold", 8.0))
    hmin = float(r.get("half_day_min", 3.5))
    hmax = float(r.get("half_day_max", 4.5))

    if hmin <= h <= hmax:
        return 0.5

    if fdt <= 0:
        fdt = 8.0

    return round(h / fdt, 3)


def _days_to_hours(days: Optional[float], rules: Optional[Dict[str, Any]] = None) -> Optional[float]:
    """
    Convertit des jours → heures selon full_day_threshold (par défaut 8.0).
    Retourne un float (heures, arrondi 2 déc.) ou None si input invalide.
    """
    if days is None:
        return None
    try:
        d = float(days)
    except Exception:
        d = _parse_days(days)  # tente un parse libre si c'est du texte
        if d is None:
            return None

    r = rules or {"full_day_threshold": 8.0}
    fdt = float(r.get("full_day_threshold", 8.0))
    if fdt <= 0:
        fdt = 8.0
    return round(d * fdt, 2)

# --- Upload guard ------------------------------------------------------------
MAX_UPLOAD_BYTES = 15 * 1024 * 1024  # 15 MB

# --- Helpers entêtes/feuilles -----------------------------------------------
def _row_values(ws, row_index: int):
    """
    Retourne les valeurs de la ligne (sans planter si la ligne dépasse les bornes).
    """
    try:
        # openpyxl renvoie une séquence de cellules; si l'index est hors bornes, on retourne vide.
        if row_index < 1 or row_index > getattr(ws, "max_row", 0):
            return []
        return [cell.value for cell in ws[row_index]]
    except Exception:
        return []


def _count_nonempty(vals) -> int:
    """Compte le nombre de cellules non vides (après trim)."""
    n = 0
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str):
            if v.strip() == "":
                continue
        n += 1
    return n


def _is_numeric_like(v) -> bool:
    """
    Heuristique: True si la cellule ressemble à un nombre.
    Accepte virgule décimale, espaces fines, séparateurs de milliers, signes.
    """
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return True
    s = str(v).strip()
    if s == "":
        return False
    # normalisations soft
    s = s.replace("\u202f", "").replace("\xa0", "").replace(" ", "")
    s = s.replace(",", ".")
    # garde seulement chiffres, '.', '+', '-', éventuellement un seul séparateur décimal
    try:
        float(s)
        return True
    except Exception:
        return False


# Regex date pré-compilées (perf)
_DATE_PATTERNS = (
    re.compile(r"^\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4}$"),  # 31/12/2025, 31-12-25, 31.12.2025
    re.compile(r"^\d{4}[\/\-.]\d{1,2}[\/\-.]\d{1,2}$"),    # 2025/12/31, 2025-12-31, 2025.12.31
)

def _is_date_like_text(s: str) -> bool:
    if not isinstance(s, str):
        return False
    s = s.strip().lower()
    return any(p.match(s) for p in _DATE_PATTERNS)


def _header_keywords_vocab() -> set:
    """
    Construit un vocabulaire de mots d'entêtes (normalisés) à partir de TARGET_SYNONYMS si disponible.
    Ne casse pas si TARGET_SYNONYMS n'est pas encore défini au moment de l'import.
    """
    vocab = set()
    try:
        mapping = TARGET_SYNONYMS  # noqa: F821
        for k, arr in mapping.items():
            vocab.add(_normalize(k))
            for s in arr:
                vocab.add(_normalize(s))
    except Exception:
        # Pas grave: vocab vide => score basé uniquement sur non-vides/textes
        pass
    # Mots génériques utiles même sans TARGET_SYNONYMS
    for kw in (
        "date", "jour", "nom", "prénom", "prenom", "matricule", "service",
        "heures", "hs", "absence", "observations", "contrat", "avenant"
    ):
        vocab.add(_normalize(kw))
    return vocab


def _header_score(vals) -> float:
    """
    Score une ligne pour estimer si c'est une ligne d'entêtes.
    +2 pour chaque cellule texte non vide
    +3 si le texte matche un mot du vocabulaire d'entêtes
    -1 pour chaque cellule numérique
    -1 pour chaque cellule qui ressemble à une date (texte)
    Bonus léger selon # de non-vides.
    """
    vocab = _header_keywords_vocab()
    score = 0.0
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str):
            txt = v.strip()
            if not txt:
                continue
            score += 2.0
            norm = _normalize(txt)
            # match exact ou inclusion simple
            if norm in vocab:
                score += 3.0
            else:
                if any(w in norm for w in vocab):
                    score += 1.5
            if _is_date_like_text(txt):
                score -= 1.0
        else:
            if _is_numeric_like(v):
                score -= 1.0
    nonempty = _count_nonempty(vals)
    score += min(nonempty, 10) * 0.2
    return score


def _best_header_row(ws, max_scan: int = 25) -> int:
    """
    Scanne les 'max_scan' premières lignes et choisit celle qui ressemble le plus
    à une ligne d'en-têtes (score heuristique).
    """
    best_row, best_score = 1, float("-inf")
    last_row = min(getattr(ws, "max_row", 1), max_scan)
    for r in range(1, last_row + 1):
        vals = _row_values(ws, r)
        score = _header_score(vals)
        if score > best_score:
            best_row, best_score = r, score
    return best_row


def _headers_at(ws, header_row_index: int) -> Dict[str, str]:
    """
    Retourne un dict { 'A': 'Nom de colonne', 'B': '...' } sur la ligne d’entête.
    Tolère cellules vides; ignore celles qui le sont.
    Nettoie (trim, remplace retours ligne), garde la chaîne brute sinon.
    """
    from openpyxl.utils import get_column_letter as _gcl  # import local safe
    headers: Dict[str, str] = {}
    try:
        if header_row_index < 1 or header_row_index > getattr(ws, "max_row", 0):
            return headers
        for idx, cell in enumerate(ws[header_row_index], start=1):
            val = getattr(cell, "value", None)
            if val is None:
                continue
            txt = str(val).replace("\n", " ").strip()
            if not txt:
                continue
            headers[_gcl(idx)] = txt
    except Exception:
        # on renvoie ce qui a pu être lu
        return headers
    return headers


def _extract_headers(ws, header_row_index: int) -> Dict[str, str]:
    """
    Récupère les entêtes à partir de la ligne candidate.
    Si très peu d’entêtes détectées, essaie la ligne suivante/précédente et garde le meilleur résultat.
    """
    base = _headers_at(ws, header_row_index)
    if len(base) >= 3:
        return base

    # Essaie r+1 et r-1 pour rattraper les lignes fusionnées/vides
    candidates = [(header_row_index, base)]
    max_row = getattr(ws, "max_row", 1)
    if header_row_index + 1 <= max_row:
        candidates.append((header_row_index + 1, _headers_at(ws, header_row_index + 1)))
    if header_row_index - 1 >= 1:
        candidates.append((header_row_index - 1, _headers_at(ws, header_row_index - 1)))

    # Choisit celui avec le plus d’entrées, puis score d’entête
    def _hdr_quality(item):
        _r, mapping = item
        vals = list(mapping.values())
        return (len(mapping), _header_score(vals))

    _r_best, mapping_best = max(candidates, key=_hdr_quality)
    return mapping_best if mapping_best else base


def _pick_best_sheet(wb):
    """
    Parcourt toutes les feuilles visibles et choisit celle dont la 'meilleure ligne d'entêtes'
    a le score heuristique le plus élevé.
    Retourne (worksheet, header_row_index).
    """
    # point de départ: active si visible
    def _visible_sheets(workbook):
        for ws in workbook.worksheets:
            # sheet_state peut être 'visible', 'hidden', 'veryHidden'
            state = getattr(ws, "sheet_state", "visible")
            if state == "visible":
                yield ws

    visible_ws = list(_visible_sheets(wb)) or [wb.active]

    # init
    best_ws = visible_ws[0]
    best_row_idx = _best_header_row(best_ws)
    best_vals = _row_values(best_ws, best_row_idx)
    best_score = _header_score(best_vals) if best_vals else float("-inf")

    for ws in visible_ws:
        r = _best_header_row(ws)
        vals = _row_values(ws, r)
        sc = _header_score(vals) if vals else float("-inf")
        if sc > best_score:
            best_ws, best_row_idx, best_score = ws, r, sc

    # Garde-fou si la feuille candidate est vide ou quasi vide
    try:
        if getattr(best_ws, "max_row", 0) < 1 or getattr(best_ws, "max_column", 0) < 1:
            return best_ws, 1
    except Exception:
        return best_ws, 1

    return best_ws, best_row_idx


def _normalize(s: str) -> str:
    """
    Normalisation robuste:
      - trim + lower
      - remplace '_' et tirets par espace
      - supprime les accents (NFKD)
      - compacte les espaces multiples (inclut espaces insécables)
    """
    import unicodedata as _ud
    import re as _re
    s = "" if s is None else str(s)
    s = _ud.normalize("NFKD", s)
    s = "".join(c for c in s if not _ud.combining(c))
    # normalisation espaces
    s = s.replace("\u202f", " ").replace("\xa0", " ")
    # séparateurs usuels -> espace
    s = s.replace("_", " ").replace("-", " ")
    s = s.lower().strip()
    s = _re.sub(r"\s+", " ", s)
    return s
# --- Synonymes d'en-têtes attendues (FR/EN/ES + variations usuelles) ---------
TARGET_SYNONYMS = {
    # ────────────────── Identités / clés
    "matricule_salarie": [
        "matricule salarie", "matricule salarié", "matricule", "employee id", "emp id",
        "id salarie", "id salarié", "code salarie", "code salarié", "code agent",
        "badge", "pernr", "personnel number", "matricule rh", "worker id", "staff id",
        "employee number", "num employe", "numéro employé", "numero employe",
    ],
    "matricule_client": [
        "matricule client", "client id", "code client", "code chantier", "code site",
        "site id", "affectation", "code affectation", "cost center", "centre de cout",
        "centre de coût", "centro de costo", "center code", "cost centre",
    ],
    "matricule": [
        "matricule", "employee id", "empid", "id", "code", "code salarie",
        "code salarié", "badge", "pernr", "employee number", "num employe",
    ],
    "cin": [
        "cin", "c.i.n", "id card", "identity", "identity card", "numero cin",
        "num cin", "n cin", "id national", "national id", "piece identite",
        "pièce identité", "dni", "carnet", "national identity",
    ],

    # ────────────────── Référentiel template (identité/contrat)
    "nombre": ["nombre", "nb", "qty", "quantité", "quantite", "quantity", "cantidad"],
    "nom": ["nom", "last name", "surname", "family name", "patronyme", "lastname", "apellidos"],
    "prenom": ["prenom", "prénom", "first name", "given name", "firstname", "nombre"],
    "num_contrat": [
        "n° contrat", "num contrat", "numero contrat", "contract no", "contract number",
        "contrat", "contrat no", "contrat n", "id contrat", "n contrat",
    ],
    "num_avenant": [
        "n° avenant", "num avenant", "avenant", "amendment no", "amendement",
        "avenant no", "avenant n", "contract amendment",
    ],
    "date_debut": [
        "date debut", "date début", "start date", "date debut contrat",
        "debut contrat", "date d debut", "fecha inicio",
    ],
    "date_fin": [
        "date fin", "end date", "date fin contrat", "fin contrat",
        "date d fin", "date de fin", "fecha fin",
    ],
    "service": [
        "service", "departement", "département", "department", "site", "unité", "unite",
        "direction", "pole", "secteur", "equipe", "équipe", "area", "unidad", "departamento",
        "atelier", "workshop", "team",
    ],

    # ────────────────── Jours / compteurs
    "nb_jt": [
        "nb jt", "jours travailles", "jours travaillés", "jours", "nb jours",
        "jours presents", "jours présence", "jours presence", "working days",
        "dias trabajados",
    ],
    "nb_ji": [
        "nb ji", "jours injustifies", "jours injustifiés", "ji", "abs injustifiee",
        "absence injustifiee", "absence injustifiée", "abs non justifiee", "non justifie",
        "unjustified leave", "ausencia injustificada",
    ],
    "nb_cp_280": [
        "280 - nb cp", "cp", "conges payes", "congés payés", "paid leave days",
        "conge paye", "jours cp", "nb cp", "paid leave",
    ],
    "nb_sans_solde": [
        "sans solde", "conge sans solde", "css", "unpaid leave", "conge non paye",
        "non paye", "non payé", "unpaid",
    ],
    "nb_jf": [
        "nb jf", "jours feries", "jours fériés", "public holidays", "jf",
        "jours ferie", "jour ferié", "jours férié", "feriados", "public holiday days",
    ],
    "tx_sal": [
        "tx sal", "taux sal", "taux salarié", "salary rate", "taux horaire", "th",
        "tarif horaire", "rate", "hourly rate",
    ],

    # ────────────────── Heures / primes (codes paie)
    "hrs_norm_010": [
        "010 - hrs norm", "heures normales", "hrs normales", "heure normal", "h. normal",
        "nb heures", "heures", "heures de base", "base hours", "normal hours",
        "regular hours", "horas normales",
    ],
    "rappel_hrs_norm_140": [
        "140 - rappel hrs norm", "rappel heures normales", "rappel 140", "rappel salaire",
        "rappel base", "backpay normal", "regularization normal hours", "regularisation heures normales",
    ],
    "hs_25_020": [
        "020 - hs 25%", "heures sup 25", "hs 25", "maj 25", "hs 25 pourcent",
        "overtime 25", "ot 25", "25%", "o/t 25",
    ],
    "hs_50_030": [
        "030 - hs 50%", "heures sup 50", "hs 50", "maj 50", "hs 50 pourcent",
        "overtime 50", "ot 50", "50%", "o/t 50",
    ],
    "hs_100_050": [
        "050 - hs 100%", "heures sup 100", "hs 100", "maj 100", "hs 100 pourcent",
        "overtime 100", "ot 100", "100%", "o/t 100",
    ],
    "hrs_feries_091": [
        "091 - hrs feries", "heures feries", "heures fériées", "ferie", "férié",
        "jour férié", "heures ferie travaillees", "jf travaille", "holiday worked hours",
        "public holiday hours", "horas feriado trabajadas",
    ],

    "prime_astreinte_462": [
        "462 - prime astreinte", "astreinte", "prime astreinte", "on-call", "on call allowance",
        "oncall", "astreinte prime",
    ],
    "ind_panier_771": [
        "771 - indemn. panier/mois", "panier", "indemnite panier", "prime panier",
        "meal allowance", "panier repas",
    ],
    "ind_transport_777": [
        "777 - ind.transport/mois", "transport", "indemnite transport", "prime transport",
        "transport allowance", "allowance transport",
    ],
    "ind_deplacement_780": [
        "780 - indemnité deplacement", "deplacement", "indemnite deplacement",
        "frais deplacement", "travel allowance", "per diem", "perdiem",
    ],
    "heures_jour_ferie_chome_090": [
        "090 - heures jour ferie chome", "jour ferie chome", "ferie chome", "jf chome",
        "holiday idle hours", "public holiday idle",
    ],

    "observations": [
        "observations", "commentaire", "comments", "notes", "remarques", "note",
        "remarks", "observaciones",
    ],
    "fin_mission": [
        "fin mission", "fin de mission", "end of assignment", "fin contrat mission",
        "mission terminee", "mission terminée", "end of contract", "terminacion de mision",
    ],

    # ────────────────── Timesheet (généraux)
    "date": [
        "date", "jour", "day", "date jour", "work date", "date travail",
        "fecha", "fecha trabajo",
    ],
    "absence": [
        "absence", "motif", "type jour", "statut jour", "am/pm", "am pm",
        "demi journee", "half day", "leave type", "reason", "motivo",
    ],
    "heures_norm": [
        "heures", "heures travaillees", "heures travaillées", "nbr heures",
        "hours worked", "h. normal", "heures jour", "total heures",
        "temps de travail", "duree travail", "durée travail", "worked hours",
    ],
    "hs_25": ["hs 25", "heures sup 25", "maj 25", "overtime 25", "ot 25", "25%", "o/t 25"],
    "hs_50": ["hs 50", "heures sup 50", "maj 50", "overtime 50", "ot 50", "50%", "o/t 50"],
    "hs_100": ["hs 100", "heures sup 100", "maj 100", "overtime 100", "ot 100", "100%", "o/t 100"],
    "hs_feries": [
        "heures feries", "férié", "ferie", "public holiday hours", "holiday worked",
        "jf travaille", "horas feriado",
    ],

    # ────────────────── Colonnes « full name » (utile si le fichier n’a qu’une seule colonne pour nom+prenom)
    "full_name": [
        "nom prenom", "nom et prenom", "nom & prenom", "full name", "employee name",
        "salarié", "salarie", "agent", "collaborateur", "nombre completo", "nombre y apellido",
        "name", "employee",
    ],
}
def _detect_columns(headers: Dict[str, str]) -> Dict[str, str]:
    """
    headers: dict { 'A': 'Intitulé', ... }
    Retourne un mapping { target_key -> column_letter } avec :
      - match exact (après _normalize)
      - détection directe par codes paie (010, 020, 030, 050, 090, 091, 140, 462, 771, 777, 780)
      - fuzzy match via rapidfuzz (token_set_ratio / partial_ratio)
      - résolution des collisions par score (méthode > score)
      - règle spéciale: si 'nom' et 'prenom' tombent sur la même colonne → préférer 'full_name'
    """
    if not headers:
        return {}

    # Vue normalisée des entêtes : {col -> texte normalisé}
    header_norm: Dict[str, str] = {col: _normalize(txt or "") for col, txt in headers.items()}
    header_items: List[Tuple[str, str]] = list(header_norm.items())  # [(col, norm_text), ...]

    # Détection par codes paie (regex à bornes numériques)
    code_to_target = {
        "010": "hrs_norm_010",
        "020": "hs_25_020",
        "030": "hs_50_030",
        "050": "hs_100_050",
        "090": "heures_jour_ferie_chome_090",
        "091": "hrs_feries_091",
        "140": "rappel_hrs_norm_140",
        "462": "prime_astreinte_462",
        "771": "ind_panier_771",
        "777": "ind_transport_777",
        "780": "ind_deplacement_780",
    }
    # Pré-compile les regex de codes avec vraies bornes
    code_regex = {code: re.compile(rf"(?<!\d){code}(?!\d)") for code in code_to_target.keys()}

    # Conteneurs de scores: target -> (col, score, method)
    # method ∈ {"exact", "code", "fuzzy"} ; exact > code > fuzzy à score égal
    ranked: Dict[str, Tuple[str, float, str]] = {}
    # Occupation colonnes: col -> (target, score, method)
    col_taken: Dict[str, Tuple[str, float, str]] = {}

    def _try_assign(target: str, col: str, score: float, method: str):
        """
        Assigne (ou réassigne) target->col si meilleur que l'existant.
        Résout les collisions en gardant la meilleure paire selon (méthode, score).
        """
        if not col:
            return
        prev = ranked.get(target)
        if prev is None or _is_better(method, score, prev[2], prev[1]):
            ranked[target] = (col, score, method)

        # Collision de colonne : si une autre target possède déjà cette colonne, on arbitre
        other = col_taken.get(col)
        if other is None:
            col_taken[col] = (target, score, method)
        else:
            other_target, other_score, other_method = other
            # Qui garde la colonne ?
            if _is_better(method, score, other_method, other_score):
                # La nouvelle paire gagne la colonne
                col_taken[col] = (target, score, method)
                # L'ancienne target perd la colonne si elle pointait dessus
                if ranked.get(other_target, (None, 0, ""))[0] == col:
                    ranked.pop(other_target, None)
            else:
                # L'ancienne paire garde la colonne, on annule la tentative pour cette target si elle y pointait
                if ranked.get(target, (None, 0, ""))[0] == col:
                    ranked.pop(target, None)

    def _is_better(method_a: str, score_a: float, method_b: str, score_b: float) -> bool:
        """Compare (méthode, score). Priorité: exact > code > fuzzy ; à méthode égale, le score tranche."""
        rank = {"exact": 3, "code": 2, "fuzzy": 1}
        ra, rb = rank.get(method_a, 0), rank.get(method_b, 0)
        if ra != rb:
            return ra > rb
        return score_a > score_b

    # 1) Passe exact & codes paie
    for col, raw_norm in header_items:
        # Codes paie explicites
        for code, target in code_to_target.items():
            if code_regex[code].search(raw_norm):
                _try_assign(target, col, 95.0, "code")

        # Match exact vs TARGET_SYNONYMS (ou vs nom de clé)
        for target, syns in TARGET_SYNONYMS.items():
            if target in ranked:  # déjà un match exact pour ce target
                continue
            norm_targets = [_normalize(s) for s in syns] + [_normalize(target)]
            if raw_norm in norm_targets:
                _try_assign(target, col, 100.0, "exact")

    # 2) Passe fuzzy pour les cibles restantes
    for target, syns in TARGET_SYNONYMS.items():
        if target in ranked:
            continue

        syns_norm = [_normalize(s) for s in syns] + [_normalize(target)]
        best_col, best_score = None, 0.0

        for col, htxt in header_items:
            # calcule un score fuzzy robuste : max(token_set, partial)
            try:
                score = max(
                    fuzz.token_set_ratio(htxt, s) for s in syns_norm
                )
                # petit bonus si partial_ratio dépasse aussi un seuil
                pr = max(fuzz.partial_ratio(htxt, s) for s in syns_norm)
                score = max(score, pr * 0.98)  # léger lissage
            except Exception:
                continue

            if score > best_score:
                best_score, best_col = score, col

        if best_col is not None and best_score >= 80:
            _try_assign(target, best_col, float(best_score), "fuzzy")

    # 3) Règles simples de collisions métier
    # - Si même colonne pour nom & prénom => préfère mapper 'full_name'
    nom_col = ranked.get("nom", (None, 0.0, ""))[0]
    prenom_col = ranked.get("prenom", (None, 0.0, ""))[0]
    if nom_col and prenom_col and nom_col == prenom_col:
        # libère nom/prenom et place full_name
        ranked.pop("nom", None)
        ranked.pop("prenom", None)
        # N'affecte pas si full_name existe déjà (et diffère)
        fl = ranked.get("full_name", (None, 0.0, ""))[0]
        if not fl:
            # score médian, méthode 'fuzzy' (ne pas prétendre à un exact)
            _try_assign("full_name", nom_col, 85.0, "fuzzy")

    # Sortie finale: target -> column_letter
    detected: Dict[str, str] = {}
    for target, (col, _score, _method) in ranked.items():
        if col:
            detected[target] = col
    return detected

# ─────────────────────────── Legacy endpoint (utilisé dans Bubble actuellement)
@app.post("/parse-excel-upload")
async def parse_excel_upload(
    file: UploadFile = File(...),
    holiday_dates: Optional[str] = Form(default=None),
    rules: Optional[str] = Form(default=None),
):
    filename = (getattr(file, "filename", "") or "").lower()
    if not filename or not (filename.endswith(".xlsx") or filename.endswith(".xlsm")):
        logger.info(
            "parse-excel-upload | rejected filename=%s mime=%s",
            getattr(file, "filename", None), getattr(file, "content_type", None)
        )
        raise HTTPException(status_code=400, detail="Only .xlsx/.xlsm are supported")
    logger.info(
        "parse-excel-upload | filename=%s mime=%s",
        file.filename, getattr(file, "content_type", None)
    )

    # Lecture + garde-fous taille
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File too large")

    try:
        wb = load_workbook(filename=BytesIO(content), data_only=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read Excel: {e}")

    # Choix feuille + entêtes robustes
    ws, header_row_index = _pick_best_sheet(wb)
    headers = _extract_headers(ws, header_row_index)
    if not headers:
        raise HTTPException(status_code=400, detail="No headers detected on the selected sheet.")
    detected = _detect_columns(headers)

    logger.info(
        "parse-excel-upload | sheet=%s header_row=%s headers_sample=%s",
        ws.title, header_row_index, list(headers.values())[:10]
    )

    rules_dict = _parse_rules(rules)
    holidays = _parse_holidays(holiday_dates)

    # Helpers locaux
    def _val_at(r: int, key: str):
        """Lit la valeur à la ligne r pour la clé détectée 'key' (via 'detected')."""
        col = detected.get(key)
        if not col:
            return None
        try:
            return ws[f"{col}{r}"].value
        except Exception:
            return None

    def _hours_at(r: int, col_letter: Optional[str]) -> Optional[float]:
        """Lit et convertit les heures à la ligne r pour la colonne 'col_letter'."""
        if not col_letter:
            return None
        try:
            return _parse_hours_to_decimal(ws[f"{col_letter}{r}"].value)
        except Exception:
            return None

    # Colonnes utiles (fallback entre équivalents)
    COL_MATRICULE = detected.get("matricule") or detected.get("matricule_salarie")
    COL_NOM = detected.get("nom")
    COL_PRENOM = detected.get("prenom")
    COL_FULLNAME = detected.get("full_name")
    COL_CIN = detected.get("cin")
    COL_DATE = detected.get("date")

    # Heures normales : accepte 'heures_norm' (timesheet) ou code paie 'hrs_norm_010'
    COL_HN = detected.get("heures_norm") or detected.get("hrs_norm_010")
    # Heures sup décomposées
    COL_HS25 = detected.get("hs_25") or detected.get("hs_25_020")
    COL_HS50 = detected.get("hs_50") or detected.get("hs_50_030")
    COL_HS100 = detected.get("hs_100") or detected.get("hs_100_050")
    # Heures fériées (travaillées)
    COL_HFER = detected.get("hs_feries") or detected.get("hrs_feries_091")
    # Absence (pour 'demi_journee' heuristique)
    COL_ABS = detected.get("absence")
    # Jours (compteur direct côté client)
    COL_NBJT = detected.get("nb_jt")

    rows: List[Dict[str, Any]] = []
    start = max(1, header_row_index + 1)
    max_row = getattr(ws, "max_row", start - 1)
    max_col = getattr(ws, "max_column", 0)
    end = max_row

    # Heuristique de split full name
    def split_full_name(fn: Any) -> Tuple[Optional[str], Optional[str]]:
        """Heuristique : fichiers 'NOM PRENOM' ou 'Prenom Nom' → on sépare."""
        if not fn:
            return None, None
        s = str(fn).strip()
        if not s:
            return None, None
        parts = [p for p in re.split(r"\s+", s) if p]
        if len(parts) == 1:
            return parts[0], None
        prenom = parts[-1]
        nom = " ".join(parts[:-1])
        return nom, prenom

    if end < start:
        # Feuille sans données utiles sous l'entête
        return {
            "rules_used": rules_dict,
            "holiday_dates": holidays,
            "rows": rows,
            "rows_count": 0,
        }

    for r in range(start, end + 1):
        # Identité
        v_matricule = _val_at(r, "matricule") or _val_at(r, "matricule_salarie") or (
            ws[f"{COL_MATRICULE}{r}"].value if COL_MATRICULE else None
        )
        v_cin = _val_at(r, "cin")

        v_nom = _val_at(r, "nom")
        v_prenom = _val_at(r, "prenom")

        if (not v_nom or not v_prenom) and COL_FULLNAME:
            n, p = split_full_name(
                _val_at(r, "full_name") or (ws[f"{COL_FULLNAME}{r}"].value if COL_FULLNAME else None)
            )
            v_nom = v_nom or n
            v_prenom = v_prenom or p

        # Date
        v_date = _coerce_date(
            _val_at(r, "date") or (ws[f"{COL_DATE}{r}"].value if COL_DATE else None)
        )

        # Heures
        h_norm = _hours_at(r, COL_HN)
        hs25 = _hours_at(r, COL_HS25)
        hs50 = _hours_at(r, COL_HS50)
        hs100 = _hours_at(r, COL_HS100)
        hfer = _hours_at(r, COL_HFER)

        hs_normales_agg = None
        parts = [x for x in (hs25, hs50, hs100) if isinstance(x, (int, float)) and math.isfinite(x)]
        if parts:
            hs_normales_agg = round(sum(parts), 2)

        # Jours saisis (nb_jt) si dispo
        nb_jt_val_raw = None
        if COL_NBJT:
            try:
                nb_jt_val_raw = ws[f"{COL_NBJT}{r}"].value
            except Exception:
                nb_jt_val_raw = None
        nb_jt_days = _parse_days(nb_jt_val_raw)

        # Demi-journée (absence)
        abs_raw = None
        if COL_ABS:
            try:
                abs_raw = ws[f"{COL_ABS}{r}"].value
            except Exception:
                abs_raw = None
        abs_txt = (str(abs_raw).lower().strip()) if abs_raw is not None else ""
        demi_j = True if ("demi" in abs_txt or "1/2" in abs_txt or "half" in abs_txt or abs_txt in {"am", "pm"}) else None

        # Conversions heures ↔ jours
        jours_calcules: Optional[float] = None
        heures_calculees: Optional[float] = None

        # Si uniquement heures → calcule jours
        if (h_norm is not None) and (nb_jt_days is None or nb_jt_days == 0):
            jours_calcules = _hours_to_days(h_norm, rules_dict)

        # Si uniquement jours → calcule heures
        if (nb_jt_days is not None) and (h_norm is None or h_norm == 0):
            heures_calculees = _days_to_hours(nb_jt_days, rules_dict)

        # Fallback demi-journée si rien fourni
        if (h_norm is None) and (nb_jt_days is None) and demi_j:
            jours_calcules = 0.5
            heures_calculees = _days_to_hours(0.5, rules_dict)

        # Raw body (aperçu ligne)
        raw_vals = []
        try:
            for row_tuple in ws.iter_rows(min_col=1, max_col=max_col, min_row=r, max_row=r, values_only=True):
                raw_vals = list(row_tuple)
        except Exception:
            pass
        raw_body_text = " | ".join([str(x) for x in raw_vals if x is not None])[:1000]

        rows.append({
            "matricule": v_matricule,
            "nom": v_nom,
            "prenom": v_prenom,
            "cin": v_cin,
            "date": v_date,
            "heures_travaillees_decimal": h_norm,
            "hs_normales": hs_normales_agg,
            "hs_ferie": hfer,
            "nb_jt": nb_jt_days,               # jours saisis
            "jours_calcules": jours_calcules,  # si seulement heures
            "heures_calculees": heures_calculees,  # si seulement jours
            "demi_journee": demi_j,
            "raw_body_text": raw_body_text,
        })

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
    name = (getattr(file_template, "filename", "") or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xlsm")):
        logger.info(
            "template-intake | rejected filename=%s mime=%s client_id=%s",
            getattr(file_template, "filename", None),
            getattr(file_template, "content_type", None),
            client_id,
        )
        raise HTTPException(status_code=400, detail="Only .xlsx/.xlsm are supported")
    logger.info(
        "template-intake | filename=%s mime=%s client_id=%s",
        file_template.filename, getattr(file_template, "content_type", None), client_id
    )

    # Lecture + garde-fous taille
    try:
        content = await file_template.read()
        if not content:
            raise HTTPException(status_code=400, detail="Empty file")
        if len(content) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="File too large")
        wb = load_workbook(filename=BytesIO(content), data_only=True)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read Excel: {e}")

    # Choisir la meilleure feuille + ligne d'entêtes (heuristique robuste)
    ws, header_row_index = _pick_best_sheet(wb)
    headers_dict = _extract_headers(ws, header_row_index)
    if not headers_dict:
        raise HTTPException(status_code=400, detail="No headers detected on the selected sheet.")
    logger.info(
        "template-intake | sheet=%s header_row=%s headers_sample=%s",
        ws.title, header_row_index, list(headers_dict.values())[:10]
    )

    # Colonnes attendues côté template paie
    expected = [
        "matricule_salarie","matricule_client","nombre","nom","prenom","num_contrat","num_avenant",
        "date_debut","date_fin","service","nb_jt","nb_ji","nb_cp_280","nb_sans_solde","nb_jf","tx_sal",
        "hrs_norm_010","rappel_hrs_norm_140","hs_25_020","hs_50_030","hs_100_050","hrs_feries_091",
        "prime_astreinte_462","ind_panier_771","ind_transport_777","ind_deplacement_780",
        "heures_jour_ferie_chome_090","observations","fin_mission"
    ]

    # 1) Mapping exact (normalisé) + 2) Fuzzy via _detect_columns
    header_norm: Dict[str, str] = {col: _normalize(txt) for col, txt in headers_dict.items()}
    detected_all = _detect_columns(headers_dict)

    column_map: Dict[str, str] = {}
    # a) ce que la détection a trouvé
    for key in expected:
        if key in detected_all:
            column_map[key] = detected_all[key]

    # b) égalité stricte sur intitulé normalisé (fallback exact)
    for key in expected:
        if key in column_map:
            continue
        key_norm = _normalize(key)
        for col, htxt in header_norm.items():
            if htxt == key_norm:
                column_map[key] = col
                break

    # c) alias de pont (fallback supplémentaire)
    alias_map = {
        "matricule_salarie": ["matricule"],
        "hrs_norm_010":      ["heures_norm"],
        "hs_25_020":         ["hs_25"],
        "hs_50_030":         ["hs_50"],
        "hs_100_050":        ["hs_100"],
        "hrs_feries_091":    ["hs_feries"],
    }
    for target, aliases in alias_map.items():
        if target in column_map:
            continue
        for a in aliases:
            col = detected_all.get(a)
            if col:
                column_map[target] = col
                break

    def _read_cell(col_letter: Optional[str], row: int):
        if not col_letter:
            return None
        try:
            return ws[f"{col_letter}{row}"].value
        except Exception:
            return None

    # Roster (aperçu) : on évite les lignes totalement vides
    roster: List[Dict[str, Any]] = []
    col_matsal = column_map.get("matricule_salarie")
    col_matcli = column_map.get("matricule_client")
    col_nom    = column_map.get("nom")
    col_prenom = column_map.get("prenom")
    col_srv    = column_map.get("service")

    start = header_row_index + 1
    PREVIEW_ROSTER_MAX = 100
    end = min(getattr(ws, "max_row", start - 1), start + PREVIEW_ROSTER_MAX)

    for r in range(start, end + 1):
        row_obj = {
            "row_index_excel": r,
            "matricule_salarie": _read_cell(col_matsal, r),
            "matricule_client": _read_cell(col_matcli, r),
            "nom": _read_cell(col_nom, r),
            "prenom": _read_cell(col_prenom, r),
            "service": _read_cell(col_srv, r),
        }
        # skip si toutes les valeurs affichées sont None/vides
        if all((v is None or (isinstance(v, str) and v.strip() == "")) for k, v in row_obj.items() if k != "row_index_excel"):
            continue
        roster.append(row_obj)

    missing = [k for k in expected if k not in column_map]

    # ID de template déterministe (nom fichier + feuille + header_row)
    template_id = f"tpl_{hex(abs(hash((name, ws.title, header_row_index))))[2:12]}"

    return {
        "template_id": template_id,
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
    fname = (getattr(file_timesheet, "filename", "") or "").lower()
    if not fname or not (fname.endswith(".xlsx") or fname.endswith(".xlsm")):
        logger.info(
            "timesheet-intake | rejected filename=%s mime=%s",
            getattr(file_timesheet, "filename", None),
            getattr(file_timesheet, "content_type", None),
        )
        raise HTTPException(status_code=400, detail="Only .xlsx/.xlsm are supported")
    logger.info(
        "timesheet-intake | filename=%s mime=%s",
        file_timesheet.filename, getattr(file_timesheet, "content_type", None)
    )

    # Lecture + garde-fous
    try:
        content = await file_timesheet.read()
        if not content:
            raise HTTPException(status_code=400, detail="Empty file")
        if len(content) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="File too large")
        wb = load_workbook(filename=BytesIO(content), data_only=True)

        # feuille + ligne d'entêtes robustes
        ws, header_row_index = _pick_best_sheet(wb)
        headers_dict = _extract_headers(ws, header_row_index)
        if not headers_dict:
            raise HTTPException(status_code=400, detail="No headers detected on the selected sheet.")
        logger.info(
            "timesheet-intake | sheet=%s header_row=%s headers_sample=%s",
            ws.title, header_row_index, list(headers_dict.values())[:10]
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read Excel: {e}")

    rules_dict = _parse_rules(rules)
    holidays = set(_parse_holidays(holiday_dates))

    # Détection auto des colonnes (fuzzy)
    base_detected = _detect_columns(headers_dict)

    # Ponts d’alias
    def _resolve_detected(d: Dict[str, str]) -> Dict[str, str]:
        out = dict(d)
        alias_bridge = {
            "heures_norm": ["hrs_norm_010"],
            "hs_25": ["hs_25_020"],
            "hs_50": ["hs_50_030"],
            "hs_100": ["hs_100_050"],
            "hs_feries": ["hrs_feries_091"],
            "date": ["date_debut"],
        }
        for target, aliases in alias_bridge.items():
            if target not in out:
                for a in aliases:
                    if a in out:
                        out[target] = out[a]
                        break
        return out

    detected = _resolve_detected(base_detected)

    # Si la date reste introuvable → heuristique
    if "date" not in detected and headers_dict:
        start_check = header_row_index + 1
        end_check = min(getattr(ws, "max_row", start_check - 1), start_check + 10)
        best_col, best_hits = None, 0
        for col_letter in headers_dict.keys():
            hits = 0
            for r in range(start_check, end_check + 1):
                try:
                    if _coerce_date(ws[f"{col_letter}{r}"].value):
                        hits += 1
                except Exception:
                    continue
            if hits > best_hits:
                best_hits, best_col = hits, col_letter
        sample_len = max(0, end_check - start_check + 1)
        if best_col and best_hits >= max(2, sample_len // 3):
            detected["date"] = best_col

    # Helpers locaux
    def _val_at(r: int, col_key: str):
        col_letter = detected.get(col_key)
        if not col_letter:
            return None
        try:
            return ws[f"{col_letter}{r}"].value
        except Exception:
            return None

    def _split_full_name(fn: Any) -> Tuple[Optional[str], Optional[str]]:
        """Heuristique : fichiers 'NOM PRENOM' ou 'Prenom Nom' → on sépare."""
        if not fn:
            return None, None
        s = str(fn).strip()
        if not s:
            return None, None
        parts = [p for p in re.split(r"\s+", s) if p]
        if len(parts) == 1:
            return parts[0], None
        prenom = parts[-1]
        nom = " ".join(parts[:-1])
        return nom, prenom

    # Preview (5 lignes), bornes sûres
    preview_rows: List[Dict[str, Any]] = []
    start = header_row_index + 1
    end = min(getattr(ws, "max_row", start - 1), start + 4)
    if end < start:
        return {
            "sheet_name": ws.title,
            "header_row_index": header_row_index,
            "detected_columns": detected,
            "preview_rows": [],
            "warnings": ["Aucune ligne de données trouvée sous la ligne d'entêtes."],
            "rules_used": rules_dict,
            "holiday_dates": list(holidays),
        }

    for r in range(start, end + 1):
        absence_raw = _val_at(r, "absence")

        demi_j = None
        if isinstance(absence_raw, str):
            ar = absence_raw.strip().lower()
            if (
                "demi" in ar
                or ar in {"am", "pm", "1/2", "0.5", "demi-j", "demi journee", "demi-journée"}
                or ar.replace(",", ".") == "0.5"
            ):
                demi_j = True

        # Date & fériés
        row_date = _coerce_date(_val_at(r, "date"))
        is_holiday = (row_date in holidays) if row_date else False

        # Identité avec fallback full_name → (nom, prenom)
        nom_v = _val_at(r, "nom")
        prenom_v = _val_at(r, "prenom")
        if (not nom_v or not prenom_v) and ("full_name" in detected):
            n, p = _split_full_name(_val_at(r, "full_name"))
            nom_v = nom_v or n
            prenom_v = prenom_v or p

        # Heures et jours (nb_jt)
        h_norm = _parse_hours_to_decimal(_val_at(r, "heures_norm"))
        nb_jt_val = _parse_days(_val_at(r, "nb_jt"))

        # Conversions heures ↔ jours
        jours_calc: Optional[float] = None
        heures_from_days: Optional[float] = None

        if (h_norm is not None) and (nb_jt_val is None or nb_jt_val == 0):
            jours_calc = _hours_to_days(h_norm, rules_dict)

        if (nb_jt_val is not None) and (h_norm is None or h_norm == 0):
            heures_from_days = _days_to_hours(nb_jt_val, rules_dict)

        # Fallback demi-journée si rien de fourni
        if (h_norm is None) and (nb_jt_val is None) and demi_j:
            nb_jt_val = 0.5
            jours_calc = 0.5
            heures_from_days = _days_to_hours(0.5, rules_dict)

        preview_rows.append({
            "row_index_excel": r,
            "matricule": _val_at(r, "matricule"),
            "cin": _val_at(r, "cin"),
            "nom": nom_v,
            "prenom": prenom_v,
            "service": _val_at(r, "service"),
            "date": row_date,
            "heures_norm_dec": h_norm,
            "hs_25_dec": _parse_hours_to_decimal(_val_at(r, "hs_25")),
            "hs_50_dec": _parse_hours_to_decimal(_val_at(r, "hs_50")),
            "hs_100_dec": _parse_hours_to_decimal(_val_at(r, "hs_100")),
            "hs_feries_dec": _parse_hours_to_decimal(_val_at(r, "hs_feries")),
            "nb_jt": nb_jt_val,                 # <-- lecture directe des jours saisis
            "jours_calc": jours_calc,           # <-- calcul si seulement heures
            "heures_from_days": heures_from_days,  # <-- calcul si seulement jours
            "demi_journee": demi_j,
            "is_holiday": is_holiday,
            "observations": _val_at(r, "observations"),
        })

    # Warnings
    warnings: List[str] = []
    if not detected:
        warnings.append("Aucune colonne n’a été reconnue automatiquement (en-têtes très atypiques).")
    for k in ("date", "heures_norm"):
        if k not in detected:
            warnings.append(f"Colonne importante non détectée : {k}")
    if detected.get("nom") and detected.get("prenom") and detected["nom"] == detected["prenom"]:
        warnings.append("Une seule colonne semble contenir NOM & PRÉNOM : les deux clés pointent sur la même colonne.")
    if "full_name" in detected and (("nom" not in detected) or ("prenom" not in detected)):
        warnings.append("Colonne 'full_name' détectée sans 'nom'/'prenom' dédiés : split heuristique appliqué dans l’aperçu.")

    def _column_has_hours(col_key: str) -> bool:
        col = detected.get(col_key)
        if not col:
            return False
        s0 = header_row_index + 1
        e0 = min(getattr(ws, "max_row", s0 - 1), s0 + 10)
        ok = 0
        for rr in range(s0, e0 + 1):
            try:
                if _parse_hours_to_decimal(ws[f"{col}{rr}"].value) is not None:
                    ok += 1
            except Exception:
                continue
        return ok >= 2

    for ck in ("heures_norm", "hs_25", "hs_50", "hs_100", "hs_feries"):
        if ck in detected and not _column_has_hours(ck):
            warnings.append(f"La colonne '{ck}' ne semble pas contenir des heures interprétables (échantillon).")

    return {
        "sheet_name": ws.title,
        "header_row_index": header_row_index,
        "detected_columns": detected,
        "preview_rows": preview_rows,
        "warnings": warnings,
        "rules_used": rules_dict,
        "holiday_dates": list(holidays),
    }


