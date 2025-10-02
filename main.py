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
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ia-parse-excel")

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
      - CSV/texte: '2025-01-01, 2025-05-01' ou séparé par ';' ou espaces multiples
      - Nombres Excel (sérialisés) si présents
    Retourne une liste ISO 'YYYY-MM-DD'.
    """
    if not holiday_raw:
        return []
    holidays: List[str] = []
    try:
        # Essai JSON liste
        data = json.loads(holiday_raw)
        if isinstance(data, list):
            items = data
        else:
            # Si ce n’est pas une liste JSON, on tombera dans le except plus bas
            raise ValueError("Not a JSON list")
        for x in items:
            iso = _coerce_date(x)
            if iso:
                holidays.append(iso)
        return holidays
    except Exception:
        # Parse en CSV/texte
        # Remplace ; nouvelle ligne par des virgules, split, trim
        txt = str(holiday_raw).replace("\n", ",").replace(";", ",")
        parts = [p.strip() for p in txt.split(",") if p.strip()]
        for p in parts:
            iso = _coerce_date(p)
            if iso:
                holidays.append(iso)
        return holidays


def _coerce_bool(x: Any) -> Optional[bool]:
    """
    True si: true/yes/1/oui/o/y/vrai
    False si: false/no/0/non/n/faux
    """
    if isinstance(x, bool):
        return x
    if x is None:
        return None
    s = str(x).strip().lower()
    true_set = {"yes", "true", "1", "oui", "o", "y", "vrai"}
    false_set = {"no", "false", "0", "non", "n", "faux"}
    if s in true_set:
        return True
    if s in false_set:
        return False
    return None


def _excel_serial_to_date(n: float) -> Optional[str]:
    """
    Convertit un nombre Excel (système 1900) en 'YYYY-MM-DD'.
    Excel 'jour 1' = 1899-12-31, mais la conversion usuelle est base 1899-12-30.
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
    Gère: datetime/date, formats FR/EN/variations, nombres Excel sérialisés.
    """
    if x is None:
        return None
    if isinstance(x, (datetime, date)):
        # Si datetime, on garde la date seulement.
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
    if re.fullmatch(r"\d{1,5}", s):
        iso = _excel_serial_to_date(int(s))
        if iso:
            return iso

    # Essais de formats
    candidates = (
        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y",
        "%d.%m.%Y", "%Y/%m/%d"
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
    """
    if s is None:
        return None
    if isinstance(s, (int, float)):
        # Déjà numérique → on suppose des heures décimales
        return round(float(s), 2)

    txt = str(s).strip().lower()
    if not txt:
        return None
    # nettoyages de base
    txt = txt.replace(" ", "")
    txt = txt.replace(",", ".")  # 7,5 -> 7.5

    # cas "90m" / "90min" / "90mn"
    m_match = re.fullmatch(r"(\d+)\s*(m|min|mn)$", txt)
    if m_match:
        minutes = int(m_match.group(1))
        return round(minutes / 60.0, 2)

    # cas "1h30" / "1h30m" / "1h30mn" / "1h"
    h_match = re.fullmatch(r"(\d+)\s*h(?:\s*(\d{1,2}))?(?:m|mn)?$", txt)
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
            # garde uniquement les chiffres pour le mm (ex: "30mn")
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
# --- Upload guard ------------------------------------------------------------
MAX_UPLOAD_BYTES = 15 * 1024 * 1024  # 15 MB

# --- Helpers entêtes/feuilles -----------------------------------------------
def _row_values(ws, row_index: int):
    """Retourne les valeurs de la ligne (sans casser si la ligne dépasse max_column)."""
    return [cell.value for cell in ws[row_index]]

def _count_nonempty(vals) -> int:
    """Compte le nombre de cellules non vides (après trim)."""
    n = 0
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        n += 1
    return n

def _is_numeric_like(v) -> bool:
    """Heuristique: True si la cellule ressemble à un nombre."""
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return True
    s = str(v).strip().replace(",", ".")
    if s == "":
        return False
    try:
        float(s)
        return True
    except Exception:
        return False

def _is_date_like_text(s: str) -> bool:
    if not isinstance(s, str):
        return False
    s = s.strip().lower()
    date_patterns = [
        r"^\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4}$",
        r"^\d{4}[\/\-.]\d{1,2}[\/\-.]\d{1,2}$",
    ]
    import re as _re
    return any(_re.match(p, s) for p in date_patterns)


def _header_keywords_vocab() -> set:
    """
    Construit un vocabulaire de mots d'entêtes (normalisés) à partir de TARGET_SYNONYMS si disponible.
    Ne casse pas si TARGET_SYNONYMS n'est pas encore défini au moment de l'import.
    """
    vocab = set()
    try:
        # Import local pour éviter la dépendance d’ordre de définition
        mapping = TARGET_SYNONYMS  # noqa: F821 (peut ne pas être défini lors du parsing, mais le sera à l'exécution)
        for k, arr in mapping.items():
            vocab.add(_normalize(k))
            for s in arr:
                vocab.add(_normalize(s))
    except Exception:
        # Pas grave: vocab vide => score basé uniquement sur non-vides/textes
        pass
    # Mots génériques utiles même sans TARGET_SYNONYMS
    for kw in ("date", "jour", "nom", "prénom", "prenom", "matricule", "service",
               "heures", "hs", "absence", "observations", "contrat", "avenant"):
        vocab.add(_normalize(kw))
    return vocab

def _header_score(vals) -> float:
    """
    Score une ligne pour estimer si c'est une ligne d'entêtes.
    +2 pour chaque cellule texte non vide
    +3 si le texte matche un mot du vocabulaire d'entêtes
    -1 pour chaque cellule numérique
    -1 pour chaque cellule qui ressemble à une date (texte)
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
                # inclusion faible (ex: "heures normales" contient "heures")
                if any(w in norm for w in vocab):
                    score += 1.5
            if _is_date_like_text(txt):
                score -= 1.0
        else:
            # nombres bruts → peu probable pour des entêtes
            if _is_numeric_like(v):
                score -= 1.0
    # bonus léger si beaucoup de non-vides
    nonempty = _count_nonempty(vals)
    score += min(nonempty, 10) * 0.2
    return score

def _best_header_row(ws, max_scan: int = 25) -> int:
    """
    Scanne les 'max_scan' premières lignes et choisit celle qui ressemble le plus
    à une ligne d'en-têtes (score heuristique).
    """
    best_row, best_score = 1, float("-inf")
    last_row = min(ws.max_row, max_scan)
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
    for idx, cell in enumerate(ws[header_row_index], start=1):
        val = cell.value
        if val is None:
            continue
        txt = str(val).replace("\n", " ").strip()
        if not txt:
            continue
        headers[_gcl(idx)] = txt
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
    if header_row_index + 1 <= ws.max_row:
        candidates.append((header_row_index + 1, _headers_at(ws, header_row_index + 1)))
    if header_row_index - 1 >= 1:
        candidates.append((header_row_index - 1, _headers_at(ws, header_row_index - 1)))

    # Choisit celui avec le plus d’entrées, puis avec le score d’entête le plus élevé
    def _hdr_quality(item):
        r, mapping = item
        vals = list(mapping.values())
        return (len(mapping), _header_score(vals))

    r_best, mapping_best = max(candidates, key=_hdr_quality)
    return mapping_best if mapping_best else base

def _pick_best_sheet(wb):
    """
    Parcourt toutes les feuilles et choisit celle dont la 'meilleure ligne d'entêtes'
    a le score heuristique le plus élevé.
    Retourne (worksheet, header_row_index).
    """
    best_ws = wb.active
    best_row_idx = _best_header_row(best_ws)
    best_vals = _row_values(best_ws, best_row_idx)
    best_score = _header_score(best_vals) if best_vals else float("-inf")

    for ws in wb.worksheets:
        r = _best_header_row(ws)
        vals = _row_values(ws, r)
        sc = _header_score(vals) if vals else float("-inf")
        if sc > best_score:
            best_ws, best_row_idx, best_score = ws, r, sc

    # Garde-fou si la feuille candidate est vide ou quasi vide
    try:
        if best_ws.max_row < 1 or best_ws.max_column < 1:
            return best_ws, 1
    except Exception:
        return best_ws, 1

    return best_ws, best_row_idx


def _normalize(s: str) -> str:
    """
    Normalisation robuste:
      - trim + lower
      - remplace '_' par espace
      - supprime les accents (NFKD)
      - compacte les espaces multiples
    """
    import unicodedata as _ud
    import re as _re
    s = "" if s is None else str(s)
    s = _ud.normalize("NFKD", s)
    s = "".join(c for c in s if not _ud.combining(c))
    s = s.replace("_", " ").lower().strip()
    s = _re.sub(r"\s+", " ", s)
    return s


# --- Synonymes d'en-têtes attendues (FR/EN/var.) -----------------------------
TARGET_SYNONYMS = {
    # ────────────────── Identités / clés
    "matricule_salarie": [
        "matricule salarie", "matricule salarié", "matricule", "employee id", "emp id",
        "id salarie", "id salarié", "code salarie", "code salarié", "code agent",
        "badge", "pernr", "personnel number", "matricule rh"
    ],
    "matricule_client": [
        "matricule client", "client id", "code client", "code chantier", "code site",
        "site id", "affectation", "code affectation", "cost center", "centre de cout", "centre de coût"
    ],
    "matricule": [
        "matricule", "employee id", "empid", "id", "code", "code salarie", "code salarié", "badge", "pernr"
    ],
    "cin": [
        "cin", "c.i.n", "id card", "identity", "identity card", "numero cin", "num cin", "n cin",
        "id national", "national id", "piece identite", "pièce identité"
    ],

    # ────────────────── Référentiel template (identité/contrat)
    "nombre": ["nombre", "nb", "qty", "quantité", "quantite"],
    "nom": ["nom", "last name", "surname", "family name", "patronyme"],
    "prenom": ["prenom", "prénom", "first name", "given name"],
    "num_contrat": [
        "n° contrat", "num contrat", "numero contrat", "contract no", "contract number",
        "contrat", "contrat no", "contrat n", "id contrat"
    ],
    "num_avenant": [
        "n° avenant", "num avenant", "avenant", "amendment no", "amendement", "avenant no", "avenant n"
    ],
    "date_debut": [
        "date debut", "date début", "start date", "date debut contrat", "debut contrat", "date d debut"
    ],
    "date_fin": [
        "date fin", "end date", "date fin contrat", "fin contrat", "date d fin", "date de fin"
    ],
    "service": [
        "service", "departement", "département", "department", "site", "unité", "unite",
        "direction", "pole", "secteur", "equipe", "équipe"
    ],

    # ────────────────── Jours / compteurs
    "nb_jt": [
        "nb jt", "jours travailles", "jours travaillés", "jours", "nb jours",
        "jours presents", "jours présence", "jours presence"
    ],
    "nb_ji": [
        "nb ji", "jours injustifies", "jours injustifiés", "ji", "abs injustifiee", "absence injustifiee",
        "absence injustifiée", "abs non justifiee", "non justifie"
    ],
    "nb_cp_280": [
        "280 - nb cp", "cp", "conges payes", "congés payés", "paid leave days", "conge paye",
        "jours cp", "nb cp"
    ],
    "nb_sans_solde": [
        "sans solde", "conge sans solde", "css", "unpaid leave", "conge non paye", "non paye", "non payé"
    ],
    "nb_jf": [
        "nb jf", "jours feries", "jours fériés", "public holidays", "jf", "jours ferie", "jour ferié"
    ],
    "tx_sal": [
        "tx sal", "taux sal", "taux salarié", "salary rate", "taux horaire", "th", "tarif horaire", "rate"
    ],

    # ────────────────── Heures / primes (codes paie)
    "hrs_norm_010": [
        "010 - hrs norm", "heures normales", "hrs normales", "heure normal", "h. normal",
        "nb heures", "heures", "heures de base", "base hours", "normal hours"
    ],
    "rappel_hrs_norm_140": [
        "140 - rappel hrs norm", "rappel heures normales", "rappel 140", "rappel salaire",
        "rappel base", "backpay normal", "regularization normal hours"
    ],
    "hs_25_020": [
        "020 - hs 25%", "heures sup 25", "hs 25", "maj 25", "hs 25 pourcent", "overtime 25", "ot 25"
    ],
    "hs_50_030": [
        "030 - hs 50%", "heures sup 50", "hs 50", "maj 50", "hs 50 pourcent", "overtime 50", "ot 50"
    ],
    "hs_100_050": [
        "050 - hs 100%", "heures sup 100", "hs 100", "maj 100", "hs 100 pourcent", "overtime 100", "ot 100"
    ],
    "hrs_feries_091": [
        "091 - hrs feries", "heures feries", "heures fériées", "ferie", "férié", "jour férié",
        "heures ferie travaillees", "jf travaille", "holiday worked hours"
    ],

    "prime_astreinte_462": [
        "462 - prime astreinte", "astreinte", "prime astreinte", "on-call", "on call allowance"
    ],
    "ind_panier_771": [
        "771 - indemn. panier/mois", "panier", "indemnite panier", "prime panier", "meal allowance"
    ],
    "ind_transport_777": [
        "777 - ind.transport/mois", "transport", "indemnite transport", "prime transport", "transport allowance"
    ],
    "ind_deplacement_780": [
        "780 - indemnité deplacement", "deplacement", "indemnite deplacement", "frais deplacement",
        "travel allowance", "per diem", "perdiem"
    ],
    "heures_jour_ferie_chome_090": [
        "090 - heures jour ferie chome", "jour ferie chome", "ferie chome", "jf chome", "holiday idle hours"
    ],

    "observations": ["observations", "commentaire", "comments", "notes", "remarques", "note"],
    "fin_mission": [
        "fin mission", "fin de mission", "end of assignment", "fin contrat mission", "mission terminee", "mission terminée"
    ],

    # ────────────────── Timesheet (généraux)
    "date": [
        "date", "jour", "day", "date jour", "work date", "date travail"
    ],
    "absence": [
        "absence", "motif", "type jour", "statut jour", "am/pm", "am pm", "demi journee",
        "half day", "leave type", "reason"
    ],
    "heures_norm": [
        "heures", "heures travaillees", "heures travaillées", "nbr heures", "hours worked",
        "h. normal", "heures jour", "total heures", "temps de travail", "duree travail"
    ],
    "hs_25": ["hs 25", "heures sup 25", "maj 25", "overtime 25", "ot 25", "25%"],
    "hs_50": ["hs 50", "heures sup 50", "maj 50", "overtime 50", "ot 50", "50%"],
    "hs_100": ["hs 100", "heures sup 100", "maj 100", "overtime 100", "ot 100", "100%"],
    "hs_feries": [
        "heures feries", "férié", "ferie", "public holiday hours", "holiday worked", "jf travaille"
    ],

    # ────────────────── Colonnes « full name » (utile si le fichier n’a qu’une seule colonne pour nom+prenom)
    "full_name": [
        "nom prenom", "nom et prenom", "nom & prenom", "full name", "employee name", "salarié", "salarie",
        "agent", "collaborateur"
    ],
}


def _detect_columns(headers: Dict[str, str]) -> Dict[str, str]:
    """
    headers: dict { 'A': 'Intitulé', ... }
    Retourne un mapping { target_key -> column_letter } avec :
      - passe exact (après _normalize)
      - détection directe par codes paie (010, 020, 030, 050, 090, 091, 140, 462, 771, 777, 780)
      - fuzzy match via rapidfuzz (token_set_ratio)
      - petites règles de collision : évite d’affecter 'nom' et 'prenom' sur la même colonne,
        préfère 'full_name' si c’est clairement un nom complet.
    """
    # Prépare des vues normalisées
    header_items: List[Tuple[str, str]] = [(col, _normalize(txt)) for col, txt in headers.items()]
    
    # Détection par codes paie (si présents tels quels dans l'intitulé)
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

    detected: Dict[str, str] = {}
    chosen_cols: set = set()  # pour éviter d’assigner la même colonne à 10 cibles

    # 1) Passe exact / par codes
    for col, raw in headers.items():
        raw_txt = (raw or "")
        norm = _normalize(raw_txt)

        # Codes paie explicites (ex: "010 - HRS NORM", "020", etc.)
        for k, target in code_to_target.items():
            if k in norm.split() or norm.startswith(k) or f"{k}-" in norm or f"{k} " in norm:
                if target not in detected and col not in chosen_cols:
                    detected[target] = col
                    chosen_cols.add(col)

        # Match exact vs TARGET_SYNONYMS (ou vs nom de clé)
        for target, syns in TARGET_SYNONYMS.items():
            if target in detected:
                continue
            norm_targets = [_normalize(s) for s in syns] + [_normalize(target)]
            if norm in norm_targets:
                if col not in chosen_cols:
                    detected[target] = col
                    chosen_cols.add(col)

    # 2) Passe fuzzy pour ce qui reste
    for target, syns in TARGET_SYNONYMS.items():
        if target in detected:
            continue

        best_col, best_score = None, 0
        syns_norm = [_normalize(s) for s in syns] + [_normalize(target)]

        for col, htxt in header_items:
            if col in chosen_cols:
                continue
            # meilleur score parmi toutes les variantes de synonymes
            score = max(fuzz.token_set_ratio(htxt, s) for s in syns_norm)
            # seuils adaptatifs : 90 = quasi-certain, 80 = bon, <80 = douteux
            if score > best_score:
                best_score, best_col = score, col

        if best_col is not None and best_score >= 80:
            detected[target] = best_col
            chosen_cols.add(best_col)

    # 3) Règles simples de collisions
    # - Si même colonne pour nom & prénom => préfère mapper 'full_name'
    if detected.get("nom") and detected.get("prenom") and detected["nom"] == detected["prenom"]:
        full_col = detected["nom"]
        # on libère nom/prenom et on place 'full_name'
        detected.pop("nom", None)
        detected.pop("prenom", None)
        if "full_name" not in detected:
            detected["full_name"] = full_col

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
        logger.info("parse-excel-upload | rejected filename=%s mime=%s", file.filename, getattr(file, "content_type", None))
        raise HTTPException(status_code=400, detail="Only .xlsx/.xlsm are supported")
    logger.info("parse-excel-upload | filename=%s mime=%s", file.filename, getattr(file, "content_type", None))

    # Lecture + garde-fous taille
    content = await file.read()
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
    def val_at_row(r: int, key: str):
        col = detected.get(key)
        if not col:
            return None
        try:
            return ws[f"{col}{r}"].value
        except Exception:
            return None

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

    rows: List[Dict[str, Any]] = []
    start = header_row_index + 1
    end = ws.max_row

    for r in range(start, end + 1):
        v_matricule = val_at_row(r, "matricule") or val_at_row(r, "matricule_salarie")
        v_cin = val_at_row(r, "cin")

        v_nom = val_at_row(r, "nom")
        v_prenom = val_at_row(r, "prenom")

        if (not v_nom or not v_prenom) and COL_FULLNAME:
            n, p = split_full_name(val_at_row(r, "full_name"))
            v_nom = v_nom or n
            v_prenom = v_prenom or p

        v_date = _coerce_date(val_at_row(r, "date"))

        h_norm = _parse_hours_to_decimal(ws[f"{COL_HN}{r}"].value) if COL_HN else None
        hs25 = _parse_hours_to_decimal(ws[f"{(COL_HS25)}{r}"].value) if COL_HS25 else None
        hs50 = _parse_hours_to_decimal(ws[f"{(COL_HS50)}{r}"].value) if COL_HS50 else None
        hs100 = _parse_hours_to_decimal(ws[f"{(COL_HS100)}{r}"].value) if COL_HS100 else None
        hfer = _parse_hours_to_decimal(ws[f"{(COL_HFER)}{r}"].value) if COL_HFER else None

        hs_normales_agg = None
        parts = [x for x in (hs25, hs50, hs100) if isinstance(x, (int, float))]
        if parts:
            hs_normales_agg = round(sum(parts), 2)

        demi_j = None
        abs_txt = str(ws[f"{COL_ABS}{r}"].value).lower().strip() if COL_ABS else ""
        if "demi" in abs_txt or "1/2" in abs_txt or "half" in abs_txt:
            demi_j = True

        raw_vals = []
        try:
            for c in ws.iter_cols(min_col=1, max_col=ws.max_column, min_row=r, max_row=r, values_only=True):
                raw_vals.append(c[0])
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
    name = (file_template.filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xlsm")):
        logger.info("template-intake | rejected filename=%s mime=%s", file_template.filename, getattr(file_template, "content_type", None))
        raise HTTPException(status_code=400, detail="Only .xlsx/.xlsm are supported")
    logger.info("template-intake | filename=%s mime=%s", file_template.filename, getattr(file_template, "content_type", None))

    # Lecture + garde-fous taille
    try:
        content = await file_template.read()
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
    header_norm = {col: _normalize(txt) for col, txt in headers_dict.items()}
    detected_all = _detect_columns(headers_dict)

    column_map: Dict[str, str] = {}
    for key in expected:
        if key in detected_all:
            column_map[key] = detected_all[key]

    for key in expected:
        if key in column_map:
            continue
        key_norm = _normalize(key)
        for col, htxt in header_norm.items():
            if htxt == key_norm:
                column_map[key] = col
                break

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
            if a in detected_all:
                column_map[target] = detected_all[a]
                break

    def _read_cell(col_letter: Optional[str], row: int):
        if not col_letter:
            return None
        try:
            return ws[f"{col_letter}{row}"].value
        except Exception:
            return None

    roster = []
    col_matsal = column_map.get("matricule_salarie")
    col_matcli = column_map.get("matricule_client")
    col_nom    = column_map.get("nom")
    col_prenom = column_map.get("prenom")
    col_srv    = column_map.get("service")

    start = header_row_index + 1
    end   = min(ws.max_row, start + 100)

    for r in range(start, end + 1):
        roster.append({
            "row_index_excel": r,
            "matricule_salarie": _read_cell(col_matsal, r),
            "matricule_client": _read_cell(col_matcli, r),
            "nom": _read_cell(col_nom, r),
            "prenom": _read_cell(col_prenom, r),
            "service": _read_cell(col_srv, r),
        })

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
        logger.info("timesheet-intake | rejected filename=%s mime=%s", file_timesheet.filename, getattr(file_timesheet, "content_type", None))
        raise HTTPException(status_code=400, detail="Only .xlsx/.xlsm are supported")
    logger.info("timesheet-intake | filename=%s mime=%s", file_timesheet.filename, getattr(file_timesheet, "content_type", None))

    # Lecture + garde-fous
    try:
        content = await file_timesheet.read()
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
        end_check = min(ws.max_row, start_check + 10)
        best_col, best_hits = None, 0
        for col_letter in headers_dict.keys():
            hits = 0
            for r in range(start_check, end_check + 1):
                if _coerce_date(ws[f"{col_letter}{r}"].value):
                    hits += 1
            if hits > best_hits:
                best_hits, best_col = hits, col_letter
        sample_len = max(0, end_check - start_check + 1)
        if best_col and best_hits >= max(2, sample_len // 3):
            detected["date"] = best_col

    # Helper de lecture sécurisé
    def _val_at(r: int, col_key: str):
        col_letter = detected.get(col_key)
        if not col_letter:
            return None
        try:
            return ws[f"{col_letter}{r}"].value
        except Exception:
            return None

    # Preview (5 lignes)
    preview_rows: List[Dict[str, Any]] = []
    start = header_row_index + 1
    end = min(ws.max_row, start + 4)

    for r in range(start, end + 1):
        absence_raw = _val_at(r, "absence")

        demi_j = None
        if isinstance(absence_raw, str):
            ar = absence_raw.strip().lower()
            if "demi" in ar or ar in {"am", "pm", "1/2", "0.5", "demi-j", "demi journee"}:
                demi_j = True

        row_date = _coerce_date(_val_at(r, "date"))
        is_holiday = (row_date in holidays) if row_date else False

        preview_rows.append({
            "row_index_excel": r,
            "matricule": _val_at(r, "matricule"),
            "cin": _val_at(r, "cin"),
            "nom": _val_at(r, "nom"),
            "prenom": _val_at(r, "prenom"),
            "service": _val_at(r, "service"),
            "date": row_date,
            "heures_norm_dec": _parse_hours_to_decimal(_val_at(r, "heures_norm")),
            "hs_25_dec": _parse_hours_to_decimal(_val_at(r, "hs_25")),
            "hs_50_dec": _parse_hours_to_decimal(_val_at(r, "hs_50")),
            "hs_100_dec": _parse_hours_to_decimal(_val_at(r, "hs_100")),
            "hs_feries_dec": _parse_hours_to_decimal(_val_at(r, "hs_feries")),
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

    def _column_has_hours(col_key: str) -> bool:
        col = detected.get(col_key)
        if not col:
            return False
        s0 = header_row_index + 1
        e0 = min(ws.max_row, s0 + 10)
        ok = 0
        for rr in range(s0, e0 + 1):
            if _parse_hours_to_decimal(ws[f"{col}{rr}"].value) is not None:
                ok += 1
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

