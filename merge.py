# merge.py
from typing import List, Dict, Any, Optional, Tuple
from datetime import date
import unicodedata
from rapidfuzz import process, fuzz

# ------------------------- Normalisation & helpers -------------------------
def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = _strip_accents(str(s)).lower()
    s = s.replace("-", " ").replace("_", " ")
    s = " ".join(s.split())  # compress spaces
    return s

def _name_key(nom: Optional[str], prenom: Optional[str]) -> str:
    return f"{_norm(nom)}|{_norm(prenom)}"

def _initials_ok(ts_nom: str, ts_prenom: str, ref_nom: str, ref_prenom: str) -> bool:
    a = (_norm(ts_nom)[:1] or "") + (_norm(ts_prenom)[:1] or "")
    b = (_norm(ref_nom)[:1] or "") + (_norm(ref_prenom)[:1] or "")
    # si côté TS il manque les initiales (très court), on ne bloque pas
    return (a.strip() == "") or (a == b)

def _period_bounds(yyyymm: Optional[str]) -> Tuple[Optional[date], Optional[date]]:
    if not yyyymm or len(yyyymm) != 7 or yyyymm[4] != "-":
        return None, None
    y, m = map(int, yyyymm.split("-"))
    from calendar import monthrange
    d0 = date(y, m, 1)
    d1 = date(y, m, monthrange(y, m)[1])
    return d0, d1

def _to_date(d: Any) -> Optional[date]:
    if not d:
        return None
    if isinstance(d, date):
        return d
    try:
        return date.fromisoformat(str(d)[:10])
    except Exception:
        return None

def _active_in_period(ref_row: Dict[str, Any], d0: Optional[date], d1: Optional[date]) -> bool:
    if not d0 or not d1:
        return True
    debut = _to_date(ref_row.get("date_debut"))
    fin   = _to_date(ref_row.get("date_fin"))
    if debut and debut > d1:
        return False
    if fin and fin < d0:
        return False
    return True

def _roster_key(r: Dict[str, Any]) -> Tuple[str, str, str]:
    # clé stable et déterministe pour identifier une ligne roster
    return (_norm(r.get("nom")), _norm(r.get("prenom")), _norm(r.get("cin")))

# ------------------------- Index roster (ref) -------------------------
def build_roster_index(template_roster: List[Dict[str, Any]], period: Optional[str]) -> Dict[str, Any]:
    d0, d1 = _period_bounds(period)
    by_cin: Dict[str, Dict[str, Any]] = {}
    name_bank: List[Tuple[str, Dict[str, Any]]] = []
    name_lookup: Dict[str, Dict[str, Any]] = {}

    for r in template_roster:
        if not _active_in_period(r, d0, d1):
            continue
        cin = _norm(r.get("cin"))
        if cin:
            by_cin[cin] = r
        key = _name_key(r.get("nom"), r.get("prenom"))
        name_bank.append((key, r))
        name_lookup[key] = r

    return {"by_cin": by_cin, "name_bank": name_bank, "name_lookup": name_lookup}

# ------------------------- Fuzzy matching Nom+Prénom -------------------------
def fuzzy_match_name(
    ts_row: Dict[str, Any],
    name_bank: List[Tuple[str, Dict[str, Any]]],
    strict: int,
    loose: int,
    require_initials: bool,
    *,
    name_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[int], List[Tuple[str, int]]]:
    # Pré-filtrage: ignorer les noms trop courts côté TS
    ts_nom = _norm(ts_row.get("nom"))
    ts_pre = _norm(ts_row.get("prenom"))
    if len(ts_nom) < 2 or len(ts_pre) < 2:
        return None, None, []

    q = f"{ts_nom}|{ts_pre}"
    if not q.strip():
        return None, None, []

    # Scoring de base
    choices = [k for (k, _) in name_bank]
    top = process.extract(q, choices, scorer=fuzz.WRatio, limit=5)

    # Appliquer bonus/contraintes
    candidates: List[Tuple[Dict[str, Any], int]] = []
    ts_service = _norm(ts_row.get("service"))
    ts_matcli  = _norm(ts_row.get("matricule_client"))

    for target, base_score, _ in top:
        ref = name_lookup.get(target) if name_lookup else next(r for (k, r) in name_bank if k == target)

        # Initiales obligatoires ?
        if require_initials and not _initials_ok(ts_row.get("nom",""), ts_row.get("prenom",""), ref.get("nom",""), ref.get("prenom","")):
            continue

        score = base_score

        # Bonus Service si présent des deux côtés
        ref_service = _norm(ref.get("service"))
        if ts_service and ref_service and ts_service == ref_service:
            score += 3

        # Petit bonus Matricule Client si égalité
        ref_matcli = _norm(ref.get("matricule_client"))
        if ts_matcli and ref_matcli and ts_matcli == ref_matcli:
            score += 2

        score = max(0, min(100, score))
        candidates.append((ref, score))

    if not candidates:
        return None, None, [(t, s) for (t, s, _) in top]

    candidates.sort(key=lambda x: x[1], reverse=True)
    best_ref, best_score = candidates[0]

    if best_score >= strict:
        if len(candidates) > 1 and (candidates[1][1] + 2) >= best_score:
            return None, None, [(_name_key(c[0].get("nom"), c[0].get("prenom")), c[1]) for c in candidates[:3]]
        return best_ref, best_score, []

    if best_score >= loose:
        if len(candidates) > 1 and (candidates[1][1] + 3) >= best_score:
            return None, None, [(_name_key(c[0].get("nom"), c[0].get("prenom")), c[1]) for c in candidates[:3]]
        return best_ref, best_score, []

    return None, None, [(_name_key(c[0].get("nom"), c[0].get("prenom")), c[1]) for c in candidates[:3]]

# ------------------------- Fusion -------------------------
KEEP_FROM_REF = [
    "matricule", "matricule_salarie", "matricule_client", "nom", "prenom", "cin",
    "num_contrat", "num_avenant", "date_debut", "date_fin", "service",
    "nombre"
]

TAKE_FROM_TS = [
    "nb_jt","nb_ji","nb_cp_280","nb_sans_solde","nb_jf","tx_sal",
    "heures_norm_dec","heures_travaillees_decimal","rappel_hrs_norm_140",
    "hs_25_dec","hs_50_dec","hs_100_dec","hs_feries_dec",
    "ind_panier_771","ind_transport_777","ind_deplacement_780",
    "heures_jour_ferie_chome_090","observations","fin_mission"
]

def _merge_one(ref: Dict[str, Any], ts: Dict[str, Any], mode: str, score: int) -> Dict[str, Any]:
    out = {k: ref.get(k) for k in KEEP_FROM_REF}
    out.update({k: ts.get(k) for k in TAKE_FROM_TS})

    # Garantit heures_norm_dec même si seul 'heures_travaillees_decimal' est fourni
    if out.get("heures_norm_dec") is None and ts.get("heures_travaillees_decimal") is not None:
        out["heures_norm_dec"] = ts.get("heures_travaillees_decimal")

    out["match_mode"] = mode
    out["match_score"] = int(round(float(score)))  # score entier lisible
    return out

def merge_rows(
    template_roster: List[Dict[str, Any]],
    timesheet_rows: List[Dict[str, Any]],
    timesheet_period: Optional[str],
    strict: int = 92,
    loose: int = 85,
    require_initials: bool = True
) -> Dict[str, Any]:

    idx = build_roster_index(template_roster, timesheet_period)
    matched: List[Dict[str, Any]] = []
    missing_in_roster: List[Dict[str, Any]] = []
    ambiguous: List[Dict[str, Any]] = []

    matched_keys = set()           # clé logique pour référencer les refs matchées
    dup_check: Dict[Tuple[str,str,str], int] = {}  # contrôle doublons côté ref

    for ts in timesheet_rows:
        # 1) CIN exact
        cin = _norm(ts.get("cin"))
        if cin and cin in idx["by_cin"]:
            ref = idx["by_cin"][cin]
            merged = _merge_one(ref, ts, "cin", 100)
            matched.append(merged)

            key = _roster_key(ref)
            matched_keys.add(key)
            dup_check[key] = dup_check.get(key, 0) + 1
            continue

        # 2) Fuzzy Nom+Prénom
        ref, score, amb = fuzzy_match_name(
            ts, idx["name_bank"], strict, loose, require_initials, name_lookup=idx.get("name_lookup")
        )
        if ref and score is not None:
            merged = _merge_one(ref, ts, "name_fuzzy", score)
            matched.append(merged)

            key = _roster_key(ref)
            matched_keys.add(key)
            dup_check[key] = dup_check.get(key, 0) + 1
        elif amb:
            ambiguous.append({"timesheet": ts, "candidates": amb})
        else:
            missing_in_roster.append(ts)

    # 3) Référence non trouvée côté client (actifs)
    missing_in_client = [r for (_, r) in idx["name_bank"] if _roster_key(r) not in matched_keys]

    # 4) Doublons sur la même référence (contrôle qualité)
    duplicates = [k for (k, n) in dup_check.items() if n > 1]

    return {
        "matched_rows": matched,
        "missing_in_client": missing_in_client,
        "missing_in_roster": missing_in_roster,
        "ambiguous": ambiguous,
        "stats": {
            "template_count": len(template_roster),
            "timesheet_count": len(timesheet_rows),
            "matched": len(matched),
            "missing_in_client": len(missing_in_client),
            "missing_in_roster": len(missing_in_roster),
            "ambiguous": len(ambiguous),
            "duplicates_ref_count": len(duplicates),
        },
        "duplicates_ref_keys": duplicates  # utile pour debug/QA
    }

