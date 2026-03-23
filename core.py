"""
core.py — WoS → MyOrg v5
=========================
Processes the InCites-format export (one row per author×UT, all MUV) instead
of the raw WoS export. No affiliation filtering needed.

Key differences from v3:
  • parse_incites_csv()        — reads the new 6-column format
  • group_authors()            — unifies name variants (Atanasova, E. → Atanasova, Elka)
  • match_author_to_roster()   — matches canonical name against ResearcherAndDocument
  • suborg_candidates()        — maps WoS SubOrg abbreviations to OrganizationIDs
  • build_person_index()       — unchanged (reads ResearcherAndDocument.csv)
  • parse_org_hierarchy()      — unchanged

Input CSV columns (renamed from InCites export):
  UT, Affiliation, SubOrg, AuthorFullName, LastName, FirstName

Medical University of Varna · Research Information Systems
"""

from __future__ import annotations

import csv
import io
import re
import sqlite3
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ── Text normalisation ────────────────────────────────────────────────────────

def strip_diacritics(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text)
                   if unicodedata.category(c) != "Mn")

def normalize_name(name: str) -> str:
    n = strip_diacritics(name.lower().strip())
    n = re.sub(r"[''`]", "", n)
    n = re.sub(r"[^a-z0-9\s\-]", " ", n)
    return re.sub(r"\s+", " ", n).strip()

def name_similarity(a: str, b: str) -> float:
    from difflib import SequenceMatcher
    return SequenceMatcher(None, a, b).ratio()

def get_initials_key(name: str) -> str:
    parts = normalize_name(name).split()
    if not parts:
        return ""
    return parts[0][0] if parts else ""


# ── Config ────────────────────────────────────────────────────────────────────

def load_config(path: str = "config.json") -> dict:
    import json
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


# ── Name variant helpers ──────────────────────────────────────────────────────

def is_initial(name: str) -> bool:
    """True if the name consists entirely of initials, e.g. 'E.' or 'A.B.'"""
    if not name:
        return False
    return all(len(p.rstrip(".")) == 1 for p in name.split() if p)

def initials_match(init_form: str, full_form: str) -> bool:
    """
    True if init_form is a plausible initial abbreviation of full_form.
    'E.' matches 'Elka'; 'Z.' matches 'Zhaklin'.
    """
    full_parts = full_form.split()
    init_parts = [p.rstrip(".") for p in init_form.split() if p]
    if len(init_parts) > len(full_parts):
        return False
    return all(fp[0].lower() == ip[0].lower()
               for fp, ip in zip(full_parts, init_parts))

def canonical_first_name(names: list[str]) -> str:
    """
    From a list of first-name variants, return the canonical (most complete) form.
    Prefers non-initial names, then longest.
    """
    non_init = [n for n in names if not is_initial(n)]
    pool = non_init if non_init else names
    return max(pool, key=len)


# ── Input parsing ─────────────────────────────────────────────────────────────

def parse_incites_csv(content: str) -> list[dict]:
    """
    Parse the InCites-format export CSV.

    Expected columns (in any order):
      UT, Affiliation, SubOrg, AuthorFullName, LastName, FirstName

    Returns a list of row dicts with normalised keys.
    """
    rows = []
    f = io.StringIO(content.strip())
    reader = csv.DictReader(f)
    for row in reader:
        ut = row.get("UT", "").strip()
        if not ut:
            continue
        rows.append({
            "UT":             ut,
            "Affiliation":    row.get("Affiliation", "").strip(),
            "SubOrg":         row.get("SubOrg", "").strip(),
            "AuthorFullName": row.get("AuthorFullName", "").strip(),
            "LastName":       row.get("LastName", "").strip(),
            "FirstName":      row.get("FirstName", "").strip(),
        })
    return rows


# ── Author grouping & name unification ───────────────────────────────────────

def group_authors(rows: list[dict]) -> list[dict]:
    """
    Group input rows by person identity, unifying name variants within each group.

    Algorithm:
      1. Group rows by LastName (exact match).
      2. Within each last-name group, cluster compatible FirstName variants:
         - Exact first-name matches go in the same cluster.
         - An initial form (e.g. 'E.') is compatible with a full form that
           starts with the same letter (e.g. 'Elka').
         - Incompatible first names (e.g. 'Lyudmila' vs 'Sirma') form separate
           clusters → they are different people.
      3. The canonical name for each cluster is: LastName, <longest non-initial FirstName>.
      4. 'had_initials' flag is set when any variant was an initial form.

    Returns a list of author-group dicts:
      canonical_name   : str   e.g. 'Atanasova, Elka'
      canonical_first  : str
      canonical_last   : str
      variants         : list[str]  all AuthorFullName strings seen (longest first)
      rows             : list[dict] all input rows for this group
      suborgs          : list[str]  unique non-empty SubOrg values
      had_initials     : bool  True if any name variant was an initial form
    """
    by_last: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_last[r["LastName"]].append(r)

    groups = []
    for last, recs in sorted(by_last.items()):
        firsts = list({r["FirstName"] for r in recs})

        # Build compatibility clusters
        clusters: list[set] = []
        for fn in firsts:
            placed = False
            for cluster in clusters:
                for ex in list(cluster):
                    if fn == ex:
                        cluster.add(fn); placed = True; break
                    if is_initial(fn) and initials_match(fn, ex):
                        cluster.add(fn); placed = True; break
                    if is_initial(ex) and initials_match(ex, fn):
                        cluster.add(fn); placed = True; break
                if placed:
                    break
            if not placed:
                clusters.append({fn})

        for cluster in clusters:
            crecs = [r for r in recs if r["FirstName"] in cluster]
            cf    = canonical_first_name(list(cluster))
            cname = f"{last}, {cf}"
            variants = sorted({r["AuthorFullName"] for r in crecs},
                               key=len, reverse=True)
            suborgs = sorted({r["SubOrg"] for r in crecs if r["SubOrg"]})

            groups.append({
                "canonical_name":  cname,
                "canonical_first": cf,
                "canonical_last":  last,
                "variants":        variants,
                "rows":            crecs,
                "suborgs":         suborgs,
                "had_initials":    any(is_initial(fn) for fn in cluster),
            })

    return groups


# ── Roster matching ───────────────────────────────────────────────────────────

def match_author_to_roster(
    canonical_name: str,
    person_index: list[dict],
    threshold: float = 0.75,
    max_results: int = 5,
) -> list[tuple[float, dict]]:
    """
    Match canonical_name against the person_index.
    Returns [(score, person_dict)] sorted by score descending.
    """
    norm_q = normalize_name(canonical_name)
    results = []
    for p in person_index:
        score = name_similarity(norm_q, p["NormName"])
        if score >= threshold:
            results.append((score, p))
    results.sort(key=lambda x: -x[0])
    return results[:max_results]


# ── SubOrg → OrgID mapper ────────────────────────────────────────────────────

_SUBORG_STOPS = {
    "of", "and", "the", "for", "in", "department", "dept",
    "fac", "faculty", "univ", "university", "med", "medical",
}

def suborg_candidates(
    suborg_str: str,
    orgs: list[dict],
    threshold: float = 0.35,
    max_results: int = 4,
) -> list[tuple[float, str, str]]:
    """
    Map a WoS SubOrg string (semicolon-separated, most-specific last) to
    OrganizationID candidates using token-overlap scoring.

    Returns [(score, OrgID, OrgName)] sorted by score descending.
    'score' is the Jaccard overlap between content tokens.
    """
    parts = [p.strip() for p in suborg_str.split(";") if p.strip()]
    if not parts:
        return []

    seen: dict[str, tuple[float, str, str]] = {}
    for part in reversed(parts):          # most specific part first
        norm_part = normalize_name(part)
        pt = set(norm_part.split()) - _SUBORG_STOPS
        if not pt:
            continue
        for org in orgs:
            norm_org = normalize_name(org["OrganizationName"])
            ot = set(norm_org.split()) - _SUBORG_STOPS
            if not ot:
                continue
            overlap = len(pt & ot) / max(len(pt), len(ot))
            if overlap >= threshold:
                oid = org["OrganizationID"]
                if oid not in seen or overlap > seen[oid][0]:
                    seen[oid] = (overlap, oid, org["OrganizationName"])

    return sorted(seen.values(), key=lambda x: -x[0])[:max_results]


# ── ResearcherAndDocument.csv ─────────────────────────────────────────────────

def build_person_index(csv_content: str) -> tuple[list[dict], int, set]:
    """
    Parse ResearcherAndDocument.csv.
    Returns (person_list, max_PersonID, existing_pairs).

    existing_pairs: set of (PersonID, DocumentID) already in MyOrg.
    """
    persons: dict[str, dict] = {}
    max_pid = 0
    existing_pairs: set[tuple[str, str]] = set()

    f = io.StringIO(csv_content.strip())
    reader = csv.DictReader(f)
    for row in reader:
        pid_str = row.get("PersonID", "").strip()
        if not pid_str:
            continue

        try:
            pid_int = int(pid_str)
            if pid_int > max_pid:
                max_pid = pid_int
        except ValueError:
            pass

        oid    = row.get("OrganizationID", "").strip()
        doc_id = row.get("DocumentID", "").strip()

        if doc_id:
            existing_pairs.add((pid_str, doc_id))

        if pid_str in persons:
            if oid and oid not in persons[pid_str]["OrganizationIDs"]:
                persons[pid_str]["OrganizationIDs"].append(oid)
            continue

        first_name = row.get("FirstName", "").strip()
        last_name  = row.get("LastName", "").strip()
        full_name  = f"{last_name}, {first_name}"
        norm       = normalize_name(full_name)

        norm_last  = re.sub(r"[^a-z0-9\s]", "", strip_diacritics(last_name.lower().strip()))
        norm_first = re.sub(r"[^a-z0-9\s]", "", strip_diacritics(first_name.lower().strip()))
        is_init    = all(len(p) == 1 for p in norm_first.split() if p)
        initials   = "".join(p[0] for p in norm_first.split() if p)

        persons[pid_str] = {
            "PersonID":        pid_str,
            "AuthorFullName":  full_name,
            "FullName":        full_name,
            "NormName":        norm,
            "Surname":         norm_last,
            "GivenName":       norm_first,
            "Initials":        initials,
            "IsInitialsOnly":  is_init,
            "InitialsKey":     initials[0] if initials else "",
            "OrganizationID":  oid,
            "OrganizationIDs": [oid] if oid else [],
        }

    return list(persons.values()), max_pid, existing_pairs


# ── OrganizationHierarchy.csv ─────────────────────────────────────────────────

def parse_org_hierarchy(csv_content: str) -> list[dict]:
    orgs = []
    f = io.StringIO(csv_content.strip())
    reader = csv.DictReader(f)
    for row in reader:
        if row.get("OrganizationID"):
            orgs.append({
                "OrganizationID":   row.get("OrganizationID", "").strip(),
                "OrganizationName": row.get("OrganizationName", "").strip(),
                "ParentOrgaID":     row.get("ParentOrgaID", "").strip(),
            })
    return orgs


# ── Batch processing ──────────────────────────────────────────────────────────

AUTO_THRESHOLD  = 0.99   # exact name match → auto-confirm
FUZZY_THRESHOLD = 0.75   # below this → new person

def process_incites(
    groups:         list[dict],
    person_index:   list[dict],
    orgs:           list[dict],
    existing_pairs: set,
    pid_counter:    int,
) -> dict:
    """
    Classify each author group into one of three buckets:
      confirmed    — exact match (score ≥ AUTO_THRESHOLD), roster OrgID matches
      needs_review — fuzzy match or new person or org ambiguity
      already_in_mo — all UTs for this author already in existing_pairs

    Returns:
      {
        'confirmed':    [author_result, ...],
        'needs_review': [author_result, ...],
        'already_in_mo':[author_result, ...],
        'pid_counter':  int,   # updated counter for new persons
      }

    Each author_result dict contains:
      canonical_name, canonical_first, canonical_last, variants,
      rows (input rows), suborgs, had_initials,
      resolved_pid, resolved_name, org_ids,
      match_type: 'exact' | 'fuzzy' | 'new',
      match_score: float,
      candidates: [(score, person_dict)],
      org_candidates: [(score, OrgID, OrgName)],
      already_in_mo_uts: [UT, ...],   # UTs already in existing_pairs
      new_uts: [UT, ...],             # UTs not yet in existing_pairs
    }
    """
    confirmed    = []
    needs_review = []
    already_in_mo = []

    for g in groups:
        matches = match_author_to_roster(g["canonical_name"], person_index)
        top_score  = matches[0][0] if matches else 0.0
        top_person = matches[0][1] if matches else None

        # SubOrg candidates (merge all suborg strings for this author)
        all_suborg = ";".join(g["suborgs"])
        org_cands  = suborg_candidates(all_suborg, orgs) if all_suborg else []

        # Determine which UTs are already in MyOrg
        uts = [r["UT"] for r in g["rows"]]
        if top_person:
            pid = top_person["PersonID"]
            already_uts = [ut for ut in uts if (pid, ut) in existing_pairs]
            new_uts     = [ut for ut in uts if (pid, ut) not in existing_pairs]
        else:
            already_uts = []
            new_uts     = uts

        base = {
            **{k: g[k] for k in ("canonical_name","canonical_first","canonical_last",
                                  "variants","rows","suborgs","had_initials")},
            "candidates":       matches,
            "org_candidates":   org_cands,
            "already_in_mo_uts": already_uts,
            "new_uts":          new_uts,
        }

        # ── Exact match: auto-confirm ────────────────────────────────────────
        if top_score >= AUTO_THRESHOLD and top_person:
            org_ids = top_person.get("OrganizationIDs") or (
                [top_person["OrganizationID"]] if top_person.get("OrganizationID") else [""])

            result = {
                **base,
                "resolved_pid":   top_person["PersonID"],
                "resolved_name":  top_person["AuthorFullName"],
                "org_ids":        org_ids,
                "match_type":     "exact",
                "match_score":    top_score,
            }
            # If ALL UTs already in MyOrg → already_in_mo
            if new_uts:
                confirmed.append(result)
            else:
                already_in_mo.append(result)

        # ── Fuzzy match or new ───────────────────────────────────────────────
        else:
            if top_score >= FUZZY_THRESHOLD and top_person:
                match_type    = "fuzzy"
                resolved_pid  = top_person["PersonID"]
                resolved_name = top_person["AuthorFullName"]
                org_ids       = top_person.get("OrganizationIDs") or (
                    [top_person["OrganizationID"]] if top_person.get("OrganizationID") else [""])
            else:
                match_type    = "new"
                resolved_pid  = str(pid_counter)
                resolved_name = g["canonical_name"]
                org_ids       = [org_cands[0][1]] if org_cands else [""]
                pid_counter  += 1

            needs_review.append({
                **base,
                "resolved_pid":   resolved_pid,
                "resolved_name":  resolved_name,
                "org_ids":        org_ids,
                "match_type":     match_type,
                "match_score":    top_score,
            })

    return {
        "confirmed":     confirmed,
        "needs_review":  needs_review,
        "already_in_mo": already_in_mo,
        "pid_counter":   pid_counter,
    }


# ── Output helpers ────────────────────────────────────────────────────────────

def split_name(full_name: str) -> tuple[str, str]:
    """'Last, First' → (first, last)"""
    if "," in full_name:
        last, _, first = full_name.partition(",")
        return first.strip(), last.strip()
    return "", full_name.strip()


def note_for(match_type: str, had_initials: bool) -> str:
    if match_type == "exact" and not had_initials:
        return "AU same"
    if match_type == "exact" and had_initials:
        return "AU unified (initial expanded)"
    if match_type == "fuzzy":
        return "User confirmed (fuzzy match)"
    if match_type == "new":
        return "New author"
    return "User confirmed"


def build_output_rows(
    author_result: dict,
    action: str = "approve",
) -> list[dict]:
    """
    Build one output row per UT for a confirmed author result.
    Returns list of dicts: PersonID, FirstName, LastName, OrganizationID,
                           DocumentID, Status, Note.
    """
    if action == "reject":
        return [
            {
                "PersonID": "", "FirstName": "", "LastName": author_result["canonical_last"],
                "OrganizationID": "", "DocumentID": r["UT"],
                "Status": "2SKIP", "Note": "Rejected",
            }
            for r in author_result["rows"]
        ]

    pid        = str(author_result.get("resolved_pid", ""))
    name       = author_result.get("resolved_name", author_result["canonical_name"])
    org_ids    = [o for o in author_result.get("org_ids", [""]) if o] or [""]
    first, last = split_name(name)
    mt         = author_result.get("match_type", "new")
    note       = note_for(mt, author_result.get("had_initials", False))

    rows = []
    for r in author_result["rows"]:
        for oid in org_ids:
            rows.append({
                "PersonID":       pid,
                "FirstName":      first,
                "LastName":       last,
                "OrganizationID": oid,
                "DocumentID":     r["UT"],
                "Status":         "4UP",
                "Note":           note,
            })
    return rows
