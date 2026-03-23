"""
app.py — WoS → MyOrg Affiliation Tool v5
=========================================
Author-by-author review of the InCites format input.

Input:  InCites export CSV  (UT, Affiliation, SubOrg, AuthorFullName, LastName, FirstName)
        ResearcherAndDocument.csv  (MyOrg roster)
        OrganizationHierarchy.csv  (optional — bundled copy used if omitted)

Processing:
  1. Name unification  — Atanasova, E. + Atanasova, Elka → Atanasova, Elka
  2. Roster matching   — exact (auto) · fuzzy (review) · not found (new)
  3. Org suggestion    — SubOrg tokens mapped to OrganizationIDs

Review unit: one AUTHOR at a time (all their UTs together).
  Auto-confirmed authors are shown collapsed; only ambiguous ones need decisions.

Output: upload.csv  (PersonID, FirstName, LastName, OrganizationID, DocumentID)
        full_output.csv  (adds Status and Note columns)

Medical University of Varna · Research Information Systems
"""

from __future__ import annotations

import re
from datetime import datetime

import pandas as pd
import streamlit as st

from core import (
    load_config,
    build_person_index,
    parse_org_hierarchy,
    parse_incites_csv,
    group_authors,
    process_incites,
    suborg_candidates,
    match_author_to_roster,
    normalize_name,
    name_similarity,
    build_output_rows,
    note_for,
    split_name,
    is_initial,
)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="WoS → MyOrg  v5", page_icon="🔬",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
.app-header{background:#0f1923;color:#e8f4f8;padding:1.4rem 2rem 1.2rem;border-bottom:3px solid #1a9dc8;margin:-1rem -1rem 1.5rem;display:flex;align-items:baseline;gap:1rem;}
.app-header h1{font-size:1.4rem;font-weight:600;margin:0;color:#fff;}
.app-header .sub{font-family:'IBM Plex Mono',monospace;font-size:.75rem;color:#7ec8e3;}
.author-card{border:1px solid #e0e8ef;border-radius:6px;padding:.9rem 1rem;margin-bottom:.8rem;background:#fafcfe;}
.author-card.auto{background:#f0faf4;border-color:#27ae60;}
.author-card.decided-approve{background:#f0faf4;border-color:#27ae60;}
.author-card.decided-reject{background:#fff5f5;border-color:#e74c3c;}
.author-card.in-mo{background:#f5f5f5;border-color:#bbb;}
.badge{display:inline-block;padding:.15rem .55rem;border-radius:3px;font-size:.72rem;font-weight:600;letter-spacing:.04em;font-family:'IBM Plex Mono',monospace;margin-right:.4rem;}
.badge-auto{background:#d4edda;color:#155724;}
.badge-fuzzy{background:#d1ecf1;color:#0c5460;}
.badge-new{background:#e8d5f5;color:#5a1f8a;}
.badge-unified{background:#fff3cd;color:#856404;}
.badge-mo{background:#e2e3e5;color:#383d41;}
.prog-bar-wrap{background:#e0e8ef;border-radius:4px;height:8px;margin:.5rem 0 1rem;}
.prog-bar-fill{background:linear-gradient(90deg,#1a9dc8,#27ae60);border-radius:4px;height:8px;transition:width .3s;}
.sec-head{font-size:.7rem;font-weight:600;letter-spacing:.1em;color:#1a9dc8;text-transform:uppercase;margin:1.2rem 0 .5rem;border-bottom:1px solid #d0e8f0;padding-bottom:.3rem;}
.chip{display:inline-block;background:#e8f4f8;color:#0c5460;border-radius:3px;padding:.1rem .45rem;font-size:.72rem;margin:.1rem;font-family:'IBM Plex Mono',monospace;border:1px solid #b8dde8;}
.metric-grid{display:flex;gap:.8rem;flex-wrap:wrap;margin:.8rem 0 1.2rem;}
.metric-card{background:#fff;border:1px solid #d0e8f0;border-radius:6px;padding:.7rem 1.1rem;min-width:110px;text-align:center;}
.metric-card .num{font-size:1.6rem;font-weight:700;font-family:'IBM Plex Mono',monospace;color:#0f1923;}
.metric-card .num-blue{color:#1a9dc8;}.metric-card .num-green{color:#27ae60;}
.metric-card .num-amber{color:#e67e22;}.metric-card .num-red{color:#e74c3c;}
.metric-card .lbl{font-size:.68rem;color:#7a8fa0;text-transform:uppercase;letter-spacing:.06em;margin-top:.15rem;}
.ut-pill{display:inline-block;background:#e8f4f8;color:#0c5460;border-radius:3px;padding:.1rem .5rem;font-size:.72rem;font-family:'IBM Plex Mono',monospace;border:1px solid #b8dde8;margin:.1rem;}
.ut-pill.done{background:#d4edda;color:#155724;border-color:#a3d9b1;}
.out-table{width:100%;border-collapse:collapse;font-family:'IBM Plex Mono',monospace;font-size:.78rem;margin:.4rem 0 .8rem;}
.out-table th{background:#0f1923;color:#7ec8e3;padding:.4rem .6rem;text-align:left;font-weight:600;letter-spacing:.05em;}
.out-table td{padding:.35rem .6rem;border-bottom:1px solid #e8f0f5;color:#2c3e50;}
</style>
""", unsafe_allow_html=True)

st.markdown(
    '<div class="app-header"><h1>🔬 WoS → MyOrg</h1>'
    '<span class="sub">v5 · Author-by-author review · Medical University of Varna</span>'
    '</div>',
    unsafe_allow_html=True,
)

# ── Session state ─────────────────────────────────────────────────────────────
for k, v in {
    "processed":       False,
    "author_order":    [],     # list of canonical_name strings
    "author_index":    0,
    "author_results":  {},     # canonical_name → result dict
    "author_decs":     {},     # canonical_name → decision dict
    "person_index":    [],
    "existing_pairs":  set(),
    "orgs":            [],
    "max_pid":         0,
    "staging_counter": None,   # int after first use
    "confirmed_rows":  [],
    "skipped_rows":    [],
    "source_file":     "",
}.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ── Helpers ───────────────────────────────────────────────────────────────────
def _safe_key(*parts):
    raw  = "_".join(str(p) for p in parts)
    safe = re.sub(r"[^a-z0-9]", "_", raw.lower())
    return re.sub(r"_+", "_", safe).strip("_")

def org_label(oid, org_map):
    for lbl, v in org_map.items():
        if v == oid:
            return lbl
    return oid

def build_org_map(orgs):
    m = {f"[{o['OrganizationID']}] {o['OrganizationName']}": o["OrganizationID"] for o in orgs}
    return m, ["— none / skip —"] + list(m.keys())

def _next_staging_pid() -> str:
    counter = st.session_state.get("staging_counter")
    if not isinstance(counter, int):
        res_new = []
        for r in st.session_state.author_results.values():
            if r.get("match_type") == "new":
                try:
                    res_new.append(int(r.get("resolved_pid", 0)))
                except (ValueError, TypeError):
                    pass
        base = max(res_new) if res_new else st.session_state.get("max_pid", 0)
        counter = base + 1
    pid = str(counter)
    st.session_state["staging_counter"] = counter + 1
    return pid

def _author_is_done(cname: str) -> bool:
    """True if the author has been decided or is auto-confirmed."""
    r = st.session_state.author_results.get(cname, {})
    if r.get("match_type") == "exact" and r.get("new_uts"):
        return True   # auto-confirmed, has rows to output
    if r.get("match_type") == "exact" and not r.get("new_uts"):
        return True   # all UTs already in MO
    dec = st.session_state.author_decs.get(cname, {})
    return dec.get("decided", False)

def _n_done():
    return sum(1 for cn in st.session_state.author_order if _author_is_done(cn))

def _build_and_store(cname: str):
    """Build output rows for a confirmed/decided author and store them."""
    r   = st.session_state.author_results.get(cname, {})
    dec = st.session_state.author_decs.get(cname, {})
    mt  = r.get("match_type", "new")

    # Remove any previously stored rows for this author (re-lock scenario)
    ut_set = {row["UT"] for row in r.get("rows", [])}
    st.session_state.confirmed_rows = [
        x for x in st.session_state.confirmed_rows if x["DocumentID"] not in ut_set]
    st.session_state.skipped_rows = [
        x for x in st.session_state.skipped_rows if x["UT"] not in ut_set]

    if mt == "exact":
        action = "approve"
    else:
        action = dec.get("action", "approve")

    if action == "reject":
        for row in r.get("rows", []):
            st.session_state.skipped_rows.append({
                "AuthorFullName": r["canonical_name"],
                "UT":            row["UT"],
                "Reason":        "Rejected",
            })
        return

    # Merge dec overrides into result
    result = {**r}
    if mt != "exact":
        result["resolved_pid"]  = dec.get("resolved_pid",  r.get("resolved_pid",  ""))
        result["resolved_name"] = dec.get("resolved_name", r.get("resolved_name", ""))
        result["org_ids"]       = dec.get("org_ids",       r.get("org_ids",       [""]))
        result["match_type"]    = dec.get("match_type",    mt)

    rows = build_output_rows(result, action="approve")
    st.session_state.confirmed_rows.extend(rows)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    if st.session_state.processed:
        st.markdown("### Authors")
        for i, cn in enumerate(st.session_state.author_order):
            r   = st.session_state.author_results.get(cn, {})
            dec = st.session_state.author_decs.get(cn, {})
            mt  = r.get("match_type", "")
            done = _author_is_done(cn)
            if mt == "exact" and not r.get("new_uts"):
                icon = "⏭"
            elif done:
                icon = "✅"
            elif mt in ("fuzzy", "new"):
                icon = "⏳"
            else:
                icon = "—"
            label = f"{icon} {cn}  ({len(r.get('rows',[]))} UTs)"
            if st.button(label, key=f"sb_{i}", use_container_width=True):
                st.session_state.author_index = i
                st.rerun()


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_load, tab_review, tab_export = st.tabs([
    "📂 1 · Load Files",
    "🔍 2 · Review",
    "⬇️  3 · Export",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — LOAD
# ══════════════════════════════════════════════════════════════════════════════
with tab_load:
    st.markdown('<div class="sec-head">Upload files</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    with c1:
        inc_file = st.file_uploader(
            "InCites Export CSV",
            type=["csv"], key="inc_up",
            help="Columns: UT, Affiliation, SubOrg, AuthorFullName, LastName, FirstName",
        )
    with c2:
        res_file = st.file_uploader("ResearcherAndDocument.csv", type=["csv"], key="res_up")
    with c3:
        org_file = st.file_uploader(
            "OrganizationHierarchy.csv *(optional)*",
            type=["csv"], key="org_up",
        )

    if inc_file and res_file:
        if st.button("⚙️  Process files", type="primary", use_container_width=True):
            with st.spinner("Parsing, grouping, matching…"):
                inc_c = inc_file.read().decode("utf-8-sig")
                res_c = res_file.read().decode("utf-8-sig")
                if org_file:
                    org_c = org_file.read().decode("utf-8-sig")
                else:
                    try:
                        with open("OrganizationHierarchy.csv", encoding="utf-8-sig") as f:
                            org_c = f.read()
                    except FileNotFoundError:
                        org_c = ""

                pi, mpid, ep = build_person_index(res_c)
                orgs         = parse_org_hierarchy(org_c) if org_c else []
                rows         = parse_incites_csv(inc_c)
                groups       = group_authors(rows)
                result       = process_incites(groups, pi, orgs, ep, mpid + 1)

                author_results = {}
                author_order   = []
                # Order: needs_review first, then confirmed, then already_in_mo
                for a in result["needs_review"]:
                    cn = a["canonical_name"]
                    author_results[cn] = a
                    author_order.append(cn)
                for a in result["confirmed"]:
                    cn = a["canonical_name"]
                    author_results[cn] = a
                    author_order.append(cn)
                    # Auto-build rows immediately
                    _st_tmp = st.session_state
                    _st_tmp.author_results = author_results  # need for _build_and_store
                    _st_tmp.confirmed_rows = []
                    _st_tmp.skipped_rows   = []

                for a in result["already_in_mo"]:
                    cn = a["canonical_name"]
                    author_results[cn] = a
                    author_order.append(cn)

                st.session_state.update({
                    "processed":       True,
                    "author_order":    author_order,
                    "author_index":    0,
                    "author_results":  author_results,
                    "author_decs":     {},
                    "person_index":    pi,
                    "existing_pairs":  ep,
                    "orgs":            orgs,
                    "max_pid":         mpid,
                    "staging_counter": None,
                    "confirmed_rows":  [],
                    "skipped_rows":    [],
                    "source_file":     inc_file.name,
                })

                # Auto-store confirmed rows now
                for a in result["confirmed"]:
                    cn = a["canonical_name"]
                    rows_out = build_output_rows(a, action="approve")
                    st.session_state.confirmed_rows.extend(rows_out)

            nr = len(result["needs_review"])
            nc = len(result["confirmed"])
            nm = len(result["already_in_mo"])
            st.success(
                f"✅ {len(rows)} input rows → {len(groups)} authors · "
                f"{nc} auto-confirmed · {nr} need review · {nm} already in MO"
            )
            st.info("➡️ Go to **Tab 2** to review authors.")
    else:
        st.info("Upload InCites Export CSV and ResearcherAndDocument.csv to begin.")

    if st.session_state.processed:
        ar = st.session_state.author_results
        nc = sum(1 for a in ar.values() if a.get("match_type") == "exact" and a.get("new_uts"))
        nr = sum(1 for a in ar.values() if a.get("match_type") in ("fuzzy","new"))
        nm = sum(1 for a in ar.values() if a.get("match_type") == "exact" and not a.get("new_uts"))
        st.markdown('<div class="sec-head">Summary</div>', unsafe_allow_html=True)
        st.markdown(f"""
<div class="metric-grid">
  <div class="metric-card"><div class="num num-blue">{len(ar)}</div><div class="lbl">Authors</div></div>
  <div class="metric-card"><div class="num num-green">{nc}</div><div class="lbl">Auto-confirmed</div></div>
  <div class="metric-card"><div class="num num-amber">{nr}</div><div class="lbl">Need review</div></div>
  <div class="metric-card"><div class="num">{nm}</div><div class="lbl">All UTs in MO</div></div>
</div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — REVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_review:
    if not st.session_state.processed:
        st.info("⬅️ Load files in Tab 1 first.")
        st.stop()

    author_order  = st.session_state.author_order
    author_results = st.session_state.author_results
    person_index  = st.session_state.person_index
    orgs          = st.session_state.orgs
    org_map, _    = build_org_map(orgs)

    if not author_order:
        st.success("Nothing to review.")
        st.stop()

    n_total = len(author_order)
    n_done  = _n_done()
    pct     = int(100 * n_done / n_total) if n_total else 100

    st.markdown(f"""
<div style="display:flex;justify-content:space-between;font-size:.8rem;color:#5a7080;">
  <span>Progress</span>
  <span><b>{n_done}</b>/{n_total} done</span>
</div>
<div class="prog-bar-wrap"><div class="prog-bar-fill" style="width:{pct}%"></div></div>
""", unsafe_allow_html=True)

    idx = max(0, min(st.session_state.author_index, n_total - 1))

    nav_l, nav_c, nav_r = st.columns([1, 4, 1])
    with nav_l:
        if st.button("◀ Prev", use_container_width=True, disabled=(idx == 0)):
            st.session_state.author_index = idx - 1; st.rerun()
    with nav_r:
        if st.button("Next ▶", use_container_width=True, disabled=(idx >= n_total - 1)):
            st.session_state.author_index = idx + 1; st.rerun()
    with nav_c:
        def _icon(cn):
            r = author_results.get(cn, {})
            if r.get("match_type") == "exact" and not r.get("new_uts"): return "⏭"
            if _author_is_done(cn): return "✅"
            if r.get("match_type") in ("fuzzy","new"): return "⏳"
            return "—"
        au_disp = [f"{_icon(cn)}  {cn}  ({len(author_results.get(cn,{}).get('rows',[]))} UTs)"
                   for cn in author_order]
        jump = st.selectbox("Jump", au_disp, index=idx, label_visibility="collapsed")
        ji   = au_disp.index(jump)
        if ji != idx:
            st.session_state.author_index = ji; st.rerun()

    cname  = author_order[idx]
    r      = author_results.get(cname, {})
    dec    = st.session_state.author_decs.get(cname, {})
    mt     = r.get("match_type", "new")
    decided = dec.get("decided", False)
    is_auto = (mt == "exact")
    is_mo   = is_auto and not r.get("new_uts")

    # ── Author header card ────────────────────────────────────────────────────
    if r.get("had_initials"):
        unified_note = f"  <span class='badge badge-unified'>UNIFIED from {', '.join(repr(v) for v in r.get('variants',[]))}</span>"
    elif len(r.get("variants", [])) > 1:
        unified_note = f"  <span class='badge badge-unified'>VARIANTS: {', '.join(repr(v) for v in r.get('variants',[]))}</span>"
    else:
        unified_note = ""

    badge_cls  = {"exact":"badge-auto","fuzzy":"badge-fuzzy","new":"badge-new"}.get(mt,"badge-new")
    badge_lbl  = {"exact":"AUTO","fuzzy":"FUZZY","new":"NEW"}.get(mt,"NEW")
    if is_mo:
        badge_cls = "badge-mo"; badge_lbl = "ALL IN MO"

    # UT pills
    new_uts  = r.get("new_uts", [row["UT"] for row in r.get("rows", [])])
    all_uts  = [row["UT"] for row in r.get("rows", [])]
    ep_set   = st.session_state.existing_pairs
    pid_r    = str(r.get("resolved_pid",""))
    ut_pills = " ".join(
        f'<span class="ut-pill {"done" if (pid_r, ut) in ep_set else ""}">{ut}</span>'
        for ut in all_uts
    )

    card_cls = "author-card auto" if is_auto else ("author-card in-mo" if is_mo else "author-card")
    if decided and not is_auto:
        card_cls = "author-card decided-approve" if dec.get("action")=="approve" else "author-card decided-reject"

    st.markdown(f"""
<div class="{card_cls}">
  <span class="badge {badge_cls}">{badge_lbl}</span>
  <b style="font-size:1.05rem;">{cname}</b>{unified_note}
  <div style="margin-top:.4rem;">{ut_pills}</div>
</div>""", unsafe_allow_html=True)

    # ── AUTO-CONFIRMED ────────────────────────────────────────────────────────
    if is_auto:
        pid   = r.get("resolved_pid","")
        name  = r.get("resolved_name","")
        oids  = r.get("org_ids",[""])
        first, last = split_name(name)
        has_initials = r.get("had_initials", False)
        note = note_for("exact", has_initials)

        if is_mo:
            st.success("All UTs for this author are already in MyOrg — no action needed.")
        else:
            st.success(f"✅ Auto-confirmed → **[{pid}] {name}**  orgs: {', '.join(oids)}")
            if has_initials:
                st.info(f"📝 Name unified from variants: {r.get('variants')}")

            # Show the output rows
            rows_html = "".join(
                f"<tr><td>{pid}</td><td>{first}</td><td>{last}</td>"
                f"<td>{oid}</td><td>{row['UT']}</td></tr>"
                for row in r.get("rows", []) for oid in (oids or [""])
            )
            st.markdown(f"""
<table class="out-table">
<thead><tr><th>PersonID</th><th>FirstName</th><th>LastName</th>
<th>OrganizationID</th><th>DocumentID</th></tr></thead>
<tbody>{rows_html}</tbody>
</table>""", unsafe_allow_html=True)

    # ── NEEDS REVIEW ─────────────────────────────────────────────────────────
    else:
        left, right = st.columns([3, 2])

        with left:
            st.markdown('<div class="sec-head">Identity</div>', unsafe_allow_html=True)
            cands = r.get("candidates", [])

            if mt == "fuzzy" and cands:
                cl = [f"[{p['PersonID']}] {p['AuthorFullName']}  ({s:.2f})"
                      for s, p in cands]
                cl.append("➕ Create as NEW PERSON")
                saved = dec.get("_cand_choice", cl[0])
                di    = cl.index(saved) if saved in cl else 0
                ch = st.selectbox("Best match in roster", cl, index=di,
                                  key=_safe_key("cand", cname), disabled=decided)
                dec["_cand_choice"] = ch

                if not dec.get("_override_pid"):
                    if "NEW PERSON" in ch:
                        if (dec.get("match_type") != "new" or
                                not isinstance(dec.get("resolved_pid"), str) or
                                not dec.get("resolved_pid","").isdigit() or
                                int(dec.get("resolved_pid","0")) <= st.session_state.get("max_pid",0)):
                            dec["resolved_pid"] = _next_staging_pid()
                        dec.update({"resolved_name": cname, "match_type": "new",
                                    "org_ids": r.get("org_ids",[""])})
                    else:
                        ci = cl.index(ch); s, p = cands[ci]
                        dec.update({
                            "resolved_pid":  p["PersonID"],
                            "resolved_name": p["AuthorFullName"],
                            "match_type":    "resolved",
                            "org_ids":       p.get("OrganizationIDs") or
                                             ([p["OrganizationID"]] if p.get("OrganizationID") else [""]),
                        })

            else:
                # New person — search
                sk = _safe_key("search", cname); pk = _safe_key("pick", cname)
                NL = "➕ Create as NEW PERSON"
                pq = dec.get("_search", cname)
                sq = st.text_input("Search roster", value=pq, key=sk,
                                   disabled=decided, placeholder="Type a name and press Enter…")
                if sq != pq:
                    dec["_search"] = sq; dec["_search_choice"] = NL
                    st.session_state.pop(pk, None)

                hits = []
                if sq and len(sq) >= 2:
                    q = normalize_name(sq)
                    for p in person_index:
                        score = name_similarity(q, p["NormName"])
                        if any(part in p["NormName"] for part in q.split() if len(part) > 2):
                            score = max(score, 0.45)
                        if score >= 0.28:
                            hits.append((score, p))
                    hits.sort(key=lambda x: -x[0])
                    hits = hits[:8]

                opts = [NL] + [f"[{p['PersonID']}] {p['AuthorFullName']}  ·  {int(s*100)}%"
                               for s, p in hits]
                hm   = {f"[{p['PersonID']}] {p['AuthorFullName']}  ·  {int(s*100)}%": p
                        for s, p in hits}
                if sq and sq != cname:
                    st.caption(f"🔍 {len(hits)} match{'es' if len(hits)!=1 else ''}" if hits else "No matches")
                ss  = dec.get("_search_choice", NL)
                sd  = opts.index(ss) if ss in opts else 0
                sch = st.selectbox("Select identity", opts, index=sd, key=pk, disabled=decided)
                dec["_search_choice"] = sch; dec["_search"] = sq

                if not dec.get("_override_pid"):
                    if sch == NL:
                        if (dec.get("match_type") != "new" or
                                not isinstance(dec.get("resolved_pid"), str) or
                                not dec.get("resolved_pid","").isdigit() or
                                int(dec.get("resolved_pid","0")) <= st.session_state.get("max_pid",0)):
                            dec["resolved_pid"] = _next_staging_pid()
                        dec.update({"resolved_name": cname, "match_type": "new",
                                    "org_ids": r.get("org_ids",[""])})
                    else:
                        p = hm[sch]
                        dec.update({
                            "resolved_pid":  p["PersonID"],
                            "resolved_name": p["AuthorFullName"],
                            "match_type":    "resolved",
                            "org_ids":       p.get("OrganizationIDs") or
                                             ([p["OrganizationID"]] if p.get("OrganizationID") else [""]),
                        })

            # Resolution caption
            if dec.get("resolved_pid") and dec.get("resolved_name"):
                rmt = dec.get("match_type","")
                if rmt == "new":
                    st.caption(f"📋 New person · staging ID {dec['resolved_pid']}")
                else:
                    st.caption(f"✔ → {dec['resolved_name']} (ID {dec['resolved_pid']})")

            # Affiliation context
            aff_vals = list({row["Affiliation"] for row in r.get("rows",[]) if row["Affiliation"]})
            if aff_vals:
                st.markdown('<div class="sec-head">Affiliation context</div>', unsafe_allow_html=True)
                for aff in aff_vals[:3]:
                    st.caption(aff)

        with right:
            st.markdown('<div class="sec-head">Organisation</div>', unsafe_allow_html=True)

            # Build org candidates from SubOrg
            all_suborg = ";".join(r.get("suborgs", []))
            org_cands  = suborg_candidates(all_suborg, orgs) if all_suborg else []

            if org_cands and not decided:
                top_cand = org_cands[0]
                st.caption(
                    f"💡 Suggested: [{top_cand[1]}] {top_cand[2]}  "
                    f"(score {top_cand[0]:.0%})  from SubOrg: *{all_suborg[:60]}*"
                )

            ok = _safe_key("orgs", cname)
            current_oids = dec.get("org_ids", r.get("org_ids", [""]))
            dl = [org_label(o, org_map) for o in current_oids
                  if o and org_label(o, org_map) in org_map]

            # Pre-populate from top org candidate if no selection yet
            if not dl and org_cands and ok not in st.session_state:
                top_lbl = org_label(org_cands[0][1], org_map)
                if top_lbl in org_map:
                    dl = [top_lbl]

            sel = st.multiselect(
                "Organisation(s)", list(org_map.keys()),
                default=dl if ok not in st.session_state else None,
                key=ok, disabled=decided,
            )
            dec["org_ids"] = [org_map[l] for l in sel] or [""]

            st.markdown("")
            # SubOrg raw values for reference
            if r.get("suborgs"):
                st.markdown('<div class="sec-head">SubOrg (raw)</div>', unsafe_allow_html=True)
                for s in r.get("suborgs", []):
                    st.caption(s)

            st.markdown("")
            ac, rc = st.columns(2)
            with ac:
                if st.button(
                    "✅ Approve" if not decided or dec.get("action")=="reject" else "✅ Approved",
                    key=_safe_key("approve", cname), use_container_width=True,
                    type="primary" if not decided else "secondary",
                    disabled=(decided and dec.get("action") == "approve"),
                ):
                    dec["decided"] = True; dec["action"] = "approve"
                    st.session_state.author_decs[cname] = dec
                    _build_and_store(cname)
                    # Advance to next undecided
                    for i in range(idx + 1, n_total):
                        if not _author_is_done(author_order[i]):
                            st.session_state.author_index = i; break
                    st.rerun()
            with rc:
                if st.button(
                    "❌ Reject" if not decided or dec.get("action")=="approve" else "❌ Rejected",
                    key=_safe_key("reject", cname), use_container_width=True,
                    disabled=(decided and dec.get("action") == "reject"),
                ):
                    dec["decided"] = True; dec["action"] = "reject"
                    st.session_state.author_decs[cname] = dec
                    _build_and_store(cname)
                    for i in range(idx + 1, n_total):
                        if not _author_is_done(author_order[i]):
                            st.session_state.author_index = i; break
                    st.rerun()
            if decided:
                if st.button("✏️ Undo", key=_safe_key("undo", cname), use_container_width=True):
                    dec["decided"] = False
                    st.session_state.author_decs[cname] = dec
                    # Remove stored rows for this author
                    ut_set = {row["UT"] for row in r.get("rows", [])}
                    st.session_state.confirmed_rows = [
                        x for x in st.session_state.confirmed_rows if x["DocumentID"] not in ut_set]
                    st.session_state.skipped_rows = [
                        x for x in st.session_state.skipped_rows if x["UT"] not in ut_set]
                    st.rerun()

        st.session_state.author_decs[cname] = dec

        # Preview of output rows if decided
        if decided and dec.get("action") == "approve":
            st.markdown('<div class="sec-head">Output preview</div>', unsafe_allow_html=True)
            pid_d  = dec.get("resolved_pid","")
            name_d = dec.get("resolved_name","")
            oids_d = [o for o in dec.get("org_ids",[""]) if o] or [""]
            first_d, last_d = split_name(name_d)
            rows_html = "".join(
                f"<tr><td>{pid_d}</td><td>{first_d}</td><td>{last_d}</td>"
                f"<td>{oid}</td><td>{row['UT']}</td></tr>"
                for row in r.get("rows",[]) for oid in oids_d
            )
            st.markdown(f"""
<table class="out-table">
<thead><tr><th>PersonID</th><th>FirstName</th><th>LastName</th>
<th>OrganizationID</th><th>DocumentID</th></tr></thead>
<tbody>{rows_html}</tbody>
</table>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — EXPORT
# ══════════════════════════════════════════════════════════════════════════════
with tab_export:
    if not st.session_state.processed:
        st.info("⬅️ Load files in Tab 1 first.")
        st.stop()

    conf = st.session_state.confirmed_rows
    skip = st.session_state.skipped_rows
    n_done_exp = _n_done()
    n_total_exp = len(st.session_state.author_order)

    st.markdown(f"""
<div class="metric-grid">
  <div class="metric-card"><div class="num num-blue">{n_done_exp}</div><div class="lbl">Authors done</div></div>
  <div class="metric-card"><div class="num num-green">{len(conf)}</div><div class="lbl">4UP rows</div></div>
  <div class="metric-card"><div class="num">{len(skip)}</div><div class="lbl">2SKIP rows</div></div>
  <div class="metric-card"><div class="num num-amber">{n_total_exp - n_done_exp}</div><div class="lbl">Pending</div></div>
</div>""", unsafe_allow_html=True)

    if n_done_exp < n_total_exp:
        st.warning(f"⚠️ {n_total_exp - n_done_exp} author(s) not yet decided — rows not included.")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # upload.csv — 5 clean columns
    st.markdown('<div class="sec-head">upload.csv — ready for MyOrg</div>', unsafe_allow_html=True)
    if conf:
        up_cols = ["PersonID","FirstName","LastName","OrganizationID","DocumentID"]
        up_df   = pd.DataFrame(conf)[up_cols]
        st.dataframe(up_df, use_container_width=True, height=300)
        st.download_button(
            "⬇️ Download upload.csv",
            data=up_df.to_csv(index=False).encode("utf-8"),
            file_name=f"upload_{ts}.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True,
        )
    else:
        st.info("No rows yet — confirm authors in Tab 2.")

    # Full output with Status + Note
    st.markdown('<div class="sec-head">Full output with notes</div>', unsafe_allow_html=True)
    if conf or skip:
        all_rows = []
        for r in conf:
            all_rows.append({**r, "Status": r.get("Status","4UP"),
                             "Note": r.get("Note","")})
        for r in skip:
            all_rows.append({
                "PersonID":"","FirstName":"","LastName": r["AuthorFullName"],
                "OrganizationID":"","DocumentID": r["UT"],
                "Status":"2SKIP","Note": r["Reason"],
            })
        full_df = pd.DataFrame(all_rows)
        st.dataframe(full_df, use_container_width=True, height=300)
        st.download_button(
            "⬇️ Download full output CSV",
            data=full_df.to_csv(index=False).encode("utf-8"),
            file_name=f"output_full_{ts}.csv",
            mime="text/csv",
            use_container_width=True,
        )
