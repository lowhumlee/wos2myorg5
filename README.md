# WoS → MyOrg v5

Author-by-author review tool for Medical University of Varna.

## Input format (InCites export CSV)

| Column | Description |
|--------|-------------|
| `UT` | WoS accession number |
| `Affiliation` | Full address string |
| `SubOrg` | Semicolon-separated sub-organisation names |
| `AuthorFullName` | `Last, First` format |
| `LastName` | Last name |
| `FirstName` | First name (may be initial only, e.g. `E.`) |

All rows are assumed to be MUV — no affiliation filtering is performed.

## Setup

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Workflow

| Tab | Action |
|-----|--------|
| 📂 Load | Upload InCites CSV + ResearcherAndDocument.csv (+ optional OrgHierarchy) |
| 🔍 Review | Author by author: exact matches auto-confirmed, fuzzy/new need a decision |
| ⬇️ Export | Download `upload.csv` (5 clean columns) + full output with Status/Note |

## Key features

- **Name unification** — `Atanasova, E.` + `Atanasova, Elka` → `Atanasova, Elka` (all UTs together)
- **Auto-confirm** — exact roster match (score ≥ 0.99) confirmed without user action
- **Org suggestion** — SubOrg tokens matched against OrganizationHierarchy, top candidate pre-selected
- **Author-centric review** — one decision covers all UTs for that author

## Output

`upload.csv`: `PersonID, FirstName, LastName, OrganizationID, DocumentID`

Full output adds: `Status (4UP/2SKIP), Note (AU same / AU unified / User confirmed / New author / Rejected)`
