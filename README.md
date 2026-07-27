# ONE ENGINE

ONE ENGINE is a Snowflake-native deterministic rules platform. Its Rules
Distillery learns executable catalogs from paired BEFORE/AFTER evidence,
validates the learned logic, and promotes approved catalogs directly into the
Snowflake rules table.

The first workflow is **Product Request**: PRF, SORF, and SRF daily decision
workbooks. Neon is not a runtime dependency.

## Snowflake deployment contract

The entire application and Distillery live in one deployable file:

```text
streamlit_app.py
```

In this repository that file is
[`app/streamlit_app.py`](app/streamlit_app.py). Copy or upload it as the
Snowflake Streamlit main file.

Branding is a separate optional Snowflake project asset:

```text
oneengine_brand.png
```

Keep it beside `streamlit_app.py`. The app discovers that filename at runtime;
the image is not encoded or embedded in Python. If the file is uploaded after
the Snowflake Streamlit session has already started, restart the session so
Snowflake copies the new project asset into the running app. The artwork is
shown in the expanded sidebar and registered with `st.logo`, which keeps it
available in the app shell when the sidebar is collapsed.

## Foodbuy design-system alignment

ONE ENGINE's visual layer follows the
[Foodbuy Design System Storybook](https://69925e4ee40e16a198c7c5cf-xdindjzhxi.chromatic.com/?path=/story/introduction--getting-started).
The design compatibility layer remains inside `streamlit_app.py` and maps
Foodbuy foundations onto Streamlit's native components:

- the exact primary, secondary, success, warning, danger, and neutral tokens;
- DM Sans/Inter typography roles and hierarchy;
- the 4px spacing rhythm and responsive 12-column breakpoints;
- 8px control and 12px card/panel radii;
- Foodbuy elevation 100–900 shadows;
- buttons, inputs, selects, uploaders, forms, feedback alerts, tabs, sidebar
  navigation, cards, metrics, tables, and status badges;
- 44px minimum interaction targets, visible focus rings, accessible contrast,
  and reduced-motion support.

The Storybook's Stencil custom elements are not runtime dependencies. Native
Streamlit widgets keep their Snowflake behavior and receive the Foodbuy visual
contract through CSS, preserving the one-Python-file deployment model.

There are no generated catalog files, profile JSON files, loader scripts, or
runtime Python packages to deploy. Rules, aliases, candidates, gaps, immutable
catalog versions, and audit history are Snowflake data. An isolated candidate
test never changes active rules or stored workflow rows.

## Product Request evidence result

The current corpus contains ten paired BEFORE/AFTER workbooks. The XLSX parser
rejects two physically present but completely empty spreadsheet rows, leaving
6,920 logical evidence rows.

| Measure | Result |
|---|---:|
| Logical BEFORE/AFTER pairs | 6,920 / 6,920 |
| Unmatched rows | 0 |
| Ambiguous duplicate alignments | 2 |
| Atomic AFTER outcome fields | 3 |

The responsive discovery pass recompiles the uploaded policy pack and
recomputes stage-scoped residual coverage, aliases, gaps, conflicts, and corpus
parity. The expensive mandatory leave-one-date-out suite is an explicit
activation-validation action, so reviewing a table or changing a screen does
not rerun the corpus. No fixed generated catalog is kept in this repository,
and ONE ENGINE does not manufacture row-specific fallback rules to claim
parity. A corpus that still contains unexplained decisions can be saved and
tested as an ineligible immutable draft with persistent gaps; activation
remains blocked until those gaps are resolved with governed business logic.

## Rules Distillery workflow

1. Open **Rules Distillery** in ONE ENGINE and choose **New distillation**.
2. Select the workflow profile.
3. Upload the standardized logic matrix and process documents. The 53 currently
   known Product Request rules are partial anchors, not a completeness target.
4. Map or upload the governed reference datasets used by the policy.
5. Upload the accumulated BEFORE and AFTER files. A ZIP can contain many
   matching source files.
6. Run the quick discovery pass, then use **Review latest run** to work one
   operator surface at a time instead of rendering the entire evidence package.
7. Classify non-output mutations as enrichment, correction, volatile metadata,
   or unresolved. Review raw and canonical permutations for the atomic result:
   `ACTION`, `If In Stock: Action`, and `Audit Action`.
8. Review uncertain aliases, source-backed rules, draft amendments, one-date
   rules, residual clusters, conflicts, and persistent gaps.
9. Save an immutable draft whenever a useful iteration should be preserved.
   Saving never changes active rules and does not imply activation eligibility.
10. From **Saved versions**, compare active versus candidate results against an uploaded file, live
   Product Request data, or an existing batch. This comparison is read-only.
11. Run full activation validation. Activate only an eligible candidate, or
    roll back to any retained version.

The promotion gate requires:

- 100% exact accumulated-corpus parity;
- zero unmatched rows;
- zero unresolved ambiguous evidence matches;
- zero contradictory identical input states;
- zero filter conflicts and unresolved gaps;
- zero pending outcome-alias reviews;
- zero missing, contradictory, or pending governed reference contracts;
- zero pending policy amendments;
- explicit approval for every filter supported by only one date;
- 100% minimum leave-one-date-out accuracy;
- zero SHA, Case#, pair-ID, source-date, raw-note, arbitrary product-text, or
  sampled-threshold predicates;
- passing numeric-boundary and metamorphic invariance tests.

SHA-256 values remain source and evidence lineage metadata only. They are not
available in the runtime predicate context and cannot route a Product Request.

The current adapters accept XLSX/XLSM, CSV/TSV/text, JSON/JSONL/NDJSON,
Parquet, Feather, and ZIP collections. Rules are always scoped with
`__ruleset_id`, preventing catalogs for future workflows from executing against
Product Request rows.

## Long-game extension model

ONE ENGINE is one runtime, not one hard-coded workflow. Each workflow has an
internal profile that defines source adapters, aliases, atomic outcome fields,
governed predicate fields, matching strategy, validation grouping, and feature
projection.

Under the single-file Snowflake constraint, add the next workflow directly in
`streamlit_app.py`:

1. Add its profile to `DISTILLERY_PROFILES`.
2. Add a projector only if generic raw-field features are insufficient.
3. Register that projector in `DISTILLERY_PROJECTORS`.
4. add paired evidence and require the same deployment gate;
5. add a runtime contract proving the promoted predicates and actions execute
   through ONE ENGINE.

The evidence and resulting catalogs remain database data, never source files.

## Snowflake backend

[`snowflake/compliance_rules_backend.sql`](snowflake/compliance_rules_backend.sql)
provisions the eleven-table backend:

- `COMPLIANCE_RULES_BATCHES`
- `COMPLIANCE_RULES_WORKFLOW_ROWS`
- `COMPLIANCE_RULES_RULES`
- `COMPLIANCE_RULES_RUNS`
- `COMPLIANCE_RULES_ROW_RESULTS`
- `COMPLIANCE_RULES_AUDIT_EVENTS`
- `COMPLIANCE_RULES_REFERENCE_LISTS`
- `COMPLIANCE_RULES_CATALOG_VERSIONS`
- `COMPLIANCE_RULES_CATALOG_VERSION_RULES`
- `COMPLIANCE_RULES_DISTILLERY_GAPS`
- `COMPLIANCE_RULES_OUTCOME_ALIASES`

The script also migrates existing `COMPLIANCE_RULES_WORKFLOW_ROWS` tables with
the `AUDIT_ACTION` column and upgrades `COMPLIANCE_RULES_REFERENCE_LISTS` into
a workflow-aware, typed, versioned dataset contract while retaining legacy
simple-list compatibility. The app performs no runtime DDL. Run the backend
SQL before deploying this app version, then use **Settings → Verify backend
tables**.

### Live Product Request source

**Process Workbook** offers two equivalent ingestion paths:

- **Upload a file** for CSV, TSV, XLSX, or XLSM input.
- **Use Live Product Request Data** to snapshot
  `FOODBUY_MASALA_PROD.COMPLIANCE_LAB.V_OE_PRODUCTREQUESTS`.

The live view is read directly through the active Snowflake session. Its rows
use the same header normalization, preview, duplicate detection, persistent
batch, audit, and optional immediate rules execution as uploaded files. Each
snapshot receives a deterministic SHA-256 lineage value, so the exact source
state can be identified and duplicate ingestion can be blocked or explicitly
allowed.

The Streamlit owner role needs `USAGE` on the database and schema plus `SELECT`
on `V_OE_PRODUCTREQUESTS`. The app does not create, replace, or modify the view.

## Verification

Run:

```powershell
py -3 -m unittest discover -s tests -v
```

The contracts verify the embedded seed catalog, application self-check,
complete 10-date/6,920-row evidence alignment, the three-field atomic outcome,
safe alias review, policy-stage minimization, policy boundary enforcement,
typed reference enrichment, preserve/set/clear effects, identity-predicate
exclusion, blank clearing, candidate isolation, and the versioned Snowflake
contract.

Do not commit `.env` or `.streamlit/secrets.toml`.
