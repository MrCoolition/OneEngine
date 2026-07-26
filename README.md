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
runtime Python packages to deploy. Candidate catalogs exist only in the current
browser session until an authorized user promotes them. Promotion writes the
rules to `COMPLIANCE_RULES_RULES` and the complete validation report to
`COMPLIANCE_RULES_AUDIT_EVENTS`.

## Product Request evidence result

The current corpus contains ten paired BEFORE/AFTER workbooks. The XLSX parser
rejects two physically present but completely empty spreadsheet rows, leaving
6,920 logical evidence rows.

| Measure | Result |
|---|---:|
| Logical BEFORE/AFTER pairs | 6,920 / 6,920 |
| Unmatched rows | 0 |
| Ambiguous duplicate alignments | 2 |
| Contradictory identical BEFORE states | 0 |
| Generated rules | 169 |
| Exact accumulated-corpus parity | 100.00% |
| Mean leave-one-source-file-out accuracy | 69.37% |

The catalog contains:

- 112 reusable general rules, promoted only from 100%-pure leaves with at
  least three supporting rows.
- 57 evidence rules that deterministically close the residual states already
  observed in reviewed evidence.

Historical parity and unseen-file generalization are reported separately. The
Distillery's leave-one-source-group-out validation is the honest estimate for
a new daily file; evidence rules are never allowed to disguise that score.

## Rules Distillery workflow

1. Open **Rules Distillery** in ONE ENGINE.
2. Select the workflow profile.
3. Upload the accumulated BEFORE and AFTER files. A ZIP can contain many
   matching source files.
4. Run alignment, rule induction, residual closure, corpus validation, and
   optional source-group holdouts.
5. Review the validation report and deployment gate.
6. Confirm promotion. ONE ENGINE atomically retires obsolete rules for that
   workflow and upserts the new catalog into Snowflake.

The promotion gate requires:

- 100% exact accumulated-corpus parity;
- zero unmatched rows;
- zero contradictory identical input states.

The current adapters accept XLSX/XLSM, CSV/TSV/text, JSON/JSONL/NDJSON,
Parquet, Feather, and ZIP collections. Rules are always scoped with
`__ruleset_id`, preventing catalogs for future workflows from executing against
Product Request rows.

## Long-game extension model

ONE ENGINE is one runtime, not one hard-coded workflow. Each workflow has an
internal profile that defines aliases, output fields, identity strategies,
similarity fields, induction fields, validation grouping, and feature
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
provisions the seven-table backend:

- `COMPLIANCE_RULES_BATCHES`
- `COMPLIANCE_RULES_WORKFLOW_ROWS`
- `COMPLIANCE_RULES_RULES`
- `COMPLIANCE_RULES_RUNS`
- `COMPLIANCE_RULES_ROW_RESULTS`
- `COMPLIANCE_RULES_AUDIT_EVENTS`
- `COMPLIANCE_RULES_REFERENCE_LISTS`

The app performs no runtime DDL. Use **Settings → Verify backend tables** after
deployment.

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
complete evidence alignment, compact distilled catalog, rule scoping, and exact
execution of all 6,920 expected decisions through the actual Streamlit runtime.

Do not commit `.env` or `.streamlit/secrets.toml`.
