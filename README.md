# ONE ENGINE

ONE ENGINE is a Snowflake-native compliance rules platform for deterministic,
auditable PRF/SORF/SRF decisioning.

## Runtime architecture

- **Streamlit in Snowflake** provides ingestion, execution, analyst review,
  reporting, rule administration, simulation, and diagnostics.
- **Snowpark** is the only application persistence path.
- **Snowflake** is the runtime source of truth for batches, rows, rules,
  executions, results, audit events, and reference lists.
- The historical Neon database is reference-only and is not required by the
  application or deployment.

## Repository layout

```text
app/
  streamlit_app.py                  Main Streamlit in Snowflake application
snowflake/
  compliance_rules_backend.sql     Idempotent seven-table backend provisioning
before.zip                          Source workbooks without decisions
after.zip                           Expected/analyst-completed workbooks
Rules_Engine-main (3).zip           Original implementation archive
```

## Snowflake deployment

1. Run `snowflake/compliance_rules_backend.sql` with an administrative role
   that can use the target warehouse, database, and schema.
2. Verify that all seven `COMPLIANCE_RULES_*` tables are returned by the
   readiness queries at the end of the script.
3. Create or update a Streamlit in Snowflake app using
   `app/streamlit_app.py` as the main file.
4. Add the packages listed in `environment.yml`.
5. Run **Settings → Verify backend tables** in ONE ENGINE.

The application does not create tables at runtime. It uses explicit,
fully-qualified Snowflake object names and seeds only missing bundled rules and
reference values.

## Current data contract

The active Snowflake backend contains:

- 53 rules
- 2 source batches
- 1,426 workflow rows
- 2 execution runs
- 1,426 row results
- 5 audit events
- 7 reference-list values

This matches the expected 53-rule catalog. No Neon synchronization is part of
the production path.

## Safety and auditability

- Dry-run, full-batch, and selected-row execution modes
- Ordered predicates and actions with stop-processing behavior
- Per-row execution traces and result snapshots
- Auditable analyst overrides and administrative changes
- CSV/XLSX exports and downloadable diagnostics
- Runtime DDL disabled

## Verification

Run the deterministic engine contracts:

```powershell
py -3 -m unittest discover -s tests -v
```

Measure decisions against the paired historical workbooks:

```powershell
py -3 tools/golden_parity.py
```

The parity tool joins rows by their complete non-decision payload, so workbook
reordering does not create false mismatches. It reports exact and semantic
agreement separately. BuySmart closeout is excluded from the parity score
because the supplied “after” workbooks leave that field blank.

Do not commit `.env` or `.streamlit/secrets.toml`.
