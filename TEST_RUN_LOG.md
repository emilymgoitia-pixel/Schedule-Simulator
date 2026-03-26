# Functional Test Run Log (2026-03-26)

- `python -m py_compile app.py schedule_engine.py opc_parser.py` passed.
- `recalc_schedule` dependency propagation sanity checks passed:
  - Permitting extension pushed Civil start to 2026-07-16.
  - Commissioning start matched max(MEP Fitup finish, Site Power finish).
- Data Hall RFS rollup sanity checks passed:
  - `Project Complete == Final RFS`.
- Streamlit startup smoke check passed on `0.0.0.0:8080`.
