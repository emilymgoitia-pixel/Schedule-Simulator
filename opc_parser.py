"""
OPC / P6 Excel export parser.

Handles column-name variations via fuzzy matching and parses P6-style
Predecessor Details strings into a phase-level relationship list compatible
with schedule_engine.recalc_schedule.

Returns a 3-tuple: (phases_df, dh_df, relationships_or_None)
If relationships_or_None is None the caller should fall back to
schedule_engine.DEFAULT_RELATIONSHIPS.
"""

import re
import pandas as pd
from schedule_engine import DEFAULT_RELATIONSHIPS

# ---------------------------------------------------------------------------
# Phase name mapping — raw OPC/P6 phase names → simulator phase names
# ---------------------------------------------------------------------------

PHASE_MAP: dict[str, str] = {
    "Design":                          "Design",
    "Permitting":                      "Permitting",
    "Utility Power":                   "Site Power",
    "Site Power Utility Construction": "Site Power",
    "Natural Gas":                     "Site Power",
    "OFCI Production":                 "OFCI Procurement",
    "OFCI Procurement":                "OFCI Procurement",
    "Early Civil":                     "Civil",
    "Site/Civil":                      "Civil",
    "Civil":                           "Civil",
    "Construction":                    "Shell",
    "Core and Shell":                  "Shell",
    "Shell":                           "Shell",
    "Equipment Yard":                  "Equipment Yard",
    "MEP Fitout":                      "MEP Fitup",
    "MEP Fitup":                       "MEP Fitup",
    "Commissioning":                   "Commissioning",
    "Tenant Fitout":                   "Tenant Fitout",
}

PHASE_ORDER: list[str] = [
    "Design",
    "Permitting",
    "Site Power",
    "OFCI Procurement",
    "Civil",
    "Shell",
    "Equipment Yard",
    "MEP Fitup",
    "Commissioning",
    "Tenant Fitout",
]

# ---------------------------------------------------------------------------
# Regex for P6 predecessor strings
# Handles forms like: A1000FS, A-1000 FS+5d, E100SS-3, DESIGN-01 FF+10d
# ---------------------------------------------------------------------------
_PRED_RE = re.compile(
    r"""
    ([A-Za-z][A-Za-z0-9_\-\.]+)   # Activity ID or name (must start with a letter)
    \s*
    (FS|SS|FF|SF)                  # Relationship type (case-insensitive)
    \s*
    ([+\-]\s*\d+)?                 # Optional lag (e.g. +5 or -3)
    \s*[dD]?                       # Optional 'd' unit suffix
    """,
    re.IGNORECASE | re.VERBOSE,
)


# ---------------------------------------------------------------------------
# Fuzzy column finder
# ---------------------------------------------------------------------------

def _fuzzy_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    Return the first column whose normalised name matches any candidate.
    Normalisation: lowercase, strip spaces/underscores/asterisks.
    """
    def norm(s: str) -> str:
        return s.lower().strip().replace(" ", "").replace("_", "").replace("*", "")

    col_norm = {norm(c): c for c in df.columns}
    for candidate in candidates:
        hit = col_norm.get(norm(candidate))
        if hit is not None:
            return hit
    return None


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().replace("*", "") for c in df.columns]
    return df


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_opc(
    file,
    selected_project: str | None = None,
    selected_area: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list | None]:
    """
    Parse a raw OPC / P6 Excel export.

    Returns
    -------
    phases : DataFrame  — phase-level schedule (PHASE_ORDER rows)
    dh_seed : DataFrame — data-hall defaults for the simulator
    relationships : list of dicts | None
        Phase-level predecessor relationships parsed from the file.
        None if no usable predecessor data was found (caller uses DEFAULT_RELATIONSHIPS).
    """
    df = pd.read_excel(file)
    df = clean_columns(df)

    # ---- Fuzzy column resolution ----------------------------------------
    mano_col  = _fuzzy_col(df, ["Mano Phases", "Mano Phase", "ManoPhasees",
                                 "Activity Phase", "Phase"])
    start_col = _fuzzy_col(df, ["Start", "Start Date", "Early Start", "ES"])
    finish_col = _fuzzy_col(df, ["Finish", "Finish Date", "Early Finish", "EF"])
    proj_col  = _fuzzy_col(df, ["Project", "Project Name", "Project ID"])
    area_col  = _fuzzy_col(df, ["Area", "Location", "Site"])
    name_col  = _fuzzy_col(df, ["Name", "Activity Name", "Task Name"])
    id_col    = _fuzzy_col(df, ["Activity ID", "ActivityID", "Act ID", "ID", "Task ID"])
    pred_col  = _fuzzy_col(df, ["Predecessor Details", "Predecessors", "Predecessor",
                                 "Pred", "Dependencies", "Predecessor List"])
    mw_col    = _fuzzy_col(df, ["MW", "Megawatts", "Capacity MW", "Capacity"])
    dh_col    = _fuzzy_col(df, ["Data Hall", "DataHall", "Hall", "Data Hall Name"])

    # ---- Validate required columns --------------------------------------
    missing = []
    if not mano_col:  missing.append("Mano Phases")
    if not start_col: missing.append("Start")
    if not finish_col: missing.append("Finish")
    if missing:
        available = ", ".join(df.columns.tolist())
        raise ValueError(
            f"Missing required columns: {', '.join(missing)}. "
            f"Columns found in file: {available}"
        )

    # ---- Rename to canonical names ---------------------------------------
    rename: dict[str, str] = {
        start_col:  "Start",
        finish_col: "Finish",
        mano_col:   "Mano Phases",
    }
    optional = {
        proj_col:  "Project",
        area_col:  "Area",
        name_col:  "Name",
        id_col:    "ActivityID",
        pred_col:  "Predecessor Details",
        mw_col:    "MW",
        dh_col:    "Data Hall",
    }
    for src, dst in optional.items():
        if src and src != dst:
            rename[src] = dst
    df = df.rename(columns=rename)

    # Ensure optional columns exist (as NA) even if absent from the file
    for col in ("Project", "Area", "Name", "ActivityID",
                "Predecessor Details", "MW", "Data Hall"):
        if col not in df.columns:
            df[col] = pd.NA

    # ---- Project / Area filter ------------------------------------------
    if selected_project and "Project" in df.columns:
        df = df[df["Project"] == selected_project]
    if selected_area and "Area" in df.columns:
        df = df[df["Area"] == selected_area]

    # ---- Parse dates and map phases -------------------------------------
    df["Start"] = pd.to_datetime(df["Start"], errors="coerce")
    df["Finish"] = pd.to_datetime(df["Finish"], errors="coerce")
    df["Phase"] = df["Mano Phases"].map(PHASE_MAP)
    df = df.dropna(subset=["Phase", "Start", "Finish"])

    # ---- Aggregate to phase level (earliest start, latest finish) --------
    grouped = (
        df.groupby("Phase", as_index=False)
        .agg(Start=("Start", "min"), Finish=("Finish", "max"))
    )
    grouped["DurationDays"] = (
        (grouped["Finish"] - grouped["Start"]).dt.days.clip(lower=0)
    )
    grouped["Enabled"] = True

    # Add any phases missing from the file as zero-duration placeholders
    existing = set(grouped["Phase"])
    fallback_start = (
        grouped["Start"].min()
        if not grouped.empty
        else pd.Timestamp.today().normalize()
    )
    for phase in PHASE_ORDER:
        if phase not in existing:
            grouped = pd.concat(
                [
                    grouped,
                    pd.DataFrame([{
                        "Phase": phase,
                        "Start": fallback_start,
                        "Finish": fallback_start,
                        "DurationDays": 0,
                        "Enabled": phase != "Tenant Fitout",
                    }]),
                ],
                ignore_index=True,
            )

    grouped["Phase"] = pd.Categorical(grouped["Phase"], categories=PHASE_ORDER, ordered=True)
    grouped = grouped.sort_values("Phase").reset_index(drop=True)

    # ---- Parse predecessor relationships --------------------------------
    relationships = _parse_relationships(df)

    # ---- Data Hall seed -------------------------------------------------
    dh_seed = build_datahall_seed_from_opc(df)

    return grouped, dh_seed, relationships


# ---------------------------------------------------------------------------
# Predecessor relationship parser
# ---------------------------------------------------------------------------

def _parse_relationships(df: pd.DataFrame) -> list | None:
    """
    Build a phase-level RELATIONSHIPS list from P6 Predecessor Details.

    Each row in the OPC export represents one activity. The Predecessor Details
    column lists that activity's predecessors in P6 format (e.g. 'A1000FS+0d').
    We map activity IDs → phases and emit phase-to-phase relationships,
    deduplicating within the same phase pair and relationship type.

    Returns None if the column is absent or contains no parseable data,
    signalling the caller to fall back to DEFAULT_RELATIONSHIPS.
    """
    if (
        "Predecessor Details" not in df.columns
        or df["Predecessor Details"].isna().all()
    ):
        return None

    # ---- Build activity-ID → phase and activity-name → phase lookups ----
    id_to_phase: dict[str, str] = {}
    name_to_phase: dict[str, str] = {}

    for _, row in df.iterrows():
        phase = row.get("Phase")
        if pd.isna(phase) or phase not in PHASE_ORDER:
            continue

        act_id = str(row.get("ActivityID", "")).strip()
        act_name = str(row.get("Name", "")).strip()
        mano_name = str(row.get("Mano Phases", "")).strip()

        for key, mapping in ((act_id, id_to_phase), (act_name, name_to_phase), (mano_name, name_to_phase)):
            if key and key not in ("nan", ""):
                mapping[key] = phase

    def lookup(key: str) -> str | None:
        k = str(key).strip()
        return id_to_phase.get(k) or name_to_phase.get(k)

    # ---- Parse each row's predecessor list ------------------------------
    # Track (pred_phase, succ_phase, rel_type) to deduplicate; on collision
    # keep the most conservative lag (smallest value) so the tightest
    # constraint wins at the phase level.
    seen: dict[tuple, int] = {}          # key → index in relationships list
    relationships: list[dict] = []

    for _, row in df.iterrows():
        succ_phase = row.get("Phase")
        if pd.isna(succ_phase) or succ_phase not in PHASE_ORDER:
            continue

        pred_details = str(row.get("Predecessor Details", "")).strip()
        if not pred_details or pred_details in ("nan", ""):
            continue

        for m in _PRED_RE.finditer(pred_details):
            pred_id   = m.group(1)
            rel_type  = m.group(2).upper()
            lag_str   = m.group(3)
            lag       = int(lag_str.replace(" ", "")) if lag_str else 0

            pred_phase = lookup(pred_id)
            if not pred_phase or pred_phase == succ_phase:
                continue

            key = (pred_phase, succ_phase, rel_type)
            if key in seen:
                # Keep the smallest (most conservative) lag seen for this pair
                relationships[seen[key]]["lag"] = min(relationships[seen[key]]["lag"], lag)
            else:
                seen[key] = len(relationships)
                relationships.append({
                    "pred": pred_phase,
                    "succ": succ_phase,
                    "type": rel_type,
                    "lag":  lag,
                })

    if not relationships:
        return None  # Nothing parseable — signal to use defaults

    return relationships


# ---------------------------------------------------------------------------
# Data Hall helpers
# ---------------------------------------------------------------------------

def build_datahall_seed_from_opc(df: pd.DataFrame) -> pd.DataFrame:
    """Build Data Hall / MW defaults from OPC rows when fields are present."""
    if "Data Hall" not in df.columns:
        return build_default_datahall_table()

    working = df.copy()
    working["Data Hall"] = working["Data Hall"].astype(str).str.strip()
    working = working[
        working["Data Hall"].notna()
        & (working["Data Hall"] != "")
        & (working["Data Hall"] != "nan")
    ]
    if working.empty:
        return build_default_datahall_table()

    working["MW"] = (
        pd.to_numeric(working.get("MW", pd.NA), errors="coerce")
        if "MW" in working.columns else pd.NA
    )

    dh = (
        working.groupby("Data Hall", as_index=False)
        .agg(MW=("MW", "max"))
        .sort_values("Data Hall")
        .reset_index(drop=True)
    )
    dh["CxDurationDays"] = 60
    dh["LagFromPriorDH"] = 30
    if not dh.empty:
        dh.at[0, "LagFromPriorDH"] = 0

    return dh.rename(columns={"Data Hall": "DataHall"})[
        ["DataHall", "MW", "CxDurationDays", "LagFromPriorDH"]
    ]


def build_default_datahall_table() -> pd.DataFrame:
    return pd.DataFrame([
        {"DataHall": "DH1", "MW": 16.8, "CxDurationDays": 60, "LagFromPriorDH":  0},
        {"DataHall": "DH2", "MW": 16.8, "CxDurationDays": 60, "LagFromPriorDH": 30},
        {"DataHall": "DH3", "MW": 16.8, "CxDurationDays": 60, "LagFromPriorDH": 30},
    ])
