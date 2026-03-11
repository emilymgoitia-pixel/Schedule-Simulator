import pandas as pd

PHASE_MAP = {
    "Design": "Design",
    "Permitting": "Permitting",
    "Utility Power": "Site Power",
    "Site Power Utility Construction": "Site Power",
    "Natural Gas": "Site Power",
    "OFCI Production": "OFCI Procurement",
    "Early Civil": "Civil",
    "Site/Civil": "Civil",
    "Construction": "Shell",
    "Core and Shell": "Shell",
    "Equipment Yard": "Equipment Yard",
    "MEP Fitout": "MEP Fitup",
    "Commissioning": "Commissioning",
    "Tenant Fitout": "Tenant Fitout",
}

PHASE_ORDER = [
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

RAW_OPC_COLUMNS = [
    "Name",
    "Start",
    "Finish",
    "Mano Phases",
    "Area",
    "Project",
    "Data Hall",
    "MW",
    "Predecessor Details",
]


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().replace("*", "") for c in df.columns]
    return df


def _require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required OPC columns: {', '.join(missing)}")


def parse_opc(file, selected_project=None, selected_area=None):
    """Parse raw OPC export into simulator phases and optional Data Hall seed data."""
    df = pd.read_excel(file)
    df = clean_columns(df)

    # Support the raw export shape expected by internal teams.
    _require_columns(df, ["Mano Phases", "Start", "Finish", "Project", "Area"])

    if selected_project:
        df = df[df["Project"] == selected_project]
    if selected_area:
        df = df[df["Area"] == selected_area]

    df["Start"] = pd.to_datetime(df["Start"], errors="coerce")
    df["Finish"] = pd.to_datetime(df["Finish"], errors="coerce")

    df["Phase"] = df["Mano Phases"].map(PHASE_MAP)
    df = df.dropna(subset=["Phase", "Start", "Finish"])

    grouped = (
        df.groupby("Phase", as_index=False)
        .agg(
            Start=("Start", "min"),
            Finish=("Finish", "max"),
        )
    )

    grouped["DurationDays"] = (grouped["Finish"] - grouped["Start"]).dt.days.clip(lower=0)
    grouped["Enabled"] = True

    existing = set(grouped["Phase"])
    missing_phases = [p for p in PHASE_ORDER if p not in existing]

    fallback_start = grouped["Start"].min() if not grouped.empty else pd.Timestamp.today().normalize()

    for phase in missing_phases:
        grouped = pd.concat(
            [
                grouped,
                pd.DataFrame(
                    [{
                        "Phase": phase,
                        "Start": fallback_start,
                        "Finish": fallback_start,
                        "DurationDays": 0,
                        "Enabled": phase != "Tenant Fitout",
                    }]
                ),
            ],
            ignore_index=True,
        )

    grouped["Phase"] = pd.Categorical(grouped["Phase"], categories=PHASE_ORDER, ordered=True)
    grouped = grouped.sort_values("Phase").reset_index(drop=True)

    dh_seed = build_datahall_seed_from_opc(df)
    return grouped, dh_seed


def build_datahall_seed_from_opc(df: pd.DataFrame) -> pd.DataFrame:
    """Build Data Hall/MW defaults from OPC rows when fields are present."""
    if "Data Hall" not in df.columns:
        return build_default_datahall_table()

    working = df.copy()
    working["Data Hall"] = working["Data Hall"].astype(str).str.strip()
    working = working[working["Data Hall"].notna() & (working["Data Hall"] != "")]
    if working.empty:
        return build_default_datahall_table()

    if "MW" in working.columns:
        working["MW"] = pd.to_numeric(working["MW"], errors="coerce")
    else:
        working["MW"] = pd.NA

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

    return dh.rename(columns={"Data Hall": "DataHall"})[["DataHall", "MW", "CxDurationDays", "LagFromPriorDH"]]


def build_default_datahall_table():
    return pd.DataFrame([
        {"DataHall": "DH1", "MW": 16.8, "CxDurationDays": 60, "LagFromPriorDH": 0},
        {"DataHall": "DH2", "MW": 16.8, "CxDurationDays": 60, "LagFromPriorDH": 30},
        {"DataHall": "DH3", "MW": 16.8, "CxDurationDays": 60, "LagFromPriorDH": 30},
    ])
