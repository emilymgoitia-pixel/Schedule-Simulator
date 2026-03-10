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


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().replace("*", "") for c in df.columns]
    return df


def parse_opc(file, selected_project=None, selected_area=None):
    df = pd.read_excel(file)
    df = clean_columns(df)

    required = ["Project", "Area", "Mano Phases", "Start", "Finish"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required OPC columns: {', '.join(missing)}")

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

    grouped["DurationDays"] = (grouped["Finish"] - grouped["Start"]).dt.days
    grouped["Enabled"] = True

    # Make sure all expected phases exist
    existing = set(grouped["Phase"])
    missing_phases = [p for p in PHASE_ORDER if p not in existing]

    if not grouped.empty:
        fallback_start = grouped["Start"].min()
    else:
        fallback_start = pd.Timestamp.today().normalize()

    for i, phase in enumerate(missing_phases):
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

    return grouped


def build_default_datahall_table():
    return pd.DataFrame([
        {"DataHall": "DH1", "MW": 16.8, "CxDurationDays": 60, "LagFromPriorDH": 0},
        {"DataHall": "DH2", "MW": 16.8, "CxDurationDays": 60, "LagFromPriorDH": 30},
        {"DataHall": "DH3", "MW": 16.8, "CxDurationDays": 60, "LagFromPriorDH": 30},
    ])
