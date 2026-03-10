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
    "Tenant Fitout": "Tenant Fitout"
}

def parse_opc(file):

    df = pd.read_excel(file)

    df["SimulationPhase"] = df["Mano Phases"].map(PHASE_MAP)

    df = df.dropna(subset=["SimulationPhase"])

    grouped = df.groupby("SimulationPhase").agg(
        Start=("Start", "min"),
        Finish=("Finish", "max")
    ).reset_index()

    grouped["DurationDays"] = (grouped["Finish"] - grouped["Start"]).dt.days

    grouped.rename(columns={"SimulationPhase": "Phase"}, inplace=True)

    grouped["Enabled"] = True

    return grouped