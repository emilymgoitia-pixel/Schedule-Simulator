import pandas as pd

RELATIONSHIPS = [
    {"pred": "Design", "succ": "Permitting", "type": "FS", "lag": 0},
    {"pred": "Permitting", "succ": "Civil", "type": "FS", "lag": 0},
    {"pred": "Civil", "succ": "Shell", "type": "FS", "lag": 0},
    {"pred": "Civil", "succ": "Equipment Yard", "type": "FS", "lag": 0},
    {"pred": "Shell", "succ": "MEP Fitup", "type": "FS", "lag": 0},
    {"pred": "OFCI Procurement", "succ": "MEP Fitup", "type": "FF", "lag": 0},
    {"pred": "MEP Fitup", "succ": "Commissioning", "type": "FS", "lag": 0},
    {"pred": "Site Power", "succ": "Commissioning", "type": "FS", "lag": 0},
    {"pred": "Commissioning", "succ": "Tenant Fitout", "type": "FS", "lag": 0},
]


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Start"] = pd.to_datetime(df["Start"], errors="coerce")
    df["Finish"] = pd.to_datetime(df["Finish"], errors="coerce")
    df["DurationDays"] = pd.to_numeric(df["DurationDays"], errors="coerce").fillna(0).astype(int)
    df["Enabled"] = df["Enabled"].fillna(True).astype(bool)
    return df


def recalc_schedule(phases: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    df = normalize_dates(phases)

    # keep finish aligned to start + duration
    df["Finish"] = df["Start"] + pd.to_timedelta(df["DurationDays"], unit="D")

    conflicts = []
    phase_to_idx = {row["Phase"]: idx for idx, row in df.iterrows()}

    for _ in range(len(df) + 5):
        changed = False

        for rel in RELATIONSHIPS:
            pred = rel["pred"]
            succ = rel["succ"]
            rel_type = rel["type"].upper()
            lag = int(rel.get("lag", 0))

            if pred not in phase_to_idx or succ not in phase_to_idx:
                continue

            p = phase_to_idx[pred]
            s = phase_to_idx[succ]

            if not bool(df.at[p, "Enabled"]) or not bool(df.at[s, "Enabled"]):
                continue

            pred_finish = df.at[p, "Finish"]
            succ_start = df.at[s, "Start"]
            succ_finish = df.at[s, "Finish"]
            duration = int(df.at[s, "DurationDays"])

            if pd.isna(pred_finish) or pd.isna(succ_start) or pd.isna(succ_finish):
                continue

            if rel_type == "FS":
                required_start = pred_finish + pd.Timedelta(days=lag)
                if succ_start < required_start:
                    df.at[s, "Start"] = required_start
                    df.at[s, "Finish"] = required_start + pd.Timedelta(days=duration)
                    changed = True

            elif rel_type == "FF":
                required_finish = pred_finish + pd.Timedelta(days=lag)
                if succ_finish < required_finish:
                    df.at[s, "Finish"] = required_finish
                    df.at[s, "Start"] = required_finish - pd.Timedelta(days=duration)
                    changed = True

        if not changed:
            break

    # collect conflicts after recalculation
    for rel in RELATIONSHIPS:
        pred = rel["pred"]
        succ = rel["succ"]
        rel_type = rel["type"].upper()
        lag = int(rel.get("lag", 0))

        if pred not in phase_to_idx or succ not in phase_to_idx:
            continue

        p = phase_to_idx[pred]
        s = phase_to_idx[succ]

        if not bool(df.at[p, "Enabled"]) or not bool(df.at[s, "Enabled"]):
            continue

        pred_finish = df.at[p, "Finish"]
        succ_start = df.at[s, "Start"]
        succ_finish = df.at[s, "Finish"]

        if rel_type == "FS":
            required_start = pred_finish + pd.Timedelta(days=lag)
            if succ_start < required_start:
                conflicts.append(
                    f"{succ} violates FS from {pred}: {succ_start.date()} < {required_start.date()}"
                )

        elif rel_type == "FF":
            required_finish = pred_finish + pd.Timedelta(days=lag)
            if succ_finish < required_finish:
                conflicts.append(
                    f"{succ} violates FF from {pred}: {succ_finish.date()} < {required_finish.date()}"
                )

    return df, conflicts


def derive_milestones(
    phases: pd.DataFrame,
    ntp_date=None,
    power_on_date=None,
    esa_date=None,
) -> pd.DataFrame:
    df = phases.copy()
    lookup = df.set_index("Phase")

    def phase_start(name: str):
        if name in lookup.index and bool(lookup.at[name, "Enabled"]):
            return lookup.at[name, "Start"]
        return pd.NaT

    def phase_finish(name: str):
        if name in lookup.index and bool(lookup.at[name, "Enabled"]):
            return lookup.at[name, "Finish"]
        return pd.NaT

    tfo_enabled = False
    if "Tenant Fitout" in lookup.index:
        tfo_enabled = bool(lookup.at["Tenant Fitout", "Enabled"])

    rfs_date = phase_finish("Tenant Fitout") if tfo_enabled else phase_finish("Commissioning")

    milestones = [
        {"Milestone": "NTP", "Date": pd.to_datetime(ntp_date) if ntp_date else pd.NaT},
        {"Milestone": "Design Finish", "Date": phase_finish("Design")},
        {"Milestone": "Building Permit Issued", "Date": phase_finish("Permitting")},
        {"Milestone": "Shell Start", "Date": phase_start("Shell")},
        {"Milestone": "Building Civil Start", "Date": phase_start("Civil")},
        {"Milestone": "Power On", "Date": pd.to_datetime(power_on_date) if power_on_date else pd.NaT},
        {"Milestone": "Civil Complete", "Date": phase_finish("Civil")},
        {"Milestone": "Commissioning Start", "Date": phase_start("Commissioning")},
        {"Milestone": "Shell Complete", "Date": phase_finish("Shell")},
        {"Milestone": "Yard Complete", "Date": phase_finish("Equipment Yard")},
        {"Milestone": "OFCI Procurement Complete", "Date": phase_finish("OFCI Procurement")},
        {"Milestone": "Fitup Complete", "Date": phase_finish("MEP Fitup")},
        {"Milestone": "ESA", "Date": pd.to_datetime(esa_date) if esa_date else pd.NaT},
        {"Milestone": "RFS", "Date": rfs_date},
        {"Milestone": "Project Complete", "Date": rfs_date},
    ]

    return pd.DataFrame(milestones)


def calculate_datahall_rfs(phases: pd.DataFrame, dh_df: pd.DataFrame) -> pd.DataFrame:
    lookup = phases.set_index("Phase")

    if "Commissioning" not in lookup.index or pd.isna(lookup.at["Commissioning", "Start"]):
        out = dh_df.copy()
        out["CxStart"] = pd.NaT
        out["CxFinish"] = pd.NaT
        out["RFSDate"] = pd.NaT
        return out

    commissioning_start = lookup.at["Commissioning", "Start"]

    out = dh_df.copy().reset_index(drop=True)
    out["CxStart"] = pd.NaT
    out["CxFinish"] = pd.NaT
    out["RFSDate"] = pd.NaT

    previous_finish = pd.NaT

    for i, row in out.iterrows():
        duration = int(row["CxDurationDays"])
        lag = int(row["LagFromPriorDH"])

        if pd.isna(previous_finish):
            cx_start = commissioning_start
        else:
            cx_start = previous_finish + pd.Timedelta(days=lag)

        cx_finish = cx_start + pd.Timedelta(days=duration)
        rfs_date = cx_finish

        out.at[i, "CxStart"] = cx_start
        out.at[i, "CxFinish"] = cx_finish
        out.at[i, "RFSDate"] = rfs_date

        previous_finish = cx_finish

    return out


def apply_first_final_rfs(milestones: pd.DataFrame, dh_results: pd.DataFrame) -> pd.DataFrame:
    milestones = milestones.copy()

    if dh_results.empty or dh_results["RFSDate"].isna().all():
        first_rfs = pd.NaT
        final_rfs = pd.NaT
    else:
        first_rfs = dh_results["RFSDate"].min()
        final_rfs = dh_results["RFSDate"].max()

    extra = pd.DataFrame([
        {"Milestone": "First RFS", "Date": first_rfs},
        {"Milestone": "Final RFS", "Date": final_rfs},
    ])

    milestones = pd.concat([milestones, extra], ignore_index=True)
    return milestones


def months_after_ntp_text(ntp_date, target_date) -> str:
    if pd.isna(ntp_date) or pd.isna(target_date):
        return ""
    total_days = (target_date - ntp_date).days
    months_decimal = total_days / 30.4375
    rounded = round(months_decimal * 2) / 2
    if rounded < 1:
        return f"{total_days} days"
    if float(rounded).is_integer():
        return f"{int(rounded)} months"
    return f"{rounded:.1f} months"
