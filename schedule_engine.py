import pandas as pd
from collections import defaultdict, deque

# ---------------------------------------------------------------------------
# Default hard-coded relationship network (used when no file data is available)
# ---------------------------------------------------------------------------
DEFAULT_RELATIONSHIPS = [
    {"pred": "Design",        "succ": "Permitting",       "type": "FS", "lag": 0},
    {"pred": "Permitting",    "succ": "Civil",            "type": "FS", "lag": 0},
    {"pred": "Civil",         "succ": "Shell",            "type": "FS", "lag": 0},
    {"pred": "Civil",         "succ": "Equipment Yard",   "type": "FS", "lag": 0},
    {"pred": "Shell",         "succ": "MEP Fitup",        "type": "FS", "lag": 0},
    {"pred": "MEP Fitup",     "succ": "OFCI Procurement", "type": "FF", "lag": 0},
    {"pred": "MEP Fitup",     "succ": "Commissioning",    "type": "FS", "lag": 0},
    # Site Power / Power On is always a hard requirement for Commissioning.
    {"pred": "Site Power",    "succ": "Commissioning",    "type": "FS", "lag": 0},
    {"pred": "Commissioning", "succ": "Tenant Fitout",    "type": "FS", "lag": 0},
]


# ---------------------------------------------------------------------------
# Date normalisation
# ---------------------------------------------------------------------------

def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Start"] = pd.to_datetime(df["Start"], errors="coerce")
    df["Finish"] = pd.to_datetime(df["Finish"], errors="coerce")
    df["DurationDays"] = pd.to_numeric(df["DurationDays"], errors="coerce").fillna(0).astype(int)
    df["Enabled"] = df["Enabled"].fillna(True).astype(bool)
    return df


# ---------------------------------------------------------------------------
# Topological sort (Kahn's algorithm)
# ---------------------------------------------------------------------------

def _topological_sort(phase_names: set, relationships: list) -> list:
    """
    Return phase names in topological order using Kahn's algorithm.
    Raises ValueError if a circular dependency is detected.
    """
    in_degree: dict[str, int] = defaultdict(int)
    successors: dict[str, list] = defaultdict(list)

    for rel in relationships:
        p, s = rel["pred"], rel["succ"]
        if p in phase_names and s in phase_names:
            successors[p].append(s)
            in_degree[s] += 1

    queue = deque(n for n in phase_names if in_degree[n] == 0)
    order: list[str] = []

    while queue:
        node = queue.popleft()
        order.append(node)
        for s in successors[node]:
            in_degree[s] -= 1
            if in_degree[s] == 0:
                queue.append(s)

    if len(order) != len(phase_names):
        cycle_phases = phase_names - set(order)
        raise ValueError(
            f"Circular dependency detected among: {', '.join(sorted(cycle_phases))}"
        )
    return order


# ---------------------------------------------------------------------------
# Single-constraint forward push (push-forward only = CPM max semantics)
# ---------------------------------------------------------------------------

def _push_forward(df: pd.DataFrame, s_idx, p_idx, rel_type: str, lag: int) -> bool:
    """
    Apply one predecessor constraint to successor in-place.
    Only moves the successor *forward* (never backward), which correctly implements
    the CPM rule that an activity's Early Start is the MAX over all predecessor constraints.
    Returns True if any date was changed.
    """
    duration = int(df.at[s_idx, "DurationDays"])
    rel_type = rel_type.upper()

    if rel_type == "FS":
        pred_finish = df.at[p_idx, "Finish"]
        succ_start = df.at[s_idx, "Start"]
        if pd.isna(pred_finish) or pd.isna(succ_start):
            return False
        required = pred_finish + pd.Timedelta(days=lag)
        if succ_start < required:
            df.at[s_idx, "Start"] = required
            df.at[s_idx, "Finish"] = required + pd.Timedelta(days=duration)
            return True

    elif rel_type == "SS":
        pred_start = df.at[p_idx, "Start"]
        succ_start = df.at[s_idx, "Start"]
        if pd.isna(pred_start) or pd.isna(succ_start):
            return False
        required = pred_start + pd.Timedelta(days=lag)
        if succ_start < required:
            df.at[s_idx, "Start"] = required
            df.at[s_idx, "Finish"] = required + pd.Timedelta(days=duration)
            return True

    elif rel_type == "FF":
        pred_finish = df.at[p_idx, "Finish"]
        succ_finish = df.at[s_idx, "Finish"]
        if pd.isna(pred_finish) or pd.isna(succ_finish):
            return False
        required = pred_finish + pd.Timedelta(days=lag)
        if succ_finish < required:
            df.at[s_idx, "Finish"] = required
            df.at[s_idx, "Start"] = required - pd.Timedelta(days=duration)
            return True

    return False


# ---------------------------------------------------------------------------
# Backward pass — Late dates and Total Float
# ---------------------------------------------------------------------------

def _backward_pass(
    df: pd.DataFrame,
    enabled_phases: dict,
    relationships: list,
    topo_order: list,
) -> pd.DataFrame:
    """
    Compute LateStart, LateFinish, and TotalFloat for each enabled phase.
    Processes phases in reverse topological order so every successor's Late dates
    are available when constraining its predecessor.
    """
    for col in ("LateStart", "LateFinish", "TotalFloat"):
        if col not in df.columns:
            df[col] = pd.NA

    # Project end = maximum Early Finish across all enabled phases
    finishes = [
        df.at[i, "Finish"]
        for i in enabled_phases.values()
        if pd.notna(df.at[i, "Finish"])
    ]
    if not finishes:
        return df
    project_end = max(finishes)

    # Initialise all Late Finishes to project end (upper bound, will be tightened)
    for idx in enabled_phases.values():
        df.at[idx, "LateFinish"] = project_end

    # For each phase, which outbound relationships drive its successors?
    succ_rels: dict[str, list] = defaultdict(list)
    for rel in relationships:
        if rel["pred"] in enabled_phases and rel["succ"] in enabled_phases:
            succ_rels[rel["pred"]].append(rel)

    # Reverse-topological backward pass
    for phase in reversed(topo_order):
        if phase not in enabled_phases:
            continue
        idx = enabled_phases[phase]
        duration = int(df.at[idx, "DurationDays"])

        # Tighten LateFinish based on each successor's already-computed Late dates
        for rel in succ_rels.get(phase, []):
            succ = rel["succ"]
            if succ not in enabled_phases:
                continue
            s_idx = enabled_phases[succ]
            lag = int(rel.get("lag", 0))
            rt = rel["type"].upper()

            if rt == "FS":
                # pred_LF ≤ succ_LS − lag
                succ_ls = df.at[s_idx, "LateStart"]
                if pd.notna(succ_ls):
                    constrained = succ_ls - pd.Timedelta(days=lag)
                    if pd.isna(df.at[idx, "LateFinish"]) or constrained < df.at[idx, "LateFinish"]:
                        df.at[idx, "LateFinish"] = constrained

            elif rt == "FF":
                # pred_LF ≤ succ_LF − lag
                succ_lf = df.at[s_idx, "LateFinish"]
                if pd.notna(succ_lf):
                    constrained = succ_lf - pd.Timedelta(days=lag)
                    if pd.isna(df.at[idx, "LateFinish"]) or constrained < df.at[idx, "LateFinish"]:
                        df.at[idx, "LateFinish"] = constrained

            elif rt == "SS":
                # pred_LS ≤ succ_LS − lag  →  pred_LF ≤ (succ_LS − lag) + pred_duration
                succ_ls = df.at[s_idx, "LateStart"]
                if pd.notna(succ_ls):
                    constrained_ls = succ_ls - pd.Timedelta(days=lag)
                    constrained_lf = constrained_ls + pd.Timedelta(days=duration)
                    if pd.isna(df.at[idx, "LateFinish"]) or constrained_lf < df.at[idx, "LateFinish"]:
                        df.at[idx, "LateFinish"] = constrained_lf

        # Derive LateStart and TotalFloat
        lf = df.at[idx, "LateFinish"]
        if pd.notna(lf):
            ls = lf - pd.Timedelta(days=duration)
            df.at[idx, "LateStart"] = ls
            es = df.at[idx, "Start"]
            if pd.notna(es):
                df.at[idx, "TotalFloat"] = int((ls - es).days)

    return df


# ---------------------------------------------------------------------------
# Main recalculation entry point
# ---------------------------------------------------------------------------

def recalc_schedule(
    phases: pd.DataFrame,
    relationships: list = None,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Forward pass (CPM Early dates) + backward pass (Late dates, Total Float).

    Forward pass uses a single topologically-ordered scan when the network is
    acyclic, which guarantees correct MAX-over-predecessors semantics without
    oscillation. Falls back to a multi-pass Bellman-Ford loop (also fixed) if a
    cycle is detected, with a conflict reported.

    Parameters
    ----------
    phases : DataFrame with columns Phase, Start, Finish, DurationDays, Enabled
    relationships : list of dicts {pred, succ, type, lag}; defaults to DEFAULT_RELATIONSHIPS

    Returns
    -------
    (updated DataFrame, list of conflict strings)
    """
    if relationships is None:
        relationships = DEFAULT_RELATIONSHIPS

    df = normalize_dates(phases)
    # Align Finish to Start + Duration as the baseline before propagation
    df["Finish"] = df["Start"] + pd.to_timedelta(df["DurationDays"], unit="D")

    phase_to_idx = {row["Phase"]: idx for idx, row in df.iterrows()}
    enabled_phases = {
        p: i for p, i in phase_to_idx.items() if bool(df.at[i, "Enabled"])
    }

    # Build index of relationships keyed by successor for the forward pass
    rels_by_succ: dict[str, list] = defaultdict(list)
    for rel in relationships:
        rels_by_succ[rel["succ"]].append(rel)

    # Attempt topological sort (detects cycles)
    cycle_conflict: str | None = None
    try:
        topo_order = _topological_sort(set(enabled_phases.keys()), relationships)
        # Single forward pass in topological order — O(E) and always correct
        for phase in topo_order:
            if phase not in phase_to_idx:
                continue
            s_idx = phase_to_idx[phase]
            if not bool(df.at[s_idx, "Enabled"]):
                continue
            for rel in rels_by_succ.get(phase, []):
                pred = rel["pred"]
                if pred not in phase_to_idx:
                    continue
                p_idx = phase_to_idx[pred]
                if not bool(df.at[p_idx, "Enabled"]):
                    continue
                _push_forward(df, s_idx, p_idx, rel["type"], int(rel.get("lag", 0)))

    except ValueError as exc:
        cycle_conflict = str(exc)
        # Fallback multi-pass with correct push-forward semantics
        for _ in range(len(df) + 5):
            changed = False
            for rel in relationships:
                pred, succ = rel["pred"], rel["succ"]
                if pred not in phase_to_idx or succ not in phase_to_idx:
                    continue
                p_idx = phase_to_idx[pred]
                s_idx = phase_to_idx[succ]
                if not bool(df.at[p_idx, "Enabled"]) or not bool(df.at[s_idx, "Enabled"]):
                    continue
                if _push_forward(df, s_idx, p_idx, rel["type"], int(rel.get("lag", 0))):
                    changed = True
            if not changed:
                break
        # topo_order not available; build a simple list for backward pass
        topo_order = list(enabled_phases.keys())

    # Backward pass (Late Start / Late Finish / Total Float)
    df = _backward_pass(df, enabled_phases, relationships, topo_order)

    # Collect constraint violations for display
    conflicts: list[str] = []
    if cycle_conflict:
        conflicts.append(cycle_conflict)

    for rel in relationships:
        pred, succ = rel["pred"], rel["succ"]
        rt = rel["type"].upper()
        lag = int(rel.get("lag", 0))
        if pred not in phase_to_idx or succ not in phase_to_idx:
            continue
        p_idx = phase_to_idx[pred]
        s_idx = phase_to_idx[succ]
        if not bool(df.at[p_idx, "Enabled"]) or not bool(df.at[s_idx, "Enabled"]):
            continue
        pred_finish = df.at[p_idx, "Finish"]
        pred_start = df.at[p_idx, "Start"]
        succ_start = df.at[s_idx, "Start"]
        succ_finish = df.at[s_idx, "Finish"]

        if rt == "FS" and pd.notna(pred_finish) and pd.notna(succ_start):
            required = pred_finish + pd.Timedelta(days=lag)
            if succ_start < required:
                conflicts.append(
                    f"{succ} violates FS from {pred}: "
                    f"{succ_start.date()} < {required.date()}"
                )
        elif rt == "FF" and pd.notna(pred_finish) and pd.notna(succ_finish):
            required = pred_finish + pd.Timedelta(days=lag)
            if succ_finish < required:
                conflicts.append(
                    f"{succ} violates FF from {pred}: "
                    f"{succ_finish.date()} < {required.date()}"
                )
        elif rt == "SS" and pd.notna(pred_start) and pd.notna(succ_start):
            required = pred_start + pd.Timedelta(days=lag)
            if succ_start < required:
                conflicts.append(
                    f"{succ} violates SS from {pred}: "
                    f"{succ_start.date()} < {required.date()}"
                )

    return df, conflicts


# ---------------------------------------------------------------------------
# Milestone derivation
# ---------------------------------------------------------------------------

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

    tfo_enabled = (
        "Tenant Fitout" in lookup.index
        and bool(lookup.at["Tenant Fitout", "Enabled"])
    )
    rfs_date = phase_finish("Tenant Fitout") if tfo_enabled else phase_finish("Commissioning")

    milestones = [
        {"Milestone": "NTP",                    "Date": pd.to_datetime(ntp_date)       if ntp_date       else pd.NaT},
        {"Milestone": "Design Finish",           "Date": phase_finish("Design")},
        {"Milestone": "IFP",                     "Date": phase_start("Design")},
        {"Milestone": "IFC",                     "Date": phase_finish("Design")},
        {"Milestone": "Land Disturbance Permit", "Date": phase_start("Civil")},
        {"Milestone": "Building Permit",         "Date": phase_finish("Permitting")},
        {"Milestone": "Shell Start",             "Date": phase_start("Shell")},
        {"Milestone": "Building Civil Start",    "Date": phase_start("Civil")},
        {"Milestone": "Power On",                "Date": pd.to_datetime(power_on_date) if power_on_date else pd.NaT},
        {"Milestone": "Civil Complete",          "Date": phase_finish("Civil")},
        {"Milestone": "Commissioning Start",     "Date": phase_start("Commissioning")},
        {"Milestone": "Shell Complete",          "Date": phase_finish("Shell")},
        {"Milestone": "Yard Complete",           "Date": phase_finish("Equipment Yard")},
        {"Milestone": "OFCI Procurement Complete","Date": phase_finish("OFCI Procurement")},
        {"Milestone": "Fitup Complete",          "Date": phase_finish("MEP Fitup")},
        {"Milestone": "ESA",                     "Date": pd.to_datetime(esa_date)      if esa_date       else pd.NaT},
        {"Milestone": "RFS",                     "Date": rfs_date},
        {"Milestone": "Project Complete",        "Date": rfs_date},
    ]
    return pd.DataFrame(milestones)


# ---------------------------------------------------------------------------
# Data Hall RFS calculation
# ---------------------------------------------------------------------------

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
        cx_start = commissioning_start if pd.isna(previous_finish) else previous_finish + pd.Timedelta(days=lag)
        cx_finish = cx_start + pd.Timedelta(days=duration)
        out.at[i, "CxStart"] = cx_start
        out.at[i, "CxFinish"] = cx_finish
        out.at[i, "RFSDate"] = cx_finish
        previous_finish = cx_finish

    return out


def apply_first_final_rfs(milestones: pd.DataFrame, dh_results: pd.DataFrame) -> pd.DataFrame:
    milestones = milestones.copy()
    if dh_results.empty or dh_results["RFSDate"].isna().all():
        first_rfs = final_rfs = pd.NaT
    else:
        first_rfs = dh_results["RFSDate"].min()
        final_rfs = dh_results["RFSDate"].max()

    extra = pd.DataFrame([
        {"Milestone": "First RFS", "Date": first_rfs},
        {"Milestone": "Final RFS", "Date": final_rfs},
    ])
    milestones = pd.concat([milestones, extra], ignore_index=True)
    milestones.loc[milestones["Milestone"] == "Project Complete", "Date"] = final_rfs
    return milestones


# ---------------------------------------------------------------------------
# Shared time-display utilities (used by both engine logic and app.py) 
# ---------------------------------------------------------------------------

def months_after_ntp_text(ntp_date, target_date) -> str:
    """Express target_date as months/days after ntp_date."""
    return months_from_reference_text(ntp_date, target_date, "NTP")


def months_from_reference_text(reference_date, target_date, suffix_label: str) -> str:
    """Express target_date relative to reference_date with a readable suffix."""
    if pd.isna(reference_date) or pd.isna(target_date):
        return ""
    total_days = (target_date - reference_date).days
    months_decimal = total_days / 30.4375
    rounded = round(months_decimal * 2) / 2
    if rounded < 1:
        base = f"{total_days} days"
    elif float(rounded).is_integer():
        base = f"{int(rounded)} months"
    else:
        base = f"{rounded:.1f} months"
    return f"{base} from {suffix_label}"
