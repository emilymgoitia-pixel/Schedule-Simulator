import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from schedule_engine import (
    recalc_schedule,
    derive_milestones,
    calculate_datahall_rfs,
    apply_first_final_rfs,
    months_after_ntp_text,
)
from opc_parser import parse_opc, build_default_datahall_table

st.set_page_config(page_title="CADC Schedule Simulator", layout="wide")

st.markdown(
    """
    <style>
        :root {
            --primary-text: #1D264E;
            --primary-accent: #6B7AF5;
            --secondary-accent: #84A8A4;
            --highlight-accent: #D9DE6A;
            --risk-accent: #F24E5A;
            --bg: #f7f8fd;
            --panel: #FAFBFF;
            --border: #E5E8F3;
        }

        .stApp {
            background: linear-gradient(180deg, #fbfcff 0%, var(--bg) 65%);
            color: var(--primary-text);
        }
        .block-container {
            max-width: 1680px;
            padding-top: 0.75rem;
            padding-bottom: 0.55rem;
        }

        * { color: var(--primary-text); }

        h1, h2, h3, h4, h5, h6 { margin-top: 0; margin-bottom: 0.25rem; }
        div[data-testid="stHorizontalBlock"] > div { align-self: stretch; }

        section[data-testid="stSidebar"] {
            width: 252px !important;
            min-width: 252px !important;
            background: #FAFBFF;
            border-right: 1px solid #e5e9f8;
        }
        section[data-testid="stSidebar"] .block-container {
            padding-top: 1rem;
            padding-left: 0.9rem;
            padding-right: 0.9rem;
        }

        .dashboard-card {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 0.5rem 0.62rem 0.46rem;
            box-shadow: 0 2px 8px rgba(29, 38, 78, 0.05);
            margin-bottom: 0.38rem;
        }

        .section-title {
            font-size: 0.72rem;
            letter-spacing: 0.08em;
            color: var(--primary-text);
            font-weight: 700;
            margin-bottom: 0.18rem;
            text-transform: uppercase;
            opacity: 0.88;
        }

        .kpi-card {
            background: #FAFBFF;
            border: 1px solid #E5E8F3;
            border-left: 4px solid var(--primary-accent);
            border-radius: 12px;
            padding: 0.28rem 0.42rem;
            min-height: 64px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            gap: 0.3rem;
        }
        .kpi-title {
            font-size: 0.58rem;
            line-height: 1.1;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            font-weight: 700;
            color: var(--primary-text);
            opacity: 0.85;
        }
        .kpi-date {
            font-size: 0.84rem;
            line-height: 1.2;
            font-weight: 700;
            color: var(--primary-text);
            white-space: normal;
            word-break: normal;
        }
        .kpi-chip {
            display: inline-block;
            font-size: 0.56rem;
            font-weight: 700;
            color: var(--primary-text);
            background: color-mix(in srgb, var(--secondary-accent) 30%, white);
            border: 1px solid color-mix(in srgb, var(--secondary-accent) 70%, white);
            border-radius: 999px;
            padding: 0.1rem 0.45rem;
            width: fit-content;
        }
        .kpi-chip.highlight {
            background: color-mix(in srgb, var(--highlight-accent) 35%, white);
            border-color: color-mix(in srgb, var(--highlight-accent) 80%, white);
        }

        [data-testid="stDataFrame"] {
            border: 1px solid #E5E8F3;
            border-radius: 10px;
            overflow: hidden;
        }
        [data-testid="stDataFrame"] [role="columnheader"] {
            background: #f5f7ff;
            color: var(--primary-text);
            font-size: 0.75rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }
        [data-testid="stDataFrame"] [role="gridcell"] {
            color: var(--primary-text);
            font-size: 13px;
            line-height: 1.15;
            white-space: normal !important;
            overflow-wrap: anywhere;
            word-break: break-word;
        }

        .logic-flag {
            border: 1px solid color-mix(in srgb, var(--risk-accent) 45%, white);
            background: color-mix(in srgb, var(--risk-accent) 10%, white);
            color: var(--risk-accent);
            border-radius: 8px;
            font-size: 13px;
            font-weight: 600;
            padding: 0.42rem 0.5rem;
            margin-top: 0.35rem;
        }

        .stExpander {
            border: 1px solid #E5E8F3;
            border-radius: 12px;
            background: #fcfdff;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

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

CADC_ROLLUP_MAP = {
    "Design": "Phase 3A",
    "Permitting": "Phase 3A",
    "Site Power": "Power",
    "OFCI Procurement": "OFCI Production",
    "Civil": "Phase 3B",
    "Shell": "Phase 3B",
    "Equipment Yard": "Phase 3B",
    "MEP Fitup": "Phase 3B",
    "Commissioning": "Phase 3B",
    "Tenant Fitout": "Phase 4",
}

KPI_MILESTONES = [
    "Civil Complete",
    "Shell Complete",
    "OFCI Procurement Complete",
    "Fitup Complete",
    "First RFS",
    "Final RFS",
    "Project Complete",
]


def get_default_phases():
    df = pd.DataFrame([
        {"Phase": "Design", "Start": "2026-01-01", "Finish": "2026-02-15", "DurationDays": 45, "Enabled": True},
        {"Phase": "Permitting", "Start": "2026-02-16", "Finish": "2026-05-01", "DurationDays": 75, "Enabled": True},
        {"Phase": "Site Power", "Start": "2026-03-01", "Finish": "2026-11-01", "DurationDays": 245, "Enabled": True},
        {"Phase": "OFCI Procurement", "Start": "2026-02-01", "Finish": "2026-10-15", "DurationDays": 256, "Enabled": True},
        {"Phase": "Civil", "Start": "2026-05-02", "Finish": "2026-08-01", "DurationDays": 91, "Enabled": True},
        {"Phase": "Shell", "Start": "2026-08-02", "Finish": "2026-11-15", "DurationDays": 105, "Enabled": True},
        {"Phase": "Equipment Yard", "Start": "2026-08-10", "Finish": "2026-12-15", "DurationDays": 127, "Enabled": True},
        {"Phase": "MEP Fitup", "Start": "2026-11-16", "Finish": "2027-02-01", "DurationDays": 77, "Enabled": True},
        {"Phase": "Commissioning", "Start": "2027-02-02", "Finish": "2027-04-01", "DurationDays": 58, "Enabled": True},
        {"Phase": "Tenant Fitout", "Start": "2027-04-02", "Finish": "2027-05-15", "DurationDays": 43, "Enabled": False},
    ])
    df["Start"] = pd.to_datetime(df["Start"])
    df["Finish"] = pd.to_datetime(df["Finish"])
    return df


def build_gantt(current_df: pd.DataFrame, datahall_rfs: pd.DataFrame | None = None) -> go.Figure:
    df = current_df.copy()
    df = df[df["Enabled"]].copy()

    gantt_phase_order = [
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
    df["Phase"] = pd.Categorical(df["Phase"], categories=gantt_phase_order, ordered=True)
    df = df.sort_values("Phase", ascending=False)

    df["DurationDays"] = (df["Finish"] - df["Start"]).dt.days.clip(lower=0)
    df["DurationMs"] = (df["Finish"] - df["Start"]).dt.total_seconds() * 1000

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df["DurationMs"],
            y=df["Phase"],
            base=df["Start"],
            customdata=df[["Finish", "DurationDays"]],
            marker=dict(color="#6B7AF5", opacity=0.9, line=dict(color="#5f6ee3", width=1.2)),
            width=0.32,
            orientation="h",
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Start: %{base|%d %b %Y}<br>"
                "Finish: %{customdata[0]|%d %b %Y}<br>"
                "Duration: %{customdata[1]} days"
                "<extra></extra>"
            ),
            name="Schedule",
        )
    )

    today = pd.Timestamp.today().normalize()
    fig.add_shape(
        type="line",
        x0=today,
        x1=today,
        y0=0,
        y1=1,
        xref="x",
        yref="paper",
        line=dict(color="#1D264E", dash="dot", width=1),
        opacity=0.25,
    )
    fig.add_annotation(
        x=today,
        y=1.02,
        xref="x",
        yref="paper",
        text="Today",
        showarrow=False,
        font=dict(color="#1D264E", size=11),
        opacity=0.75,
    )

    if datahall_rfs is not None and not datahall_rfs.empty:
        rfs_points = datahall_rfs.copy()
        rfs_points["RFSDate"] = pd.to_datetime(rfs_points["RFSDate"], errors="coerce")
        rfs_points = rfs_points[rfs_points["RFSDate"].notna()].copy()
        if not rfs_points.empty and "Commissioning" in df["Phase"].astype(str).tolist():
            rfs_points["RFSLabel"] = rfs_points["RFSDate"].dt.strftime("%m %d RFS")
            fig.add_trace(
                go.Scatter(
                    x=rfs_points["RFSDate"],
                    y=["Commissioning"] * len(rfs_points),
                    mode="markers+text",
                    text=rfs_points["RFSLabel"],
                    textposition="top center",
                    textfont=dict(color="#1D264E", size=10),
                    marker=dict(size=8, color="#1D264E", line=dict(color="#ffffff", width=1)),
                    customdata=rfs_points[["DataHall"]],
                    hovertemplate="<b>Data Hall RFS</b><br>Hall: %{customdata[0]}<br>Date: %{x|%d %b %Y}<extra></extra>",
                    name="Data Hall RFS",
                )
            )

    fig.update_layout(
        barmode="overlay",
        height=max(600, len(df) * 42),
        margin=dict(l=8, r=8, t=22, b=10),
        xaxis_title="Timeline",
        yaxis_title="",
        plot_bgcolor="#fdfdff",
        paper_bgcolor="#ffffff",
        font=dict(size=12, color="#1D264E"),
        showlegend=False,
    )
    fig.update_xaxes(
        type="date",
        showline=True,
        linewidth=1,
        linecolor="#d5dcf5",
        gridcolor="#E7EBF4",
        tickfont=dict(color="#1D264E", size=11),
        title_font=dict(color="#1D264E", size=12),
    )
    fig.update_yaxes(
        showline=False,
        gridcolor="#E7EBF4",
        tickfont=dict(color="#1D264E", size=12),
        automargin=True,
    )
    return fig


def add_cadc_rollups(phases_calc: pd.DataFrame) -> pd.DataFrame:
    out = phases_calc.copy()
    out["CADC Rollup"] = out["Phase"].map(CADC_ROLLUP_MAP)
    return out


def months_from_reference_text(reference_date, target_date, suffix_label: str) -> str:
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


def render_kpi_cards(kpi_rows: pd.DataFrame) -> None:
    first_row_order = [
        "Civil Complete",
        "Shell Complete",
        "OFCI Procurement Complete",
        "Fitup Complete",
        "Project Complete",
    ]
    row1 = kpi_rows.set_index("Milestone").reindex(first_row_order).dropna(subset=["Date Text"], how="all").reset_index()
    remaining = kpi_rows[~kpi_rows["Milestone"].isin(first_row_order)].copy()

    rows = []
    if not row1.empty:
        rows.append(row1)
    if not remaining.empty:
        rows.append(remaining)

    for row_idx, kpi_chunk in enumerate(rows):
        cols = st.columns(max(1, len(kpi_chunk)), gap="small")
        for col, (_, row) in zip(cols, kpi_chunk.iterrows()):
            with col:
                chip_class = "kpi-chip highlight" if row["Milestone"] in {"First RFS", "Final RFS"} else "kpi-chip"
                st.markdown(
                    f"""
                    <div class="kpi-card">
                        <div class="kpi-title">{row['Milestone']}</div>
                        <div class="kpi-date">{row['Date Text'] if pd.notna(row['Date']) else '-'}</div>
                        <div class="{chip_class}">{row.get('Months Label', row['Months After NTP']) or '—'}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
        if row_idx < len(rows) - 1:
            st.markdown("<div style='height:0.15rem;'></div>", unsafe_allow_html=True)


if "phases" not in st.session_state:
    st.session_state.phases = get_default_phases()
if "dh_table" not in st.session_state:
    st.session_state.dh_table = build_default_datahall_table()

if "ntp_date_value" not in st.session_state:
    st.session_state.ntp_date_value = pd.Timestamp("2026-01-01").date()
if "power_on_date_value" not in st.session_state:
    st.session_state.power_on_date_value = pd.Timestamp("2027-01-15").date()
if "prev_ntp_date_value" not in st.session_state:
    st.session_state.prev_ntp_date_value = st.session_state.ntp_date_value
if "prev_power_on_date_value" not in st.session_state:
    st.session_state.prev_power_on_date_value = st.session_state.power_on_date_value

st.title("CADC Schedule Simulator")
st.caption("Executive-style schedule dashboard with milestone tracking and Data Hall RFS rollups.")

uploaded = st.file_uploader("Upload OPC Excel Export", type=["xlsx"])
if uploaded is not None:
    try:
        parsed_phases, parsed_dh = parse_opc(uploaded)
        st.session_state.phases = parsed_phases
        if not parsed_dh.empty:
            st.session_state.dh_table = parsed_dh
        st.success("OPC file loaded.")
    except Exception as e:
        st.error(str(e))

with st.sidebar:
    st.subheader("Planning Inputs")
    ntp_date = st.date_input("NTP", value=st.session_state.ntp_date_value)
    power_on_date = st.date_input("Power On", value=st.session_state.power_on_date_value)

    current_hall_count = int(len(st.session_state.dh_table.index)) if not st.session_state.dh_table.empty else 0
    hall_count = st.number_input(
        "Number of Data Halls",
        min_value=1,
        max_value=20,
        value=max(1, current_hall_count),
        step=1,
        help="Adds/removes Data Halls and duplicates Cx/RFS assumptions using DH2/DH3 pattern.",
    )

    target_hall_count = int(hall_count)
    if target_hall_count != current_hall_count:
        existing_dh = st.session_state.dh_table.copy().reset_index(drop=True)
        template_rows = existing_dh if not existing_dh.empty else build_default_datahall_table()
        resized_rows = []
        for i in range(target_hall_count):
            if i < len(existing_dh):
                row = existing_dh.iloc[i].copy()
            else:
                if len(template_rows) >= 3:
                    row = template_rows.iloc[2].copy()
                elif len(template_rows) >= 2:
                    row = template_rows.iloc[1].copy()
                else:
                    row = template_rows.iloc[0].copy()
            row["DataHall"] = f"DH{i + 1}"
            row["LagFromPriorDH"] = 0 if i == 0 else int(pd.to_numeric(row.get("LagFromPriorDH", 30), errors="coerce") or 30)
            row["CxDurationDays"] = int(pd.to_numeric(row.get("CxDurationDays", 60), errors="coerce") or 60)
            row["MW"] = float(pd.to_numeric(row.get("MW", 16.8), errors="coerce") or 16.8)
            resized_rows.append(row)

        st.session_state.dh_table = pd.DataFrame(resized_rows)[["DataHall", "MW", "CxDurationDays", "LagFromPriorDH"]]

    prev_ntp = pd.to_datetime(st.session_state.prev_ntp_date_value)
    prev_power = pd.to_datetime(st.session_state.prev_power_on_date_value)
    new_ntp = pd.to_datetime(ntp_date)
    new_power = pd.to_datetime(power_on_date)

    ntp_changed = new_ntp != prev_ntp
    power_changed = new_power != prev_power

    if ntp_changed and not power_changed:
        delta = new_ntp - prev_ntp
        adjusted_power = (prev_power + delta).date()
        power_on_date = adjusted_power
        new_power = pd.to_datetime(power_on_date)

    st.session_state.ntp_date_value = new_ntp.date()
    st.session_state.power_on_date_value = new_power.date()
    st.session_state.prev_ntp_date_value = new_ntp.date()
    st.session_state.prev_power_on_date_value = new_power.date()

    esa_use = st.checkbox("Use ESA")
    esa_date = st.date_input("ESA", value=pd.Timestamp("2027-03-01")) if esa_use else None
    st.divider()
    st.caption("Adjust high-level assumptions here. Keep detailed edits in collapsed controls.")

with st.expander("Editing Controls", expanded=False):
    st.markdown('<div class="section-title">Phase Inputs</div>', unsafe_allow_html=True)
    prior_phases = st.session_state.phases.copy()
    edited_phases = st.data_editor(
        prior_phases,
        width="stretch",
        hide_index=True,
        column_config={
            "Phase": st.column_config.TextColumn("Phase", disabled=True),
            "Start": st.column_config.DateColumn("Start"),
            "Finish": st.column_config.DateColumn("Finish"),
            "DurationDays": st.column_config.NumberColumn("Duration (days)", step=1),
            "Enabled": st.column_config.CheckboxColumn("Enabled"),
        },
    )

    edited_phases["Start"] = pd.to_datetime(edited_phases["Start"], errors="coerce")
    edited_phases["Finish"] = pd.to_datetime(edited_phases["Finish"], errors="coerce")
    edited_phases["DurationDays"] = pd.to_numeric(edited_phases["DurationDays"], errors="coerce")

    prior_lookup = prior_phases.set_index("Phase")
    for idx, row in edited_phases.iterrows():
        phase = row["Phase"]
        if phase not in prior_lookup.index:
            continue

        prev_start = pd.to_datetime(prior_lookup.at[phase, "Start"], errors="coerce")
        prev_finish = pd.to_datetime(prior_lookup.at[phase, "Finish"], errors="coerce")
        prev_duration = int(pd.to_numeric(prior_lookup.at[phase, "DurationDays"], errors="coerce") or 0)

        new_start = row["Start"]
        new_finish = row["Finish"]
        new_duration = row["DurationDays"]

        start_changed = pd.notna(new_start) and pd.notna(prev_start) and new_start != prev_start
        finish_changed = pd.notna(new_finish) and pd.notna(prev_finish) and new_finish != prev_finish
        duration_changed = pd.notna(new_duration) and int(new_duration) != prev_duration

        if start_changed and not finish_changed:
            edited_phases.at[idx, "Finish"] = new_start + pd.Timedelta(days=prev_duration)
            edited_phases.at[idx, "DurationDays"] = prev_duration
        elif finish_changed and not start_changed and pd.notna(new_start):
            edited_phases.at[idx, "DurationDays"] = max(0, int((new_finish - new_start).days))
        elif start_changed and finish_changed and pd.notna(new_start) and pd.notna(new_finish):
            edited_phases.at[idx, "DurationDays"] = max(0, int((new_finish - new_start).days))
        elif duration_changed and pd.notna(new_start):
            edited_phases.at[idx, "DurationDays"] = max(0, int(new_duration))
            edited_phases.at[idx, "Finish"] = new_start + pd.Timedelta(days=int(edited_phases.at[idx, "DurationDays"]))
        elif pd.notna(new_start) and pd.notna(new_finish):
            edited_phases.at[idx, "DurationDays"] = max(0, int((new_finish - new_start).days))

    edited_phases["DurationDays"] = pd.to_numeric(edited_phases["DurationDays"], errors="coerce").fillna(0).clip(lower=0).astype(int)
    edited_phases["Enabled"] = edited_phases["Enabled"].fillna(True).astype(bool)
    st.session_state.phases = edited_phases.copy()

    st.markdown('<div class="section-title">Data Hall Inputs</div>', unsafe_allow_html=True)
    edited_dh = st.data_editor(
        st.session_state.dh_table,
        width="stretch",
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "DataHall": st.column_config.TextColumn("Data Hall"),
            "MW": st.column_config.NumberColumn("MW", step=0.1),
            "CxDurationDays": st.column_config.NumberColumn("Cx Duration", step=1),
            "LagFromPriorDH": st.column_config.NumberColumn("Lag From Prior DH", step=1),
        },
    )
    edited_dh["MW"] = pd.to_numeric(edited_dh["MW"], errors="coerce")
    edited_dh["CxDurationDays"] = pd.to_numeric(edited_dh["CxDurationDays"], errors="coerce").fillna(0).clip(lower=0).astype(int)
    edited_dh["LagFromPriorDH"] = pd.to_numeric(edited_dh["LagFromPriorDH"], errors="coerce").fillna(0).astype(int)
    st.session_state.dh_table = edited_dh.copy()

phases_for_calc = st.session_state.phases.copy()
power_on_ts = pd.to_datetime(power_on_date, errors="coerce")

site_power_mask_input = phases_for_calc["Phase"] == "Site Power"
if site_power_mask_input.any() and pd.notna(power_on_ts):
    sp_idx = phases_for_calc.index[site_power_mask_input][0]
    sp_enabled = bool(phases_for_calc.at[sp_idx, "Enabled"])
    sp_start = pd.to_datetime(phases_for_calc.at[sp_idx, "Start"], errors="coerce")
    if sp_enabled and pd.notna(sp_start) and power_on_ts >= sp_start:
        phases_for_calc.at[sp_idx, "Finish"] = power_on_ts
        phases_for_calc.at[sp_idx, "DurationDays"] = int((power_on_ts - sp_start).days)

commissioning_mask_input = phases_for_calc["Phase"] == "Commissioning"
if commissioning_mask_input.any() and pd.notna(power_on_ts):
    cx_idx = phases_for_calc.index[commissioning_mask_input][0]
    cx_enabled = bool(phases_for_calc.at[cx_idx, "Enabled"])
    cx_start = pd.to_datetime(phases_for_calc.at[cx_idx, "Start"], errors="coerce")
    cx_duration = int(pd.to_numeric(phases_for_calc.at[cx_idx, "DurationDays"], errors="coerce") or 0)
    if cx_enabled and pd.notna(cx_start) and cx_start < power_on_ts:
        phases_for_calc.at[cx_idx, "Start"] = power_on_ts
        phases_for_calc.at[cx_idx, "Finish"] = power_on_ts + pd.Timedelta(days=cx_duration)

phases_calc, conflicts = recalc_schedule(phases_for_calc)
phases_calc = add_cadc_rollups(phases_calc)

# Data Hall RFS runs within Commissioning. If Tenant Fitout is disabled,
# keep Commissioning bar through Final RFS so Gantt and milestone logic align.
dh_results = calculate_datahall_rfs(phases_calc, st.session_state.dh_table)
if not dh_results.empty and dh_results["RFSDate"].notna().any():
    final_rfs = pd.to_datetime(dh_results["RFSDate"]).max()
    commissioning_mask = phases_calc["Phase"] == "Commissioning"
    tfo_mask = phases_calc["Phase"] == "Tenant Fitout"
    tfo_enabled = bool(phases_calc.loc[tfo_mask, "Enabled"].any()) if tfo_mask.any() else False
    if commissioning_mask.any() and not tfo_enabled:
        cx_start = pd.to_datetime(phases_calc.loc[commissioning_mask, "Start"]).iloc[0]
        if pd.notna(cx_start) and pd.notna(final_rfs) and final_rfs >= cx_start:
            phases_calc.loc[commissioning_mask, "Finish"] = final_rfs
            phases_calc.loc[commissioning_mask, "DurationDays"] = int((final_rfs - cx_start).days)

milestones = derive_milestones(phases_calc, ntp_date=ntp_date, power_on_date=power_on_date, esa_date=esa_date)
milestones = apply_first_final_rfs(milestones, dh_results)

ntp_ts = pd.to_datetime(ntp_date)
milestones["Months After NTP"] = milestones["Date"].apply(lambda d: months_after_ntp_text(ntp_ts, d))
milestones["Date Text"] = pd.to_datetime(milestones["Date"]).dt.strftime("%d %b %Y")

st.markdown('<div class="section-title">Key Milestones</div>', unsafe_allow_html=True)
kpi_df = milestones[milestones["Milestone"].isin(KPI_MILESTONES)].copy()
manual_kpis = pd.DataFrame([
    {
        "Milestone": "NTP",
        "Date": pd.to_datetime(ntp_date),
        "Date Text": pd.to_datetime(ntp_date).strftime("%d %b %Y"),
        "Months After NTP": months_after_ntp_text(ntp_ts, pd.to_datetime(ntp_date)),
    },
    {
        "Milestone": "Power On",
        "Date": pd.to_datetime(power_on_date),
        "Date Text": pd.to_datetime(power_on_date).strftime("%d %b %Y"),
        "Months After NTP": months_after_ntp_text(ntp_ts, pd.to_datetime(power_on_date)),
    },
])
kpi_df = pd.concat([kpi_df, manual_kpis], ignore_index=True)
today_ts = pd.Timestamp.today().normalize()
reference_ts = today_ts if pd.notna(ntp_ts) and today_ts >= ntp_ts else ntp_ts
reference_label = "today" if pd.notna(ntp_ts) and today_ts >= ntp_ts else "LOI/NTP"
kpi_df["Months Label"] = kpi_df["Date"].apply(lambda d: months_from_reference_text(reference_ts, d, reference_label))
render_kpi_cards(kpi_df)

left, center, right = st.columns([1.2, 3.05, 1.25], gap="small")
with left:
    st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Milestone Panel</div>', unsafe_allow_html=True)
    milestone_panel = milestones[["Milestone", "Date", "Date Text"]].copy()
    milestone_panel = milestone_panel[milestone_panel["Date"].notna()].copy()
    milestone_panel["IsPast"] = pd.to_datetime(milestone_panel["Date"], errors="coerce") < pd.Timestamp.today().normalize()
    milestone_display = milestone_panel[["Milestone", "Date Text", "IsPast"]].copy()
    milestone_styler = milestone_display[["Milestone", "Date Text"]].style.apply(
        lambda row: ["", "color: #9ca3af;" if milestone_display.loc[row.name, "IsPast"] else ""],
        axis=1,
    )
    st.dataframe(
        milestone_styler,
        width="stretch",
        hide_index=True,
        height=600,
        row_height=28,
        column_config={
            "Milestone": st.column_config.TextColumn("Milestone", width="medium"),
            "Date Text": st.column_config.TextColumn("Date", width="small"),
        },
    )
    st.caption("Months-after-NTP remains available in KPI cards above.")
    st.markdown('</div>', unsafe_allow_html=True)
    if conflicts:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Logic Flags</div>', unsafe_allow_html=True)
        for c in conflicts:
            st.markdown(f'<div class="logic-flag">{c}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

with center:
    st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Master Schedule Gantt</div>', unsafe_allow_html=True)
    st.plotly_chart(build_gantt(phases_calc, dh_results), width="stretch", config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Data Hall RFS</div>', unsafe_allow_html=True)
    dh_display = dh_results.copy()
    dh_display = dh_display[dh_display["DataHall"].notna()].copy()
    cx_start_col = "CxStart" if "CxStart" in dh_display.columns else "Cx Start" if "Cx Start" in dh_display.columns else None
    has_value_mask = dh_display["RFSDate"].notna() | dh_display["MW"].notna()
    if cx_start_col is not None:
        has_value_mask = has_value_mask | dh_display[cx_start_col].notna()
    dh_display = dh_display[(dh_display["DataHall"].astype(str).str.strip() != "") & has_value_mask].copy()
    dh_display["MW Label"] = dh_display["MW"].apply(lambda x: "" if pd.isna(x) else f"{x:g} MW")
    dh_display["RFS"] = pd.to_datetime(dh_display["RFSDate"]).dt.strftime("%d %b %Y")
    dh_table_height = 40 + (28 * max(1, len(dh_display)))
    st.dataframe(
        dh_display[["DataHall", "MW Label", "RFS"]],
        width="stretch",
        hide_index=True,
        height=dh_table_height,
        row_height=28,
        column_config={
            "DataHall": st.column_config.TextColumn("Data Hall", width="small"),
            "MW Label": st.column_config.TextColumn("MW", width="small"),
            "RFS": st.column_config.TextColumn("RFS", width="medium"),
        },
    )
    st.markdown('</div>', unsafe_allow_html=True)

with st.expander("Calculated Schedule (with CADC rollups)", expanded=False):
    st.dataframe(phases_calc, width="stretch", hide_index=True)
