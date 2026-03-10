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


def build_gantt(current_df: pd.DataFrame) -> go.Figure:
    df = current_df.copy()
    df = df[df["Enabled"]].copy()
    df["Phase"] = pd.Categorical(df["Phase"], categories=PHASE_ORDER, ordered=True)
    df = df.sort_values("Phase", ascending=False)

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=(df["Finish"] - df["Start"]).dt.days,
            y=df["Phase"],
            base=df["Start"],
            orientation="h",
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Start: %{base|%Y-%m-%d}<br>"
                "Duration: %{x} days"
                "<extra></extra>"
            ),
            name="Schedule",
        )
    )

    today = pd.Timestamp.today().normalize()
    fig.add_vline(
        x=today,
        line_dash="dash",
        annotation_text="Today",
        annotation_position="top"
    )

    fig.update_layout(
        barmode="overlay",
        height=max(450, len(df) * 36),
        margin=dict(l=10, r=10, t=40, b=10),
        xaxis_title="Date",
        yaxis_title="",
        legend_title="",
    )
    fig.update_xaxes(type="date")
    return fig


# session state
if "phases" not in st.session_state:
    st.session_state.phases = get_default_phases()

if "dh_table" not in st.session_state:
    st.session_state.dh_table = build_default_datahall_table()

st.title("CADC Schedule Simulator")

uploaded = st.file_uploader("Upload OPC Excel Export", type=["xlsx"])

if uploaded is not None:
    try:
        parsed = parse_opc(uploaded)
        st.session_state.phases = parsed
        st.success("OPC file loaded.")
    except Exception as e:
        st.error(str(e))

# sidebar manual milestone inputs
with st.sidebar:
    st.header("Standalone Milestones")
    ntp_date = st.date_input("NTP", value=pd.Timestamp("2026-01-01"))
    power_on_date = st.date_input("Power On", value=pd.Timestamp("2027-01-15"))
    esa_use = st.checkbox("Use ESA")
    esa_date = st.date_input("ESA", value=pd.Timestamp("2027-03-01")) if esa_use else None

st.subheader("Editable Schedule Grid")
edited_phases = st.data_editor(
    st.session_state.phases,
    use_container_width=True,
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
edited_phases["DurationDays"] = pd.to_numeric(edited_phases["DurationDays"], errors="coerce").fillna(0).astype(int)
edited_phases["Enabled"] = edited_phases["Enabled"].fillna(True).astype(bool)

st.session_state.phases = edited_phases.copy()

# Data Hall editor
st.subheader("Data Hall Commissioning Inputs")
edited_dh = st.data_editor(
    st.session_state.dh_table,
    use_container_width=True,
    hide_index=True,
    column_config={
        "DataHall": st.column_config.TextColumn("Data Hall"),
        "MW": st.column_config.NumberColumn("MW", step=0.1),
        "CxDurationDays": st.column_config.NumberColumn("Cx Duration", step=1),
        "LagFromPriorDH": st.column_config.NumberColumn("Lag From Prior DH", step=1),
    },
)
edited_dh["MW"] = pd.to_numeric(edited_dh["MW"], errors="coerce")
edited_dh["CxDurationDays"] = pd.to_numeric(edited_dh["CxDurationDays"], errors="coerce").fillna(0).astype(int)
edited_dh["LagFromPriorDH"] = pd.to_numeric(edited_dh["LagFromPriorDH"], errors="coerce").fillna(0).astype(int)
st.session_state.dh_table = edited_dh.copy()

# calculate everything
phases_calc, conflicts = recalc_schedule(st.session_state.phases)

milestones = derive_milestones(
    phases_calc,
    ntp_date=ntp_date,
    power_on_date=power_on_date,
    esa_date=esa_date,
)

dh_results = calculate_datahall_rfs(phases_calc, st.session_state.dh_table)
milestones = apply_first_final_rfs(milestones, dh_results)

# enrich milestone display
ntp_ts = pd.to_datetime(ntp_date)
milestones["Months After NTP"] = milestones["Date"].apply(lambda d: months_after_ntp_text(ntp_ts, d))
milestones["Date Text"] = milestones["Date"].dt.strftime("%Y-%m-%d")

# layout
left, center, right = st.columns([1.1, 2.2, 1.1])

with left:
    st.subheader("Milestones")
    st.dataframe(
        milestones[["Milestone", "Date Text", "Months After NTP"]],
        use_container_width=True,
        hide_index=True,
    )

    if conflicts:
        st.subheader("Logic Flags")
        for c in conflicts:
            st.warning(c)

with center:
    st.subheader("Gantt")
    fig = build_gantt(phases_calc)
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Data Hall RFS")
    dh_display = dh_results.copy()
    dh_display["MW Label"] = dh_display["MW"].apply(lambda x: "" if pd.isna(x) else f"{x:g} MW")
    dh_display["RFS"] = pd.to_datetime(dh_display["RFSDate"]).dt.strftime("%Y-%m-%d")
    st.dataframe(
        dh_display[["DataHall", "MW Label", "RFS"]],
        use_container_width=True,
        hide_index=True,
    )

st.subheader("KPI Cards")
kpi_df = milestones[milestones["Milestone"].isin(KPI_MILESTONES)].copy()
kpi_cols = st.columns(max(1, len(kpi_df)))

for col, (_, row) in zip(kpi_cols, kpi_df.iterrows()):
    with col:
        st.metric(
            row["Milestone"],
            row["Date Text"] if pd.notna(row["Date"]) else "",
            row["Months After NTP"] if row["Months After NTP"] else None,
        )

st.subheader("Calculated Schedule")
st.dataframe(phases_calc, use_container_width=True, hide_index=True)
