import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from schedule_engine import recalc_schedule
from opc_parser import parse_opc

st.title("CADC Schedule Simulator")

uploaded = st.file_uploader("Upload OPC Excel Export")

if uploaded:
    phases = parse_opc(uploaded)
else:

    phases = pd.DataFrame([
        {"Phase":"Design","Start":"2026-01-01","Finish":"2026-02-15","DurationDays":45,"Enabled":True},
        {"Phase":"Permitting","Start":"2026-02-16","Finish":"2026-05-01","DurationDays":75,"Enabled":True},
        {"Phase":"Civil","Start":"2026-05-02","Finish":"2026-08-01","DurationDays":90,"Enabled":True},
        {"Phase":"Shell","Start":"2026-08-02","Finish":"2026-11-15","DurationDays":105,"Enabled":True},
        {"Phase":"MEP Fitup","Start":"2026-11-16","Finish":"2027-02-01","DurationDays":75,"Enabled":True},
        {"Phase":"Commissioning","Start":"2027-02-02","Finish":"2027-04-01","DurationDays":60,"Enabled":True},
        {"Phase":"Tenant Fitout","Start":"2027-04-02","Finish":"2027-05-15","DurationDays":45,"Enabled":False},
    ])

phases["Start"] = pd.to_datetime(phases["Start"])
phases["Finish"] = pd.to_datetime(phases["Finish"])

edited = st.data_editor(phases)

phases_calc = recalc_schedule(edited)

fig = go.Figure()

fig.add_trace(go.Bar(
    x=(phases_calc["Finish"] - phases_calc["Start"]).dt.days,
    y=phases_calc["Phase"],
    base=phases_calc["Start"],
    orientation="h"
))

fig.update_layout(barmode="overlay")

st.plotly_chart(fig, use_container_width=True)

st.dataframe(phases_calc)