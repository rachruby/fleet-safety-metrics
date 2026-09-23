"""
Fleet safety dashboard.

    streamlit run dashboard/app.py -- --data data
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

EVENT_LABELS = {
    "hard_brake": "Hard brake",
    "hard_accel": "Hard acceleration",
    "low_ttc": "Low time-to-collision",
    "brake_for_closing_lead": "Braked for closing lead",
}


def data_dir() -> Path:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data")
    args, _ = ap.parse_known_args()
    return Path(args.data)


@st.cache_data
def load(root: str) -> dict[str, pd.DataFrame]:
    r = Path(root)
    return {
        name: pd.read_parquet(r / "gold" / name)
        for name in ["fleet_metrics", "segment_metrics", "daily_metrics", "scenarios", "events", "dq_report"]
    }


@st.cache_data
def load_timeline(root: str, segment_id: str) -> pd.DataFrame:
    return pd.read_parquet(
        Path(root) / "silver" / "timeline", filters=[("segment_id", "==", segment_id)]
    ).sort_values("t")


st.set_page_config(page_title="Fleet Safety Metrics", page_icon="🚗", layout="wide")
root = str(data_dir())
if not (Path(root) / "gold").exists():
    st.error(f"No gold tables under `{root}/gold`. Run the pipeline first (see README).")
    st.stop()
d = load(root)

# ---------------- sidebar filters ----------------
st.sidebar.header("Filters")
vehicles = sorted(d["segment_metrics"].vehicle.unique())
sel_vehicles = st.sidebar.multiselect("Vehicle", vehicles, default=vehicles)
seg = d["segment_metrics"][d["segment_metrics"].vehicle.isin(sel_vehicles)]
fleet = d["fleet_metrics"][d["fleet_metrics"].vehicle.isin(sel_vehicles)]

# ---------------- header + data provenance + DQ gate ----------------
st.title("Fleet Safety Metrics")
st.caption("comma2k19 driving logs → Spark SQL → events per 1,000 miles · "
           "[source code](https://github.com/rachruby/fleet-safety-metrics)")

src_file = Path(root) / "source.json"
if src_file.exists():
    src = json.loads(src_file.read_text())
    if src.get("source") == "synthetic":
        st.warning("**Demo mode: synthetic data.** " + src.get("note", ""))
    else:
        st.info(f"Data source: {src.get('source')}. {src.get('note', '')}")

dq = d["dq_report"]
failed = dq[dq.status == "FAIL"]
if failed.empty:
    st.success(f"All {len(dq)} data-quality checks passed")
else:
    st.error("Data-quality checks failing: " + ", ".join(failed.check_name) + " — treat metrics with caution")

# ---------------- KPIs ----------------
miles = seg.miles.sum()
c = st.columns(5)
c[0].metric("Miles", f"{miles:,.1f}")
mins = seg.minutes.sum()
c[1].metric("Drive time", f"{mins / 60:,.1f} h" if mins >= 60 else f"{mins:.0f} min")
c[2].metric("Segments", f"{len(seg):,}")
for col, (field, label) in zip(c[3:], [("hard_brakes", "Hard brakes / 1k mi"), ("low_ttc_events", "Low-TTC / 1k mi")]):
    col.metric(label, f"{1000 * seg[field].sum() / miles:,.1f}" if miles else "–")

# ---------------- rates with confidence intervals ----------------
left, right = st.columns(2)
with left:
    st.subheader("Event rate by vehicle")
    f = fleet.assign(
        event=fleet.event_type.map(EVENT_LABELS),
        err_hi=fleet.per_1k_miles_hi - fleet.per_1k_miles,
        err_lo=fleet.per_1k_miles - fleet.per_1k_miles_lo,
    )
    fig = px.bar(f, x="event", y="per_1k_miles", color="vehicle", barmode="group",
                 error_y="err_hi", error_y_minus="err_lo",
                 labels={"per_1k_miles": "events per 1,000 miles", "event": ""})
    st.plotly_chart(fig, width="stretch")
    st.caption("Error bars: approx. 95% Poisson interval. Overlapping bars = no evidence of a real difference.")

with right:
    st.subheader("Daily trend")
    daily = d["daily_metrics"][d["daily_metrics"].vehicle.isin(sel_vehicles)].sort_values("drive_date")
    fig = px.line(daily, x="drive_date", y="hard_brakes_per_1k_miles", color="vehicle", markers=True,
                  labels={"hard_brakes_per_1k_miles": "hard brakes per 1,000 miles", "drive_date": ""})
    st.plotly_chart(fig, width="stretch")

# ---------------- scenario explorer ----------------
st.subheader("Scenario mining: most severe moments")
sc = d["scenarios"][d["scenarios"].vehicle.isin(sel_vehicles)]
types = sorted(sc.scenario_type.unique())
sel_type = st.selectbox("Scenario type", types, format_func=lambda x: EVENT_LABELS.get(x, x)) if types else None

if sel_type is None:
    st.info("No scenarios found for this selection.")
else:
    top = sc[sc.scenario_type == sel_type].sort_values("rank_in_type")
    st.dataframe(
        top[["rank_in_type", "severity", "peak_value", "vehicle", "drive_date", "segment_id", "start_t"]]
        .rename(columns={"rank_in_type": "rank"}).round(3),
        width="stretch", hide_index=True, height=220,
    )
    pick = st.selectbox("Inspect scenario", top.scenario_id.tolist())
    row = top[top.scenario_id == pick].iloc[0]
    tl = load_timeline(root, row.segment_id)
    tl = tl[(tl.t >= row.start_t - 10) & (tl.t <= row.end_t + 10)]

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05,
                        subplot_titles=("Speed (m/s)", "Acceleration (m/s²)", "Time-to-collision (s)"))
    fig.add_trace(go.Scatter(x=tl.t, y=tl.speed_mps, name="speed"), row=1, col=1)
    fig.add_trace(go.Scatter(x=tl.t, y=tl.accel_mps2, name="accel"), row=2, col=1)
    fig.add_trace(go.Scatter(x=tl.t, y=tl.ttc_s.clip(upper=15), name="TTC (capped 15 s)"), row=3, col=1)
    for r in (1, 2, 3):
        fig.add_vrect(x0=row.start_t, x1=row.end_t, fillcolor="red", opacity=0.15, line_width=0, row=r, col=1)
    fig.update_layout(height=600, showlegend=False)
    fig.update_xaxes(title_text="t (s since device boot)", row=3, col=1)
    st.plotly_chart(fig, width="stretch")

with st.expander("Data-quality report"):
    st.dataframe(dq, width="stretch", hide_index=True)
