#!/usr/bin/env python
"""
Lab 3C - Streamlit manufacturing dashboard for the Lab 2 conveyor.

LIVE status:  subscribes to MQTT FWH2311/+/status (published by the edge gateway, lab3a.py)
              and refreshes a live panel every 2 s.
HISTORY:      queries public.sensor_data in the Render PostgreSQL database
              for KPIs, counter and throughput trends.
Optional Controls tab publishes start/stop to the Lab 2B command topic, which
the edge gateway validates before writing the PLC.

Local:   streamlit run manufacturing_app.py
Secrets: .streamlit/secrets.toml locally, or App settings -> Secrets on Streamlit Cloud.
"""
import json
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg as pg
import streamlit as st

st.set_page_config(page_title="Conveyor Dashboard", page_icon="🏭", layout="wide")

DB_URL = st.secrets["DATABASE_URL"]
MQTT_HOST = st.secrets.get("MQTT_HOST", "test.mosquitto.org")
CONTROLS_ENABLED = bool(st.secrets.get("ENABLE_CONTROLS", False))
STALE_AFTER_S = 15


@st.cache_resource
def live_status():
    """Background MQTT subscriber: keeps the latest status message per station (source)."""
    import threading
    import paho.mqtt.client as mqtt

    store = {"latest": {}, "connected": False, "lock": threading.Lock()}

    def on_connect(client, userdata, flags, reason_code, properties):
        store["connected"] = not reason_code.is_failure
        client.subscribe(LIVE_TOPIC, qos=1)

    def on_disconnect(client, userdata, flags, reason_code, properties):
        store["connected"] = False

    def on_message(client, userdata, msg):
        try:
            d = json.loads(msg.payload.decode("utf-8", "ignore"))
            if isinstance(d, dict) and d.get("source"):
                d["_received"] = datetime.now(timezone.utc)
                with store["lock"]:
                    store["latest"][d["source"]] = d
        except Exception:
            pass                                  # ignore other teams' non-JSON payloads

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"lab3c-live-{uuid.uuid4().hex[:6]}")
    client.on_connect, client.on_disconnect, client.on_message = on_connect, on_disconnect, on_message
    try:
        client.connect_async(MQTT_HOST, 1883, 60)
        client.loop_start()
    except Exception:
        pass
    return store


LIVE_TOPIC = st.secrets.get("LIVE_TOPIC", "FWH2311/+/status")


@st.cache_resource
def get_conn():
    conn = pg.connect(DB_URL, sslmode=st.secrets.get("PGSSLMODE", "require"),
                      autocommit=True)         # read-only dashboard; avoids idle transactions
    conn.execute("SET TIME ZONE 'UTC'")        # event_time is stored as UTC
    return conn


def _run(conn, sql, params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def query(sql, params=None):
    try:
        return _run(get_conn(), sql, params)
    except pg.OperationalError:
        get_conn.clear()                       # Render free tier drops idle connections
        return _run(get_conn(), sql, params)


@st.cache_data(ttl=60)
def stations():
    return query("SELECT DISTINCT asset_id FROM public.sensor_data ORDER BY asset_id")["asset_id"].tolist()


def utc_naive(dt):
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


@st.cache_data(ttl=5)
def load(station, start, end):
    df = query(
        """SELECT event_time AS ts, asset_id, item_ready, at_entry, at_exit, machine_running,
                  entry_conveyor_status, exit_conveyor_status, system_enable, counter_value
           FROM public.sensor_data
           WHERE asset_id = %s AND event_time BETWEEN %s AND %s
           ORDER BY event_time""",
        (station, utc_naive(start), utc_naive(end)),
    )
    df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize("UTC")    # event_time is stored as UTC
    # Rolling 60 s throughput (parcels/min) from counter steps; a reset counts as the new value
    step = df["counter_value"].diff()
    step = step.where(step >= 0, df["counter_value"]).fillna(0)
    df["throughput_ppm"] = step.set_axis(df["ts"]).rolling("60s").sum().values
    return df


@st.cache_data(ttl=30)
def per_minute(station, start, end):
    df = query(
        """SELECT minute, parcels_per_minute FROM public.throughput_per_minute
           WHERE asset_id = %s AND minute BETWEEN %s AND %s ORDER BY minute""",
        (station, utc_naive(start), utc_naive(end)),
    )
    df["minute"] = pd.to_datetime(df["minute"]).dt.tz_localize("UTC")
    return df


# ---------------- sidebar filters ----------------
st.sidebar.header("Filters")
all_stations = stations()
if not all_stations:
    st.warning("No rows in public.sensor_data yet. Start edge_gateway.py and db_logger_service.py.")
    st.stop()
station = st.sidebar.selectbox("Station", all_stations)
window = st.sidebar.selectbox("Time window", ["Last 15 min", "Last 1 h", "Last 24 h", "Custom"], index=1)
now = datetime.now(timezone.utc)
if window == "Custom":
    d0 = st.sidebar.date_input("From (UTC)", now.date() - timedelta(days=1))
    d1 = st.sidebar.date_input("To (UTC)", now.date())
    start = datetime.combine(d0, datetime.min.time(), timezone.utc)
    end = datetime.combine(d1, datetime.max.time(), timezone.utc)
else:
    start = now - {"Last 15 min": timedelta(minutes=15), "Last 1 h": timedelta(hours=1),
                   "Last 24 h": timedelta(hours=24)}[window]
    end = now
if st.sidebar.button("Query / refresh", type="primary"):
    st.cache_data.clear()

# ---------------- data ----------------
st.title("Conveyor 01 — Smart Manufacturing Dashboard")
st.caption(f"Station **{station}** · PLC → Modbus → edge gateway → MQTT → PostgreSQL → Streamlit")

# ---------------- LIVE panel (MQTT) ----------------
@st.fragment(run_every=2)
def live_panel(station):
    store = live_status()
    with store["lock"]:
        d = dict(store["latest"].get(station, {}))
    st.subheader("Live status (MQTT)")
    if not d:
        st.info(f"Waiting for MQTT status from {station} on {LIVE_TOPIC} "
                f"({'broker connected' if store['connected'] else 'connecting to broker...'})")
        return
    age = (datetime.now(timezone.utc) - d["_received"]).total_seconds()
    running = bool(d.get("machine_running"))
    l1, l2, l3, l4, l5 = st.columns(5)
    if age > STALE_AFTER_S:
        l1.metric("Conveyor", "NO DATA")
    else:
        l1.metric("Conveyor", "🟢 RUNNING" if running else "🔴 STOPPED")
    l2.metric("System enable", "ON" if d.get("system_enable") else "OFF")
    l3.metric("Live count", d.get("counter_value", "—"))
    l4.metric("Part at entry", "YES" if d.get("at_entry") else "no")
    l5.metric("Message age", f"{age:.0f} s")
    st.caption(f"Last message {d.get('timestamp')} · quality {d.get('quality')} · "
               f"{'SIMULATED' if d.get('simulated') else 'real PLC'}")


live_panel(station)
st.divider()
st.subheader("Historical data (PostgreSQL)")

df = load(station, start, end)
if df.empty:
    st.info("No data for this station in the selected window. Widen the window or check the gateway.")
    st.stop()

good = df                                  # the logger stores only good-quality samples
last = df.iloc[-1]
age_s = (now - last["ts"].to_pydatetime()).total_seconds()

# ---------------- KPI row ----------------
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Package count", int(last["counter_value"]) if pd.notna(last["counter_value"]) else "—")
c2.metric("Throughput (parcels/min, last 60 s)", f"{last['throughput_ppm']:.0f}")
c3.metric("Machine", "RUNNING" if last["machine_running"] else "STOPPED")
c4.metric("System enable", "—" if pd.isna(last["system_enable"]) else ("ON" if last["system_enable"] else "OFF"))
c5.metric("Last update", f"{age_s:.0f} s ago")
if age_s > STALE_AFTER_S:
    st.warning(f"Data is stale: last message {age_s:.0f} s ago. The gateway or logger may be stopped.")

tab_trend, tab_tp, tab_data, tab_ctrl = st.tabs(["Counter trend", "Throughput", "Raw data", "Controls"])

with tab_trend:
    st.subheader("Package count over time")
    st.line_chart(good.set_index("ts")[["counter_value"]], height=320)
    st.subheader("Machine running / at-entry sensor")
    states = good.set_index("ts")[["machine_running", "at_entry"]].astype(float).fillna(0)
    st.area_chart(states, height=200)

with tab_tp:
    pm = per_minute(station, start, end)
    left, right = st.columns(2)
    with left:
        st.subheader("Parcels per minute (per-minute buckets)")
        if pm.empty:
            st.info("Not enough data for per-minute throughput yet.")
        else:
            st.bar_chart(pm.set_index("minute")[["parcels_per_minute"]], height=300)
    with right:
        st.subheader("Rolling 60 s throughput")
        st.line_chart(good.set_index("ts")[["throughput_ppm"]].dropna(), height=300)
    if not pm.empty:
        a, b, c = st.columns(3)
        a.metric("Total parcels in window", int(pm["parcels_per_minute"].sum()))
        b.metric("Mean parcels/min", f"{pm['parcels_per_minute'].mean():.2f}")
        c.metric("Peak parcels/min", int(pm["parcels_per_minute"].max()))

with tab_data:
    st.dataframe(df.sort_values("ts", ascending=False), width="stretch", height=420)
    st.download_button("Download CSV", df.to_csv(index=False), file_name=f"{station}_sensor_data.csv")

with tab_ctrl:
    st.subheader("Remote start / stop (optional, Lab 3C Part 4)")
    if not CONTROLS_ENABLED:
        st.info("Controls are disabled. Set ENABLE_CONTROLS = true in secrets only if you are "
                "authorized to control this cell.")
    else:
        st.caption("Publishes to the Lab 2B command topic. The edge gateway validates the command, "
                   "writes START_CMD, and replies on .../ack. Commands are NOT retained, so a restarted "
                   "gateway never replays an old start.")
        cmd_topic = f"FWH2311/{station}/cmd"
        col_a, col_b = st.columns(2)

        def send(command):
            import paho.mqtt.publish as publish
            publish.single(cmd_topic, json.dumps({"command": command}), qos=1, retain=False,
                           hostname=MQTT_HOST, port=1883, client_id=f"lab3c-ui-{uuid.uuid4().hex[:6]}")
            st.success(f"Sent {{\"command\": \"{command}\"}} to {cmd_topic}")

        if col_a.button("▶ Start conveyor", width="stretch"):
            send("start")
        if col_b.button("■ Stop conveyor", width="stretch"):
            send("stop")
