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

DB_URL = str(st.secrets.get("DATABASE_URL", "")).strip()
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
        """SELECT event_id, event_time AS ts, ingest_time, asset_id, item_ready, at_entry, at_exit, machine_running,
                  entry_conveyor_status, exit_conveyor_status, system_enable, counter_value
           FROM public.sensor_data
           WHERE asset_id = %s AND event_time BETWEEN %s AND %s
           ORDER BY event_time, event_id""",
        (station, utc_naive(start), utc_naive(end)),
    )
    df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize("UTC")    # event_time is stored as UTC
    df["ingest_time"] = pd.to_datetime(df["ingest_time"]).dt.tz_localize("UTC")  # DB server clock
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


# ---------------- database connection check ----------------
def _db_help(err):
    t = str(err)
    if "password authentication failed" in t:
        return "Password rejected: copy the EXTERNAL Database URL from Render into Secrets again."
    if "could not translate host name" in t or "Name or service not known" in t:
        return ("Host not found: use the EXTERNAL Database URL (host ends in "
                ".oregon-postgres.render.com), not the Internal one.")
    if "does not exist" in t:
        return "Database name is wrong: the part after the last '/' must match Render's Database field."
    if "timeout" in t.lower() or "timed out" in t.lower():
        return "Connection timed out: the Render database may be suspended or the URL host is wrong."
    return "Check DATABASE_URL in App settings -> Secrets."


if not DB_URL.startswith(("postgresql://", "postgres://")) or "PASSWORD@" in DB_URL or "USER:" in DB_URL:
    st.error("DATABASE_URL in Secrets is still the template. Paste your Render EXTERNAL Database URL.")
    st.stop()
try:
    get_conn()
except pg.OperationalError as e:
    get_conn.clear()
    st.error(f"Cannot connect to PostgreSQL. {_db_help(e)}")
    st.stop()

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
# Freshness uses ingest_time (stamped by the database server), so a drifting clock on the
# gateway PC cannot make live data look stale. Report the gateway clock offset separately.
age_s = (now - df["ingest_time"].max().to_pydatetime()).total_seconds()
clock_offset_s = (last["ingest_time"] - last["ts"]).total_seconds()

# ---------------- KPI row ----------------
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Package count", int(last["counter_value"]) if pd.notna(last["counter_value"]) else "—")
c2.metric("Throughput (parcels/min, last 60 s)", f"{last['throughput_ppm']:.0f}")
c3.metric("Machine", "RUNNING" if last["machine_running"] else "STOPPED")
c4.metric("System enable", "—" if pd.isna(last["system_enable"]) else ("ON" if last["system_enable"] else "OFF"))
c5.metric("Last update", f"{age_s:.0f} s ago")
if age_s > STALE_AFTER_S:
    st.warning(f"Data is stale: last database row arrived {age_s:.0f} s ago. "
               "The gateway or logger may be stopped.")
if abs(clock_offset_s) > 5:
    st.caption(f"Note: the gateway PC clock differs from the database clock by about "
               f"{clock_offset_s:.0f} s (ingest_time − event_time). Sync the PC clock "
               "(Windows Settings → Time → Sync now) so event timestamps are accurate.")

tab_trend, tab_tp, tab_kpi, tab_data, tab_ctrl = st.tabs(
    ["Counter trend", "Throughput", "OEE / KPIs", "Raw data", "Controls"])

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

with tab_kpi:
    # KPI and OEE method from Lectures 13-14 (streamlit_sql_kpi_app.py), applied to the
    # station and time window selected in the sidebar.
    k = good[["event_id", "ts", "machine_running", "counter_value"]].copy()
    k = k.sort_values(["ts", "event_id"]).reset_index(drop=True)
    k["seconds"] = (k["ts"] - k["ts"].iloc[0]).dt.total_seconds()
    st.caption(f"Observed window: {k.ts.iloc[0]:%Y-%m-%d %H:%M:%S} to {k.ts.iloc[-1]:%H:%M:%S} UTC "
               f"• {len(k)} samples • assumption: one counter increment = one completed unit")

    # Output and throughput: the first counter value is the baseline, not window output.
    elapsed_s = k["seconds"].iloc[-1]
    counter_change = k["counter_value"].diff()
    valid_counter = (k["counter_value"].notna().all() and (k["counter_value"] >= 0).all()
                     and not (counter_change < 0).any())
    output = None
    if len(k) >= 2 and elapsed_s > 0 and valid_counter:
        output = int(k["counter_value"].iloc[-1] - k["counter_value"].iloc[0])
        throughput = output * 3600 / elapsed_s
        c1, c2 = st.columns(2)
        c1.metric("Window output", f"{output} units")
        c2.metric("Throughput", f"{throughput:.1f} units/hour")
        st.caption(f"{output} / {elapsed_s:.1f} s × 3600 = {throughput:.1f} units/hour")
    else:
        st.info("Output/rate unavailable: need two samples, elapsed time and a counter without "
                "missing values or resets in this window (narrow the window to after the last reset).")

    # Hold each machine state until the next sample; the final row has no duration.
    dt = (k["ts"].shift(-1) - k["ts"]).dt.total_seconds()
    valid_state = (len(k) >= 2 and elapsed_s > 0
                   and k["machine_running"].iloc[:-1].notna().all()
                   and dt.iloc[:-1].between(0, 3, inclusive="right").all())
    run_s = 0.0
    if valid_state:
        run_s = float(dt[k["machine_running"].eq(True)].sum())
        c1, c2, c3 = st.columns(3)
        c1.metric("Observed running time", f"{run_s:.1f} s")
        c2.metric("Observed stopped time", f"{elapsed_s - run_s:.1f} s")
        c3.metric("Observed running fraction", f"{run_s / elapsed_s:.1%}")
    else:
        st.info("Running time unavailable: missing states, duplicate times or a sampling gap above 3 s "
                "(choose a window covering one continuous gateway run).")
    st.caption("Running fraction is a sampled estimate, not OEE availability.")

    # Completion spacing is observed at polling times, not active cycle time.
    if valid_counter and len(k) >= 2 and counter_change.iloc[1:].isin([0, 1]).all():
        gaps_s = k.loc[counter_change.eq(1), "ts"].diff().dt.total_seconds().dropna()
        if not gaps_s.empty:
            st.metric("Mean observed completion spacing", f"{gaps_s.mean():.2f} s")
            st.caption("Includes stops and polling delay; not measured processing cycle time.")

    st.subheader("OEE – enter real production inputs")
    wkey = f"{station}_{k.event_id.iloc[0]}_{k.event_id.iloc[-1]}"
    c1, c2 = st.columns(2)
    planned_min = c1.number_input("Planned production time (minutes)", min_value=0.0, value=0.0,
                                  step=0.1, key="planned_" + wkey)
    ideal_s = c2.number_input("Ideal cycle time (seconds/unit)", min_value=0.0, value=0.0,
                              step=0.1, key="ideal_" + wkey)
    c1, c2 = st.columns(2)
    total = c1.number_input("Total completed units", min_value=0, value=0, step=1, key="total_" + wkey)
    good_units = c2.number_input("First-pass good units", min_value=0, value=0, step=1, key="good_" + wkey)
    matched = st.checkbox("These inputs match this station and window; measured running time belongs "
                          "to its planned production time.", key="matched_" + wkey)
    planned_s = planned_min * 60
    if not matched:
        st.info("Enter the actual production inputs and confirm the matching window to calculate OEE.")
    elif not valid_state or run_s <= 0:
        st.warning("OEE unavailable: valid positive measured running time is required.")
    elif planned_s <= 0 or ideal_s <= 0 or total <= 0 or good_units > total:
        st.warning("Planned time, ideal cycle and total must be positive; good cannot exceed total.")
    elif planned_s > elapsed_s + 0.001 or run_s > planned_s + 0.001:
        st.warning("Planned time must fit within the observed window and cannot be shorter than running time.")
    else:
        availability = run_s / planned_s
        performance = ideal_s * total / run_s
        quality = good_units / total
        oee = availability * performance * quality
        a, p_, q, o = st.columns(4)
        a.metric("Availability", f"{availability:.1%}")
        p_.metric("Performance", f"{performance:.1%}")
        q.metric("Quality", f"{quality:.1%}")
        o.metric("OEE", f"{oee:.1%}")
        if performance > 1:
            st.warning("Performance exceeds 100%: check ideal cycle, counts and window. Values are not capped.")
        if output is not None and total != output:
            st.warning(f"Entered total ({total}) differs from counter-derived output ({output}). "
                       "Reconcile count definitions and window boundaries.")
    st.caption("A = run / planned; P = ideal cycle × total / run; Q = good / total; OEE = A × P × Q. "
               "No synthetic OEE inputs are supplied.")

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
