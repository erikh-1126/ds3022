"""
Seismic Map

Streamlit app that reads earthquake events from Redis (written by
seismic-consumer.py) and plots them on a map, refreshing every few seconds.

Run with:  streamlit run 06_kafka/seismic/seismic-map.py
"""

import os
import logging
import pandas as pd
import pydeck as pdk
import redis
import streamlit as st

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")

# Magnitude bands: (lower bound, label, RGBA color)
BANDS = [
    (-10, "< 3", [44, 160, 44, 180]),
    (3, "3 – 4.9", [255, 200, 0, 190]),
    (5, "5 – 5.9", [255, 127, 14, 200]),
    (6, "6+", [214, 39, 40, 220]),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    filename="seismic-map.log",
)
log = logging.getLogger(__name__)


# One Redis connection shared across reruns and browser sessions
@st.cache_resource
def get_redis():
    return redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)


# Load every quake:* hash into a DataFrame
def load_events(r):
    rows = [r.hgetall(k) for k in r.scan_iter("quake:*")]
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for col in ["lat", "lon", "depth", "mag"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.dropna(subset=["lat", "lon", "mag"])
    # Magnitude is logarithmic, so grow radius exponentially (pixels, so it holds across zoom levels)
    df["radius"] = (2 * 1.5 ** df["mag"].clip(lower=1)).clip(upper=60)
    df["color"] = df["mag"].apply(lambda m: next(c for lo, _, c in reversed(BANDS) if m >= lo))
    df["time_str"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    return df.sort_values("time", ascending=False)


# Scatterplot layer: dot size and color both encode magnitude; hover shows details
def quake_map(df):
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df[::-1],  # newest drawn last, so it sits on top
        get_position=["lon", "lat"],
        get_radius="radius",
        radius_units="pixels",
        get_fill_color="color",
        get_line_color=[0, 0, 0, 120],
        line_width_min_pixels=1,
        stroked=True,
        pickable=True,
    )
    return pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(latitude=20, longitude=0, zoom=1),
        map_style=pdk.map_styles.CARTO_LIGHT,
        tooltip={"text": "M{mag} | {region}\n{time_str}\nDepth {depth} km"},
    )


st.set_page_config(page_title="Seismic Map", layout="wide")
st.title("Live Earthquakes (EMSC)")


# Re-runs only this block every 5 seconds, not the whole script
@st.fragment(run_every="5s")
def live_map():
    try:
        df = load_events(get_redis())
    except redis.RedisError as e:
        log.error(f"Redis read failed: {e}")
        st.error(f"Cannot reach Redis at {REDIS_HOST}:6379 — {e}")
        return
    if df.empty:
        st.info("No events in Redis yet. Is seismic-consumer.py running?")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Events (24h)", len(df))
    c2.metric("Largest magnitude", f"{df['mag'].max():.1f}")
    c3.metric("Latest", df["time"].iloc[0].strftime("%H:%M:%S UTC"))

    st.pydeck_chart(quake_map(df))
    # Simple color legend for the magnitude bands
    st.markdown(" &nbsp; ".join(
        f"<span style='color:rgb({c[0]},{c[1]},{c[2]})'>●</span> M {label}" for _, label, c in BANDS
    ), unsafe_allow_html=True)
    st.dataframe(df[["time", "mag", "depth", "region", "action"]].head(25), hide_index=True)


live_map()
