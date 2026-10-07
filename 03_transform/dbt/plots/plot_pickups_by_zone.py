"""Sunburst of taxi departures by zone from the mart_pickups_by_zone dbt model.

Inner ring = the 5 NYC boroughs; outer ring = every zone in that borough,
in zone-ID order. Seaborn has no sunburst, so this is a matplotlib nested
pie styled with a seaborn theme/palette. Run `dbt build --select
+mart_pickups_by_zone` first, then:

    python plot_pickups_by_zone.py
"""

import logging
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Patch

HERE = Path(__file__).parent
DB_PATH = HERE / "../nyc_taxi.duckdb"
OUTPUT_PATH = HERE / "pickups_by_zone.png"
LOG_PATH = HERE / "plot_pickups_by_zone.log"

# NYC's official borough codes (1-5) set the ring order
BOROUGHS = ["Manhattan", "Bronx", "Brooklyn", "Queens", "Staten Island"]
LABEL_MIN_PCT = 1.5  # only label zones with at least this share of pickups

logging.basicConfig(
    filename=LOG_PATH, level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def load_data():
    """Pull the 5-borough zones, sorted by borough code then zone ID."""
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        df = con.execute(
            "select location_id, borough, zone, trip_count from mart_pickups_by_zone"
        ).df()
    excluded = df.loc[~df["borough"].isin(BOROUGHS), "trip_count"].sum()
    df = df[df["borough"].isin(BOROUGHS)].copy()
    df["borough_code"] = df["borough"].map(BOROUGHS.index)
    return df.sort_values(["borough_code", "location_id"]), excluded


def main():
    df, excluded = load_data()
    total = df["trip_count"].sum()
    log.info("Loaded %d zones, %d pickups (%d excluded: EWR/Unknown/N/A)", len(df), total, excluded)

    sns.set_theme(style="white")
    colors = dict(zip(BOROUGHS, sns.color_palette("deep", len(BOROUGHS))))
    by_borough = df.groupby("borough_code", sort=True)["trip_count"].sum()

    fig, ax = plt.subplots(figsize=(11, 11))
    wedge = dict(edgecolor="white", linewidth=0.3)

    # Inner ring: borough totals
    ax.pie(
        by_borough, radius=0.62, startangle=90, counterclock=False,
        colors=[colors[BOROUGHS[i]] for i in by_borough.index],
        wedgeprops=dict(width=0.3, **wedge),
    )

    # Outer ring: zones, alternating shade within a borough so adjacent
    # wedges stay distinguishable
    zone_colors = [
        tuple(c + (1 - c) * (0.3 if i % 2 else 0.0) for c in colors[b])
        for i, b in enumerate(df["borough"])
    ]
    pcts = 100 * df["trip_count"] / total
    labels = [str(z) if p >= LABEL_MIN_PCT else "" for z, p in zip(df["location_id"], pcts)]
    ax.pie(
        df["trip_count"], radius=1.0, startangle=90, counterclock=False,
        colors=zone_colors, labels=labels, labeldistance=1.04,
        textprops=dict(fontsize=8), wedgeprops=dict(width=0.38, **wedge),
    )

    ax.legend(
        handles=[
            Patch(color=colors[BOROUGHS[i]], label=f"{BOROUGHS[i]}  {100 * n / total:.1f}%")
            for i, n in by_borough.items()
        ],
        loc="lower left", bbox_to_anchor=(-0.05, -0.05), frameon=False, fontsize=10,
    )
    ax.set_title(
        f"NYC Yellow Taxi Departures by Zone (2025) — {total:,} pickups\n"
        f"Outer ring: zones in ID order within each borough; labeled if ≥{LABEL_MIN_PCT}% of pickups",
        fontsize=13, loc="left",
    )
    fig.savefig(OUTPUT_PATH, dpi=150, bbox_inches="tight", facecolor="white")
    log.info("Saved %s", OUTPUT_PATH)
    print(f"Saved sunburst to {OUTPUT_PATH}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Failed to plot pickups by zone")
        raise
