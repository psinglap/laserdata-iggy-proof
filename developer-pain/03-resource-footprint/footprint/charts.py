"""Draws the result charts from results/summary.json and results/rep-*/samples.csv.

Writes results/charts/{consumer-backlog,producer-pause,restart}.png (2x resolution) and .svg.
Usage: python charts.py results
"""
from __future__ import annotations

import csv
import json
import logging
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)
import matplotlib.pyplot as plt  # noqa: E402

INK, MUTED, LINE = "#111a2b", "#5a6578", "#dde2ea"
LIME, LIME_FILL, NAVY, ROSE = "#4f8a10", "#7cc23a", "#0b1322", "#b53a32"
plt.rcParams.update({
    "font.family": ["IBM Plex Sans", "DejaVu Sans"], "font.size": 11, "text.color": INK,
    "axes.edgecolor": LINE, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.color": LINE,
    "grid.linewidth": 0.8, "axes.axisbelow": True, "figure.dpi": 100, "savefig.dpi": 200,
})


def save(fig, out: Path, name: str) -> None:
    out.mkdir(parents=True, exist_ok=True)
    fig.savefig(out / f"{name}.png", bbox_inches="tight", facecolor="white")
    fig.savefig(out / f"{name}.svg", bbox_inches="tight", facecolor="white")
    plt.close(fig)


def heading(fig, title: str, subtitle: str) -> None:
    fig.text(0.0, 1.06, title, fontsize=15, fontweight="bold", color=INK, transform=fig.axes[0].transAxes)
    fig.text(0.0, 1.0, subtitle, fontsize=10.5, color=MUTED, transform=fig.axes[0].transAxes)


def footer(fig, env: dict) -> None:
    fig.text(0.0, -0.2, f"LaserData · Laser Streams server on 2 cores, {env.get('server_memory_limit')} limit · "
             f"Laser SDK {env.get('laser_sdk')} (Python) · GitHub-hosted runner · median of repetitions, bars show lowest–highest",
             fontsize=8.5, color=MUTED, transform=fig.axes[0].transAxes)


def cell(c):
    """A combined cell, or zeros when a test produced no number."""
    return c if isinstance(c, dict) and "median" in c else {"median": 0, "min": 0, "max": 0}


def samples(rep: Path, **match):
    with open(rep / "samples.csv") as f:
        return [r for r in csv.DictReader(f) if all(r[k] == v for k, v in match.items())]


def consumer_chart(s: dict, out: Path) -> None:
    parts = ["2", "10", "50"]
    b = s["combined"]["consumer_backlog"]
    fig, ax = plt.subplots(figsize=(8, 4.2))
    x = range(len(parts))
    for offset, key, color, label in ((-0.19, "consumer_rss_peak_mib", LIME_FILL, "Laser SDK consumer, peak RSS"),
                                      (0.19, "server_rss_peak_mib", NAVY, "Laser Streams server, peak RSS")):
        cells = [cell(b[p][key]) for p in parts]
        meds = [c["median"] for c in cells]
        err = [[c["median"] - c["min"] for c in cells], [c["max"] - c["median"] for c in cells]]
        bars = ax.bar([i + offset for i in x], meds, width=0.36, color=color, label=label, yerr=err,
                      capsize=4, error_kw={"ecolor": MUTED, "elinewidth": 1})
        for bar, m in zip(bars, meds):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{m:,.0f} MiB",
                    ha="center", va="bottom", fontsize=10, color=INK, fontweight="bold")
    ax.set_xticks(list(x), [f"{p} partitions" for p in parts])
    ax.set_ylabel("MiB")
    ax.legend(frameon=False, loc="upper left")
    top = max(cell(b[p][k])["max"] for p in parts for k in ("consumer_rss_peak_mib", "server_rss_peak_mib"))
    ax.set_ylim(0, (top or 1) * 1.3)
    heading(fig, "Consumer memory while catching up on 7 million records",
            "One Laser SDK consumer reads the whole backlog from offset 0, with SDK default settings")
    footer(fig, s["env"])
    save(fig, out, "consumer-backlog")


def producer_chart(root: Path, s: dict, out: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.2))
    reps = sorted(d for d in root.glob("rep-*") if (d / "samples.csv").exists())
    for i, rep in enumerate(reps):
        rows = samples(rep, test="producer_pause", target="producer")
        if not rows:
            continue
        t0 = float(rows[0]["t"])
        xs = [(float(r["t"]) - t0) / 60 for r in rows]
        ys = [float(r["rss_mib"]) for r in rows]
        ax.plot(xs, ys, color=LIME if i == 0 else LIME_FILL, lw=2 if i == 0 else 1, alpha=1 if i == 0 else 0.6,
                label="Laser SDK producer RSS" if i == 0 else None)
        if i == 0:
            paused = [x for x, r in zip(xs, rows) if r["phase"] == "server-paused"]
            if paused:
                ax.axvspan(min(paused), max(paused), color=ROSE, alpha=0.12, lw=0)
                ax.text((min(paused) + max(paused)) / 2, 4, "server paused\n60 s", color=ROSE,
                        fontsize=10, ha="center", va="bottom")
    ax.set_xlabel("minutes")
    ax.set_ylabel("MiB")
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False, loc="lower right")
    heading(fig, "Producer memory over 30 minutes at 20,000 records/s",
            "Laser SDK background (buffered) mode with its default buffer budget; each line is one repetition")
    footer(fig, s["env"])
    save(fig, out, "producer-pause")


def restart_chart(root: Path, s: dict, out: Path) -> None:
    rep = sorted(d for d in root.glob("rep-*") if (d / "samples.csv").exists())[0]
    rows = samples(rep, test="restart", target="server")
    rows = [r for r in rows if r["phase"] != "preload"]
    if not rows:
        return
    t0 = float(rows[0]["t"])
    xs = [float(r["t"]) - t0 for r in rows]
    fig, ax = plt.subplots(figsize=(8, 4.2))
    ax.plot(xs, [float(r["rss_mib"]) if r["rss_mib"] else float("nan") for r in rows], color=NAVY, lw=2, label="Server RSS (MiB)")
    ax2 = ax.twinx()
    ax2.plot(xs, [float(r["cpu_pct"]) if r["cpu_pct"] else float("nan") for r in rows], color=LIME_FILL, lw=1.4, label="Server CPU (% of one core)")
    ax2.set_ylim(0, 220)
    ax2.spines["right"].set_visible(True)
    ax2.grid(False)
    orch = json.loads((rep / "restart-orchestrator.json").read_text()).get("restarts", [])
    ax.set_ylim(0, max(float(r["rss_mib"] or 0) for r in rows) * 1.45)
    for i, r in enumerate(orch):
        x = r["began"] - t0
        ax.axvline(x, color=ROSE, lw=1.2, ls="--")
        ax.text(x, ax.get_ylim()[1] * (0.97 - 0.14 * i), f" {r['kind']} restart, answering again in {r['seconds_to_ready']} s",
                color=ROSE, fontsize=9.5, va="top")
    ax.set_xlabel("seconds")
    ax.set_ylabel("MiB")
    ax2.set_ylabel("% of one core")
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, frameon=False, loc="lower right")
    heading(fig, "Server memory and CPU through a graceful restart and a crash",
            "4 GiB on disk, a Laser SDK producer and consumer connected throughout (repetition 1)")
    footer(fig, s["env"])
    save(fig, out, "restart")


def main() -> None:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "results")
    s = json.loads((root / "summary.json").read_text())
    out = root / "charts"
    consumer_chart(s, out)
    producer_chart(root, s, out)
    restart_chart(root, s, out)
    print("charts written to", out)


if __name__ == "__main__":
    main()
