import matplotlib.pyplot as plt
import numpy as np
from tikzplotlib import save as tikz_save

from config import *
from helper import get_average_warm_runs, get_benchmark_results



SCALE_FACTORS_LEFT = [1, 10, 100, 1000]

SCALE_FACTORS_RIGHT = [1, 10, 100, 1000]

IDENTIFIERS_LEFT = {
    "babelmr/tpc-h-q1/python/parquet": {"label": "BabelMR"},
    "synthetic-shuffle/tpc-h-q1-synth/python/parquet":{"label" : "System-side Shuffle"},
    "standalone/tpc-h-q1/python/parquet": {"label": "All Custom"},
    }

IDENTIFIERS_RIGHT = {
    "babelmr/bb-q1/c#/parquet": {"label": "BabelMR"},
    "synthetic-shuffle/bb-q1-synth/c#/parquet":{"label" : "System-side Shuffle"},
    "standalone/bb-q1/c#/parquet": {"label": "All Custom"},
}


def plot_benchmark_tpc_h_q1_tpcx_bb_q1(log: dict) -> None:

    figure, (axis_l, axis_r) = plt.subplots(1, 2 ,gridspec_kw={'width_ratios': [4, 4]})

    width = 0.22

    x_indices_l = np.arange(len(SCALE_FACTORS_LEFT))
    i = 0
    for id, desc in IDENTIFIERS_LEFT.items():
        local_ticks = x_indices_l + i *width - ((len(IDENTIFIERS_LEFT) -1)  / 2.0 * width)
        axis_l.bar(local_ticks, [get_average_warm_runs(log[id]["runtimes"][str(sf)]["times"]) for sf in SCALE_FACTORS_LEFT], width=width, color=COLORS[i], edgecolor="gray", label=desc["label"], zorder=3)
        i+=1
    i = 0
    x_indices_r = np.arange(len(SCALE_FACTORS_RIGHT))
    for id, desc in IDENTIFIERS_RIGHT.items():
        local_ticks = x_indices_r + i *width - ((len(IDENTIFIERS_RIGHT) -1)  / 2.0 * width)
        axis_r.bar(local_ticks, [get_average_warm_runs(log[id]["runtimes"][str(sf)]["times"]) for sf in SCALE_FACTORS_RIGHT], width=width, color=COLORS[i], edgecolor="gray", label=desc["label"], zorder=3)
        i+=1

    axis_l.set_title("TPC-H Q1 Python")
    axis_r.grid(axis="y")
    axis_l.grid(axis="y")
    axis_r.set_title("TPCx-BB Q1 C#")
    figure.supxlabel("Scale Factor", y=0.04)
    axis_l.set_xticks(x_indices_l, [f"{sf}" for sf in SCALE_FACTORS_LEFT])#, rotation=90)
    axis_r.set_xticks(x_indices_r, [f"{sf}" for sf in SCALE_FACTORS_RIGHT])#, rotation=90)
    axis_l.set_ylabel("Runtime (seconds)")
    figure.tight_layout(rect=[0, 0, 1, 0.93])
    plt.legend(loc='lower center', bbox_to_anchor=(-0.1, 1.05), ncol=4)
    tikz_save("graphs/e2e.tex", axis_height='5cm', axis_width='7cm')
    plt.show()


if __name__ == "__main__":
    log = get_benchmark_results()
    plot_benchmark_tpc_h_q1_tpcx_bb_q1(log)