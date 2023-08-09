import matplotlib.pyplot as plt
import numpy as np
from tikzplotlib import save as tikz_save


from helper import get_average_warm_runs, get_benchmark_results
from config import *

SCALE_FACTORS = [1, 10, 100, 1000]


IDENTIFIERS = {
    "babelmr/tpc-h-q1/python/csv": {"label": "BabelMR (Python)"},
    "babelmr/tpc-h-q1/go/csv":{"label" : "BabelMR (Go)"},
    "PyWren/tpc-h-q1/csv": {"label":"PyWren (Python)"},
    "Corral/tpc-h-q1/csv":{"label": "Corral (Go)"},
    "EMR_Serverless/tpc-h-q1/csv/conservative":{"label":"PySpark - Elastic Cluster"},
    "RAY_GLUE/tpc-h-q1/csv":{"label":"Ray (Python)"},
    }


def plot_benchmark(log: dict, identifiers: dict, scale_factors: list[int]) -> None:

    width = 0.15
    x_indices = np.arange(len(scale_factors))

    plt.clf()
    i = 0
    i_col = 0
    for id, desc in identifiers.items():
        local_ticks = x_indices + i *width - ((len(identifiers) -1)  / 2.0 * width)
        for j, sf in enumerate(scale_factors):
            # Failed Run
            if log[id]["runtimes"][str(sf)]["times"][0] == -2.0:
                plt.plot(local_ticks[j], 2.0, marker="x", color="black")
            # Run not executed
            if log[id]["runtimes"][str(sf)]["times"][0] == -1.0: 
                plt.plot(local_ticks[j], 2.0, marker="*", color="black")
        plt.bar(local_ticks, [get_average_warm_runs(log[id]["runtimes"][str(sf)]["times"]) for sf in scale_factors], width=width, color=COLORS[i_col], edgecolor="gray", label=desc["label"], zorder=3)
        
        i+=1
        i_col+=1
        if id == "EMR_Serverless/tpc-h-q1/csv/conservative":
            plt.bar(local_ticks, [get_average_warm_runs(log["EMR_Serverless/tpc-h-q1/csv/pre-initialized"]["runtimes"][str(sf)]["times"]) for sf in scale_factors], width=width, color=COLORS[i_col], edgecolor="gray",
            label="PySpark - Static Cluster", zorder=3, hatch="x")
            i_col+=1
        
        plt.ylabel("Runtime (seconds)")


    plt.legend(prop = { "size": 16 },loc='lower center', bbox_to_anchor=(0.5, 1.02), ncol=3)
    plt.ylim(bottom=0)
    plt.grid(True,axis="y",zorder=0)
    plt.xlabel("Scale Factor")
    plt.xticks(x_indices, [f"{sf}" for sf in SCALE_FACTORS])#, rotation=90)
    plt.tight_layout()
    tikz_save("graphs/systems.tex", axis_height='5cm', axis_width='7cm')
    plt.show()


if __name__ == "__main__":
    log = get_benchmark_results()
    plot_benchmark(log, IDENTIFIERS, SCALE_FACTORS)