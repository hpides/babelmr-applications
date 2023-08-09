import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import json

LW = 4
COLORS = ["silver", "orange", "black", "gold", "gray", "purple", "green", "red", "blue"]
COLOR_MAPPING = {}


def plot(function, sf, pipelines, save_file=None):
    num_invos = sum(len(x) for x in pipelines)
    i = 0
    start_point = pipelines[0][0].timestamp
    for pipeline in pipelines:
        for invocation in pipeline:
            h = num_invos - i
            for timing_space in invocation.times:
                if timing_space == {}:
                    continue
                if "timestamp" not in timing_space:
                    continue; 
                last = timing_space["timestamp"] - start_point
                for label, timestamp in {
                    k: v
                    for k, v in sorted(timing_space.items(), key=lambda item: item[1])
                }.items():
                    if label == "timestamp":
                        continue
                    plt_label = "_" if label in COLOR_MAPPING else label
                    if label not in COLOR_MAPPING:
                        COLOR_MAPPING[label] = COLORS.pop()
                    color = COLOR_MAPPING[label]
                    plt.plot(
                        (last, timestamp - start_point),
                        (h, h),
                        linewidth=LW,
                        color=color,
                        label=plt_label,
                    )
                    last = timestamp - start_point
                    
            i += 1

    plt.title(f"{function} - invocation runtimes for TPC-H-Q1 at sf {sf}")
    plt.ylabel("Invocations")
    plt.xlabel("Runtime (ms)")
    plt.legend()
    plt.tick_params(axis="y", which="both", left=False, right=False, labelleft=False)
    if save_file:
        plt.savefig(save_file)
    plt.show()
