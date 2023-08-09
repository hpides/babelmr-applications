import matplotlib.pyplot as plt
import numpy as np
from tikzplotlib import save as tikz_save

from config import *
from helper import get_initialization_benchmark_results, get_metrics

ZIP_DIRECT_COLOR = COLORS[0]
ZIP_S3_COLOR = COLORS[1]
IMAGE_ECR_COLOR = COLORS[2]

ZIP_S3 = [
    {"package_name":"S3_skyriseSizedFunction10MB", "label": "10", "pos":0},
    {"package_name":"S3_skyriseSizedFunction20MB", "label": "20","pos":2},
    {"package_name":"S3_skyriseSizedFunction30MB", "label": "30","pos":4},
    {"package_name":"S3_skyriseSizedFunction40MB", "label": "40","pos":6},
    {"package_name":"S3_skyriseSizedFunction50MB", "label": "50","pos":8},
    {"package_name":"S3_skyriseSizedFunction250MB", "label": "250","pos":10}
]


ZIP_DIRECT = [
    {"package_name":"skyriseSizedFunction10MB", "label": "10","pos":1},
    {"package_name":"skyriseSizedFunction20MB", "label": "20","pos":3},
    {"package_name":"skyriseSizedFunction30MB", "label": "30","pos":5},
    {"package_name":"skyriseSizedFunction40MB", "label": "40","pos":7},
    {"package_name":"skyriseSizedFunction50MB", "label": "50","pos":9}
]

IMAGE_ECR = [
    {"package_name":"ECR_sized_function_container500MB", "label": "500","pos":11},
    {"package_name":"ECR_sized_function_container1000MB", "label": "1000","pos":12},
    {"package_name":"ECR_sized_function_container2000MB", "label": "2000","pos":13},
    {"package_name":"ECR_sized_function_container4000MB", "label": "4000","pos":14},
]

def plot_benchmark(bench_log : dict) -> None:
    width = 0.52

    num_point = len(ZIP_S3 + ZIP_DIRECT + IMAGE_ECR)
    x_indices = np.arange(num_point)

    for type, color, type_name in zip(
            [ZIP_S3, ZIP_DIRECT, IMAGE_ECR],
            [ZIP_S3_COLOR, ZIP_DIRECT_COLOR, IMAGE_ECR_COLOR],
            ["Zip S3", "Zip Direct", "Image ECR"]):
        y_s = [get_metrics(pck["package_name"], bench_log)["initialization_total_latency_ms_latency_average_ms"] / 1000 for pck in type]
        x_s = [pkg["pos"] for pkg in type]
        plt.bar(x_s, y_s, width=width, color=color, label = type_name, zorder=3,linewidth = 1.2, edgecolor="gray",)
    plt.legend(loc='lower center',
               bbox_to_anchor=(0.5, 0.98), ncol=5, handletextpad=0.25)

    plt.grid(True,axis="y",zorder=0)
    plt.xlabel("Package Size (MB)")
    x_ticks = [0 for _ in range(num_point)]
    for type in ZIP_S3 + ZIP_DIRECT + IMAGE_ECR:
        x_ticks[type["pos"]] = int(type["label"])
    plt.xticks(x_indices, x_ticks, rotation = 90)
    plt.ylabel("Duration (seconds)")
    plt.tick_params(axis='both', which='major')
    plt.yscale("log")

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    tikz_save("graphs/initialization.tex", axis_height='5cm', axis_width='7cm')

    plt.show()


if __name__ == "__main__":
    bench_log = get_initialization_benchmark_results()
    plot_benchmark(bench_log)
