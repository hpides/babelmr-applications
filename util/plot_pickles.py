from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
# from plot_udf_invocations import plot
from evaluate_invoke_responses import LambdaInvocationResponse


class Invoke:
    """Class to store invoke context and results"""

    function: str
    pipeline_payloads: list[list[str]]
    pipeline_modes: list[str]
    pipeline_results: list[list[LambdaInvocationResponse]]

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import json

# COLORS = ["silver", "orange", "black", "gold", "gray", "purple", "green", "red", "blue"]


def plot(function, sf, pipelines,  title, save_file, format):
    COLORS =  ["limegreen", "deepskyblue","deeppink","orange"]
    # COLORS = ["orange", "limegreen", "orange"]
    # COLOR_MAPPING = {"Import" : "orange", "Calculation":"limegreen", "Export" : "orange"}
    COLOR_MAPPING = {}
    plt.clf()

    num_invos = sum(len(x) for x in pipelines)
    i = 0
    plt.figure(figsize=format)

    start_point = pipelines[0][0].timestamp


    # print(num_invos)
    LW = 4
    if (num_invos < 32):
        LW = 8

    for idx, pipeline in enumerate(pipelines):
        endpoints = []
        startpoints = []

        for  invocation in pipeline:
            h = num_invos - i
            for timing_space in invocation.times:
                print(timing_space)
                if "timestamp" not in timing_space:
                    continue;
                if "udf_read_parquet_from_fs" in timing_space:
                    copy = {"timestamp" : timing_space["timestamp"], "UDF Fileimport" : timing_space["udf_read_parquet_from_fs"],  "UDF DataFrame Initialization" : timing_space["udf_created_dataframe"], "Calculation" : timing_space["udf_export"]}
                    timing_space = copy

                if "udf_created_dataframe" in timing_space:
                    copy = {"timestamp" : timing_space["timestamp"], "UDF DataFrame Initialization" : timing_space["udf_created_dataframe"], "Calculation" : timing_space["udf_calculation"], "UDF Export" : timing_space["udf_export"]}
                    timing_space = copy


                if "Import" in timing_space:
                    copy = {"timestamp" : timing_space["timestamp"], "Skyrise Import/Partition/Export" : timing_space["ExportClose"]}
                    endpoints.append(timing_space["Total"])
                    timing_space = copy
                if "endtime" in timing_space:
                    copy = {"timestamp" : timing_space["timestamp"], "Skyrise Synthetic Shuffle" : timing_space["endtime"]}
                    # endpoints.append(timing_space["Total"])
                    timing_space = copy

                if "import_time" in timing_space:
                    copy = {"timestamp" : timing_space["timestamp"], "Import" : timing_space["df_creation"], "Export": timing_space["export_time"], "Calculation": timing_space["calc_time"]}
                    # copy = {"timestamp" : timing_space["timestamp"], "Import" : timing_space["import_time"], "Export": timing_space["export_time"], "Calculation": timing_space["calc_time"]}

                    timing_space = copy

                startpoints.append(timing_space["timestamp"])
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
                        (last % 100000, (timestamp - start_point) % 100000),
                        (h, h),
                        linewidth=LW,
                        color=color,
                        label=plt_label,
                    )
                    last = timestamp - start_point
            i += 1

        # print(idx,"min_starpoin", ":",min(startpoints))
        # print(idx,"max_endpoint", ":", max(endpoints))
    plt.title(title)
    plt.ylabel("Invocations")
    plt.xlabel("Runtime (ms)")
    plt.legend(
        loc="center left",
        bbox_to_anchor=(1, 0.5)
    )
    plt.tight_layout()
    plt.tick_params(axis="y", which="both", left=False, right=False, labelleft=False)
    if save_file:
        plt.savefig("./figures/" + save_file, dpi=400)
    # plt.show()
