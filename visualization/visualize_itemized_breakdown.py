import pickle
from tikzplotlib import save as tikz_save

import sys
import os
sys.path.append(os.path.join(sys.path[0], "..", "util"))
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

def plot_mulitple_pipelines(function, sf, pipelines_list,  title, save_file, format):
    COLORS =  ["#56B4E9", "#E69F00"]

    COLOR_MAPPING = {}
    plt.clf()

    plt.figure(figsize=format)

    fig, axes = plt.subplots(ncols=1, nrows=len(pipelines_list), sharex=True)

    ax1, ax2, ax3 = axes

    i = 0

    for idx, pipelines in enumerate(pipelines_list):
        num_invos = sum(len(x) for x in pipelines)
        num_invos *= len(pipelines_list)

        start_point = pipelines[0][0].timestamp
        LW = 4
        if (num_invos < 32):
            LW = 8
        if (num_invos > 100):
            LW = 1

        for idx2, pipeline in enumerate(pipelines):

            for invocation in pipeline:
                h = num_invos - i
                for timing_space in invocation.times:

                    if "timestamp" not in timing_space:
                        continue;

                    ### tpcx-bb
                    elif "import_time" in timing_space and "calc_time" in timing_space and "export_time" in timing_space:
                        copy = {"timestamp" : timing_space["timestamp"], "User Code" : timing_space["export_time"]}
                        timing_space = copy

                    elif "endtime" in timing_space:
                        copy = {"timestamp" : timing_space["timestamp"], "Skyrise" : timing_space["endtime"]}
                        timing_space = copy
                    elif "ExportClose" in timing_space:
                        copy = { "timestamp" : timing_space["timestamp"], "Skyrise" : timing_space["ExportClose"] }
                        timing_space = copy
                    elif "udf_export" in timing_space:
                        copy = { "timestamp" : timing_space["timestamp"], "User Code" : timing_space["udf_export"]}
                        timing_space = copy


                    last = timing_space["timestamp"] - start_point
                    for label, timestamp in {
                        k: v
                        for k, v in sorted(timing_space.items(), key=lambda item: item[1])
                    }.items():
                        if label == "timestamp" or label == "last":
                            continue
                        plt_label = "_" if label in COLOR_MAPPING else label
                        if label not in COLOR_MAPPING:
                            COLOR_MAPPING[label] = COLORS.pop()
                        color = COLOR_MAPPING[label]
                        axes[idx].plot(
                            (last / 1000 , (timestamp - start_point) / 1000),
                            (h, h),
                            linewidth=LW,
                            color=color,
                            label=plt_label,mec="gray", mew=5
                        )
                        last = timestamp - start_point
                    i += 1

    ax1.text(.99,.8,'All Custom',
    horizontalalignment='right',
    transform=ax1.transAxes, fontsize=12)

    ax2.text(.99,.8,'System-side Shuffle',
    horizontalalignment='right',
    transform=ax2.transAxes, fontsize=12)

    ax3.text(.99,.8,'BabelMR',
    horizontalalignment='right',
    transform=ax3.transAxes, fontsize=12)

    ax3.tick_params(axis="y",  # changes apply to the x-axis
        which="both",  # both major and minor ticks are affected
        bottom=False,  # ticks along the bottom edge are off
        top=False,  # ticks along the top edge are off
        labelbottom=False,
        labelleft=False,
        left=False,
        right=False
    )
    ax2.tick_params(axis="y",  # changes apply to the x-axis
        which="both",  # both major and minor ticks are affected
        bottom=False,  # ticks along the bottom edge are off
        top=False,  # ticks along the top edge are off
        labelbottom=False,
        labelleft=False,
        left=False,
        right=False
    )

    ax1.get_yaxis().set_ticks([])

    ax1.get_yaxis().set_label([])
    # ax2.get_yaxis().set_label([])
    ax3.get_yaxis().set_label([])

    ax1.tick_params(axis="y",  # changes apply to the x-axis
        which="both",  # both major and minor ticks are affected
        bottom=False,  # ticks along the bottom edge are off
        top=False,  # ticks along the top edge are off
        labelbottom=False,
        labelright=False,
        left=False,
        right=False
    )


    ax2.set_ylabel("Function Instance",fontsize=15)
    plt.xlabel("Runtime (s)",fontsize=15)

    import matplotlib.patches as mpatches

    usercode = mpatches.Patch(color="#56B4E9", label='BabelMR Engine')
    skyrise = mpatches.Patch(color="#E69F00", label='User Code')
    ax1.legend(handles=[usercode, skyrise],bbox_to_anchor=(0.5, 1.05), loc="lower center", ncol=2, fontsize=11)

    plt.tight_layout()
    plt.tick_params(axis="y", which="both", left=False, right=False, labelleft=False)

    tikz_save("graphs/itemized_breakdown.tex", axis_height='3cm', axis_width='7.5cm',
              extra_groupstyle_parameters={f'vertical sep=0.3cm'})
    #plt.show()

def plot_pickle(dir_name, title, format):
    objects = []
    with (open(f"../data/invokes/standalone/invoke.pickle", "rb")) as openfile:
        objects.append(pickle.load(openfile))
    with (open(f"../data/invokes/synth/invoke.pickle", "rb")) as openfile:
        objects.append(pickle.load(openfile))
    with (open(f"../data/invokes/babelmr/invoke.pickle", "rb")) as openfile:
        objects.append(pickle.load(openfile))

    plot_mulitple_pipelines("tpcx_bb", 100,  [objects[0].pipeline_results, objects[1].pipeline_results, objects[2].pipeline_results], title , save_file=dir_name + ".png", format=format)
    objects.clear()

plot_pickle("synthetic_shuffle_sf100_10mapper", "Synthetic Shuffle + Combining - BigBench Q1 - sf100 - C#", (12,7))
