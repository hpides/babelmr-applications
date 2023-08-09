import matplotlib.pyplot as plt
import numpy as np
from tikzplotlib import save as tikz_save

from config import *

WRITE_SPEED_COLOR = COLORS[0]
READ_SPEED_COLOR = COLORS[1]
MMU_COLOR = COLORS[2]

MEMORY = (128, 256, 512, 1024, 2048, 4096)

#LINE_STYLE = [None, "--"]
LINE_STYLE = [None, None]
DATA_LEFT_WRITE = {
    # Write Speeds
    #"32 MB" : [34.54802375119898, 52.19027149374644, 62.174575127862624, 323.14155748130776, 552.2060370716642, 562.6662400909275],
    "16 MB" : [32.4047432378991, 52.682771714042374, 65.0081256487657, 291.4389723325405, 501.32259377095676, 512.048782375448],
}

DATA_LEFT_READ = {
    # Read Speeds
    #"32 MB" : [34.20088877454879, 49.508826043214015, 55.054607484231525, 2194.963283773106, 4987.9443722621345, 9936.75102709519],
    "16 MB" : [34.26773821618583, 50.2351648519137, 57.83968860223262, 2131.79134647518, 4998.034366796684, 9922.94253697624],
}

DATA_RIGHT = {
    "Maximum Memory Usage" : [0.128, 0.256, 0.512, 1.002, 1.062, 1.063]
}

bars_per_point = len(DATA_LEFT_READ) + len(DATA_LEFT_WRITE) + len (DATA_RIGHT)
def plot_benchmark() -> None:

    width = 0.25
    x_indices = np.arange(len(MEMORY))

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    i = 0
    ax1.set_yscale('log')
    for (label_w, data_w), (label_r, data_r) in zip(DATA_LEFT_WRITE.items(), DATA_LEFT_READ.items()):
        local_ticks = x_indices + i *width - ((bars_per_point -1)  / 2.0 * width)
        ax1.bar(local_ticks, data_w, width=width, color=WRITE_SPEED_COLOR, linewidth = 1.5, edgecolor="black", label="Write" if i == 0 else "_", zorder=3, linestyle=LINE_STYLE[i // 2])
        local_ticks = x_indices + (i+1) *width - ((bars_per_point -1)  / 2.0 * width)
        ax1.bar(local_ticks, data_r, width=width, color=READ_SPEED_COLOR, linewidth = 1.5, edgecolor="black", label="Read" if i == 0 else "_", zorder=3, linestyle=LINE_STYLE[i // 2])
        ax1.set_ylabel("Throughput (MB/s)")
        i+=2
    for label, data in DATA_RIGHT.items():
        local_ticks = x_indices + i *width - ((bars_per_point -1)  / 2.0 * width)
        ax2.bar(local_ticks, data, width=width, color=MMU_COLOR, edgecolor="black", linewidth=1.5, label=label, zorder=3)
        ax2.set_ylim(ymax=4.096)
        ax2.set_ylabel("Memory Usage (GB)")
        i+=1

    #ax1.bar(1, -1, color="white", linewidth = 2.0, edgecolor="black", label="32 MB", zorder=3, linestyle=LINE_STYLE[0])
    #ax1.bar(1, -1, color="white", linewidth = 2.0, edgecolor="black", label="16 MB", zorder=3, linestyle=LINE_STYLE[1])

    handles, labels = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    handles += tuple(handles2)
    labels += tuple(labels2)
    # sort both labels and handles by labels
    #order = [0,1,4,2,3]

    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.5, 0.91), ncol=5, handletextpad=0.25)
    #fig.legend([handles[idx] for idx in order],[labels[idx] for idx in order],loc='lower center',
    #           bbox_to_anchor=(0.5, 0.91), ncol=5, handletextpad=0.25)
    #plt.ylim(bottom=0)
    ax1.grid(True,axis="y",zorder=0)
    ax1.set_xlabel("Lambda Function Memory Size (MB)")
    ax1.tick_params(axis='both', which='major')
    ax2.tick_params(axis='both', which='major')


    ax1.set_xticks(x_indices, MEMORY, rotation=90)
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    tikz_save("graphs/fs.tex", axis_height='5cm', axis_width='6.5cm')
    plt.show()


if __name__ == "__main__":
    plot_benchmark()
