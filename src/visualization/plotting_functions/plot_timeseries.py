import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np


def plot_time_series(
    data,
    x_column=None,
    y_columns=None,
    title=None,
    xlabel=None,
    ylabel=None,
    colors=None,
    linestyles=None,
    linewidths=None,
    alpha=1.0,
    save_dir=None,
    figsize=(12, 6),
    date_format="%Y-%m-%d %H:%M",
    legend_loc="best",
    show=True,
):
    """
    Plots and saves a time series plot.

    Args:
        data (pd.DataFrame or dict of pd.DataFrame): The time series data.
        x_column (str): The column name for x-axis (time).
        y_columns (list of str): The column names for y-axis (values).
        title (str): Title of the plot.
        xlabel (str): Label for x-axis.
        ylabel (str): Label for y-axis.
        colors (list of str): List of colors for each line.
        linestyles (list of str): List of line styles for each line.
        linewidths (list of float): List of line widths for each line.
        alpha (float): Transparency level for the lines.
        save_dir (str): Directory to save the plot.
        figsize (tuple): Size of the figure.
        date_format (str): Date format for x-axis.
        legend_loc (str): Location of the legend.
        show (bool): Whether to show the plot.
    """
    # Ensure colors, linestyles, and linewidths are lists of appropriate length
    num_series = len(y_columns)
    if colors is None:
        colors = plt.cm.tab10(np.linspace(0, 1, num_series))
    if linestyles is None:
        linestyles = ["-"] * num_series
    if linewidths is None:
        linewidths = [1.5] * num_series

    # Prepare the plot
    fig, ax = plt.subplots(figsize=figsize)

    if x_column:
        x_data = data[x_column]
    else:
        x_data = data.index

    # Plot each time series
    for i, y_column in enumerate(y_columns):
        color = colors[i % len(colors)]
        linestyle = linestyles[i % len(linestyles)]
        linewidth = linewidths[i % len(linewidths)]
        label = y_column

        if isinstance(data, dict):
            for key, df in data.items():
                ax.plot(
                    x_data,
                    df[y_column],
                    label=f"{key} - {label}",
                    color=color,
                    linestyle=linestyle,
                    linewidth=linewidth,
                    alpha=alpha,
                )
        else:
            ax.plot(
                x_data,
                data[y_column],
                label=label,
                color=color,
                linestyle=linestyle,
                linewidth=linewidth,
                alpha=alpha,
            )

    # Customize the plot
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.xaxis.set_major_formatter(mdates.DateFormatter(date_format))
    ax.legend(loc=legend_loc)
    ax.grid(True)

    # Rotate x-axis labels for better readability
    plt.setp(ax.get_xticklabels(), rotation=90, ha="right")

    # Save the figure
    if save_dir:
        # os.makedirs(os.path.dirname(save_dir), exist_ok=True)
        plt.savefig(save_dir, bbox_inches="tight")

    # Show the plot
    if show:
        plt.show()

    # Close the plot to free memory
    plt.close()
