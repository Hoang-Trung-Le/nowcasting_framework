import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# from save_fig import save_figure


def scatter_plot(
    x,
    y,
    labels=None,
    title=None,
    xlabel=None,
    ylabel=None,
    color=None,
    marker="o",
    s=20,
    alpha=0.7,
    save_dir=None,
):
    """
    Plots a scatter plot using x and y data.

    Parameters:
        x (array-like or DataFrame column): x-axis data
        y (array-like or DataFrame column): y-axis data
        labels (list or DataFrame column, optional): Labels for each point
        title (str, optional): Title of the plot
        xlabel (str, optional): Label for the x-axis
        ylabel (str, optional): Label for the y-axis
        color (str or array-like, optional): Color of the points
        marker (str, optional): Marker style
        s (int, optional): Size of the points
        alpha (float, optional): Transparency level of the points
    """

    # Convert Pandas Series to NumPy arrays if necessary
    if isinstance(x, pd.Series):
        x = x.values
    if isinstance(y, pd.Series):
        y = y.values
    if isinstance(labels, pd.Series):
        labels = labels.values

    plt.figure(figsize=(10, 6))
    scatter = plt.scatter(x, y, c=color, marker=marker, s=s, alpha=alpha)

    # If labels are provided, annotate each point
    # if labels is not None:
    #     for i, label in enumerate(labels):
    #         plt.annotate(label, (x[i], y[i]), fontsize=9, alpha=0.75)

    if title:
        plt.title(title)
    if xlabel:
        plt.xlabel(xlabel)
    if ylabel:
        plt.ylabel(ylabel)

    if save_dir:
        # save_fig()
        plt.savefig(save_dir)

    plt.show()
