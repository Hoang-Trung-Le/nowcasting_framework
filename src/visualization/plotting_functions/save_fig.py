# src/visualization/plotting_functions/save_fig.py

import os
from pathlib import Path
import matplotlib.pyplot as plt


def save_figure(fig, filename, sub_dir="figures"):
    """
    Save a matplotlib figure to a specified sub-directory.

    Args:
        fig (matplotlib.figure.Figure): The figure object to save.
        filename (str): The name of the file to save the figure as.
        sub_dir (str): The sub-directory within the project to save the figure. Defaults to "figures".
    """
    # Get the current file's directory
    current_file_dir = Path(__file__).resolve().parent
    # Navigate up to the project root directory (assuming project root is two levels up)
    project_root = current_file_dir.parents[2]

    # Define the path to the figures directory
    fig_dir = project_root / sub_dir

    # Create the directory if it doesn't exist
    fig_dir.mkdir(parents=True, exist_ok=True)

    # Define the full path for the figure file
    fig_path = fig_dir / filename

    # Save the figure
    fig.savefig(fig_path, bbox_inches="tight")
    print(f"Figure saved at {fig_path}")
