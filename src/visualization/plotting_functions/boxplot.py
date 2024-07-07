import os
import sys
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def plot_monthly_boxplots(all_sites_data, output_dir):
    parameters = all_sites_data["parameter"].unique()
    sites = all_sites_data["site"].unique()

    for parameter in parameters:
        plt.figure(figsize=(12, 6))
        colors = ["skyblue", "lightgreen", "lightcoral"]
        legends = []
        site_sources = []

        for site in sites:
            site_data = all_sites_data[
                (all_sites_data["parameter"] == parameter)
                & (all_sites_data["site"] == site)
            ]
            site_sources = site_data["source"].unique()

            for idx, source in enumerate(site_sources):
                source_data = site_data[site_data["source"] == source]
                source_data_grouped = source_data.groupby("month")["value"].apply(list)

                months = source_data_grouped.index.astype(str)
                positions = list(range(len(months)))
                positions = [p + 0.1 * idx for p in positions]

                if len(source_data_grouped) == len(positions):
                    bp = plt.boxplot(
                        source_data_grouped,
                        positions=positions,
                        widths=0.1,
                        patch_artist=True,
                        boxprops=dict(facecolor=colors[idx % len(colors)]),
                        showfliers=False,
                        showmeans=True,
                    )
                    legends.append(bp["boxes"][0])
                else:
                    print(
                        f"Skipping {site}-{source} for parameter {parameter} due to mismatch in lengths"
                    )
                    print(f"source_data_grouped: {source_data_grouped}")
                    print(f"positions: {positions}")

        plt.title(f"Monthly Boxplot for {parameter}")
        plt.suptitle("")
        plt.xlabel("Month")
        plt.ylabel("Value")
        plt.xticks(ticks=list(range(len(months))), labels=months, rotation=90)
        plt.legend(legends, site_sources, title="Source")
        plt.tight_layout()
        # plt.savefig(f"{output_dir}/boxplot_{parameter}.png")
        plt.show()


def plot_grouped_boxplots_seaborn(
    data,
    value_col,
    group_col,
    x_col,
    site,
    palette=None,
    showfliers=True,
    savefig_dir=None,
):
    """
    Plot grouped boxplots using Seaborn for given data.

    Args:
        data (pd.DataFrame): DataFrame containing the data to plot.
        value_col (str): Column name for the values to plot.
        group_col (str): Column name for the group categories.
        x_col (str): Column name for the x-axis categories.
        palette (str or list): Colors to use for the different groups.
    """
    # Ensure index is unique
    data = data.reset_index()

    plt.figure(figsize=(32, 8))
    sns.boxplot(
        data=data,
        x=x_col,
        y=value_col,
        hue=group_col,
        palette=palette,
        dodge=True,
        showfliers=showfliers,
    )
    plt.xlabel(x_col)
    plt.ylabel(value_col)
    plt.title(f"Grouped Boxplot for {value_col} by {x_col} at {site}")
    plt.legend(title=group_col)
    plt.xticks(rotation=0)
    plt.tight_layout()
    if savefig_dir:
        # if site == "Wagga_Wagga_North":
        #     suffix = "fliers" if showfliers else "nofliers"
        #     filename = os.path.join(
        #         savefig_dir,
        #         f"{site}_{value_col}_{suffix}_daily.png",
        #     )
        #     print(site)
        #     plt.savefig(filename)
        # else:
        plt.savefig(savefig_dir)
    plt.show()
