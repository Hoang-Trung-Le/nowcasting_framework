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
        colors = ['skyblue', 'lightgreen', 'lightcoral']
        legends = []
        site_sources = []

        for site in sites:
            site_data = all_sites_data[(all_sites_data["parameter"] == parameter) & (all_sites_data["site"] == site)]
            site_sources = site_data["source"].unique()
            
            for idx, source in enumerate(site_sources):
                source_data = site_data[site_data["source"] == source]
                source_data_grouped = source_data.groupby("month")["value"].apply(list)
                
                months = source_data_grouped.index.astype(str)
                positions = list(range(len(months)))
                positions = [p + 0.1 * idx for p in positions]
                
                if len(source_data_grouped) == len(positions):
                    bp = plt.boxplot(source_data_grouped, positions=positions, widths=0.1, patch_artist=True, boxprops=dict(facecolor=colors[idx % len(colors)]), showfliers=False, showmeans=True)
                    legends.append(bp["boxes"][0])
                else:
                    print(f"Skipping {site}-{source} for parameter {parameter} due to mismatch in lengths")
                    print(f"source_data_grouped: {source_data_grouped}")
                    print(f"positions: {positions}")

        plt.title(f'Monthly Boxplot for {parameter}')
        plt.suptitle('')
        plt.xlabel('Month')
        plt.ylabel('Value')
        plt.xticks(ticks=list(range(len(months))), labels=months, rotation=90)
        plt.legend(legends, site_sources, title='Source')
        plt.tight_layout()
        # plt.savefig(f"{output_dir}/boxplot_{parameter}.png")
        plt.show()
