import pandas as pd

class DescriptiveStatistics:
    def __init__(self, df):
        self.df = df

    def calculate_mean(self, columns):
        return self.df[columns].mean()

    def calculate_median(self, columns):
        return self.df[columns].median()

    def calculate_std(self, columns):
        return self.df[columns].std()

    def calculate_min(self, columns):
        return self.df[columns].min()

    def calculate_max(self, columns):
        return self.df[columns].max()

    def calculate_percentiles(self, columns, percentiles=[0.25, 0.75]):
        return self.df[columns].quantile(percentiles)

    def summary(self, columns):
        return self.df[columns].describe()