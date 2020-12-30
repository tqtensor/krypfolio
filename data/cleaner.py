import glob

import numpy as np
import pandas as pd
from scipy import stats


def main():
    """
    Utility to clean and remove outlier
    """

    for path in glob.glob("./data/processed/*.csv"):
        df = pd.read_csv(path)

        df = df[(np.abs(stats.zscore(df["market_cap"].values)) < 5)]  # remove outliers
        df = df[df["market_cap"] > 0]

        df.drop_duplicates(inplace=True)

        df = df[["close", "high", "low", "market_cap", "open", "timestamp", "volume"]]
        df.to_csv(path, index=False)


if __name__ == "__main__":
    main()
