import glob
import ntpath
import operator

import pandas as pd
from numpy.lib.histograms import _hist_bin_doane


def data_at_date(dt, feature):
    """
    Prepare data at a given date
    """

    all_data = glob.glob("./data/processed/*.csv")
    data = list()
    for path in all_data:
        df = pd.read_csv(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"].values)
        df["timestamp"] = df["timestamp"].dt.date
        df = df[df["timestamp"] == pd.Timestamp(dt)]
        if len(df) > 0:
            if df[feature].values[0] > 0:
                data.append(
                    {
                        "name": ntpath.basename(path).replace(".csv", ""),
                        feature: df[feature].values[0],
                    }
                )
    return data


if __name__ == "__main__":
    market_cap = data_at_date("2017-10-01", "market_cap")
    total_cap = sum([coin["market_cap"] for coin in market_cap])
    cap = 0.10

    allocations = [
        {
            "market_cap": coin["market_cap"],
            "symbol": coin["name"],
            "ratio": (coin["market_cap"] / total_cap),  # ratios (sums to 100%)
        }
        for coin in market_cap
    ]

    allocations = list(
        sorted(allocations, key=lambda alloc: -alloc["ratio"])
    )  # sort by descending ratio

    for i in range(len(allocations)):
        alloc = allocations[i]

        if alloc["ratio"] > cap:
            overflow = (
                alloc["ratio"] - cap
            )  # the amount of % that needs to be spread to the other coins
            alloc["ratio"] = cap

            remaining_allocs = allocations[i + 1 :]

            total_nested_cap = sum(
                [n_alloc["market_cap"] for n_alloc in remaining_allocs]
            )  # market cap of the remaining coins
            new_allocs = list()

            for n_alloc in remaining_allocs:
                cap_fraction = (
                    n_alloc["market_cap"] / total_nested_cap
                )  # percentage of the remainder this makes up (sums to 100%)
                n_alloc["ratio"] += overflow * cap_fraction  # weighted
                new_allocs.append(n_alloc)

            allocations = allocations[: i + 1] + new_allocs

        print("Done")

    print("Done")
