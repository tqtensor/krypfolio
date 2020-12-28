import glob
import multiprocessing
import ntpath
import pickle
from datetime import date, datetime
from multiprocessing import Pool

import numpy as np
import pandas as pd
from dateutil import rrule
from tqdm.auto import tqdm


class HODL30:
    def __init__(self) -> None:
        super().__init__()

    def data_at_date(self, dt, features):
        """
        Prepare data at a given date
        """

        all_data = sorted(glob.glob("./data/processed/*.csv"))
        data = list()
        for path in all_data:
            df = pd.read_csv(path)

            # Exponentially weighted moving average
            if "ewma_market_cap" not in df.columns:
                times = pd.to_datetime(df["timestamp"].values)
                market_cap = df.copy()
                market_cap = market_cap[["market_cap"]]
                market_cap.columns = ["ewma_market_cap"]
                market_cap = market_cap.ewm(
                    halflife="3 days", times=pd.DatetimeIndex(times)
                ).mean()
                df = pd.concat([df, market_cap], axis=1)
                df.to_csv(path, index=False)

            df["timestamp"] = pd.to_datetime(df["timestamp"].values)
            df["timestamp"] = df["timestamp"].dt.date
            df = df[df["timestamp"] == pd.Timestamp(dt)]
            if len(df) > 0:
                if all(df[ft].values[0] for ft in features) > 0:
                    tmp = {
                        "name": ntpath.basename(path).replace(".csv", ""),
                    }
                    for ft in features:
                        tmp[ft] = df[ft].values[0]
                    data.append(tmp)
        return data

    def allocate(self, dt):
        """
        Calculate krypfolio based on HODL 30 algorithm
        """

        n_coins = 30
        cap = 0.08

        market = self.data_at_date(dt, ["ewma_market_cap", "close"])
        market = list(sorted(market, key=lambda alloc: -alloc["ewma_market_cap"]))[
            :n_coins
        ]  # sort by descending ratio
        total_cap = sum([np.sqrt(coin["ewma_market_cap"]) for coin in market])

        allocations = [
            {
                "symbol": coin["name"],
                "ewma_market_cap": coin["ewma_market_cap"],
                "close": coin["close"],
                "ratio": (
                    np.sqrt(coin["ewma_market_cap"]) / total_cap
                ),  # ratios (sums to 100%)
            }
            for coin in market
        ]

        for i in range(len(allocations)):
            alloc = allocations[i]

            if alloc["ratio"] > cap:
                overflow = (
                    alloc["ratio"] - cap
                )  # the amount of % that needs to be spread to the other coins
                alloc["ratio"] = cap

                remaining_allocs = allocations[i + 1 :]

                total_nested_cap = sum(
                    [n_alloc["ewma_market_cap"] for n_alloc in remaining_allocs]
                )  # market cap of the remaining coins
                new_allocs = list()

                for n_alloc in remaining_allocs:
                    cap_fraction = (
                        n_alloc["ewma_market_cap"] / total_nested_cap
                    )  # percentage of the remainder this makes up (sums to 100%)
                    n_alloc["ratio"] += overflow * cap_fraction  # weighted
                    new_allocs.append(n_alloc)

                allocations = allocations[: i + 1] + new_allocs
        return {"timestamp": dt, "allocations": allocations}

    def main(self, start):

        start = datetime.strptime(start, "%Y-%m-%d")
        today = date.today()

        intervals = list(rrule.rrule(rrule.DAILY, dtstart=start, until=today))

        allocations = list()
        # Use multiprocessing
        with Pool(multiprocessing.cpu_count() - 2) as p:
            allocations = list(
                tqdm(p.imap(self.allocate, intervals), total=len(intervals))
            )
        return sorted(allocations, key=lambda x: x["timestamp"])  # sort by timestamp


if __name__ == "__main__":
    hodl30 = HODL30()
    allocations = hodl30.main("2014-01-01")
    pickle.dump(allocations, open("hodl30.bin", "wb"))
