import glob
import multiprocessing
import ntpath
from datetime import date, datetime
from multiprocessing import Pool

import pandas as pd
from dateutil import rrule
from tqdm.auto import tqdm


class HODL20:
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

    def wk_allocate(self, dt):
        """
        Calculate krypfolio based on HODL 20 algorithm
        """

        n_coins = 20
        cap = 0.10

        market = self.data_at_date(dt, ["market_cap", "close"])
        market = list(sorted(market, key=lambda alloc: -alloc["market_cap"]))[
            :n_coins
        ]  # sort by descending ratio
        total_cap = sum([coin["market_cap"] for coin in market])

        allocations = [
            {
                "symbol": coin["name"],
                "market_cap": coin["market_cap"],
                "close": coin["close"],
                "ratio": (coin["market_cap"] / total_cap),  # ratios (sums to 100%)
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
        return {"timestamp": dt, "allocations": allocations}

    def dl_allocate(self, dt):

        market = self.data_at_date(dt, ["market_cap", "close"])
        market = list(
            sorted(market, key=lambda alloc: -alloc["market_cap"])
        )  # sort by descending ratio

        allocations = [
            {
                "symbol": coin["name"],
                "market_cap": coin["market_cap"],
                "close": coin["close"],
            }
            for coin in market
        ]
        return {"timestamp": dt, "allocations": allocations}

    def main(self, start, period):

        start = datetime.strptime(start, "%Y-%m-%d")
        today = date.today()

        wk_intervals = list(rrule.rrule(rrule.WEEKLY, dtstart=start, until=today))
        wk_intervals = [
            wk_intervals[i] for i in range(len(wk_intervals)) if i % period == 0
        ]
        dl_intervals = list(rrule.rrule(rrule.DAILY, dtstart=start, until=today))
        dl_intervals = [x for x in dl_intervals if x not in wk_intervals]

        wk_allocations = list()
        dl_allocations = list()
        # Use multiprocessing
        with Pool(multiprocessing.cpu_count() - 2) as p:
            wk_allocations = list(
                tqdm(p.imap(self.wk_allocate, wk_intervals), total=len(wk_intervals))
            )
        with Pool(multiprocessing.cpu_count() - 2) as p:
            dl_allocations = list(
                tqdm(p.imap(self.dl_allocate, dl_intervals), total=len(dl_intervals))
            )
        allocations = wk_allocations + dl_allocations
        return sorted(allocations, key=lambda x: x["timestamp"])  # sort by timestamp


if __name__ == "__main__":
    hodl20 = HODL20()
    allocations = hodl20.main("2020-06-01", 4)
