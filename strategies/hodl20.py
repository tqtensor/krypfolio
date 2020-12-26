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

    def data_at_date(self, dt, feature):
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
                if df[feature].values[0] > 0:
                    data.append(
                        {
                            "name": ntpath.basename(path).replace(".csv", ""),
                            feature: df[feature].values[0],
                        }
                    )
        return data

    def allocate(self, dt):
        """
        Calculate krypfolio based on HODL 20 algorithm
        """

        n_coins = 20
        cap = 0.10

        market_cap = self.data_at_date(dt, "market_cap")
        close_price = self.data_at_date(dt, "close")
        market = list()
        for x in market_cap:
            for y in close_price:
                if x["name"] == y["name"]:
                    x["price"] = y["close"]
                    market.append(x)  # include close price
        market = list(sorted(market, key=lambda alloc: -alloc["market_cap"]))[
            :n_coins
        ]  # sort by descending ratio
        total_cap = sum([coin["market_cap"] for coin in market])

        allocations = [
            {
                "symbol": coin["name"],
                "market_cap": coin["market_cap"],
                "price": coin["price"],
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

    def main(self, start, period):

        start = datetime.strptime(start, "%Y-%m-%d")
        today = date.today()

        intervals = list(rrule.rrule(rrule.WEEKLY, dtstart=start, until=today))
        intervals = [intervals[i] for i in range(len(intervals)) if i % period == 0]

        allocations = list()
        # Use multiprocessing
        with Pool(multiprocessing.cpu_count() - 2) as p:
            allocations = list(
                tqdm(p.imap(self.allocate, intervals), total=len(intervals))
            )
        return sorted(allocations, key=lambda x: x["timestamp"])  # sort by timestamp


if __name__ == "__main__":
    hodl20 = HODL20()
    allocations = hodl20.main("2020-06-01", 4)
