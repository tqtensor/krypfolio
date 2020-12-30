import glob
import multiprocessing
import ntpath
import pickle
import traceback
from datetime import date, datetime
from multiprocessing import Pool

import numpy as np
import pandas as pd
from dateutil import rrule
from tqdm.auto import tqdm

# Halflife
ALPHA = 3


class CCi30:
    def __init__(self) -> None:
        super().__init__()

    def data_at_date(self, dt, features):
        """
        Prepare data at a given date
        """

        all_data = sorted(glob.glob("./data/processed/*.csv"))
        data = list()
        for path in all_data:
            try:
                df = pd.read_csv(path)

                # Exponentially weighted moving average
                if f"ewma_market_cap_{ALPHA}_days" not in df.columns:
                    times = pd.to_datetime(df["timestamp"].values)
                    market_cap = df.copy()
                    market_cap = market_cap[["market_cap"]]
                    market_cap.columns = [f"ewma_market_cap_{ALPHA}_days"]
                    market_cap = market_cap.ewm(
                        halflife=f"{ALPHA} days", times=pd.DatetimeIndex(times)
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
            except Exception as e:
                print(e)
                traceback.print_tb(e.__traceback__)
                continue
        return data

    def allocate(self, dt):
        """
        Calculate krypfolio based on HODL 30 algorithm
        """

        n_coins = 30

        market = self.data_at_date(dt, [f"ewma_market_cap_{ALPHA}_days", "close"])
        market = list(
            sorted(market, key=lambda alloc: -alloc[f"ewma_market_cap_{ALPHA}_days"])
        )[
            :n_coins
        ]  # sort by descending ratio
        total_cap = sum(
            [np.sqrt(coin[f"ewma_market_cap_{ALPHA}_days"]) for coin in market]
        )

        allocations = [
            {
                "symbol": coin["name"],
                "ewma_market_cap": coin[f"ewma_market_cap_{ALPHA}_days"],
                "close": coin["close"],
                "ratio": (
                    np.sqrt(coin[f"ewma_market_cap_{ALPHA}_days"]) / total_cap
                ),  # ratios (sums to 100%)
            }
            for coin in market
        ]
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
    cci30 = CCi30()
    allocations = cci30.main("2014-01-01")
    pickle.dump(allocations, open(f"cci30_{ALPHA}_days.bin", "wb"))
