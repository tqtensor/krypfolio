import glob
import json
import multiprocessing
import ntpath
import traceback
from datetime import date, datetime
from multiprocessing import Pool

import numpy as np
import pandas as pd
from dateutil import rrule
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from tqdm.auto import tqdm

from config import *

# Default headers for Coinmarketcap
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
}


class HODL:
    def __init__(self, alpha, n_coins, cap):
        self.alpha = alpha
        self.n_coins = n_coins
        self.cap = cap

    def list_binance(self):
        """
        Get all the trading pairs with quote asset as USDT
        and match with CoinMarketCap
        """

        session = Session()
        session.headers.update(headers)

        try:
            pairs = session.get("https://api.binance.com/api/v3/exchangeInfo").json()
            pairs = [
                p["symbol"]
                for p in pairs["symbols"]
                if ((p["quoteAsset"] == "USDT") & (p["status"] == "TRADING"))
            ]
            bnb_coins = [c.replace("USDT", "").lower() for c in pairs]

            cmc_coins_ = session.get(
                "https://web-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
                params={
                    "aux": "circulating_supply,max_supply,total_supply",
                    "convert": "USD",
                    "cryptocurrency_type": "coins",
                    "limit": "100",
                    "sort": "market_cap",
                    "sort_dir": "desc",
                    "start": "1",
                },
            ).json()
            cmc_coins = [c["symbol"].lower() for c in cmc_coins_["data"]]
            coins = [c for c in cmc_coins if c in bnb_coins]
            tmp = dict()
            for c in cmc_coins_["data"]:
                if c["symbol"].lower() in coins:
                    tmp[c["slug"]] = c["symbol"].lower()
            return tmp
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)
            return None

    def weighted_market_cap(self):
        """
        Calculate exponential weighted moving average market cap
        """

        all_data = sorted(glob.glob("./data/processed/*.csv"))
        for path in tqdm(all_data):
            df = pd.read_csv(path)

            # Exponentially weighted moving average
            if "ewma_market_cap_{}_days".format(self.alpha) not in df.columns:
                times = pd.to_datetime(df["timestamp"].values)
                market_cap = df.copy()
                market_cap = market_cap[["market_cap"]]
                market_cap.columns = ["ewma_market_cap_{}_days".format(self.alpha)]
                market_cap = market_cap.ewm(
                    halflife="{} days".format(self.alpha), times=pd.DatetimeIndex(times)
                ).mean()
                df = pd.concat([df, market_cap], axis=1)
                df.to_csv(path, index=False)

    def data_at_date(self, dt, features):
        """
        Prepare data at a given date
        """

        all_data = sorted(glob.glob("./data/processed/*.csv"))
        data = list()
        for path in all_data:
            try:
                df = pd.read_csv(path)

                df["timestamp"] = pd.to_datetime(df["timestamp"].values)
                df["timestamp"] = df["timestamp"].dt.date
                df = df[df["timestamp"] == pd.Timestamp(dt)]
                if len(df) > 0:
                    if all(df[ft].values[0] for ft in features) > 0:
                        tmp = {"name": ntpath.basename(path).replace(".csv", "")}
                        for ft in features:
                            tmp[ft] = df[ft].values[0]
                        data.append(tmp)
            except Exception as e:
                traceback.print_tb(e.__traceback__)
                continue
        return data

    def allocate(self, dt):
        """
        Calculate krypfolio based on HODL algorithm
        """

        # # Get tradable coins on Binance exchange, which has historical data
        # coins = self.list_binance()

        market = self.data_at_date(
            dt, ["ewma_market_cap_{}_days".format(self.alpha), "close"]
        )
        # market = [m for m in market if m["name"] in coins.keys()]  # filter
        market = list(
            sorted(
                market,
                key=lambda alloc: -alloc["ewma_market_cap_{}_days".format(self.alpha)],
            )
        )[
            : self.n_coins
        ]  # sort by descending ratio
        # for m in market:
        #     m["symbol"] = coins[m["name"]]
        for m in market:
            m["symbol"] = m["name"]
        total_cap = sum(
            [
                np.sqrt(coin["ewma_market_cap_{}_days".format(self.alpha)])
                for coin in market
            ]
        )

        allocations = [
            {
                "symbol": coin["symbol"],
                "ewma_market_cap": coin["ewma_market_cap_{}_days".format(self.alpha)],
                "close": coin["close"],
                "ratio": (
                    np.sqrt(coin["ewma_market_cap_{}_days".format(self.alpha)])
                    / total_cap
                ),  # ratios (sums to 100%)
            }
            for coin in market
        ]

        for i in range(len(allocations)):
            alloc = allocations[i]

            if alloc["ratio"] > self.cap:
                overflow = (
                    alloc["ratio"] - self.cap
                )  # the amount of % that needs to be spread to the other coins
                alloc["ratio"] = self.cap

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

        # Load pre-calculated portfolio
        try:
            pre = json.load(
                open(
                    "./strategies/HODL{0}-{1}-days-{2}-cap.json".format(
                        self.n_coins, self.alpha, str(int(100 * self.cap))
                    ),
                    "r",
                )
            )
            timestamps = pre.keys()
        except:
            pre = dict()
            timestamps = list()

        # Prepare EWMA
        self.weighted_market_cap()

        # Iterate daily except in timestamps
        start = datetime.strptime(start, "%Y-%m-%d")
        today = date.today()

        intervals = list(rrule.rrule(rrule.DAILY, dtstart=start, until=today))
        intervals = [i for i in intervals if i.strftime("%Y-%m-%d") not in timestamps]

        allocations = list()
        # Use multiprocessing
        with Pool(multiprocessing.cpu_count() - 2) as p:
            allocations = list(
                tqdm(p.imap(self.allocate, intervals), total=len(intervals))
            )

        # Transform to dictionary
        allocations = sorted(
            allocations, key=lambda x: x["timestamp"]
        )  # sort by timestamp
        allocations = [alloc for alloc in allocations if len(alloc["allocations"]) > 0]
        allocations = [
            alloc
            for alloc in allocations
            if alloc["allocations"][0]["symbol"] == "bitcoin"
        ]
        for alloc in allocations:
            pre[alloc["timestamp"].strftime("%Y-%m-%d")] = alloc["allocations"]
        return pre


if __name__ == "__main__":
    hodl = HODL(alpha, n_coins, cap)
    allocations = hodl.main(start)
    json.dump(
        allocations,
        open(
            "./strategies/HODL{0}-{1}-days-{2}-cap.json".format(
                n_coins, alpha, str(int(100 * cap))
            ),
            "w",
        ),
        indent=4,
        sort_keys=True,
        default=str,
    )
