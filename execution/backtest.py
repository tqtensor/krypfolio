import json
import os
import pickle
import sys
from datetime import date, datetime

import numpy as np
import pandas as pd
from dateutil import rrule

from config import *

sys.path.insert(0, "./strategies")


class Krypfolio:
    def __init__(self, debug=True) -> None:
        super().__init__()
        self.debug = debug

    def _print(self, msg):
        if self.debug:
            print(msg)
        else:
            pass

    def balance(self, portfolio):
        """
        Calculate balance of the portfolio
        """

        return sum(
            [alloc["close"] * alloc["amount"] for alloc in portfolio["allocations"]]
        )

    def price(self, portfolio):
        """
        Calculate price of the portfolio
        """

        return sum(
            [alloc["close"] * alloc["ratio"] for alloc in portfolio["allocations"]]
        )

    def update_price(self, portfolio, allocation):
        """
        Utility to update the latest prices for portfolio
        """

        # Update price of coins in the portfolio
        for x in portfolio["allocations"]:
            for y in allocation["allocations"]:
                if x["symbol"] == y["symbol"]:
                    x["close"] = y["close"]
        return portfolio

    def rebalance(self, portfolio, prices, allocation, investment):
        """
        Distribute the investment based on each coin's ratio
        """

        # Update price of coins in the portfolio
        portfolio = self.update_price(portfolio, allocation)
        balance_ = self.balance(portfolio)
        price_ = self.price(allocation)
        self._print(
            "Current price of Bitcoin: {}".format(
                int(allocation["allocations"][0]["close"])
            )
        )
        self._print("Current portfolio's balance: {}".format(int(balance_)))

        # Inject investment in three stages
        fund = 0
        injection = None
        if investment > 0:
            if len(prices) >= 3:
                a, b, c = prices[-2], prices[-1], price_
                if a <= b and b <= c:
                    fund = investment
                    injection = "Third"
                if (a <= b and b >= c and a <= c) or (a >= b and b <= c and a <= c):
                    fund = 0.25 * investment
                    injection = "Second"
                if a >= b and b <= c and a >= c:
                    fund = 0.20 * investment
                    injection = "First"
                if (a >= b and b >= c) or (a <= b and b >= c and a >= c):
                    fund = 0
                    injection = None
            elif len(prices) == 2:
                a, b = prices[-1], price_
                if a <= b:
                    fund = 0.25 * investment
                    injection = "Second"
                else:
                    fund = 0
                    injection = None
            else:
                fund = 0.20 * investment
                injection = "First"

            if balance_ == 0:
                fund = 0.20 * investment
                injection = "First"

        balance_ += fund

        for alloc in allocation["allocations"]:
            alloc["amount"] = alloc["ratio"] * balance_ / alloc["close"]
        self._print(
            "{0} investment injection: {1} - leftover investment: {2}".format(
                injection, int(fund), int(investment - fund)
            )
        )
        return allocation, investment - fund

    def main(self, strategy, loss, r, start):
        """
        Args:
            strategy: strategy name
            loss: trailing loss percentage
            r: rebalance period in week
            start: start date
        """

        # Strategy name
        strategy = strategy

        # Initial invesment
        investment = 1000
        init_investment = investment

        # Trailing stop loss
        loss = loss

        # Rebalance period in weeks
        r = r

        # Start date
        start = start
        start = datetime.strptime(start, "%Y-%m-%d")
        today = date.today()

        intervals = list(rrule.rrule(rrule.WEEKLY, dtstart=start, until=today))
        intervals = [
            intervals[i] for i in range(len(intervals)) if i % r == 0
        ]  # relance after each r weeks

        # Portfolios should follow the same structure
        # List(Dict(symbol, price, ratio, market_cap, amount))
        allocations = json.load(open(f"./strategies/{strategy}.json", "r"))
        allocations = [
            {
                "timestamp": datetime.strptime(k, "%Y-%m-%d"),
                "allocations": allocations[k],
            }
            for k in allocations.keys()
        ]
        allocations = [
            alloc
            for alloc in allocations
            if "bitcoin" in [x["symbol"] for x in alloc["allocations"]]
        ]  # bitcoin must be in valid allocation

        krypfolio = allocations[0]
        for alloc in krypfolio["allocations"]:
            alloc["amount"] = 0  # init amount

        # Prepare the folder for results
        if not os.path.exists("./execution/results"):
            os.mkdir("./execution/results")

        # Rebalance the portfolio
        start_btc = None
        start_date = None
        balance_ = None
        end_balance_ = None
        max_balance = -np.inf
        prices = list()
        kf_fund = list()
        kf_allocation = dict()
        for alloc in allocations:
            if alloc["timestamp"] in intervals:
                total_ratio = sum([x["ratio"] for x in alloc["allocations"]])
                if (
                    np.abs(total_ratio - 1) > 0.001
                ):  # check the validity of an allocation
                    self._print("You need to check the allocation strategy")
                else:
                    self._print("*********************************")
                    self._print("Rebalance at {}".format(alloc["timestamp"]))
                    krypfolio, investment = self.rebalance(
                        krypfolio, prices, alloc, investment
                    )
                    balance_ = self.balance(krypfolio)
                    self._print(
                        "Current total fund: {}".format(int(balance_ + investment))
                    )
                    kf_fund.append([alloc["timestamp"], balance_ + investment])
                    kf_allocation[alloc["timestamp"].strftime("%Y-%m-%d")] = krypfolio[
                        "allocations"
                    ]
                    price_ = self.price(krypfolio)
                    prices.append(price_)
                    if balance_ > max_balance:
                        max_balance = balance_
                    if ((max_balance - balance_) / max_balance > loss) and (
                        balance_ != 0
                    ):
                        # Reset the portfolio
                        self._print("STOP LOSS")
                        for alloc_ in krypfolio["allocations"]:
                            alloc_["amount"] = 0
                        investment += balance_
                        max_balance = -np.inf
                    if not start_btc:
                        start_btc = [
                            x["close"]
                            for x in alloc["allocations"]
                            if x["symbol"] == "bitcoin"
                        ][0]
                        start_date = alloc["timestamp"]
            else:  # daily alloc, no ratio was calculated
                krypfolio = self.update_price(krypfolio, alloc)
                balance_ = self.balance(krypfolio)
                kf_fund.append([alloc["timestamp"], balance_ + investment])
                kf_allocation[alloc["timestamp"].strftime("%Y-%m-%d")] = krypfolio[
                    "allocations"
                ]
                if balance_ > max_balance:
                    max_balance = balance_ + 0.001
                if ((max_balance - balance_) / max_balance > loss) and (balance_ != 0):
                    # Reset the portfolio
                    self._print("*********************************")
                    self._print("STOP LOSS at {}".format(alloc["timestamp"]))
                    self._print("Current portfolio's balance {}".format(int(balance_)))
                    self._print(
                        "Current loss {}".format(
                            round((max_balance - balance_) / max_balance, 3)
                        )
                    )
                    for alloc_ in krypfolio["allocations"]:
                        alloc_["amount"] = 0
                    investment += balance_
                    max_balance = -np.inf

        end_date = allocations[-1]["timestamp"]
        end_btc = [
            x["close"]
            for x in allocations[-1]["allocations"]
            if x["symbol"] == "bitcoin"
        ][0]
        end_balance_ = investment + balance_
        self._print("*********************************")
        self._print("REPORT")
        self._print("Start date: {}".format(start_date))
        self._print("End date: {}".format(end_date))
        self._print("Bitcoin: {}x".format(round(end_btc / start_btc, 1)))
        self._print("Krypfolio: {}x".format(round(end_balance_ / init_investment, 1)))
        self._print("*********************************")

        # Write Krypfolio daily results to csv
        df = pd.DataFrame(kf_fund, columns=["timestamp", "value"])
        df.to_csv(
            "./execution/results/{0}_{1}_{2}_{3}.csv".format(
                strategy, start.strftime("%Y-%m-%d"), loss, r
            ),
            index=False,
        )

        if self.debug:
            # Write Krypfolio daily allocations to json
            json.dump(
                kf_allocation,
                open(
                    "./execution/results/{0}_{1}_{2}_{3}.json".format(
                        strategy, start.strftime("%Y-%m-%d"), loss, r
                    ),
                    "w",
                ),
                indent=4,
                sort_keys=True,
                default=str,
            )


if __name__ == "__main__":
    krypfolio = Krypfolio(debug=True)
    krypfolio.main(
        strategy="HODL{0}-{1}-days-{2}-cap".format(n_coins, alpha, str(int(100 * cap))),
        loss=loss,
        r=r,
        start=start,
    )
