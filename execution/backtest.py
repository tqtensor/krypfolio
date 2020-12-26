import sys

import numpy as np

sys.path.insert(0, "./strategies")
from hodl20 import HODL20


def balance(portfolio):
    """
    Calculate balance of the portfolio
    """

    return sum([alloc["price"] * alloc["amount"] for alloc in portfolio["allocations"]])


def rebalance(portfolio, allocation, investment):
    """
    Distribute the investment based on each coin's ratio
    """

    # Update price of coins in the portfolio
    for x in portfolio["allocations"]:
        for y in allocation["allocations"]:
            if x["symbol"] == y["symbol"]:
                x["price"] = y["price"]
    balance_ = balance(portfolio)
    print("Current porfolio's balance", round(balance_, 0))
    print("Current price of Bitcoin", round(allocation["allocations"][0]["price"], 0))
    if balance_ == 0:
        balance_ = investment

    for alloc in allocation["allocations"]:
        alloc["amount"] = alloc["ratio"] * balance_ / alloc["price"]
    return allocation


def main():

    # Initial invesment
    investment = 1000

    # Portfolios should follow the same structure
    # List(Dict(symbol, price, ratio, market_cap, amount))
    strategy = HODL20()
    allocations = strategy.main("2015-01-01", 4)
    krypfolio = allocations[0]
    for alloc in krypfolio["allocations"]:
        alloc["amount"] = 0  # init amount

    # Rebalance the portfolio
    for alloc in allocations:
        try:
            total_ratio = sum([x["ratio"] for x in alloc["allocations"]])
            if np.abs(total_ratio - 1) > 0.001:  # check the validity of an allocation
                print("You need to check the allocation strategy")
            else:
                print("*********************************")
                print("Rebalance at", alloc["timestamp"])
                krypfolio = rebalance(krypfolio, alloc, investment)
        except:
            pass


if __name__ == "__main__":
    main()
