import pickle
import sys

import numpy as np

sys.path.insert(0, "./strategies")
from hodl20 import HODL20


def balance(portfolio):
    """
    Calculate balance of the portfolio
    """

    return sum([alloc["close"] * alloc["amount"] for alloc in portfolio["allocations"]])


def price(portfolio):
    """
    Calculate price of the portfolio
    """

    return sum([alloc["close"] * alloc["ratio"] for alloc in portfolio["allocations"]])


def update_price(portfolio, allocation):
    """
    Utility to update the latest prices for portfolio
    """

    # Update price of coins in the portfolio
    for x in portfolio["allocations"]:
        for y in allocation["allocations"]:
            if x["symbol"] == y["symbol"]:
                x["close"] = y["close"]
    return portfolio


def rebalance(portfolio, prices, allocation, investment):
    """
    Distribute the investment based on each coin's ratio
    """

    # Update price of coins in the portfolio
    portfolio = update_price(portfolio, allocation)
    balance_ = balance(portfolio)
    price_ = price(allocation)
    print("Current porfolio's balance", round(balance_, 0))
    print("Current price of Bitcoin", round(allocation["allocations"][0]["close"], 0))

    # Inject investment in three stages
    fund = 0
    injection = None
    if investment > 0:
        try:
            if (price_ > prices[-1]) and (price_ > prices[-2]):
                fund = investment
                injection = "Third"
            if (price_ > prices[-1]) and (price_ <= prices[-2]):
                fund = 0.25 * investment
                injection = "Second"
        except:
            pass
        if balance_ == 0:
            fund = 0.20 * investment
            injection = "First"

    balance_ += fund

    for alloc in allocation["allocations"]:
        alloc["amount"] = alloc["ratio"] * balance_ / alloc["close"]
    print(
        f"{injection} investment injection",
        round(fund, 1),
        "left-over investment",
        round(investment - fund, 1),
    )
    return allocation, investment - fund


def main():

    # Initial invesment
    investment = 1000
    init_investment = investment

    # Trailing stop loss
    loss = 0.20

    # Use pre allocated allocations or not
    use_preallocated = True

    # Portfolios should follow the same structure
    # List(Dict(symbol, price, ratio, market_cap, amount))
    if use_preallocated:
        allocations = pickle.load(open("allocations.bin", "rb"))
    else:
        strategy = HODL20()
        allocations = strategy.main("2015-01-01", 4)
        allocations = [alloc for alloc in allocations if len(alloc["allocations"]) > 0]
        pickle.dump(allocations, open("allocations.bin", "wb"))

    krypfolio = allocations[0]
    for alloc in krypfolio["allocations"]:
        alloc["amount"] = 0  # init amount

    # Rebalance the portfolio
    start_btc = None
    start_date = None
    balance_ = None
    end_balance_ = None
    max_balance = -np.inf
    prices = list()
    for alloc in allocations:
        try:
            total_ratio = sum([x["ratio"] for x in alloc["allocations"]])
            if np.abs(total_ratio - 1) > 0.001:  # check the validity of an allocation
                print("You need to check the allocation strategy")
            else:
                print("*********************************")
                print("Rebalance at", alloc["timestamp"])
                krypfolio, investment = rebalance(krypfolio, prices, alloc, investment)
                balance_ = balance(krypfolio)
                price_ = price(krypfolio)
                prices.append(price_)
                if balance_ > max_balance:
                    max_balance = balance_
                if ((max_balance - balance_) / max_balance > loss) and (balance_ != 0):
                    # Reset the portfolio
                    print("STOP LOSS")
                    for alloc_ in krypfolio["allocations"]:
                        alloc_["amount"] = 0
                    investment += balance_
                    max_balance = -np.inf
                if not start_btc:
                    start_btc = alloc["allocations"][0]["close"]
                    start_date = alloc["timestamp"]
        except:  # daily alloc, no ratio was calculated
            krypfolio = update_price(krypfolio, alloc)
            balance_ = balance(krypfolio)
            if balance_ > max_balance:
                max_balance = balance_
            if ((max_balance - balance_) / max_balance > loss) and (balance_ != 0):
                # Reset the portfolio
                print("*********************************")
                print(
                    "Balance at {0}: {1}".format(alloc["timestamp"], round(balance_, 0))
                )
                print("STOP LOSS")
                for alloc_ in krypfolio["allocations"]:
                    alloc_["amount"] = 0
                investment += balance_
                max_balance = -np.inf

    end_btc = allocations[-1]["allocations"][0]["close"]
    end_balance_ = investment + balance_
    print("*********************************")
    print("REPORT")
    print("Start date:", start_date)
    print("Bitcoin: {}x".format(round(end_btc / start_btc, 1)))
    print("HODL 20: {}x".format(round(end_balance_ / init_investment, 1)))


if __name__ == "__main__":
    main()

    print("Done")
