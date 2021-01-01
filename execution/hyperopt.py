import itertools
import warnings

import numpy as np
import pandas as pd
import quantstats as qs
from backtest import Krypfolio
from tqdm.auto import tqdm

warnings.filterwarnings("ignore")


def analysis(path, mode):
    """
    Utility to calculate Sharpe and Sortio ratio / full report
    """

    returns = pd.read_csv(path)
    returns["timestamp"] = pd.to_datetime(returns["timestamp"])
    returns.columns = ["Date", "Return"]
    returns.set_index("Date", inplace=True)
    returns = returns.iloc[:, 0].pct_change()

    if mode == "stats":
        return qs.stats.sortino(returns)
    elif mode == "report":
        qs.reports.html(
            returns,
            "BTC-USD",
            output=path.replace("csv", "html").replace("/results", ""),
        )


if __name__ == "__main__":
    # Grid search for best hyper-parameters
    _strategy = ["hodl30-3-days"]
    _start = ["2015-01-01"]
    _loss = [round(l, 2) for l in np.arange(0.05, 0.36, 0.01)]
    _r = np.arange(1, 7, 1)

    args = [_strategy, _start, _loss, _r]

    # Instantiate Krypfolio backtest class
    krypfolio = Krypfolio(debug=False)

    stats = list()
    for arg in tqdm(list(itertools.product(*args))):
        krypfolio.main(
            strategy=arg[0], start=arg[1], loss=arg[2], r=arg[3],
        )
        path = "./execution/results/{0}_{1}_{2}_{3}.csv".format(
            arg[0], arg[1], arg[2], arg[3]
        )
        stats.append([path, analysis(path, "stats")])

    # Create full report for the best hyper-parameters with strategy
    best = max(stats, key=lambda x: x[1])[0]
    analysis(best, "report")
