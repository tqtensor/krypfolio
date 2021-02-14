# Krypfolio

## A tunable crytocurrency index for you to experiment

### Guide:

1. Run `python data/vendor.py` to download the market capitalization data.
2. There several settings that you can tune in the HODL algorithm to generate the weight of each coin in the porfolio.

- Alpha: the half-life factor in the calculation of exponential weighted moving average of the market capitalization.
- Number of coins in the porfolio.
- Cap (limit) of the weights in the porfolio, for example, if based on the market capitalization Bitcoin would have the weight of 26% but the cap was set at 8% then Bitcoin would hold only 8% of the whole portfolio.

> > Set the parameters in `strategies\hodl.py` and run it.

3. Run `python execution\hyperopt.py` to find the best stop-loss and rebalance cycle setting. It will also generate a Tear sheet for you based on the best settings.
4. Run `python execution\backtest.py` to view the details of each rebalance event.
