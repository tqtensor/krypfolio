import json
import os
import time
from datetime import date, datetime, timedelta
from numpy.lib.shape_base import dsplit

import pandas as pd
from dateutil import rrule
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects


def get(url, parameters, headers):
    """
    API get utility
    """

    session = Session()
    session.headers.update(headers)

    try:
        data = session.get(url, params=parameters).json()
        return data
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
        return None


def writer(data, base, features, values):
    """
    Iterative create features for dict
    """

    for ft, v in zip(features, values):
        data[base + "_" + ft] = [v]
    return data


def market_info():
    """
    1. Get top 100 coins from Coinmarketcap
    3. Iterate back in time to get market info
    """

    # Default headers for Coinmarketcap
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    }

    # Top 100 coins by market cap
    top_coins = get(
        "https://web-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
        parameters= {
        "aux": "circulating_supply,max_supply,total_supply",
        "convert": "USD",
        "cryptocurrency_type": "coins",
        "limit": "100",
        "sort": "market_cap",
        "sort_dir": "desc",
        "start": "1",
    },
        headers=headers,
    )

    top_coins = [x["slug"] for x in top_coins["data"]]

    if not os.path.exists("./data/raw"):
        os.mkdir("./data/raw")

    # Get single coin market info data
    today = date.today()
    start = today + timedelta(weeks=-52 * 5)

    monthly_list = list(reversed(
        list(rrule.rrule(rrule.WEEKLY, dtstart=start, until=today))
    ))
    n = 8
    for i in range(0, len(monthly_list) - n, n):
        print
        print(
            monthly_list[i].timestamp(),
            (monthly_list[i + n] + timedelta(days=1)).timestamp(),
        )
        data = get(
        "https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical",
        parameters={
            "convert": "USD",
            "slug": "",
            "time_end": str(int()),
            "time_start": str(int())
        },
        headers=headers,
    )
    #     if dt_ not in ready_scraped:
    #         print(
    #             "Downloading market info data at {0} for {1} coins".format(
    #                 dt_, len(filtered_cg_coins)
    #             )
    #         )

    #         tmp_cg_coins = list()
    #         df = dict()
    #         df["timestamp"] = dt_

    #         for cg_coin_id in filtered_cg_coins:
    #             market_cap = get(
    #                 f"https://api.coingecko.com/api/v3/coins/{cg_coin_id}/history",
    #                 parameters={"date": dt_, "localization": "false"},
    #                 headers={},
    #             )

    #             try:
    #                 mk = market_cap["market_data"]
    #                 if mk["market_cap"]["usd"] > 0:
    #                     writer(
    #                         df,
    #                         cg_coin_id,
    #                         features,
    #                         [
    #                             mk["current_price"]["usd"],
    #                             mk["market_cap"]["usd"],
    #                             mk["total_volume"]["usd"],
    #                         ],
    #                     )
    #                     tmp_cg_coins.append(cg_coin_id)
    #                 else:
    #                     writer(
    #                         df,
    #                         cg_coin_id,
    #                         features,
    #                         [
    #                             None,
    #                             None,
    #                             None,
    #                         ],
    #                     )
    #             except:
    #                 writer(
    #                     df,
    #                     cg_coin_id,
    #                     features,
    #                     [
    #                         None,
    #                         None,
    #                         None,
    #                     ],
    #                 )
    #             time.sleep(1)
    #         filtered_cg_coins = tmp_cg_coins.copy()

    #         # Backfill missing coins with None
    #         for cg_coin_id in list(set(cg_coins) - set(filtered_cg_coins)):
    #             df[cg_coin_id] = None

    #         df = pd.DataFrame.from_dict(df)
    #         dfs.append(df)

    #         cache = dfs.copy()  # save during iteration
    #         cache = pd.concat(cache)
    #         cache.to_csv("./data/market_info.csv", index=False)

    # dfs = pd.concat(dfs)
    # dfs.to_csv("./data/market_info.csv", index=False)


if __name__ == "__main__":
    market_info()

    print("Done")
