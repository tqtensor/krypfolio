import glob
import json
import os
import time
from datetime import date, datetime, timedelta
from multiprocessing.dummy import Pool as ThreadPool
from random import randrange

import pandas as pd
from dateutil import rrule
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from tqdm.auto import tqdm

# Default headers for Coinmarketcap
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
}


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


def download(path):
    """
    Data downloading utility, will be used in multithread
    """

    # Parse coin, start, end from path
    coin, start, end = path.replace("./data/raw/", "").replace(".json", "").split("_")

    data = get(
        "https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical",
        parameters={
            "convert": "USD",
            "slug": coin,
            "time_end": str(int(end)),
            "time_start": str(int(start)),
        },
        headers=headers,
    )

    if data["status"]["error_code"] == 0:
        json.dump(
            data,
            open(
                "./data/raw/{0}_{1}_{2}.json".format(coin, int(start), int(end)), "w",
            ),
            indent=4,
            sort_keys=True,
            default=str,
        )
    else:
        print(data["status"])

    time.sleep(randrange(10))


def market_info():
    """
    1. Get top 150 coins from Coinmarketcap
    2. Iterate back in time to get market info
    """

    # Top 150 coins by market cap
    top_coins = get(
        "https://web-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
        parameters={
            "aux": "circulating_supply,max_supply,total_supply",
            "convert": "USD",
            "cryptocurrency_type": "coins",
            "limit": "150",
            "sort": "market_cap",
            "sort_dir": "desc",
            "start": "1",
        },
        headers=headers,
    )

    top_coins = [x["slug"] for x in top_coins["data"]]

    if not os.path.exists("./data/raw"):
        os.mkdir("./data/raw")
    if not os.path.exists("./data/processed"):
        os.mkdir("./data/processed")

    # Prepare the list of coins to download
    end = datetime(day=1, month=date.today().month, year=date.today().year)
    start = end + timedelta(weeks=-52 * 6)
    start = datetime(day=1, month=start.month, year=start.year)

    monthly_list = list(
        reversed(list(rrule.rrule(rrule.MONTHLY, dtstart=start, until=end)))
    )
    n = 1  # monthly
    all_data = list()
    for i in range(0, len(monthly_list) - n, n):
        time_end = int(monthly_list[i].timestamp())
        time_start = int((monthly_list[i + n] + timedelta(days=1)).timestamp())
        for coin in top_coins:
            all_data.append(
                "./data/raw/{0}_{1}_{2}.json".format(coin, time_start, time_end)
            )

    existing_data = glob.glob("./data/raw/*.json")
    to_download = list(set(all_data) - set(existing_data))

    # Use multithread download
    with ThreadPool(32) as p:
        _ = list(tqdm(p.imap(download, to_download), total=len(to_download)))

    # Consolidate data
    existing_data = glob.glob("./data/raw/*.json")

    for coin in top_coins:
        to_consolidate = [
            x
            for x in existing_data
            if coin == x.replace("./data/raw/", "").replace(".json", "").split("_")[0]
        ]
        data = list()
        for path in to_consolidate:
            content = json.load(open(path))
            content = [x["quote"]["USD"] for x in content["data"]["quotes"]]
            data.append(content)
        data = sum(data, [])
        data = pd.DataFrame(data)
        data.sort_values(by="timestamp", inplace=True)
        data.to_csv(f"./data/processed/{coin}.csv", index=False)


if __name__ == "__main__":
    market_info()
