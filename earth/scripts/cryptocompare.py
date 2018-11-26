import datetime
import json

import venus
import venus.objects
import venus.utils

venus.setup()


def force_int(value, default=None):
    try:
        return int(value)
    except:
        return default


class Spider:
    def __init__(self):
        self.use_cache = False
        self.name = "cryptocompare"

    @property
    def table_name(self):
        return "stock." + self.name

    def crawl(self):
        self.use_cache = True
        return self._collect(["minute", "hour", "day"])

    def update(self):
        self.use_cache = False
        return self._collect(["minute"])

    def _collect(self, interval_list):
        coins = self._symbol_list()

        for interval in interval_list:
            ticks_data = self._fetch_ticks(coins, interval)
            venus.qb.save_data(ticks_data, self.table_name, debug=True)

        self._remove_duplicates()

    def _symbol_list(self, max_order=10000):
        response = venus.objects.Request(
            url="https://www.cryptocompare.com/api/data/coinlist/",
            use_cache=self.use_cache
        ).make()
        data = json.loads(response.output)["Data"]

        result = []

        for item in data.values():
            sort_order = force_int(item.get("SortOrder"), max_order + 1)

            if sort_order > max_order:
                continue

            result.append({
                "icon_url": "https://www.cryptocompare.com" + item.get("ImageUrl", ""),
                "short_code": item.get("Symbol"),
                "full_name": item.get("CoinName"),
                "total_coin_supply": force_int(item.get("TotalCoinSupply"), 0),
                "category": "crypto",
            })

        return result

    def _fetch_ticks(self, coins, interval):
        currency = "USD"

        url_template = "https://min-api.cryptocompare.com/data/histo" + interval
        url_template += "?fsym=" + "{short_code}"
        url_template += "&tsym=" + currency
        url_template += "&limit=" + "2000"
        url_template += "&aggregate=" + "3"
        url_template += "&e=" + "CCCAGG"

        pool = [venus.objects.Request(
            url=url_template.format(short_code=item["short_code"]),
            use_cache=self.use_cache
        ) for item in coins]

        responses = venus.objects.RequestPool(pool=pool, name=interval).make_async()

        result = []

        for coin, response in zip(coins, responses):
            raw_data = json.loads(response.output)["Data"]

            tick_data = [venus.utils.merge_dicts(coin, {
                "event_at": datetime.datetime.fromtimestamp(int(tick["time"])),
                "current_value": tick["high"],
                "current_volume": tick["volumeto"],
                "currency": currency
            }) for tick in raw_data]

            result.extend(tick_data)

        return result

    def _remove_duplicates(self):
        venus.db.run_query("""
            DELETE FROM {table_name} T1
            USING {table_name} T2
            WHERE T1.ctid < T2.ctid 
              AND T1.full_name = T2.full_name 
              AND T1.event_at = T2.event_at;
        """.format(table_name=self.table_name))


if __name__ == '__main__':
    Spider().crawl()
