import datetime
import json
from collections import defaultdict

from tqdm import tqdm

import venus
import venus.objects
import venus.utils

venus.setup()


class CoinMarketCap:
    def __init__(self):
        pass

    def run(self):
        self.collect()

    def collect(self):
        currencies = self.list_currencies()[:100]
        responses = self.get_currencies(currencies)

        data = self.extract_currencies(responses)

        self.save_data(data)
        self.remove_duplicates()

    def list_currencies(self):
        url = "https://s2.coinmarketcap.com/generated/search/quick_search.json"
        response = venus.objects.Request(url=url).make()
        data = json.loads(response.output)

        return [{
            "full_name": item["name"],
            "short_code": item["symbol"],
            "slug": item["slug"],
        } for item in data]

    def get_currencies(self, currencies):
        hour = 60 * 60 * 1000
        day = 24 * hour
        month = 30 * day
        year = 365 * day

        intervals = [1 * day, 7 * day, 1 * month, 3 * month, 1 * year, 10 * year]
        current_time = venus.utils.time_in_seconds(datetime.datetime.now()) * 1000

        base_url = "https://graphs2.coinmarketcap.com/currencies"

        pool = []

        for currency in currencies:
            for interval in intervals:
                pool.append(venus.objects.Request(
                    url="{base_url}/{currency}/{start_time}/{finish_time}/".format(
                        base_url=base_url, currency=currency["slug"],
                        start_time=current_time - interval, finish_time=current_time
                    ), extra=currency, use_cache=False, use_proxy=True
                ))

        return venus.objects.RequestPool(pool=pool, name="currencies").make_async_full()

    def extract_currencies(self, responses):
        result = []

        for response in tqdm(responses, desc="extract"):
            try:
                data = json.loads(response.output)
            except:
                continue

            currency = response.extra

            rows = defaultdict(dict)

            for key, values in data.items():
                for ts, value in values:
                    rows[ts][key] = value
                    rows[ts]["event_at"] = datetime.datetime.fromtimestamp(ts / 1e3)
                    rows[ts]["full_name"] = currency["full_name"]
                    rows[ts]["short_code"] = currency["short_code"]

            result.extend(list(rows.values()))

        return result

    def save_data(self, data):
        venus.qb.save_data(data, "stock.coinmarketcap", debug=True, do_single=True)

    def remove_duplicates(self):
        venus.db.run_query("""
            DELETE FROM {table_name} T1
            USING {table_name} T2
            WHERE T1.ctid < T2.ctid 
              AND T1.event_at = T2.event_at 
              AND T1.short_code = T2.short_code;
        """.format(table_name="stock.coinmarketcap"))


if __name__ == '__main__':
    CoinMarketCap().run()
