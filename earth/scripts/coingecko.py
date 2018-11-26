import datetime
import json

import venus
import venus.objects
import venus.utils

venus.setup()


class CoinGecko:
    def __init__(self):
        pass

    def run(self):
        self.collect()

    def collect(self):
        self.collect_coin_data()
        self.collect_coin_github()

    def list_coins(self, limit=500):
        response = venus.objects.Request(url="https://www.livecoinwatch.com/api/allCoins").make()

        return json.loads(response.output)[:limit]

    #############################################

    def collect_coin_data(self):
        pool = []
        intervals = ["today", "week", "month", "quarter", "year", "allTime"]
        all_coins = self.list_coins()

        for coin in all_coins:
            for interval in intervals:
                pool.append(venus.objects.Request(
                    url="https://www.livecoinwatch.com/hist/" + interval,
                    method=venus.objects.Request.METHOD_POST,
                    use_cache=True,
                    use_proxy=True,
                    payload={
                        "code": coin["code"]
                    },
                    http_headers={
                        "X-Requested-With": "XMLHttpRequest"
                    }
                ))

        responses = venus.objects.RequestPool(pool=pool, name="data").make_async_full()
        data = [self.extract_coin_data(response) for response in responses if response]
        data = venus.utils.flatten_array(data)
        venus.qb.save_data(data, 'misc.coingecko_data', debug=True)

        return data

    def extract_coin_data(self, response):
        try:
            data = json.loads(response.output)
        except:
            return None

        for item in data:
            date_field = int(int(item["date"]) / 1000)
            item["date"] = datetime.datetime.fromtimestamp(date_field)

        return data

    #############################################

    def collect_coin_github(self):
        pool = []
        all_coins = self.list_coins()

        for coin in all_coins:
            pool.append(venus.objects.Request(
                url="https://www.livecoinwatch.com/github/stats",
                method=venus.objects.Request.METHOD_POST,
                payload={
                    "github": coin["title"].lower(),
                    "core": coin["title"].lower(),
                    "filter": "0"
                },
                http_headers={
                    "X-Requested-With": "XMLHttpRequest"
                },
                extra=coin
            ))

        responses = venus.objects.RequestPool(pool=pool, name="github").make_async_full()
        data = [self.extract_coin_github(response) for response in responses]
        data = venus.utils.flatten_array(data)
        venus.qb.save_data(data, 'misc.coingecko_github', debug=True)

        return data

    def extract_coin_github(self, response):
        return venus.utils.insert_all_dicts(json.loads(response.output), {
            "code": response.extra["code"]
        })


if __name__ == '__main__':
    CoinGecko().run()
