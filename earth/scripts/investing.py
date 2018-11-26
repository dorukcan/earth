import datetime
import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

import venus
import venus.objects
import venus.utils


def make_float(txt):
    return float(txt.replace(".", "").replace(",", "."))


class Investing:
    def __init__(self):
        pass

    def run(self):
        self.collect()

    def collect(self):
        currencies = [
            "https://tr.investing.com/currencies/eur-usd",
            "https://tr.investing.com/crypto/bitcoin/btc-usd",
        ]

        current_date = datetime.datetime.now().strftime("%d/%m/%Y")

        for currency_url in currencies:
            venus.utils.logger.info("Investing currencies: {}".format(currency_url))

            currency, payload = self.collect_currency(currency_url)

            history = self.collect_history(payload, current_date)
            history = venus.utils.insert_all_dicts(history, {
                "currency": currency
            })

            venus.qb.save_data(history, "investing.history")

        return currencies

    def collect_currency(self, currency_url):
        parsed_url = urlparse(currency_url)
        parsed_url = parsed_url._replace(path=parsed_url.path + '-historical-data')
        currency_url = parsed_url.geturl()

        response = venus.objects.Request(url=currency_url).make()

        return self.extract_currency(response.output)

    def extract_currency(self, raw_data):
        soup = BeautifulSoup(raw_data, 'lxml')

        currency_head = soup.select(".instrumentHeader h2.float_lang_base_1")[0].text.strip()
        currency = currency_head.replace('Geçmiş Verileri', '').replace('Vadeli İşlemleri', '').replace(
            'Geçmiş Veriler', '').strip()

        payload_str = [item for item in soup.select("script") if "histDataExcessInfo" in item.text][0].text.strip()
        re_payload = re.search("pairId: (.*?),.*?\n.*?smlId: (.*?) ", payload_str)
        payload = {
            "curr_id": re_payload.group(1),
            "smlID": re_payload.group(2),
            "header": currency_head
        }

        return currency, payload

    def collect_history(self, payload, current_date):
        st_date = "01/01/1970"

        base_payload = {
            "st_date": st_date,
            "end_date": current_date,
            "interval_sec": "Daily",
            "sort_col": "date",
            "sort_ord": "DESC",
            "action": "historical_data"
        }

        payload = venus.utils.merge_dicts(base_payload, payload)

        response = venus.objects.Request(
            url="https://tr.investing.com/instruments/HistoricalDataAjax",
            method=venus.objects.Request.METHOD_POST,
            payload=payload,
            http_headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Requested-With": "XMLHttpRequest",
            }
        ).make()

        return self.extract_history(response)

    def extract_history(self, response):
        output = []

        soup = BeautifulSoup(response.output, 'lxml')
        table = soup.select(".historicalTbl")[0]

        result = venus.utils.extract_horizontal_table(table)

        for item in result:
            if "-" not in item.get("Hac.", "-"):
                volume = item["Hac."].replace(",", ".")
                volume = float(volume.replace("B", "")) * 1000000000 if "B" in volume else volume
                volume = float(volume.replace("M", "")) * 1000000 if type(volume) is str and "M" in volume else volume
                volume = float(volume.replace("K", "")) * 1000 if type(volume) is str and "K" in volume else volume
            else:
                volume = None

            output.append({
                "event_date": datetime.datetime.strptime(item["Tarih"], "%d.%m.%Y"),
                "current_val": make_float(item["Şimdi"]),
                "open_val": make_float(item["Açılış"]),
                "high_val": make_float(item["Yüksek"]),
                "low_val": make_float(item["Düşük"]),
                "diff_val": make_float(item["Fark %"].replace('%', '')),
                "volume": volume,
            })

        return output


if __name__ == "__main__":
    Investing().run()
