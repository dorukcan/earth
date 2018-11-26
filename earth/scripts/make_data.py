import venus
from earth.base import Earth
from earth.base.io_models import Symbol, Tick
from earth.maintenance import Maintenance

venus.setup()


def crawl():
    from earth.scripts.coinmarketcap import CoinMarketCap
    CoinMarketCap().run()


class Generator:
    def __init__(self):
        pass

    def generate(self):
        result = {}

        raw_data = self._get_raw_data("stock.coinmarketcap", limit=False)

        for row in raw_data:
            short_code = row["short_code"]

            if short_code not in result:
                symbol = self._make_symbol(row)

                result[short_code] = {
                    "symbol": symbol,
                    "ticks": []
                }

            result[short_code]["ticks"].append(self._make_tick(row))

        return result

    def _get_raw_data(self, table_name, limit=False):
        if limit:
            group_counts = venus.db.run_query("""
                SELECT short_code, COUNT(*) AS count_val
                FROM {}
                GROUP BY short_code
                ORDER BY count_val DESC, short_code DESC
                LIMIT 2 
            """.format(table_name))
            groups = tuple([item["short_code"] for item in group_counts])

            raw_data = venus.db.run_query("""
                SELECT * FROM {}
                WHERE short_code IN %(groups)s
            """.format(table_name), groups=groups)
        else:
            raw_data = venus.db.run_query("SELECT * FROM {}".format(table_name))

        return raw_data

    def _make_symbol(self, tick):
        return Symbol(
            short_code=tick["short_code"],
            full_name=tick["full_name"],
        )

    def _make_tick(self, tick):
        return Tick(
            short_code=tick["short_code"],
            event_at=tick["event_at"],
            current_value=float(tick["price_usd"]),
            current_volume=int(tick["volume_usd"]),
        )


def run():
    crawl()

    stock_data = Generator().generate()
    engine = Earth()

    for symbol_key in stock_data:
        engine.writer.write(
            symbol=stock_data[symbol_key]["symbol"],
            ticks=stock_data[symbol_key]["ticks"],
        )

    Maintenance().run()


if __name__ == '__main__':
    run()
