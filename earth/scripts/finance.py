import datetime
from pprint import pprint

from earth.base import Earth


class Finance:
    def __init__(self):
        self.engine = Earth()

    ######################################################

    def latest(self, short_code, hours_ago=24):
        start_date = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)

        return self.engine.reader.read(
            short_code=short_code,
            start_date=start_date)

    def latest_all(self, hours_ago):
        start_date = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)

        return self.engine.reader.read(
            start_date=start_date)

    def last_tick(self, short_code):
        end_date = datetime.datetime.now()

        return self.engine.reader.read_last(
            short_code=short_code,
            end_date=end_date)

    def historical_tick(self, short_code, hours_ago=24):
        # Returns the oldest item since hours ago

        start_date = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)

        return self.engine.reader.read_first(
            short_code=short_code,
            start_date=start_date)

    ######################################################

    def last_price(self, short_code):
        tick = self.last_tick(short_code)

        return float(tick.current_value) if tick else None

    def historical_price(self, short_code, hours_ago=24):
        tick = self.historical_tick(short_code, hours_ago)
        pprint(tick)
        return float(tick.current_value) if tick else None

    ######################################################

    def calculate_change(self, short_code, hours_ago=24):
        price_last = self.last_price(short_code)
        price_historical = self.historical_price(short_code, hours_ago)

        price_last = 0 if not price_last else price_last
        price_historical = price_last if not price_historical else price_historical

        price_diff = price_last - price_historical
        percentage = 100 * price_diff / price_historical if price_historical > 0 else 0

        return {
            "diff": price_diff,
            "percentage": percentage
        }

    ######################################################

    def sort_by_change(self, hours_ago=24 * 7):
        changes = []

        for label in self.engine.reader.register.labels:
            changes.append({
                "symbol": label.metadata,
                "change": self.calculate_change(label.value, hours_ago)
            })

        return list(sorted(changes, key=lambda x: x["change"]["percentage"], reverse=True))


def run():
    fin = Finance()
    response = fin.calculate_change("BTC", 24 * 7)
    pprint(response)


if __name__ == '__main__':
    run()
