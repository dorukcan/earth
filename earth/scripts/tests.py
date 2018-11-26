import unittest

from earth.base.io_models import Tick
from earth.scripts.finance import Finance


class TestFinance(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.finance = Finance()

    def test_last_tick(self):
        self.assertIsInstance(self.finance.last_tick("BTC"), Tick)

    def test_last_price(self):
        self.assertTrue(self.finance.last_price("BTC"))

    def test_historical_tick(self):
        self.assertIsInstance(self.finance.historical_tick("BTC"), Tick)

    def test_historical_price(self):
        self.assertTrue(self.finance.historical_price("BTC"))

    def test_calculate_change(self):
        self.assertTrue(self.finance.calculate_change("BTC"))

    def test_sort_by_change(self):
        self.assertTrue(self.finance.sort_by_change())
