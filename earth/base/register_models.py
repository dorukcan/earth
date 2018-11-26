import attr

from earth.base.database import Database
from earth.settings import SCHEMA_NAME, SYMBOLS_TABLE
from earth.utils import from_timestamp, generate_ranges, time_in_seconds


@attr.s(slots=True, frozen=True)
class Table:
    label = attr.ib(default=None)
    date_range = attr.ib(default=None)

    SEPARATOR = '__'

    ###########################

    @classmethod
    def from_key(cls, candidate):
        label_key, range_key = candidate.split(cls.SEPARATOR)

        return cls(
            label=Label.from_key(label_key),
            date_range=DateRange.make_from_key(range_key)
        )

    @classmethod
    def from_value(cls, label, date_range):
        return cls(
            label=label,
            date_range=date_range
        )

    ###########################

    @property
    def name(self):
        return "{label}{sep}{range_key}".format(
            label=self.label.machine_key,
            sep=self.SEPARATOR,
            range_key=self.date_range.machine_key,
        )

    @property
    def full_name(self):
        return SCHEMA_NAME + "." + self.name


@attr.s(slots=True, frozen=True)
class Label:
    machine_key = attr.ib(default=None)
    value = attr.ib(default=None)

    @classmethod
    def make_key(cls, value):
        return str(value).lower()

    @classmethod
    def make_value(cls, key):
        return key.upper()

    ###########################

    @classmethod
    def from_key(cls, value):
        return cls(
            machine_key=cls.make_key(value),
            value=cls.make_value(value),
        )

    @classmethod
    def from_value(cls, value):
        return cls(
            value=value,
            machine_key=cls.make_key(value)
        )

    ###########################

    @property
    def metadata(self):
        table_full = SCHEMA_NAME + "." + SYMBOLS_TABLE

        return Database().run_query("""
            SELECT * 
            FROM {table_full} 
            WHERE LOWER(short_code) = '{label}' 
            LIMIT 1
        """.format(table_full=table_full, label=self.machine_key))


@attr.s(slots=True, frozen=True)
class DateRange:
    machine_key = attr.ib(default=None)
    start = attr.ib(default=None)
    finish = attr.ib(default=None)

    SEPARATOR = '_'

    @classmethod
    def from_values(cls, machine_key, start, finish):
        return cls(machine_key=machine_key, start=start, finish=finish)

    @classmethod
    def from_timestamp(cls, event_at, interval=None):
        ranges = generate_ranges(event_at, interval=interval)

        machine_key = cls.make_key(ranges)

        return cls.from_values(machine_key, ranges["start"], ranges["finish"])

    @classmethod
    def make_key(cls, ranges):
        return "{start}{sep}{finish}".format(
            start=time_in_seconds(ranges["start"]),
            sep=cls.SEPARATOR,
            finish=time_in_seconds(ranges["finish"])
        )

    @classmethod
    def make_from_key(cls, range_key):
        raw_ranges = range_key.split(cls.SEPARATOR)

        ranges = dict(
            start=from_timestamp(raw_ranges[0]),
            finish=from_timestamp(raw_ranges[1])
        )

        return cls.from_values(range_key, ranges["start"], ranges["finish"])

    ###########################

    @classmethod
    def make_from_ranges(cls, start_dt, finish_dt):
        return cls(
            machine_key="",
            start=start_dt,
            finish=finish_dt
        )

    def overlaps(self, table_range):
        if self.contains(table_range.start) or self.contains(table_range.finish):
            return True

        return False

    def contains(self, dt):
        if self.start <= dt <= self.finish:
            return True

        return False
