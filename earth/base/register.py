import datetime
from collections import defaultdict

from earth.base.database import Database
from earth.base.register_models import DateRange, Label, Table
from earth.exceptions import NotFound
from earth.utils import Singleton


class Register(metaclass=Singleton):
    def __init__(self):
        self.db = Database()

    ###########################

    # unshaped collections

    @property
    def tables(self):
        """
        [Table(label=Label, date_range=DateRange), ]
        """
        table_keys = self.db.table_list()

        return set([Table.from_key(table_key) for table_key in table_keys])

    @property
    def labels(self):
        """
        [Label(value), ]
        """

        return set([table.label for table in self.tables])

    ###########################

    # collections as tree

    @property
    def label_range_tables_tree(self):
        """
        {Label(value):
            {DateRange(value, start, finish):
                Table(label=Label, date_range=DateRange),
        """
        result = defaultdict(dict)

        for table in self.tables:
            result[table.label.machine_key][table.date_range] = table

        return result

    ###########################

    # operations

    # for reader
    def tables_by_short_code(self, short_code, start_date, end_date):
        # FIXME

        # store for table names
        table_name_list = []

        # extract information
        label = Label.from_key(short_code)
        date_range = DateRange.make_from_ranges(start_date, end_date)
        range_tables = self.label_range_tables_tree[label.machine_key]

        if not range_tables:
            raise NotFound(short_code, start_date, end_date)

        # logic
        for table_range in range_tables:
            if date_range.overlaps(table_range):
                table_name_list.append(range_tables[table_range])

        # response
        return table_name_list

    # for writer
    def separate_ticks_to_tables(self, ticks):
        # FIXME
        tables = defaultdict(list)

        ticks = sorted(list(set(ticks)), key=lambda x: x.event_at)

        for tick in ticks:
            label = Label.from_value(value=tick.short_code)
            date_range = DateRange.from_timestamp(
                event_at=tick.event_at,
                interval=datetime.timedelta(days=365)
            )

            table = Table.from_value(label, date_range)
            tables[table].append(tick)

        return tables

    ###########################

    def get_table_column(self, table, field_name):
        if table in self.tables:
            timestamps = self.db.run_query(
                "SELECT {} FROM {}".format(field_name, table.full_name))

            return set([item[field_name] for item in timestamps])

        return set()

    def get_table_size(self, table):
        if table in self.tables:
            result = self.db.run_query("SELECT COUNT(*) AS count_val FROM {}".format(table.full_name))
            return result[0]["count_val"]

        return 0
