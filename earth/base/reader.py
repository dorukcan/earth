import datetime

from earth.base.database import Database
from earth.base.io_models import Tick
from earth.base.register import Register
from earth.exceptions import NotFound
from earth.settings import TIME_FIELD
from earth.utils import time_in_seconds


class Reader:
    def __init__(self):
        self.db = Database()
        self.register = Register()
        self.query_builder = QueryBuilder()

    ##################################################################

    # slicers

    def read(self, short_code, start_date=None, end_date=None, limit=None, ascending=True):
        # what time is it
        current_time = datetime.datetime.now()

        # determine the start date
        # default is one year
        if not start_date:
            start_date = current_time - datetime.timedelta(days=365)

        # determine the end date
        end_date = current_time if not end_date else end_date

        # slice the database
        return self.read_from_db(short_code, start_date, end_date, limit, ascending)

    def read_from_db(self, short_code, start_date, end_date, limit, ascending):
        # detect the tables to read
        # register module
        try:
            table_list = self.register.tables_by_short_code(
                short_code, start_date, end_date)
        except NotFound:
            return []

        # query that fetches the database
        # query builder module
        read_query = self.query_builder.make_read_query(
            table_list, start_date, end_date, limit, ascending)

        # make database query
        # database module
        response = self.db.run_query(read_query)

        if not response:
            return []

        return [Tick(short_code=short_code, **row) for row in response]

    ##################################################################

    # selectors

    def read_first(self, short_code, start_date=None, end_date=None):
        response = self.read(
            short_code, start_date, end_date, limit=1, ascending=True)

        return response[0] if response else None

    def read_last(self, short_code, start_date=None, end_date=None):
        response = self.read(
            short_code, start_date, end_date, limit=1, ascending=False)

        return response[0] if response else None

    def read_all(self, short_code):
        return self.read(
            short_code, start_date=datetime.datetime(year=1970, month=1, day=1))


class QueryBuilder:
    def __init__(self):
        pass

    def make_read_query(self, table_list, start_date, end_date, limit, ascending):
        fields = "event_at, current_value, current_volume"

        select_queries = self.make_select_queries(
            table_list, fields, start_date, end_date)

        if not select_queries:
            return None

        read_query = self.make_union_queries(
            select_queries, ascending, limit)

        return read_query

    def make_select_queries(self, table_list, fields, start_date, end_date):
        queries = []

        for table in table_list:
            where_stmts = []

            base_query = "SELECT {fields} FROM {table_name}".format(
                fields=fields, table_name=table.full_name)

            if start_date >= table.date_range.start:
                where_stmts.append("{time_field} >= to_timestamp({start_date})".format(
                    time_field=TIME_FIELD, start_date=time_in_seconds(start_date)))

            if end_date <= table.date_range.finish:
                where_stmts.append("{time_field} <= to_timestamp({end_date})".format(
                    time_field=TIME_FIELD, end_date=time_in_seconds(end_date)))

            select_query = base_query
            select_query += (" WHERE " + ' AND '.join(where_stmts)) if where_stmts else ""

            queries.append(select_query)

        return queries

    def make_union_queries(self, queries, ascending, limit):
        merge_query = ' UNION ALL '.join(queries)

        merge_query += " ORDER BY {time_field} {direction}".format(
            time_field="event_at",
            direction="ASC" if ascending is True else "DESC")

        merge_query += (" LIMIT " + str(limit)) if limit and type(limit) is int else ""

        return merge_query
