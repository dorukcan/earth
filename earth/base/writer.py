from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

import venus
from earth.base.database import Database
from earth.base.register import Register
from earth.settings import SCHEMA_NAME, SYMBOLS_TABLE, TIME_FIELD


class Writer:
    def __init__(self):
        self.db = Database()
        self.register = Register()
        self.qb = venus.qb

    def write(self, symbol, ticks):
        self.write_symbol(symbol)

        self.write_ticks(ticks)

    #####################################################################

    # database api

    def write_ticks(self, ticks):
        tables_map = self.register.separate_ticks_to_tables(ticks)

        self.write_tables(tables_map, single=True)

    def write_tables(self, tables, single=False):
        if single:
            for table, content in tqdm(tables.items(), desc="write"):
                self.save_content(table, content)
        else:
            with ThreadPoolExecutor(max_workers=8) as pool:
                # generate payload
                future_to_url = {
                    pool.submit(self.save_content, table_name, content): table_name
                    for table_name, content in tables.items()
                }

                # start async jobs
                for future in tqdm(as_completed(future_to_url), total=len(tables), desc="write"):
                    try:
                        future.result()
                    except Exception as exc:
                        table_name = future_to_url[future]
                        print('%r generated an exception: %s' % (table_name, exc))

    def write_symbol(self, new_symbol):
        # determines insert or update

        old_symbol = self.register.labels.get(new_symbol.short_code)

        if old_symbol is not None:
            # update already saved symbol
            self.update_symbol(old_symbol, new_symbol)
        else:
            # create metadata table if not exists
            if len(self.register.labels) == 0:
                self.create_metadata_table(old_symbol)

            # insert new symbol
            self.insert_symbol(new_symbol)

    #####################################################################

    # save ticks

    def save_content(self, table, content):
        valid_content = self.clean_content(table, content)

        if not valid_content:
            return None

        self.create_tick_table(table, valid_content)
        self.insert_ticks_to_table(table, valid_content)

    def clean_content(self, table, content):
        result = []

        timestamps = self.register.get_table_column(table, TIME_FIELD)

        for item in content:
            not_saved = item.event_at not in timestamps
            symbol_valid = item.short_code.isalnum() and not item.short_code[0].isdigit()

            if not_saved and symbol_valid:
                valid_item = item.as_dict()
                result.append(valid_item)

        return result

    def create_tick_table(self, table, sample):
        if table not in self.register.tables:
            create_table_query = self.qb.dict_to_create_table_query(table.full_name, sample)
            self.db.run_query(create_table_query)

            disable_wal_query = "ALTER TABLE {} SET UNLOGGED".format(table.full_name)
            self.db.run_query(disable_wal_query)

    def insert_ticks_to_table(self, table, content):
        insert_data_query = self.qb.dict_to_insert_multiple_query(table.full_name, content)
        self.db.run_query(insert_data_query, [tuple(x.values()) for x in content])

    #####################################################################

    # save symbol

    def update_symbol(self, old_symbol, new_symbol):
        short_code = old_symbol.short_code

        new_fields = {
            old_field: new_symbol[new_field]
            for old_field, new_field
            in zip(old_symbol.as_dict(), new_symbol.as_dict())
            if old_symbol[old_field] != new_symbol[new_field]
        }

        sets = ["{} = {}".format(name, value) for name, value in new_fields.items()]
        set_stmt = " ".join(sets)

        self.db.run_query("UPDATE earth.symbols SET {set_stmt} WHERE short_code = {short_code}".format(
            set_stmt=set_stmt,
            short_code=short_code
        ))

    def insert_symbol(self, symbol):
        # make metadata
        metadata = symbol.as_dict()

        # save new metadata
        table_full_name = SCHEMA_NAME + "." + SYMBOLS_TABLE

        insert_data_query = self.qb.dict_to_insert_query(table_full_name, metadata)
        return self.db.run_query(insert_data_query, [tuple(x.values()) for x in metadata])

    def create_metadata_table(self, sample):
        table_full_name = SCHEMA_NAME + "." + SYMBOLS_TABLE

        create_table_query = self.qb.dict_to_create_table_query(table_full_name, sample)
        self.db.run_query(create_table_query)
