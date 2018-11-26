from tqdm import tqdm

from earth.base.database import Database
from earth.base.register import Register
from earth.settings import SCHEMA_NAME, TIME_FIELD


class Maintenance:
    def __init__(self):
        self.db = Database()
        self.register = Register()

    def run(self):
        worker = Maintenance()

        worker.remove_duplicates()
        worker.drop_empty_tables()

        worker.create_indexes()
        worker.vacuum_database()

    def remove_duplicates(self):
        queries = []

        for full_table_name in self.db.table_list(full_name=True):
            queries.append("""
                DELETE FROM {table_name} T1
                USING {table_name} T2
                WHERE T1.ctid < T2.ctid 
                  AND T1.{time_field} = T2.{time_field};
            """.format(table_name=full_table_name, time_field=TIME_FIELD))

        self.db.run_multiple_queries_iter(queries, desc="remove_duplicates")

    def drop_empty_tables(self):
        queries = []

        for full_table_name in tqdm(self.db.table_list(full_name=True), desc="detect_empty"):
            item_count = self.db.table_size(full_table_name)

            if item_count == 0:
                queries.append("""
                    DROP TABLE IF EXISTS {table_name}
                """.format(table_name=full_table_name, time_field=TIME_FIELD))

        self.db.run_multiple_queries_iter(queries, desc="drop_empty")

    def create_indexes(self):
        queries = []

        for table_name in self.db.table_list():
            full_table_name = SCHEMA_NAME + "." + table_name

            queries.append(
                "CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_event_at_idx ON {full_table_name} ({time_field} ASC)".format(
                    table_name=table_name, full_table_name=full_table_name, time_field=TIME_FIELD)
            )

        self.db.run_multiple_queries_iter(queries, desc="create_indexes")

    def drop_all_tables(self):
        queries = []

        for full_table_name in self.db.table_list(full_name=True):
            query = "DROP TABLE IF EXISTS {}".format(full_table_name)
            queries.append(query)

        self.db.run_multiple_queries_iter(queries, "drop_all")

    def vacuum_database(self):
        queries = []

        for full_table_name in self.db.table_list(full_name=True):
            queries.append("VACUUM ANALYZE {}".format(full_table_name))

        conn = self.db.connection_pool.getconn()
        old_isolation_level = conn.isolation_level
        conn.set_isolation_level(0)

        cur = conn.cursor()
        for query in tqdm(queries, desc="vacuum"):
            cur.execute(query)

        conn.set_isolation_level(old_isolation_level)
        self.db.connection_pool.putconn(conn)
