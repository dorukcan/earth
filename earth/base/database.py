import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from slugify import slugify
from tqdm import tqdm

from earth.settings import DB_PARAMS, SCHEMA_NAME
from earth.utils import Singleton


class Database(metaclass=Singleton):
    def __init__(self):
        self.query_cache = {}

        self.connection_pool = self.connect_db()

    def connect_db(self):
        return ThreadedConnectionPool(1, 20, **DB_PARAMS)

    def run_query(self, query, payload=None):
        if not query:
            return None

        cache_key = slugify(query)
        response = None

        if query.startswith("SELECT"):
            response = self.query_cache.get(cache_key)

        if response:
            return response

        conn = self.connection_pool.getconn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        try:
            cur.execute(query, payload)
            conn.commit()
            response = cur.fetchall()
        except psycopg2.ProgrammingError as e:
            if str(e) not in ['no results to fetch']:
                raise psycopg2.ProgrammingError
        finally:
            self.connection_pool.putconn(conn)

        self.query_cache[cache_key] = response

        return response

    def run_multiple_queries(self, queries):
        return self.run_query(";".join(queries))

    def run_multiple_queries_iter(self, queries, desc=None):
        return [self.run_query(query) for query in tqdm(queries, desc=desc)]

    ###########################

    def table_list(self, full_name=False):
        response = self.run_query("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}'
              AND table_name LIKE '%\_\_%'
        """.format(schema_name=SCHEMA_NAME))

        if full_name is True:
            return [SCHEMA_NAME + "." + row["table_name"] for row in response]
        else:
            return [row["table_name"] for row in response]

    def table_size(self, full_table_name):
        response = self.run_query("SELECT COUNT(*) AS count_val FROM {}".format(full_table_name))
        return response[0]["count_val"] if response else 0
