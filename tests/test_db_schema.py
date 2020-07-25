import sys
import os
import web
from toolkit.db import Schema

db_url = os.getenv("TEST_DATABASE_URL")
if not db_url:
    print("Please specify TEST_DATABASE_URL in the env.")
    sys.exit(1)

db = web.database(db_url)

def setup_module():
    db.query("""
        CREATE TYPE abc as ENUM ('a', 'b', 'c');
        CREATE TABLE t1(
            id serial primary key
        );

        CREATE TABLE t2(
            id serial primary key
        );
        CREATE VIEW t1_view as SELECT * FROM t1;
        CREATE VIEW t2_view as SELECT * FROM t2;
    """)

def teardown_module():
    db.query("""
        DROP TYPE abc;
        DROP TABLE t1 CASCADE;
        DROP TABLE t2 CASCADE;
    """)

class TestSchema:
    def test_get_tables(self):
        """Make sure that both t1 and t2 are present in db.
        """
        schema = Schema(db)
        tables = schema.get_tables(table_type='BASE TABLE')
        assert ['t1', 't2'] == sorted([t.table_name for t in tables])

    def test_get_table(self):
        """Make sure that t1 is present in db.
        """
        schema = Schema(db)
        table = schema.get_table(table_name='t1')
        assert table.table_name == 't1'

    def test_has_table(self):
        schema = Schema(db)
        assert schema.has_table(table_name='t1')

    def test_get_enum_types(self):
        schema = Schema(db)
        assert ['abc'] == [enum.name for enum in schema.get_enum_types()]

    def test_get_enum_type(self):
        schema = Schema(db)
        assert 'abc' == schema.get_enum_type('abc').name

    def test_has_enum_type(self):
        schema = Schema(db)
        assert schema.has_enum_type('abc')

    def test_get_views(self):
        schema = Schema(db)
        assert ['t1_view', 't2_view'] == sorted([t.table_name for t in schema.get_views()])

    def test_table_index(self):
        schema = Schema(db)
        table = schema.get_table('t1')
        assert ['t1_pkey'] == table.get_indexes()
