"""DB utilities to work with web.py databases.

This module provides utilities to inspect the database schema to find the
available tables, coulmns and constraints.

This is tested only on postgres db.
"""
from collections import defaultdict
from typing import List

class Column:
    """A column in a database table.

    Important attributes:

        - column_name
        - data_type
        - column_default
        - is_nullable
    """
    def __init__(self, table, column_data):
        self.table = table
        self.__dict__.update(column_data)

class Table:
    """Table in a database.

    Please note that view is also one type of a table, So this works even with views.

    Important Attributes:
        - table_schema
        - table_name
        - table_type (VIEW, BASE TABLE)
    """
    def __init__(self, db, table_data):
        self.db = db
        self.__dict__.update(table_data)

    def get_columns(self, **filters) -> List[Column]:
        """Returns all columns of the table.
        """
        rows = self.db.where("information_schema.columns",
                table_schema=self.table_schema,
                table_name=self.table_name,
                **filters)
        return [Column(self, row) for row in rows]

    def get_column(self, column_name: str) -> Column:
        """Returns the column with given name.
        """
        columns = self.get_columns(column_name=column_name)
        return columns and columns[0] or None

    def has_column(self, column_name: str) -> bool:
        return self.get_column(column_name) is not None

    def get_indexes(self) -> List[str]:
        """Returns all the indexes of the table.
        """
        rows = self.db.where("pg_indexes", tablename=self.table_name, schemaname="public")
        return [row.indexname for row in rows]

class EnumType:
    """EnumType in postgres database.
    """
    def __init__(self, name, values):
        self.name = name
        self.values = values

    def __eq__(self, other):
        return isinstance(other, EnumType) \
            and self.name == other.name \
            and self.values == other.values

    def __repr__(self):
        return "<TYPE {} ENUM {}>".format(self.name, self.values)

    @classmethod
    def find_all(cls, db):
        q = """
        SELECT
            n.nspname as enum_schema,
            t.typname as enum_name,
            e.enumlabel as enum_value,
            e.enumsortorder as sort_order
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'public'
        ORDER BY t.typname, e.enumsortorder
        """
        types = defaultdict(list)
        for row in db.query(q):
            types[row.enum_name].append(row.enum_value)
        return [EnumType(name, values) for name, values in types.items()]

    @classmethod
    def find(cls, db, name):
        for enum in cls.find_all(db):
            if enum.name == name:
                return enum

class Schema:
    """Database Schema

    Provides access tables in the database.
    """
    def __init__(self, db):
        self.db = db

    def get_tables(self, table_schema: str='public', **filters) -> List[Table]:
        """Returns all tables in the database.

        :param table_schema: specified the table_schema to list tables in
        :return: list of table objects
        """
        rows = self.db.where("information_schema.tables",
                table_schema=table_schema,
                **filters)
        return [Table(self.db, row) for row in rows]

    def get_table(self, table_name: str, table_schema: str="public") -> Table:
        """Returns the table with specified table name.

        :param table_name: name of the table
        :param table_schema: name of the table schema
        :return: the table object
        """
        tables = self.get_tables(table_name=table_name, table_schema=table_schema)
        return tables and tables[0] or None

    def has_table(self, table_name: str, table_schema: str='public') -> bool:
        return self.get_table(table_name=table_name, table_schema=table_schema) is not None

    def get_enum_types(self) -> List[EnumType]:
        """Returns all the enum types preset in database.
        """
        return EnumType.find_all(self.db)

    def get_enum_type(self, type_name: str) -> EnumType:
        """Returns the enum with the specified type name.
        """
        return EnumType.find(self.db, type_name)

    def has_enum_type(self, type_name: str) -> bool:
        return self.get_enum_type(type_name) is not None

    def get_views(self, table_schema: str='public') -> List[Table]:
        """Returns all views in the database.

        :param table_schema: name of the table_schema
        :returns: list of view objects
        """
        rows = self.db.where(
                "information_schema.views",
                table_schema=table_schema)

        views = []
        for row in rows:
            row['table_type'] = 'VIEW'
            views.append(Table(self.db, row))
        return views

    def get_constraints(self) -> List[str]:
        """Returns names of all constraints present in database.
        """
        rows = self.db.query("select conname from pg_constraint")
        return [row.conname for row in rows]
