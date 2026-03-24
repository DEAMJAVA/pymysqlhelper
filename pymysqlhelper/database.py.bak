import re
import time
from sqlalchemy import create_engine, Column, MetaData, Table, select, func, text, SmallInteger, event
from sqlalchemy import Integer, String, Text, Boolean, Float, DateTime, Date, Time, LargeBinary, ForeignKey, BigInteger, JSON, DECIMAL
from sqlalchemy.orm import sessionmaker
from decimal import Decimal
from urllib.parse import quote_plus


class InsertBuilder:
    def __init__(self, db, table, data):
        self.db = db
        self.table = table
        self.data = data
        self.prefix = None
        self.executed = False

        # Automatically execute standard insert unless replaced/ignored later
        self._auto_execute()

    def ignore(self):
        self.prefix = "OR IGNORE"
        return self._execute()

    def replace(self):
        self.prefix = "OR REPLACE"
        return self._execute()

    def execute(self):
        return self._execute()

    def _auto_execute(self):
        # Delay auto execution slightly to allow chaining
        import threading
        def run():
            import time
            time.sleep(0.01)  # short delay for potential chaining
            if not self.executed:
                self._execute()
        threading.Thread(target=run).start()

    def _sanitize_params(self, params):
        if isinstance(params, dict):
            return {
                k: float(v) if isinstance(v, Decimal) else v
                for k, v in params.items()
            }
        elif isinstance(params, (list, tuple)):
            return [
                float(v) if isinstance(v, Decimal) else v
                for v in params
            ]
        return params

    def _execute(self):
        if self.executed:
            return self
        self.executed = True
        self.db.ensure_table_exists(self.table)
        sanitized_data = self._sanitize_params(self.data)
        stmt = self.db.tables[self.table].insert().values(**sanitized_data)

        dialect_name = self.db.engine.dialect.name

        if dialect_name == "sqlite":
            if self.prefix == "OR REPLACE":
                stmt = stmt.prefix_with("OR REPLACE")
            elif self.prefix == "OR IGNORE":
                stmt = stmt.prefix_with("OR IGNORE")

        elif dialect_name == "mysql":
            if self.prefix == "OR REPLACE":
                stmt = stmt.on_duplicate_key_update(**sanitized_data)

        with self.db.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        return self

def map_sqlite_to_mysql(sqlite_type):
    """Convert SQLite types to MySQL-compatible SQLAlchemy types."""
    sqlite_type = sqlite_type.upper().strip()

    # Match VARCHAR(n)
    if match := re.match(r"VARCHAR\((\d+)\)", sqlite_type):
        length = int(match.group(1))
        return String(length if length > 0 else 255)

    # Match CHAR(n)
    if match := re.match(r"CHAR\((\d+)\)", sqlite_type):
        length = int(match.group(1))
        return String(length if length > 0 else 1)

    # Match DECIMAL/NUMERIC(p, s)
    if match := re.match(r"(DECIMAL|NUMERIC)\((\d+),\s*(\d+)\)", sqlite_type):
        precision, scale = map(int, match.groups()[1:])
        return DECIMAL(precision, scale)

    mapping = {
        "INTEGER": BigInteger,
        "INT": Integer,
        "BIGINT": BigInteger,
        "SMALLINT": SmallInteger,
        "TINYINT": SmallInteger,
        "MEDIUMINT": Integer,
        "UNSIGNED BIG INT": BigInteger,
        "TEXT": Text,
        "CLOB": Text,
        "CHAR": String(1),
        "BOOLEAN": Boolean,
        "REAL": Float,
        "DOUBLE": Float,
        "FLOAT": DECIMAL,
        "BLOB": LargeBinary,
        "DATETIME": DateTime,
        "DATE": Date,
        "TIME": Time,
        "NUMERIC": DECIMAL,
    }

    return mapping.get(sqlite_type, String(255))



def map_mysql_to_sqlite(mysql_type):
    """Convert MySQL types to SQLite-compatible SQLAlchemy types."""
    mysql_type = mysql_type.upper().strip()

    if match := re.match(r"(VARCHAR|CHAR)\((\d+)\)", mysql_type):
        return Text

    if match := re.match(r"(DECIMAL|NUMERIC)\((\d+),(\d+)\)", mysql_type):
        return Float

    mapping = {
        "INT": Integer,
        "INTEGER": Integer,
        "TINYINT(1)": Boolean,
        "TINYINT": Integer,
        "SMALLINT": Integer,
        "MEDIUMINT": Integer,
        "BIGINT": Integer,
        "TEXT": Text,
        "VARCHAR": Text,
        "CHAR": Text,
        "BOOLEAN": Boolean,
        "FLOAT": Float,
        "DOUBLE": Float,
        "REAL": Float,
        "DECIMAL": Float,
        "NUMERIC": Float,
        "BLOB": LargeBinary,
        "LONGTEXT": Text,
        "MEDIUMTEXT": Text,
        "TINYTEXT": Text,
        "DATETIME": DateTime,
        "DATE": Date,
        "TIME": Time,
        "YEAR": Integer
    }

    return mapping.get(mysql_type, Text)



class Database:
    def __init__(self, username, password, host, port, database):
        encoded_password = quote_plus(password)
        self.engine = create_engine(
            f"mysql+pymysql://{username}:{encoded_password}@{host}:{port}/{database}",
            pool_pre_ping=True,
            pool_recycle=1800
        )
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        self.tables = {
            table_name: Table(table_name, self.metadata, autoload_with=self.engine)
            for table_name in self.metadata.tables
        }

    def define_table(self, db_table_name, **columns):
        """Dynamically define a table and ensure it exists in the database.

        The first column provided will be set as the primary key.
        """
        if db_table_name in self.tables:
            return self.tables[db_table_name]

        columns_def = []
        column_items = list(columns.items())

        if not column_items:
            raise ValueError("At least one column must be provided.")
        first_col_name, first_col_type = column_items[0]
        for idx, (col_name, col_spec) in enumerate(column_items):
            if isinstance(col_spec, Column):
                col = col_spec
            else:
                args = []
                kwargs = {}

                # If it's a ForeignKey, wrap it as a type
                if isinstance(col_spec, ForeignKey):
                    args.append(col_spec)
                    col_type = Integer  # Default assumption; can be improved
                elif isinstance(col_spec, tuple):
                    col_type, *args = col_spec
                else:
                    col_type = col_spec

                kwargs["primary_key"] = idx == 0  # First column is primary key
                if idx == 0:
                    kwargs["autoincrement"] = False

                col = Column(col_name, col_type, *args, **kwargs)
            columns_def.append(col)

        new_table = Table(db_table_name, self.metadata, *columns_def)
        new_table.create(self.engine)
        self.metadata.reflect(bind=self.engine)
        self.tables[db_table_name] = new_table
        return new_table

    def insert(self, table, **data):
        return InsertBuilder(self, table, data)

    def search(self, table, **filters):
        """Search records in a table with optional filters"""
        if table not in self.tables:
            raise ValueError(f"Table '{table}' does not exist.")
        stmt = select(self.tables[table])
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        with self.engine.connect() as conn:
            results = [dict(row._mapping) for row in conn.execute(stmt).fetchall()]
        return results

    def get(self, table, **filters):
        """Fetch a single record based on filters (like an ID)"""
        results = self.search(table, **filters)
        return results[0] if results else None

    def update(self, table, filters, updates):
        """Update records in a table"""
        if table not in self.tables:
            raise ValueError(f"Table '{table}' does not exist.")
        stmt = self.tables[table].update()
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        stmt = stmt.values(**updates)
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    def delete(self, table, **filters):
        """Delete records from a table"""
        if table not in self.tables:
            raise ValueError(f"Table '{table}' does not exist.")
        stmt = self.tables[table].delete()
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    def list_tables(self):
        """List all tables in the database"""
        return list(self.tables.keys())

    def bulk_insert(self, table, data_list):
        """Insert multiple records into a table efficiently."""
        if table not in self.tables:
            raise ValueError(f"Table '{table}' does not exist.")
        with self.engine.connect() as conn:
            conn.execute(self.tables[table].insert(), data_list)
            conn.commit()

    def count_rows(self, table, **filters):
        """Count the number of rows in a table with optional filters."""
        if table not in self.tables:
            raise ValueError(f"Table '{table}' does not exist.")
        stmt = select(func.count()).select_from(self.tables[table])
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        with self.engine.connect() as conn:
            return conn.execute(stmt).scalar()

    def distinct_values(self, table, column):
        """Fetch distinct values of a column using a new DB connection (always fresh)."""
        if table not in self.tables or column not in self.tables[table].c:
            raise ValueError(f"Table '{table}' or column '{column}' does not exist.")
        stmt = select(self.tables[table].c[column]).distinct()
        with self.engine.connect() as conn:
            results = conn.execute(stmt).fetchall()
        return [row[0] for row in results]

    def search_paginated(self, table, page=1, page_size=10, **filters):
        """Search records with pagination."""
        if table not in self.tables:
            raise ValueError(f"Table '{table}' does not exist.")
        stmt = select(self.tables[table])
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        stmt = stmt.limit(page_size).offset((page - 1) * page_size)
        with self.engine.connect() as conn:
            return [dict(row._mapping) for row in conn.execute(stmt).fetchall()]

    def get_table_schema(self, table):
        """Retrieve table schema (columns and types)."""
        if table not in self.tables:
            raise ValueError(f"Table '{table}' does not exist.")
        return {col.name: str(col.type) for col in self.tables[table].columns}

    def ensure_table_exists(self, table):
        if table not in self.tables:
            raise ValueError(f"Table '{table}' does not exist.")
        return True

    def delete_table(self, table):
        """Delete a table from the database with a timeout."""
        self.ensure_table_exists(table)

        stmt = text(f"DROP TABLE IF EXISTS {table}")

        try:
            start_time = time.time()
            with self.engine.connect() as conn:
                conn.execution_options(stream_results=True)
                conn.execute(stmt)
                conn.commit()

                if time.time() - start_time > 10:
                    raise TimeoutError(f"Dropping table '{table}' is taking too long.")

            self.metadata.reflect(bind=self.engine)
            self.tables.pop(table, None)
        except Exception as e:
            print(f"Failed to drop table '{table}': {e}")

    def rename_table(self, old_name, new_name):
        """Rename an existing table."""
        self.ensure_table_exists(old_name)
        stmt = text(f"RENAME TABLE {old_name} TO {new_name}")
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        self.metadata.reflect(bind=self.engine)  # Refresh metadata
        self.tables[new_name] = self.tables.pop(old_name)  # Update internal reference

    def add_column(self, table, column_name, column_type):
        """Add a new column to a table."""
        self.ensure_table_exists(table)
        stmt = text(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}")
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        self.metadata.reflect(bind=self.engine)  # Refresh metadata

    def drop_column(self, table, column_name):
        """Drop a column from a table."""
        self.ensure_table_exists(table)
        if column_name not in self.tables[table].c:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table}'.")
        stmt = text(f"ALTER TABLE {table} DROP COLUMN {column_name}")
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        self.metadata.reflect(bind=self.engine)  # Refresh metadata

    def replicate_local_to_online(self, local_db):
        """Replicates data from the local SQLite database to the online MySQL database."""
        for table in local_db.list_tables():
            schema = local_db.get_table_schema(table)
            converted_schema = {col: map_sqlite_to_mysql(schema[col]) for col in schema}

            self.define_table(table, **converted_schema)
            data = local_db.search(table)

            for row in data:
                self.insert(table, **row).replace()

    def list_columns(self, table):
        if table not in self.tables:
            raise ValueError(f"Table '{table}' does not exist.")
        return list(self.tables[table].columns.keys())

    def get_column_type(self, table, column_name):
        """Retrieve the data type of a specific column in a table."""
        self.ensure_table_exists(table)
        if column_name not in self.tables[table].c:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table}'.")
        return str(self.tables[table].c[column_name].type)

    def edit_column_type(self, table, column_name, new_type):
        """Modify the data type of an existing column in a MySQL table."""
        self.ensure_table_exists(table)
        if column_name not in self.tables[table].c:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table}'.")

        type_mapping = {
            Integer: "INT",
            String: "VARCHAR",
            Text: "TEXT",
            Boolean: "TINYINT(1)",
            Float: "FLOAT",
            DateTime: "DATETIME",
            Date: "DATE",
            Time: "TIME",
            LargeBinary: "BLOB"
        }

        base_type = type(new_type)

        if base_type not in type_mapping:
            raise ValueError(f"Unsupported type: {new_type}. Add it to the type mapping.")

        mysql_type = type_mapping[base_type]

        if isinstance(new_type, String) and new_type.length:
            mysql_type += f"({new_type.length})"

        stmt = text(f"ALTER TABLE {table} MODIFY COLUMN {column_name} {mysql_type}")
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        self.metadata.reflect(bind=self.engine)


class LocalDatabase:
    def __init__(self, db_path="local.db"):
        self.engine = create_engine(f"sqlite:///{db_path}")

        @event.listens_for(self.engine, "connect")
        def enable_foreign_keys(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        self.tables = {
            table_name: Table(table_name, self.metadata, autoload_with=self.engine)
            for table_name in self.metadata.tables
        }

    def ensure_table_exists(self, table):
        if table not in self.tables:
            raise ValueError(f"Table '{table}' does not exist.")

    def define_table(self, db_table_name, **columns):
        if db_table_name in self.tables:
            return self.tables[db_table_name]

        columns_def = []
        column_items = list(columns.items())
        if not column_items:
            raise ValueError("At least one column must be provided.")

        first_col_name, first_col_type = column_items[0]
        if isinstance(first_col_type, tuple) and isinstance(first_col_type[1], ForeignKey):
            columns_def.append(Column(first_col_name, first_col_type[0], first_col_type[1], primary_key=True))
        else:
            columns_def.append(Column(first_col_name, first_col_type, primary_key=True, autoincrement=False))

        for col_name, col_type in column_items[1:]:
            if isinstance(col_type, tuple) and isinstance(col_type[1], ForeignKey):
                columns_def.append(Column(col_name, col_type[0], col_type[1]))
            else:
                columns_def.append(Column(col_name, col_type))

        new_table = Table(db_table_name, self.metadata, *columns_def)
        new_table.create(self.engine)
        self.metadata.reflect(bind=self.engine)
        self.tables[db_table_name] = new_table
        return new_table

    def insert(self, table, **data):
        return InsertBuilder(self, table, data)

    def search(self, table, **filters):
        self.ensure_table_exists(table)
        stmt = select(self.tables[table])
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        with self.engine.connect() as conn:
            result = conn.execute(stmt).fetchall()
        return [dict(row._mapping) for row in result]

    def get(self, table, **filters):
        results = self.search(table, **filters)
        return results[0] if results else None

    def update(self, table, filters, updates):
        self.ensure_table_exists(table)
        stmt = self.tables[table].update()
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        stmt = stmt.values(**updates)
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    def delete(self, table, **filters):
        self.ensure_table_exists(table)
        stmt = self.tables[table].delete()
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    def reload(self):
        # No session, so no expire_all; can be a no-op or you can re-reflect metadata if needed
        self.metadata.reflect(bind=self.engine)
        self.tables = {
            table_name: Table(table_name, self.metadata, autoload_with=self.engine)
            for table_name in self.metadata.tables
        }

    def list_tables(self):
        return list(self.tables.keys())

    def bulk_insert(self, table, data_list):
        self.ensure_table_exists(table)
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(self.tables[table].insert(), data_list)

    def count_rows(self, table, **filters):
        self.ensure_table_exists(table)
        stmt = select(func.count()).select_from(self.tables[table])
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        with self.engine.connect() as conn:
            count = conn.execute(stmt).scalar()
        return count

    def distinct_values(self, table, column):
        self.ensure_table_exists(table)
        if column not in self.tables[table].c:
            raise ValueError(f"Column '{column}' does not exist in table '{table}'.")
        stmt = select(self.tables[table].c[column]).distinct()
        with self.engine.connect() as conn:
            results = conn.execute(stmt).fetchall()
        return [row[0] for row in results]

    def search_paginated(self, table, page=1, page_size=10, **filters):
        self.ensure_table_exists(table)
        stmt = select(self.tables[table])
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        stmt = stmt.limit(page_size).offset((page - 1) * page_size)
        with self.engine.connect() as conn:
            results = conn.execute(stmt).fetchall()
        return [dict(row._mapping) for row in results]

    def get_table_schema(self, table):
        self.ensure_table_exists(table)
        return {col.name: str(col.type) for col in self.tables[table].columns}

    def delete_table(self, table):
        self.ensure_table_exists(table)
        stmt = text(f"DROP TABLE IF EXISTS {table}")
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        self.metadata.reflect(bind=self.engine)
        self.tables.pop(table, None)

    def rename_table(self, old_name, new_name):
        self.ensure_table_exists(old_name)
        stmt = text(f"ALTER TABLE {old_name} RENAME TO {new_name}")
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        self.metadata.reflect(bind=self.engine)
        self.tables[new_name] = self.tables.pop(old_name)

    def add_column(self, table, column_name, column_type):
        self.ensure_table_exists(table)
        stmt = text(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}")
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        self.metadata.reflect(bind=self.engine)

    def drop_column(self, table, column_name):
        self.ensure_table_exists(table)
        if column_name not in self.tables[table].c:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table}'.")
        stmt = text(f"ALTER TABLE {table} DROP COLUMN {column_name}")
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        self.metadata.reflect(bind=self.engine)

    def list_columns(self, table):
        self.ensure_table_exists(table)
        return list(self.tables[table].columns.keys())

    def get_column_type(self, table, column_name):
        self.ensure_table_exists(table)
        if column_name not in self.tables[table].c:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table}'.")
        return str(self.tables[table].c[column_name].type)

    def edit_column_type(self, table, column_name, new_type):
        self.ensure_table_exists(table)
        if column_name not in self.tables[table].c:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table}'.")

        type_mapping = {
            Integer: "INTEGER",
            String: "TEXT",
            Text: "TEXT",
            Boolean: "INTEGER",
            Float: "REAL",
            DateTime: "TEXT",
            Date: "TEXT",
            Time: "TEXT",
            LargeBinary: "BLOB"
        }

        base_type = type(new_type)

        if base_type not in type_mapping:
            raise ValueError(f"Unsupported type: {new_type}. Add it to the type mapping.")

        sqlite_type = type_mapping[base_type]
        if isinstance(new_type, String) and new_type.length:
            sqlite_type = "TEXT"

        schema = self.get_table_schema(table)
        schema[column_name] = sqlite_type

        temp_table = f"{table}_temp"
        new_table = self.define_table(temp_table, **{col: schema[col] for col in schema})
        columns = ", ".join(schema.keys())
        stmt = text(f"INSERT INTO {temp_table} ({columns}) SELECT {columns} FROM {table}")

        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

        self.delete_table(table)
        self.rename_table(temp_table, table)





