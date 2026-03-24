import re
from decimal import Decimal
from urllib.parse import quote_plus

from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime, DECIMAL, Float, ForeignKey,
    Integer, LargeBinary, MetaData, SmallInteger, String, Table, Text, Time,
    create_engine, event, func, select, text, JSON
)
from sqlalchemy.dialects.mysql import insert as mysql_insert


# ---------------------------------------------------------------------------
# Type helpers
# ---------------------------------------------------------------------------

def _sa_type_to_sqlite_str(sa_type) -> str:
    """Convert a SQLAlchemy type object to a SQLite DDL string."""
    mapping = {
        Integer: "INTEGER",
        BigInteger: "INTEGER",
        SmallInteger: "INTEGER",
        Boolean: "INTEGER",
        Float: "REAL",
        DECIMAL: "REAL",
        Text: "TEXT",
        String: "TEXT",
        LargeBinary: "BLOB",
        DateTime: "TEXT",
        Date: "TEXT",
        Time: "TEXT",
    }
    base = type(sa_type)
    return mapping.get(base, "TEXT")


def _sa_type_to_mysql_str(sa_type) -> str:
    """Convert a SQLAlchemy type object to a MySQL DDL string."""
    if isinstance(sa_type, String) and sa_type.length:
        return f"VARCHAR({sa_type.length})"
    if isinstance(sa_type, DECIMAL):
        p = getattr(sa_type, "precision", 10) or 10
        s = getattr(sa_type, "scale", 2) or 2
        return f"DECIMAL({p},{s})"
    mapping = {
        Integer: "INT",
        BigInteger: "BIGINT",
        SmallInteger: "SMALLINT",
        Boolean: "TINYINT(1)",
        Float: "FLOAT",
        String: "VARCHAR(255)",
        Text: "TEXT",
        LargeBinary: "BLOB",
        DateTime: "DATETIME",
        Date: "DATE",
        Time: "TIME",
    }
    return mapping.get(type(sa_type), "TEXT")


def map_sqlite_to_mysql(sqlite_type: str):
    """Convert a SQLite type string (from get_table_schema) to a SQLAlchemy MySQL type."""
    t = sqlite_type.upper().strip()

    if m := re.match(r"VARCHAR\((\d+)\)", t):
        return String(int(m.group(1)) or 255)
    if m := re.match(r"CHAR\((\d+)\)", t):
        return String(int(m.group(1)) or 1)
    if m := re.match(r"(DECIMAL|NUMERIC)\((\d+),\s*(\d+)\)", t):
        return DECIMAL(int(m.group(2)), int(m.group(3)))

    mapping = {
        "INTEGER":          BigInteger,
        "INT":              Integer,
        "BIGINT":           BigInteger,
        "SMALLINT":         SmallInteger,
        "TINYINT":          SmallInteger,
        "MEDIUMINT":        Integer,
        "UNSIGNED BIG INT": BigInteger,
        "TEXT":             Text,
        "CLOB":             Text,
        "CHAR":             String(1),
        "BOOLEAN":          Boolean,
        "REAL":             Float,
        "DOUBLE":           Float,
        "FLOAT":            Float,       # was wrongly DECIMAL in the original
        "BLOB":             LargeBinary,
        "DATETIME":         DateTime,
        "DATE":             Date,
        "TIME":             Time,
        "NUMERIC":          DECIMAL,
    }
    cls = mapping.get(t, String(255))
    # Instantiate classes that are bare (not already instances)
    return cls() if isinstance(cls, type) else cls


def map_mysql_to_sqlite(mysql_type: str):
    """Convert a MySQL type string (from get_table_schema) to a SQLAlchemy SQLite type."""
    t = mysql_type.upper().strip()

    if re.match(r"(VARCHAR|CHAR)\(\d+\)", t):
        return Text()
    if re.match(r"(DECIMAL|NUMERIC)\(\d+,\d+\)", t):
        return Float()

    mapping = {
        "INT":          Integer,
        "INTEGER":      Integer,
        "TINYINT(1)":   Boolean,
        "TINYINT":      Integer,
        "SMALLINT":     Integer,
        "MEDIUMINT":    Integer,
        "BIGINT":       Integer,
        "TEXT":         Text,
        "VARCHAR":      Text,
        "CHAR":         Text,
        "BOOLEAN":      Boolean,
        "FLOAT":        Float,
        "DOUBLE":       Float,
        "REAL":         Float,
        "DECIMAL":      Float,
        "NUMERIC":      Float,
        "BLOB":         LargeBinary,
        "LONGTEXT":     Text,
        "MEDIUMTEXT":   Text,
        "TINYTEXT":     Text,
        "DATETIME":     DateTime,
        "DATE":         Date,
        "TIME":         Time,
        "YEAR":         Integer,
    }
    cls = mapping.get(t, Text)
    return cls() if isinstance(cls, type) else cls


# ---------------------------------------------------------------------------
# InsertBuilder
# ---------------------------------------------------------------------------

class InsertBuilder:
    """
    Fluent builder for INSERT statements.

    Bare ``db.insert()`` executes immediately as a plain INSERT::

        db.insert("users", id=1, name="Alice")           # executes immediately

    Chain ``.ignore()`` or ``.replace()`` to handle conflicts instead.
    These re-run the statement with the appropriate conflict strategy::

        db.insert("users", id=1, name="Alice").ignore()  # INSERT OR IGNORE
        db.insert("users", id=1, name="Alice").replace() # upsert
    """

    def __init__(self, db, table: str, data: dict):
        self.db = db
        self.table = table
        self.data = data
        self.db.ensure_table_exists(self.table)
        self._data = self._sanitize(data)
        # Track whether the initial plain insert succeeded or hit a conflict
        self._initial_ok = False
        try:
            self._run(conflict="error")
            self._initial_ok = True
        except Exception:
            # Swallow here — .ignore() or .replace() must be chained to handle it
            pass

    def ignore(self) -> "InsertBuilder":
        """If the initial insert failed due to a conflict, silently ignore it.
        If the initial insert already succeeded, this is a no-op."""
        if not self._initial_ok:
            self._run(conflict="ignore")
        return self

    def replace(self) -> "InsertBuilder":
        """If the initial insert failed due to a conflict, overwrite the row.
        If the initial insert already succeeded, this is a no-op."""
        if not self._initial_ok:
            self._run(conflict="replace")
        return self

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _sanitize(self, params: dict) -> dict:
        return {k: float(v) if isinstance(v, Decimal) else v for k, v in params.items()}

    def _run(self, conflict: str) -> "InsertBuilder":
        data = self._data
        tbl = self.db.tables[self.table]
        dialect = self.db.engine.dialect.name

        with self.db.engine.connect() as conn:
            if dialect == "sqlite":
                stmt = tbl.insert().values(**data)
                if conflict == "ignore":
                    stmt = stmt.prefix_with("OR IGNORE")
                elif conflict == "replace":
                    stmt = stmt.prefix_with("OR REPLACE")
                conn.execute(stmt)

            elif dialect == "mysql":
                if conflict == "error":
                    stmt = tbl.insert().values(**data)
                    conn.execute(stmt)
                elif conflict == "ignore":
                    stmt = tbl.insert().prefix_with("IGNORE").values(**data)
                    conn.execute(stmt)
                elif conflict == "replace":
                    pk_cols = {c.name for c in tbl.primary_key}
                    update_data = {k: v for k, v in data.items() if k not in pk_cols}
                    if update_data:
                        stmt = mysql_insert(tbl).values(**data)
                        stmt = stmt.on_duplicate_key_update(**update_data)
                    else:
                        stmt = tbl.insert().prefix_with("IGNORE").values(**data)
                    conn.execute(stmt)

            conn.commit()

        return self


# ---------------------------------------------------------------------------
# Shared mixin so both classes have an identical public API
# ---------------------------------------------------------------------------

class _DatabaseMixin:
    """
    All public methods live here.  Subclasses set `self.engine`, `self.metadata`,
    and `self.tables`.  They also implement `_column_type_to_ddl_str()` and
    `_parse_schema_type_str()` for the two dialects.
    """

    # --- abstract hooks (overridden per dialect) ---

    def _column_type_to_ddl_str(self, sa_type) -> str:  # pragma: no cover
        raise NotImplementedError

    def _parse_schema_type_str(self, type_str: str):  # pragma: no cover
        raise NotImplementedError

    # --- table definition ---

    def define_table(self, table_name: str, **columns) -> Table:
        """
        Create *table_name* if it does not already exist and return the Table object.

        Column spec values can be:

        * A bare SQLAlchemy type class or instance: ``Integer``, ``String(50)``
        * A ``Column(...)`` instance (passed through verbatim)
        * A ``(type, ForeignKey(...))`` tuple

        The **first** column is always the primary key (``autoincrement=False``).
        """
        if table_name in self.tables:
            return self.tables[table_name]

        items = list(columns.items())
        if not items:
            raise ValueError("At least one column must be provided.")

        col_defs = []
        for idx, (col_name, spec) in enumerate(items):
            is_pk = idx == 0

            if isinstance(spec, Column):
                col_defs.append(spec)
                continue

            if isinstance(spec, tuple):
                col_type, fk = spec
                col_type = col_type if isinstance(col_type, type) else type(col_type)
                col_defs.append(
                    Column(col_name, col_type(), fk, primary_key=is_pk, autoincrement=False)
                )
                continue

            # Bare type class or instance
            col_type = spec if not isinstance(spec, type) else spec()
            col_defs.append(
                Column(col_name, col_type, primary_key=is_pk, autoincrement=False)
            )

        tbl = Table(table_name, self.metadata, *col_defs)
        tbl.create(self.engine)
        self._reload_metadata()
        return self.tables[table_name]

    # --- CRUD ---

    def insert(self, table: str, **data) -> InsertBuilder:
        """Return an InsertBuilder.  Call ``.execute()``, ``.ignore()``, or ``.replace()`` on it."""
        return InsertBuilder(self, table, data)

    def search(self, table: str, **filters) -> list[dict]:
        """Return all rows matching *filters* (empty = all rows)."""
        self.ensure_table_exists(table)
        stmt = select(self.tables[table])
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        with self.engine.connect() as conn:
            return [dict(row._mapping) for row in conn.execute(stmt).fetchall()]

    def get(self, table: str, **filters) -> dict | None:
        """Return the first row matching *filters*, or ``None``."""
        rows = self.search(table, **filters)
        return rows[0] if rows else None

    def update(self, table: str, filters: dict, updates: dict) -> None:
        """Update every row matching *filters* with *updates*."""
        self.ensure_table_exists(table)
        stmt = self.tables[table].update()
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        stmt = stmt.values(**updates)
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    def delete(self, table: str, **filters) -> None:
        """Delete every row matching *filters*."""
        self.ensure_table_exists(table)
        stmt = self.tables[table].delete()
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    def bulk_insert(self, table: str, data_list: list[dict]) -> None:
        """Insert multiple rows in a single statement."""
        self.ensure_table_exists(table)
        with self.engine.connect() as conn:
            conn.execute(self.tables[table].insert(), data_list)
            conn.commit()

    # --- introspection ---

    def list_tables(self) -> list[str]:
        """Return the names of all tables."""
        return list(self.tables.keys())

    def list_columns(self, table: str) -> list[str]:
        """Return the column names of *table*."""
        self.ensure_table_exists(table)
        return list(self.tables[table].columns.keys())

    def get_table_schema(self, table: str) -> dict[str, str]:
        """Return ``{column_name: type_string}`` for every column in *table*."""
        self.ensure_table_exists(table)
        return {col.name: str(col.type) for col in self.tables[table].columns}

    def get_column_type(self, table: str, column_name: str) -> str:
        """Return the DDL type string for a single column."""
        self.ensure_table_exists(table)
        if column_name not in self.tables[table].c:
            raise ValueError(f"Column '{column_name}' does not exist in '{table}'.")
        return str(self.tables[table].c[column_name].type)

    def count_rows(self, table: str, **filters) -> int:
        """Count rows matching *filters* (empty = all rows)."""
        self.ensure_table_exists(table)
        stmt = select(func.count()).select_from(self.tables[table])
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        with self.engine.connect() as conn:
            return conn.execute(stmt).scalar()

    def distinct_values(self, table: str, column: str) -> list:
        """Return every distinct value in *column*."""
        self.ensure_table_exists(table)
        if column not in self.tables[table].c:
            raise ValueError(f"Column '{column}' does not exist in '{table}'.")
        stmt = select(self.tables[table].c[column]).distinct()
        with self.engine.connect() as conn:
            return [row[0] for row in conn.execute(stmt).fetchall()]

    def search_paginated(self, table: str, page: int = 1, page_size: int = 10, **filters) -> list[dict]:
        """Return one page of rows matching *filters*."""
        self.ensure_table_exists(table)
        stmt = select(self.tables[table])
        for key, value in filters.items():
            stmt = stmt.where(self.tables[table].c[key] == value)
        stmt = stmt.limit(page_size).offset((page - 1) * page_size)
        with self.engine.connect() as conn:
            return [dict(row._mapping) for row in conn.execute(stmt).fetchall()]

    # --- schema mutations ---

    def add_column(self, table: str, column_name: str, column_type) -> None:
        """
        Add *column_name* to *table*.

        *column_type* may be a SQLAlchemy type instance (``String(50)``,
        ``Integer()``, …) **or** a bare class (``Integer``, ``Text``).
        """
        self.ensure_table_exists(table)
        if isinstance(column_type, type):
            column_type = column_type()
        ddl_str = self._column_type_to_ddl_str(column_type)
        with self.engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column_name} {ddl_str}"))
            conn.commit()
        self._reload_metadata()

    def drop_column(self, table: str, column_name: str) -> None:
        """Remove *column_name* from *table*."""
        self.ensure_table_exists(table)
        if column_name not in self.tables[table].c:
            raise ValueError(f"Column '{column_name}' does not exist in '{table}'.")
        with self.engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE {table} DROP COLUMN {column_name}"))
            conn.commit()
        self._reload_metadata()

    def edit_column_type(self, table: str, column_name: str, new_type) -> None:
        """
        Change the type of *column_name* to *new_type*.

        *new_type* may be a SQLAlchemy type instance (``String(50)``) or a bare
        class (``Integer``).  Both dialects accept the same argument.
        """
        self.ensure_table_exists(table)
        if column_name not in self.tables[table].c:
            raise ValueError(f"Column '{column_name}' does not exist in '{table}'.")
        if isinstance(new_type, type):
            new_type = new_type()
        self._do_edit_column_type(table, column_name, new_type)
        self._reload_metadata()

    def delete_table(self, table: str) -> None:
        """Drop *table* from the database."""
        self.ensure_table_exists(table)
        with self.engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
            conn.commit()
        self._reload_metadata()
        self.tables.pop(table, None)

    def rename_table(self, old_name: str, new_name: str) -> None:
        """Rename *old_name* to *new_name*."""
        self.ensure_table_exists(old_name)
        self._do_rename_table(old_name, new_name)
        self._reload_metadata()
        self.tables[new_name] = self.tables.pop(old_name)

    # --- utility ---

    def reload(self) -> None:
        """Re-reflect the database schema (useful after external DDL changes)."""
        self._reload_metadata()

    def ensure_table_exists(self, table: str) -> None:
        if table not in self.tables:
            raise ValueError(f"Table '{table}' does not exist.")

    # --- replication helper ---

    def replicate_from(self, source_db: "_DatabaseMixin") -> None:
        """
        Copy every table and row from *source_db* into this database.

        Type strings from *source_db* are converted via the appropriate
        mapping function before being used to create tables here.
        """
        for table in source_db.list_tables():
            raw_schema = source_db.get_table_schema(table)
            converted = {
                col: self._parse_schema_type_str(raw_schema[col])
                for col in raw_schema
            }
            self.define_table(table, **converted)
            for row in source_db.search(table):
                self.insert(table, **row).replace()

    # ------------------------------------------------------------------
    # Private – implemented per subclass
    # ------------------------------------------------------------------

    def _reload_metadata(self) -> None:
        self.metadata.reflect(bind=self.engine)
        self.tables = {
            name: Table(name, self.metadata, autoload_with=self.engine)
            for name in self.metadata.tables
        }

    def _do_rename_table(self, old_name: str, new_name: str) -> None:  # pragma: no cover
        raise NotImplementedError

    def _do_edit_column_type(self, table: str, column_name: str, new_type) -> None:  # pragma: no cover
        raise NotImplementedError

    def __repr__(self) -> str:
        tables = ", ".join(self.list_tables()) or "(none)"
        return f"<{self.__class__.__name__} tables=[{tables}]>"


# ---------------------------------------------------------------------------
# LocalDatabase  (SQLite)
# ---------------------------------------------------------------------------

class LocalDatabase(_DatabaseMixin):
    """
    SQLite-backed database.  The file is created automatically if it does not
    exist.  Pass ``db_path=":memory:"`` for an in-memory database.
    """

    def __init__(self, db_path: str = "local.db"):
        self.engine = create_engine(f"sqlite:///{db_path}")

        @event.listens_for(self.engine, "connect")
        def _enable_fk(dbapi_conn, _record):
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()

        self.metadata = MetaData()
        self._reload_metadata()

    # --- dialect hooks ---

    def _column_type_to_ddl_str(self, sa_type) -> str:
        return _sa_type_to_sqlite_str(sa_type)

    def _parse_schema_type_str(self, type_str: str):
        return map_sqlite_to_mysql(type_str)   # used only by replicate_from

    def _do_rename_table(self, old_name: str, new_name: str) -> None:
        with self.engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE {old_name} RENAME TO {new_name}"))
            conn.commit()

    def _do_edit_column_type(self, table: str, column_name: str, new_type) -> None:
        """
        SQLite cannot ALTER COLUMN, so we recreate the table.
        """
        schema = self.get_table_schema(table)   # {name: str}
        new_ddl = _sa_type_to_sqlite_str(new_type)
        schema[column_name] = new_ddl

        tmp = f"_{table}_tmp"
        cols_sql = ", ".join(f"{n} {t}" for n, t in schema.items())
        col_names = ", ".join(schema.keys())

        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE TABLE {tmp} ({cols_sql})"))
            conn.execute(text(f"INSERT INTO {tmp} ({col_names}) SELECT {col_names} FROM {table}"))
            conn.execute(text(f"DROP TABLE {table}"))
            conn.execute(text(f"ALTER TABLE {tmp} RENAME TO {table}"))
            conn.commit()


# ---------------------------------------------------------------------------
# Database  (MySQL)
# ---------------------------------------------------------------------------

class Database(_DatabaseMixin):
    """
    MySQL-backed database.
    """

    def __init__(self, username: str, password: str, host: str, port: int, database: str):
        encoded_pw = quote_plus(password)
        self.engine = create_engine(
            f"mysql+pymysql://{username}:{encoded_pw}@{host}:{port}/{database}",
            pool_pre_ping=True,
            pool_recycle=1800,
        )
        self.metadata = MetaData()
        self._reload_metadata()

    # --- dialect hooks ---

    def _column_type_to_ddl_str(self, sa_type) -> str:
        return _sa_type_to_mysql_str(sa_type)

    def _parse_schema_type_str(self, type_str: str):
        return map_sqlite_to_mysql(type_str)   # converts SQLite strings to MySQL SA types

    def _do_rename_table(self, old_name: str, new_name: str) -> None:
        with self.engine.connect() as conn:
            conn.execute(text(f"RENAME TABLE {old_name} TO {new_name}"))
            conn.commit()

    def _do_edit_column_type(self, table: str, column_name: str, new_type) -> None:
        ddl_str = _sa_type_to_mysql_str(new_type)
        with self.engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE `{table}` MODIFY COLUMN `{column_name}` {ddl_str}"))
            conn.commit()