import re
from decimal import Decimal

from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime, DECIMAL, Float, ForeignKey,
    Integer, LargeBinary, MetaData, SmallInteger, String, Table, Text, Time,
    create_engine, event, func, select, text, JSON
)
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.types import TypeEngine


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_IDENTIFIER_LEN = 64  # MySQL's limit; also plenty for SQLite


def _validate_identifier(name: str, kind: str = "identifier") -> str:
    """Raise ValueError unless *name* is a safe, boring SQL identifier."""
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name) or len(name) > _MAX_IDENTIFIER_LEN:
        raise ValueError(
            f"Invalid {kind} {name!r}: must start with a letter or underscore, contain "
            f"only letters/digits/underscores, and be at most {_MAX_IDENTIFIER_LEN} characters."
        )
    return name



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
    """Convert a SQLAlchemy type object to a MySQL DDL string.

    Numeric parameters (VARCHAR length, DECIMAL precision/scale) are coerced
    through ``int()`` before being interpolated into DDL text, so a type
    object with a tampered/non-numeric ``.length``/``.precision``/``.scale``
    fails loudly here instead of being concatenated straight into SQL.
    """
    if isinstance(sa_type, String) and sa_type.length:
        length = int(sa_type.length)
        return f"VARCHAR({length})"
    if isinstance(sa_type, DECIMAL):
        p = int(getattr(sa_type, "precision", 10) or 10)
        s = int(getattr(sa_type, "scale", 2) or 2)
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


# NOTE on unifying the type interface:
#
# Previously, `define_table` / `add_column` / `edit_column_type` accepted
# Python type objects (Integer, String(50), ...), but `get_table_schema` /
# `get_column_type` handed back bare DDL strings ("VARCHAR(100)"), and
# `replicate_from` bridged the two by re-parsing those strings back into type
# objects via `map_sqlite_to_mysql` — a regex-based guess that was also,
# confusingly, used for *both* replication directions regardless of the
# source dialect (the `map_mysql_to_sqlite` helper that was "supposed" to
# handle the other direction was never actually called anywhere).
#
# That round-trip (object -> string -> regex -> object) is both the
# inconsistency and a source of real bugs: information is lost/guessed at
# in the string form, and the guess doesn't know which dialect produced the
# string. The fix is to stop stringifying types for internal use: every
# method that hands a type to the caller now hands back the same kind of
# Python object every type-accepting method expects. `get_table_schema` and
# `get_column_type` below now return SQLAlchemy type *instances*, and
# `replicate_from` passes those objects straight into `define_table` with no
# string round-trip at all. `describe_table` is added for the old
# human-readable-string use case (printing/debugging), so nothing is lost —
# it's just no longer the thing internal plumbing relies on.


# ---------------------------------------------------------------------------
# InsertBuilder
# ---------------------------------------------------------------------------

def _sanitize_params(params: dict) -> dict:
    """Convert values that DB-API drivers can choke on (currently: Decimal)."""
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in params.items()}


class InsertBuilder:
    """
    Fluent builder for INSERT statements.

    Bare ``db.insert()`` executes immediately as a plain INSERT::

        db.insert("users", id=1, name="Alice")           # executes immediately

    Chain ``.ignore()`` or ``.replace()`` to handle conflicts instead.
    These re-run the statement with the appropriate conflict strategy, but
    only if the initial plain insert actually hit a conflict — if it already
    succeeded, ``.ignore()``/``.replace()`` are no-ops::

        db.insert("users", id=1, name="Alice").ignore()  # INSERT OR IGNORE
        db.insert("users", id=1, name="Alice").replace() # upsert
    """

    def __init__(self, db, table: str, data: dict):
        self.db = db
        self.table = table
        self.data = data
        self.db.ensure_table_exists(self.table)
        self._data = _sanitize_params(data)
        # Track whether the initial plain insert succeeded or hit a conflict
        self._initial_ok = False
        try:
            self._run(conflict="error")
            self._initial_ok = True
        except IntegrityError:
            # Only a real conflict (PK/unique violation) is swallowed here;
            # `.ignore()`/`.replace()` decide what to do about it next.
            # Anything else (connection errors, bad column names, etc.)
            # propagates immediately instead of being hidden.
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
    and `self.tables`.  They also implement `_column_type_to_ddl_str()` for
    their dialect.
    """

    # --- abstract hooks (overridden per dialect) ---

    def _column_type_to_ddl_str(self, sa_type) -> str:  # pragma: no cover
        raise NotImplementedError

    # --- identifier safety helpers (shared by both dialects) ---

    def _safe_ident(self, name: str, kind: str = "identifier") -> str:
        """Validate *name* against the identifier allow-list and return it
        quoted for direct embedding in raw SQL text via this engine's dialect."""
        _validate_identifier(name, kind=kind)
        return self.engine.dialect.identifier_preparer.quote(name)

    # --- table definition ---

    def define_table(self, table_name: str, **columns) -> Table:
        """
        Create *table_name* if it does not already exist and return the Table object.

        Column spec values can be:

        * A bare SQLAlchemy type class or instance: ``Integer``, ``String(50)``
        * A ``Column(...)`` instance (passed through verbatim)
        * A ``(type, ForeignKey(...))`` tuple

        The **first** column is always the primary key (``autoincrement=False``).

        Table and column names are validated as plain SQL identifiers
        (letters/digits/underscore, starting with a letter or underscore)
        before anything is created.
        """
        _validate_identifier(table_name, kind="table name")

        if table_name in self.tables:
            return self.tables[table_name]

        items = list(columns.items())
        if not items:
            raise ValueError("At least one column must be provided.")

        col_defs = []
        for idx, (col_name, spec) in enumerate(items):
            is_pk = idx == 0
            if not isinstance(spec, Column):
                _validate_identifier(col_name, kind="column name")

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
        """Return an InsertBuilder.  Call ``.ignore()`` or ``.replace()`` on it
        to control conflict handling."""
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
        sanitized = [_sanitize_params(row) for row in data_list]
        with self.engine.connect() as conn:
            conn.execute(self.tables[table].insert(), sanitized)
            conn.commit()

    # --- introspection ---

    def list_tables(self) -> list[str]:
        """Return the names of all tables."""
        return list(self.tables.keys())

    def list_columns(self, table: str) -> list[str]:
        """Return the column names of *table*."""
        self.ensure_table_exists(table)
        return list(self.tables[table].columns.keys())

    def get_table_schema(self, table: str) -> dict[str, TypeEngine]:
        """Return ``{column_name: sqlalchemy_type_instance}`` for every column
        in *table*.

        This returns the same kind of Python type objects that
        ``define_table``/``add_column``/``edit_column_type`` accept (e.g.
        ``Integer()``, ``String(100)``) — not DDL strings — so schema read
        anywhere in this API can be fed straight back into schema-write
        anywhere else without any string parsing in between. Use
        ``describe_table`` if you want a human-readable string instead.
        """
        self.ensure_table_exists(table)
        return {col.name: col.type for col in self.tables[table].columns}

    def describe_table(self, table: str) -> dict[str, str]:
        """Return ``{column_name: type_string}`` for display/debugging purposes."""
        self.ensure_table_exists(table)
        return {col.name: str(col.type) for col in self.tables[table].columns}

    def get_column_type(self, table: str, column_name: str) -> TypeEngine:
        """Return the SQLAlchemy type object for a single column."""
        self.ensure_table_exists(table)
        if column_name not in self.tables[table].c:
            raise ValueError(f"Column '{column_name}' does not exist in '{table}'.")
        return self.tables[table].c[column_name].type

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
    #
    # Every method below embeds table/column names directly into raw DDL text
    # because none of the three backends support binding identifiers as query
    # parameters. `_safe_ident` is used on every identifier that reaches that
    # text, including ones that are "new" (a column that doesn't exist yet, a
    # table's new name) and therefore weren't already validated when some
    # earlier table/column was created.

    def add_column(self, table: str, column_name: str, column_type) -> None:
        """
        Add *column_name* to *table*.

        *column_type* may be a SQLAlchemy type instance (``String(50)``,
        ``Integer()``, …) **or** a bare class (``Integer``, ``Text``).
        """
        self.ensure_table_exists(table)
        safe_table = self._safe_ident(table, "table name")
        safe_column = self._safe_ident(column_name, "column name")
        if isinstance(column_type, type):
            column_type = column_type()
        ddl_str = self._column_type_to_ddl_str(column_type)
        with self.engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE {safe_table} ADD COLUMN {safe_column} {ddl_str}"))
            conn.commit()
        self._reload_metadata()

    def drop_column(self, table: str, column_name: str) -> None:
        """Remove *column_name* from *table*."""
        self.ensure_table_exists(table)
        if column_name not in self.tables[table].c:
            raise ValueError(f"Column '{column_name}' does not exist in '{table}'.")
        safe_table = self._safe_ident(table, "table name")
        safe_column = self._safe_ident(column_name, "column name")
        with self.engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE {safe_table} DROP COLUMN {safe_column}"))
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
        safe_table = self._safe_ident(table, "table name")
        with self.engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {safe_table}"))
            conn.commit()
        self._reload_metadata()
        self.tables.pop(table, None)

    def rename_table(self, old_name: str, new_name: str) -> None:
        """Rename *old_name* to *new_name*."""
        self.ensure_table_exists(old_name)
        _validate_identifier(new_name, kind="table name")
        self._do_rename_table(old_name, new_name)
        # _reload_metadata() re-reflects from the engine, which by this point
        # already only knows the table under new_name — old_name is gone from
        # the DB, so there's nothing to pop/rekey manually here (doing so
        # used to raise KeyError, since old_name no longer existed post-reload).
        self._reload_metadata()

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

        Column types are taken directly from ``source_db.get_table_schema()``
        as SQLAlchemy type objects and passed straight into ``define_table`` —
        no DDL-string parsing or dialect guessing involved.
        """
        for table in source_db.list_tables():
            schema = source_db.get_table_schema(table)  # {name: TypeEngine}
            self.define_table(table, **schema)
            for row in source_db.search(table):
                self.insert(table, **row).replace()

    # ------------------------------------------------------------------
    # Private – implemented per subclass
    # ------------------------------------------------------------------

    def _reload_metadata(self) -> None:
        self.metadata.clear()
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

    def _do_rename_table(self, old_name: str, new_name: str) -> None:
        safe_old = self._safe_ident(old_name, "table name")
        safe_new = self._safe_ident(new_name, "table name")
        with self.engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE {safe_old} RENAME TO {safe_new}"))
            conn.commit()

    def _do_edit_column_type(self, table: str, column_name: str, new_type) -> None:
        """
        SQLite cannot ALTER COLUMN, so we recreate the table.

        This necessarily rebuilds from a bare {name: ddl_string} view of the
        schema, which does not preserve foreign keys, indexes, NOT
        NULL/UNIQUE constraints, or defaults that aren't captured in that
        string — those are dropped by the rebuild. Recreate such constraints
        yourself afterward if you need them.
        """
        schema = self.describe_table(table)   # {name: ddl-ish str}, for column ordering + types
        new_ddl = _sa_type_to_sqlite_str(new_type)
        schema[column_name] = new_ddl

        for col_name in schema:
            _validate_identifier(col_name, kind="column name")
        safe_table = self._safe_ident(table, "table name")

        tmp_name = f"_{table}_tmp"
        _validate_identifier(tmp_name, kind="table name")
        safe_tmp = self.engine.dialect.identifier_preparer.quote(tmp_name)

        safe_cols = {n: self.engine.dialect.identifier_preparer.quote(n) for n in schema}
        cols_sql = ", ".join(f"{safe_cols[n]} {t}" for n, t in schema.items())
        col_names_sql = ", ".join(safe_cols.values())

        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE TABLE {safe_tmp} ({cols_sql})"))
            conn.execute(text(
                f"INSERT INTO {safe_tmp} ({col_names_sql}) SELECT {col_names_sql} FROM {safe_table}"
            ))
            conn.execute(text(f"DROP TABLE {safe_table}"))
            conn.execute(text(f"ALTER TABLE {safe_tmp} RENAME TO {safe_table}"))
            conn.commit()


# ---------------------------------------------------------------------------
# Database  (MySQL)
# ---------------------------------------------------------------------------

class Database(_DatabaseMixin):
    """
    MySQL-backed database.
    """

    def __init__(self, username: str, password: str, host: str, port: int, database: str):
        # Built via URL.create rather than a manually-quoted f-string: every
        # component (not just the password) is escaped correctly for the
        # driver, so a username/host/database containing characters like
        # '@', ':', or '/' can't be misread as extra connection-string
        # fields. SQLAlchemy's URL object also keeps the password out of
        # str()/repr() output, so it isn't echoed if the engine or its URL
        # ever ends up in a log line or an unhandled-exception message.
        url = URL.create(
            drivername="mysql+pymysql",
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        self.engine = create_engine(url, pool_pre_ping=True, pool_recycle=1800)
        self.metadata = MetaData()
        self._reload_metadata()

    # --- dialect hooks ---

    def _column_type_to_ddl_str(self, sa_type) -> str:
        return _sa_type_to_mysql_str(sa_type)

    def _do_rename_table(self, old_name: str, new_name: str) -> None:
        safe_old = self._safe_ident(old_name, "table name")
        safe_new = self._safe_ident(new_name, "table name")
        with self.engine.connect() as conn:
            conn.execute(text(f"RENAME TABLE {safe_old} TO {safe_new}"))
            conn.commit()

    def _do_edit_column_type(self, table: str, column_name: str, new_type) -> None:
        safe_table = self._safe_ident(table, "table name")
        safe_column = self._safe_ident(column_name, "column name")
        ddl_str = _sa_type_to_mysql_str(new_type)
        with self.engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE {safe_table} MODIFY COLUMN {safe_column} {ddl_str}"))
            conn.commit()