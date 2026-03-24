# pymysqlhelper

A unified SQLAlchemy wrapper for SQLite and MySQL.

---

## Overview

`pymysqlhelper` provides two classes:

* `LocalDatabase` (SQLite)
* `Database` (MySQL)

Both expose an identical API, allowing you to:

* Develop locally using SQLite
* Switch to MySQL in production
* Change only the constructor — no other code modifications needed

All functionality is shared through a common `_DatabaseMixin`, ensuring consistency across both backends.

---

## Installation

```bash
pip install pymysqlhelper
```

> All dependencies are installed automatically (sqlalchemy, pymysql)

---

## Quick Start

### SQLite (Local Development)

```python
from pymysqlhelper import LocalDatabase, Integer, String, Text

db = LocalDatabase("myapp.db")

db.define_table(
    "users",
    id=Integer,
    name=String(100),
    email=Text,
)

db.insert("users", id=1, name="Alice", email="alice@example.com")
db.insert("users", id=1, name="Alice Updated", email="alice@example.com").replace()
db.insert("users", id=1, name="Alice", email="alice@example.com").ignore()

user = db.get("users", id=1)
print(user)
```

---

### MySQL (Production)

```python
from pymysqlhelper import Database

db = Database("root", "secret", "localhost", 3306, "myapp")

db.define_table("users", id=Integer, name=String(100), email=Text)
db.insert("users", id=1, name="Alice", email="alice@example.com").execute()
```

---

## Connecting

### SQLite

```python
LocalDatabase("local.db")     # File-based
LocalDatabase(":memory:")     # In-memory (testing)
```

### MySQL

```python
Database(
    username="user",
    password="password",
    host="localhost",
    port=3306,
    database="myapp",
)
```

---

## Defining Tables

```python
from sqlalchemy import Integer, String, Text, Float, ForeignKey

db.define_table(
    "orders",
    id=Integer,
    user_id=(Integer, ForeignKey("users.id")),
    total=Float(),
    note=Text,
)
```

* First column is always the primary key (`autoincrement=False`)
* Safe to call multiple times (no-op if table exists)

---

## Supported Types

* Integer, BigInteger, SmallInteger
* Boolean
* Float, DECIMAL
* String(n), Text
* DateTime, Date, Time
* LargeBinary

---

## Inserting Data

```python
db.insert("users", id=1, name="Alice").execute()
db.insert("users", id=1, name="Alice").ignore()
db.insert("users", id=1, name="Bob").replace()
```

### Bulk Insert

```python
db.bulk_insert("users", [
    {"id": 2, "name": "Bob"},
    {"id": 3, "name": "Carol"},
])
```

---

## Querying Data

### Multiple Rows

```python
db.search("users")
db.search("users", name="Alice")
```

### Single Row

```python
db.get("users", id=1)
```

### Pagination

```python
db.search_paginated("users", page=2, page_size=20)
```

### Other Helpers

```python
db.count_rows("users")
db.distinct_values("users", "role")
```

---

## Updating & Deleting

```python
db.update("users", filters={"id": 1}, updates={"name": "Alicia"})
db.delete("users", id=1)
```

---

## Schema Inspection

```python
db.list_tables()
db.list_columns("users")
db.get_table_schema("users")
db.get_column_type("users", "name")
```

---

## Schema Modifications

```python
db.add_column("users", "age", Integer)
db.drop_column("users", "bio")
db.edit_column_type("users", "age", Integer)
db.rename_table("users", "accounts")
db.delete_table("temp")
```

---

## Reload Schema

```python
db.reload()
```

Use this if schema changes outside the library.
