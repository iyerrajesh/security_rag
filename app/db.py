"""Postgres connection pool. Connection string comes from DATABASE_URL."""
import os
from psycopg_pool import ConnectionPool

_pool: ConnectionPool | None = None


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            conninfo=os.environ["DATABASE_URL"],
            min_size=1,
            max_size=int(os.environ.get("DB_POOL_MAX", "10")),
            kwargs={"connect_timeout": 10},
            open=True,
        )
    return _pool
