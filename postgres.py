import os
import uuid
import asyncpg
from dataclasses import dataclass


class PostgresAdvisoryLockException(Exception):
    """An exception occurred while acquiring the postgres advisory """

    pass

@dataclass
class DatabaseConfig:
    db_user: str = os.environ["DATABASE_USER"]
    db_pass: str = os.environ["DATABASE_PASSWORD"]
    db_host: str = os.environ["DATABASE_HOST"]
    db_name: str = os.environ["DATABASE_NAME"]


class PostgresAdvisoryLock:
    """
    Setup a global advisory lock in postgres to make sure only one
    instance of the application is processing to maintain read/write
    safety.

    Intended usage:
    >>> async with PostgresAdvisoryLock(config, "gold_leader") as connection:
    >>>     async with connection.transaction():
    >>>         async for record in connection.cursor(query):
    >>>             # do something
    >>> # Lock is released at this point

    Following properties are exposed for read:
    - `self.lock_name` - the name of the requested lock
    - `self.connection_app_name` - the name of the connection as seen by postgres
    - `self.got_lock` - `True` if the lock was acquired successfully.

    :return: connection for which the lock is taken. Use this connection to interact
    with the DB.

    .. seealso::

    https://www.postgresql.org/docs/9.4/static/explicit-locking.html#ADVISORY-LOCKS
    """

    def __init__(self, config, lock_name: str):
        self.config = config
        self.lock_name = lock_name
        self.connection_app_name = f"{uuid.uuid4()}-{self.lock_name}-lock"
        self.got_lock = False
        self.locked_connection = None

    async def _release(self):
        """
        Release the advisory lock when we leave the scope of the
        GlobalAdvisory
        """
        self.got_lock = False
        await self.locked_connection.execute("SELECT pg_advisory_unlock_all()")
        await self.locked_connection.close()
        self.locked_connection = None

    async def _set_lock(self):
        """
        Make use of stored procedure in Postgres to acquire an application
        lock managed in Postgres so that we can have multiple instances of
        the billing framework running but only one billing to prevent
        duplicated billing and tracking.

        .. seealso::
        https://www.postgresql.org/docs/9.4/explicit-locking.html#ADVISORY-LOCKS
        """
        self.got_lock = await self.locked_connection.fetchval("""SELECT pg_try_advisory_lock( ('x'||substr(md5($1),1,16))::bit(64)::bigint );""", self.lock_name)

    async def __aenter__(self) -> asyncpg.connection.Connection:
        """
        Acquire connection from the pool, start transactions
        and manage committing or rolling back the transactions
        depending on the success or failure of operations.

        All transactions with PostgresAdvisoryLock are autocommit
        just like asyncpg. To manage transactions inside of
        PostgresAdvisoryLock manually use:

        >>> async with PostgresAdvisoryLock(config, "my_lock") as connection:
        >>>     async with connection.transaction():

        or for full control:

        >>> async with PostgresAdvisoryLock(config, "my_lock") as connection:
        >>>     local_transaction = connection.transaction()
        >>>     await local_transaction.start()
        >>>     try:
        >>>         ...
        >>>     except:
        >>>         await local_transaction.rollback()
        >>>         raise
        >>>     else:
        >>>         await local_transaction.commit()

        ... seealso::
        https://magicstack.github.io/asyncpg/current/api/index.html#transactions
        """
        self.locked_connection = await asyncpg.connect(
            host=self.config.db_host,
            database=self.config.db_name,
            user=self.config.db_user,
            password=self.config.db_pass,
            server_settings={"application_name": self.connection_app_name},
        )
        await self._set_lock()
        if self.got_lock:
            return self.locked_connection
        else:
            if self.locked_connection:
                await self.locked_connection.close()
            raise PostgresAdvisoryLockException

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Release the connection and pool.
        """
        await self._release()
