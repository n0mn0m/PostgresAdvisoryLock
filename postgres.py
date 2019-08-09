import os
import uuid
import random
import asyncio
import logging
import asyncpg
from dataclasses import dataclass
from typing import Optional

logging.getLogger(__name__).addHandler(logging.NullHandler())


class AdvisoryLockException(Exception):
    """An exception occurred while acquiring the postgres advisory """

    pass


@dataclass
class DatabaseConfig:
    db_user: str = os.environ["DATABASE_USER"]
    db_pass: str = os.environ["DATABASE_PASSWORD"]
    db_host: str = os.environ["DATABASE_HOST"]
    db_name: str = os.environ["DATABASE_NAME"]


class AdvisoryLock:
    """
    Setup an advisory lock in postgres to make sure only one
    instance of the application is processing to maintain read/write
    safety.

    Intended usage:
    >>> async with AdvisoryLock("gold_leader") as connection:
    >>>     async with connection.transaction():
    >>>         async for record in connection.cursor(query):
    >>>             # do something
    >>> # Lock is released at this point

    Following properties are exposed for read:
    - `self.lock_name` - the name of the requested lock
    - `self.connection_app_name` - the name of the connection as seen by postgres
    - `self.got_lock` - `True` if the lock was acquired successfully.

    `self.retries` is available—defaulting to 0—to be used in cases when retrying
    acquisition of a lock makes sense. When set, that amount of retries will
    be made to acquire the lock, using a randomly increasing amount of sleep
    time in between attempts, before either returning the lock or raising
    AdvisoryLockException.

    :return: connection for which the lock is taken. Use this connection to interact
    with the DB.

    .. seealso::
    https://www.postgresql.org/docs/9.4/static/explicit-locking.html#ADVISORY-LOCKS
    """

    def __init__(
        self,
        lock_name: str,
        config: Optional[DatabaseConfig] = None,
        retries: Optional[int] = 0,
    ):

        self.config = config or DatabaseConfig()
        self.lock_name = lock_name
        self.retries = retries
        self.connection_app_name = f"{uuid.uuid4()}-{self.lock_name}-lock"
        self.got_lock = False
        self.locked_connection = None
        self._transaction = None

    async def _release(self):
        """
        Release the advisory lock when we leave the scope of the AdvisoryLock.
        """
        self.got_lock = False
        await self.locked_connection.execute("SELECT pg_advisory_unlock_all()")
        await self.locked_connection.close()
        self.locked_connection = None

    async def _get_lock(self) -> bool:
        """
        Make use of stored procedure in Postgres to acquire an application
        lock managed in Postgres so that we can have multiple instances of
        running but only one with control.

        This coroutine will attempt to retry the lock acquisition up to
        the amount set in `self.retries`, by sleeping a randomized amount
        between 0 and 1 seconds between attempts.

        .. seealso::
        https://www.postgresql.org/docs/9.4/explicit-locking.html#ADVISORY-LOCKS
        """
        attempts = 0
        max_attempts = 1 + self.retries
        sleep_time = 0

        got_lock = False
        while attempts <= max_attempts:
            got_lock: bool = await self.locked_connection.fetchval(
                """SELECT pg_try_advisory_lock( ('x'||substr(md5($1),1,16))::bit(64)::bigint );""", # noqa
                self.lock_name,
            )
            if got_lock:
                return True
            elif self.retries == 0:
                return False
            else:
                attempts += 1
                logging.debug(
                    "Retrying AdvisoryLock", name=self.lock_name, attempt=attempts
                )
                # If we failed to get the lock, sleep a randomly increasing
                # amount of time by fractions of one second, and then retry.
                sleep_time += random.random()
                await asyncio.sleep(sleep_time)
        return False

    async def __aenter__(self) -> asyncpg.connection.Connection:
        """
        Acquire connection from the pool, start transactions
        and manage committing or rolling back the transactions
        depending on the success or failure of operations.
        All transactions with AdvisoryLock are autocommit
        just like asyncpg. To manage transactions inside of
        AdvisoryLock manually use:
        >>> async with AdvisoryLock(config, "gold_leader") as connection:
        >>>     async with connection.transaction():
        or for full control:
        >>> async with AdvisoryLock(config, "gold_leader") as connection:
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

        if await self._get_lock():
            self.got_lock = True
            return self.locked_connection
        else:
            if self.locked_connection:
                await self.locked_connection.close()
            raise AdvisoryLockException

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Release the connection and pool.
        """
        await self._release()
