# Testing Postgres Advisory Locks with asyncio and asnycpg.

Recently on the cloud team at Elastic we started working on building a new service in Python 3.7. This service is used for fetching data from a Postgres database, transforming it and submitting that data to another service. Like many cloud based services ours runs in an orchestrated container environment where N instances can be running at any time. Often that's a good thing, but our service has a few critical sections where only one instance should be able to process data. Since we are retrieving data from Postgres we decided to go ahead and make use of `advisory locks` to control these critical sections. In this article I want to explain what advisory locks are, provide an implementation for use with `asyncio` and show some test you can use to verify functionality.

## Advisory Locks 
Postgres provides the ability to create locks that only have meaning within the context of your application. These locks are referred to as [advisory locks](https://www.postgresql.org/docs/9.4/explicit-locking.html#ADVISORY-LOCKS). Advisory locks can be acquired at the session or transaction level, and by making use of them you control an applications ability to process data. Anytime your application is about to enter a critical path you attempt to acquire a lock.
```python
async with PostgresAdvisoryLock(self.dbconfig, "gold_leader") as connection:
```
If the lock is acquired then you can continue processing. If it fails then your application may retry, wait or exit. Since this lock is external to the application this allows for multiple instances of the application to run while providing safe critical path concurrency.

## Building the lock

As part of our work we wanted to make using advisory locks easy. To do this we created the `GlobalPostgresLock` context manager. Since this is meant to be used with `asyncio` and `asyncpg` we control the acquisition and release of the lock via `__aenter__` and `__aexit__`.

```python
class PostgresAdvisoryLock:
    async def __aenter__(self) -> asyncpg.connection.Connection:
        self.locked_connection = await asyncpg.connect(...)
        await self._set_lock()
        if self.got_lock:
            return self.locked_connection
        else:
            if self.locked_connection:
                await self.locked_connection.close()
            raise PostgresAdvisoryLockException

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._release()
```
Now this can be called like any other async context manager. 
```python
async with GlobalPostgresLock(config, "appname") as connection:
    val = await connection.fetchrow("SELECT 1")
 ```
## Testing the lock

With the `GlobalPostgresLock` class implemented we need to do test it. We start with test verifying the base functionality of acquiring the lock, running a query and that we can't acquire the lock inside the same scope. I recommend using the `asynctest` library to help work with `asyncio` inside `unittest`.
```python
    async def test_get_results_with_lock(self):
        async with PostgresAdvisoryLock(self.dbconfig, "gold_leader") as connection:
            val = await connection.fetchrow("SELECT 1;")
            self.assertEqual(val[0], 1)

    async def test_lock_prevents_second_lock(self):
        with self.assertRaises(PostgresAdvisoryLockException):
            async with PostgresAdvisoryLock(self.dbconfig, "gold_leader") as connection:
                await connection.fetchrow("SELECT 1;")
                async with PostgresAdvisoryLock(self.dbconfig, "gold_leader") as second_connection:
                    await second_connection.fetchrow("SELECT 1;")
```

Since we are going to use this to control the execution of code across multiple processes we also need to verify external process behavior. To do this we can use the `asnycio` libraries `subprocess.create_subprocess_exec` function to create a new process that attempts to acquire the lock our main process already has.
```python
    async def test_advisory_lock_prevents_access_from_separate_process(self):
        with self.assertRaises(PostgresAdvisoryLockException):
            async with PostgresAdvisoryLock(self.dbconfig, "gold_leader") as connection:
                proc = await asyncio.subprocess.create_subprocess_exec(
                    sys.executable,
                    "-c",
                    executable,
                    stderr=asyncio.subprocess.PIPE,
                )
```

## Wrapping Up

With updates made to `asyncio` in recent versions of Python, more libraries in the ecosystem and a lot of network based transactions it made sense for our service to use `asyncio`. In the process we had an interesting scenario for a critical system path where we leaned on Postgres and `asnycpg`. In the process of building the solution we had created a class and series of test that we thought might be helpful to share for project use, or as a learning tool for those getting started with `asyncio` and testing.

