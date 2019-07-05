# Testing Postgres Advisory Locks with asyncio and asnycpg.
Recently on the cloud team at Elastic we started working on building a new service in Python 3.7. This service fetches data from a Postgres database, transforms it then submits that data to another service. Like many cloud based services ours runs in an orchestrated container environment where N instances can be running at any time. Often that's a good thing, but our service has a few critical sections where only one instance should be able to process data. Since we are retrieving data from Postgres we decided to go ahead and make use of `advisory locks` to control these critical sections. In this article I want to explain what advisory locks are, provide an implementation and test to verify functionality.

## Advisory Locks 
Postgres provides the ability to create locks that only have meaning within the context of your application. These are [advisory locks](https://www.postgresql.org/docs/9.4/explicit-locking.html#ADVISORY-LOCKS). You use Advisory locks to control an applications ability to process data. Anytime your application is about to enter a critical path you attempt to acquire the lock. When you acquire the lock you can continue processing. If acquisition fails then your application may retry, wait or exit. Since this lock is external  many instances can run while providing safe critical path concurrency.

```python
async with PostgresAdvisoryLock(self.dbconfig, "gold_leader") as connection:
```

If the lock is acquired then you can continue processing. If it fails then your application may retry, wait or exit. Since this lock is external to the application this allows for multiple instances of the application to run while providing safe critical path concurrency.

## Building the lock

As part of our work we wanted to make using advisory locks easy. To do this we created the `PostgresAdvisoryLock` context manager. Since this is meant to be used with `asyncio` and `asyncpg` we control the acquisition and release of the lock via `__aenter__` and `__aexit__`.

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

Now this can is called like any other async context manager. 

```python
async with PostgresAdvisoryLock(config, "appname") as connection:
    val = await connection.fetchrow("SELECT 1")
 ```
 
## Testing the lock

With the `PostgresAdvisoryLock` class implemented we need to test it. To start we verify the base functionality by acquiring the lock, running a query and validating we can't get the lock inside the same scope. I recommend using the `asynctest` library to help work with `asyncio` inside `unittest`.

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

Since we are going to use this to control the execution of code across many processes we also need to verify external process behavior. To do this we use the `asnycio`  `subprocess.create_subprocess_exec` function to create a new process. This process attempts to get the lock our main process already has, and should fail.

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
When we started to build our new application we knew we would be waiting on the network and database. Since we also had work that could happen during the wait we decided to use `asyncio`. Additionally we identified a critical path where we used Postgres to achieve concurrency control. To make critical path control easier we created a module and series of test. Once finished we realized this might be helpful to others looking for the same control, or as a reference for those learning to test with asyncio.

You can find the full implementation and docker setup [here](https://github.com/<repo_owner>/PostgresAdvisoryLock)

