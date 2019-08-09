import sys
import asyncio
import asynctest
from postgres import AdvisoryLock, AdvisoryLockException, DatabaseConfig


class Test_postgres_advisory_lock(asynctest.TestCase):
    def setUp(self) -> None:
        self.dbconfig = DatabaseConfig()

    async def test_get_results_with_lock(self):
        async with AdvisoryLock(
            lock_name="gold_leader", config=self.dbconfig
        ) as connection:
            val = await connection.fetchrow("SELECT 1;")

            self.assertEqual(val[0], 1)

    async def test_lock_prevents_second_lock(self):
        with self.assertRaises(AdvisoryLockException):
            async with AdvisoryLock(
                lock_name="gold_leader", config=self.dbconfig
            ) as connection:
                await connection.fetchrow("SELECT 1;")
                async with AdvisoryLock(
                    lock_name="gold_leader", config=self.dbconfig
                ) as second_connection:
                    await second_connection.fetchrow("SELECT 1;")

    async def test_advisory_lock_prevents_access_from_separate_process(self):
        # Run a test script that starts up an event loop in a subprocess
        # and attempts to acquire a Postgres advisory lock.

        executable = """
import asyncio
import postgres
async def main():
    dbconfig = postgres.DatabaseConfig()
    async with postgres.AdvisoryLock(
        lock_name="gold_leader", config=dbconfig
    ) as connection:
        while True:
            await asyncio.sleep(0.1)
if __name__ == "__main__":
    asyncio.run(main())
"""

        with self.assertRaises(AdvisoryLockException):
            async with AdvisoryLock(
                lock_name="gold_leader", config=self.dbconfig
            ):
                proc = await asyncio.subprocess.create_subprocess_exec(
                    sys.executable, "-c", executable, stderr=asyncio.subprocess.PIPE
                )

                err = await proc.stderr.read()
                err = err.decode("ascii").rstrip()

                # wait the subprocess so that transports are cleaned up.
                await proc.wait()

                if "AdvisoryLockException" in err:
                    raise AdvisoryLockException

    async def test_lock_retries(self):
        lock_name = "test_retries"

        async def _fn():
            async with AdvisoryLock(lock_name=lock_name, config=self.dbconfig):
                await asyncio.sleep(2)

        test_task = self.loop.create_task(_fn())
        # Sleep briefly to let _fn become scheduled and acquire the lock first.
        await asyncio.sleep(0.2)

        # Retry a lot of times (too many, really) so we can be sure we get the lock.
        async with AdvisoryLock(
            lock_name=lock_name, config=self.dbconfig, retries=20
        ):
            # If we don't get the lock, AdvisoryLockException will fail this.
            pass

        await test_task

