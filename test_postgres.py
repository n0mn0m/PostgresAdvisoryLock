import sys
import asyncio
import asynctest
from postgres import AdvisoryLock, AdvisoryLockException, DatabaseConfig


class Test_postgres_advisory_lock(asynctest.TestCase):
    def setUp(self) -> None:
        self.dbconfig = DatabaseConfig()

    async def test_get_results_with_lock(self):
        async with AdvisoryLock(self.dbconfig, "gold_leader") as connection:
            val = await connection.fetchrow("SELECT 1;")

            self.assertEqual(val[0], 1)

    async def test_lock_prevents_second_lock(self):
        with self.assertRaises(AdvisoryLockException):
            async with AdvisoryLock(
                    self.dbconfig, "gold_leader"
            ) as connection:
                await connection.fetchrow("SELECT 1;")
                async with AdvisoryLock(
                        self.dbconfig, "gold_leader"
                ) as second_connection:
                    await second_connection.fetchrow("SELECT 1;")

    async def test_advisory_lock_prevents_access_from_separate_process(self):
        # Run a test script that starts up an event loop in a subprocess
        # and attempts to acquire a Postgres advisory lock.

        executable = """
        import asyncio
        from postgres import PostgresAdvisoryLock, DatabaseConfig


        async def main():
            dbconfig = DatabaseConfig()

            async with PostgresAdvisoryLock(dbconfig, "gold_leader") as connection:
                while True:
                    await asyncio.sleep(0.1)


        if __name__ == "__main__":
            asyncio.run(main())
        """


        with self.assertRaises(AdvisoryLockException):
            async with AdvisoryLock(self.dbconfig, "gold_leader") as connection:
                proc = await asyncio.subprocess.create_subprocess_exec(
                    sys.executable,
                    "-c",
                    executable,
                    stderr=asyncio.subprocess.PIPE,
                )

                err = await proc.stderr.read()
                err = err.decode("ascii").rstrip()

                # wait the subprocess so that transports are cleaned up.
                await proc.wait()

                if "PostgresAdvisoryLockException" in err:
                    raise AdvisoryLockException
