# Getting Started

This repo provides an `asyncio` wrapper for using Postgres Advisory locks in your project.

To get stated run the test suite:

```bash
python3 -m venv ~/.virtualenv/advisory_locks
source ~/.virtualenv/advisory_locks
pip install -r requirements.txt
source env.sh
docker-compose up -d
python3 -m unittest test_postgres.py
```

### Example:

```python
from postgres import AdvisoryLock, DatabaseConfig

dbconfig = DatabaseConfig()

async with AdvisoryLock(dbconfig, "gold_leader") as connection:
    # application code
```

For more information see the [blog post](https://unexpectedeof.net/pg-lock-asyncio.html#pg-lock-asyncio).