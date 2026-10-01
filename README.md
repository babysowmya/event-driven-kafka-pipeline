# Event-Driven Data Pipeline with Apache Kafka and Python

A pipeline that generates user activity events, streams them through Kafka,
aggregates them into PostgreSQL, and serves the results via a REST API.

## What it does

1. **Producer** → generates random `view`/`click`/`purchase` events and
   sends them to Kafka topic `user_activities`
2. **Consumer** → reads events from Kafka, validates them, and saves
   counts into PostgreSQL
3. **API** → lets you look up a user's activity summary over HTTP

## Project folder structure

```
project-root/
├── docker-compose.yml
├── .env.example
├── README.md
├── services/
│   ├── producer/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       ├── __init__.py
│   │       └── producer.py
│   ├── consumer/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       ├── __init__.py
│   │       ├── consumer.py
│   │       ├── db.py
│   │       └── schemas.py
│   ├── api/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       ├── __init__.py
│   │       ├── main.py
│   │       └── db.py
│   └── db/
│       ├── init-db.sh
│       └── seed-data.sql
└── tests/
    ├── requirements.txt
    ├── unit/
    │   ├── test_consumer.py
    │   └── test_producer.py
    └── integration/
        └── test_api.py
```

## Requirements

- Docker + Docker Compose installed
- Python 3.11+ (only needed to run tests locally)

## Ports used

| Service  | Container port | Host port |
|----------|----------------|-----------|
| Zookeeper | 2181          | 2181      |
| Kafka     | 9092          | 9092      |
| Postgres  | 5432          | **5434**  |
| API       | 8000          | **8001**  |

> Postgres and API ports were changed from the usual `5432`/`8000` to
> `5434`/`8001` because other projects on this machine were already using
> those ports. Containers still talk to each other internally on the
> original ports — only access from outside Docker (e.g. `curl`) uses
> the new ones.

## How to run it

```bash
# 1. Copy environment file
cp .env.example .env

# 2. Build and start everything
docker-compose up -d --build

# 3. Check all services are healthy
docker-compose ps
```

Wait until `zookeeper`, `kafka`, and `postgres` all show `healthy`
before expecting producer/consumer/api to work correctly.

## How to check it's working

```bash
# Watch events being produced
docker-compose logs -f producer

# Watch events being consumed
docker-compose logs -f consumer

# Query the seeded test user
curl http://localhost:8001/api/user-activity-summary/22222222-2222-2222-2222-222222222222

# Browse the API docs in a browser
http://localhost:8001/docs
```

## How to run the tests

```bash
# Install test dependencies
pip install -r tests/requirements.txt

# Unit tests (no Docker needed)
pytest tests/unit/ -v

# Integration tests (stack must be running first)
# Windows:
set API_BASE_URL=http://localhost:8001
pytest tests/integration/ -v

# Mac/Linux:
API_BASE_URL=http://localhost:8001 pytest tests/integration/ -v
```

**Result:** 18/18 unit tests pass, 3/3 integration tests pass.

## How to stop it

```bash
docker-compose down        # stop, keep saved data
docker-compose down -v     # stop, delete saved data too
```

## Key design points

- **Idempotency**: the database table has a `UNIQUE (user_id, item_id, date)`
  constraint. The consumer uses `INSERT ... ON CONFLICT ... DO UPDATE`
  (an UPSERT) so reprocessing never creates duplicate rows.
- **Offset safety**: the consumer only commits a Kafka offset *after* the
  matching database write succeeds — so a crash never loses data.
- **Bad data handling**: messages that fail schema validation (missing
  fields, wrong types, bad UUIDs) are logged and skipped, not crashed on.
- **Known limitation**: this UPSERT approach prevents duplicate *rows*,
  but if the exact same event is redelivered, its counter can still be
  incremented twice. True exactly-once counting would need Kafka
  transactions or a stored event-ID dedup table — out of scope here.

## Author 
Sunkara Sowmya