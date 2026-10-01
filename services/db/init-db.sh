#!/bin/bash
# This script runs automatically because /docker-entrypoint-initdb.d/ scripts
# are executed in alphabetical order by the official postgres image, ONLY on
# the very first container start (i.e. when the data volume is empty).
#
# "init-db.sh" < "seed-data.sql" alphabetically, so this runs first and
# seed-data.sql is picked up afterwards by the same postgres entrypoint
# mechanism -- we do NOT invoke it manually here to avoid running it twice.
set -e

echo "[init-db] Ensuring required PostgreSQL extensions are available..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";
EOSQL

echo "[init-db] Extension setup complete. seed-data.sql will run next."