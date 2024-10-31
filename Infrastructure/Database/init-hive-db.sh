#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE hive_metastore;
    GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO dbuser;
EOSQL