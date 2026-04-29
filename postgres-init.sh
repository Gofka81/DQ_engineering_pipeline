#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE backend'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'backend')\gexec
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "backend" -f /docker-entrypoint-initdb.d/init.sql
