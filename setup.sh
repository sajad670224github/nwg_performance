#!/bin/bash
# postgres/setup.sh
# This script runs after the database is initialized

set -e

echo "Starting PostgreSQL setup script..."

# Wait for PostgreSQL to be ready
until pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"; do
  echo "Waiting for PostgreSQL to start..."
  sleep 1
done

# Run additional SQL commands
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<-EOSQL
    -- Create additional schemas
    CREATE SCHEMA IF NOT EXISTS analytics;

    -- Grant permissions
    GRANT ALL PRIVILEGES ON SCHEMA analytics TO ${POSTGRES_USER};

    -- Create custom types
    CREATE TYPE user_role AS ENUM ('admin', 'editor', 'viewer');
EOSQL

echo "PostgreSQL setup completed successfully"