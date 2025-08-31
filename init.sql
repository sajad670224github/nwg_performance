-- Initialize database schema and extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create custom roles
CREATE ROLE read_only;
CREATE ROLE read_write;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS audit;

-- Set privileges
GRANT USAGE ON SCHEMA app TO read_only, read_write;