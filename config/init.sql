-- Runs once when the PostgreSQL container first starts.
-- Creates schemas and extensions used by the pipeline.

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Raw landing schema — data lands here straight from parsers, no transforms
CREATE SCHEMA IF NOT EXISTS raw;

-- Staging schema — lightly cleaned, dbt will read from here in Project 2
CREATE SCHEMA IF NOT EXISTS staging;

-- Marts schema — final transformed tables, used by analysts
CREATE SCHEMA IF NOT EXISTS marts;

-- Integration schema — audit log, retry queue, idempotency keys
CREATE SCHEMA IF NOT EXISTS integration;

-- Set search path so pipeline_user can see all schemas
ALTER USER pipeline_user SET search_path TO public, raw, staging, marts, integration;
