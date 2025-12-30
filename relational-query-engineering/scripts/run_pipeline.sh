#!/bin/bash
# Relational Query Engineering Pipeline (PostgreSQL)

set -euo pipefail

DB_NAME="${DB_NAME:-postgres}"
DB_USER="${DB_USER:-postgres}"

echo "=== Starting PostgreSQL relational pipeline ==="

# Drop tables/views from prior runs (edit table names to match your schema if needed)
psql -U "$DB_USER" -d "$DB_NAME" -c "DROP TABLE IF EXISTS authors, subreddits, submissions, comments CASCADE;" || true
psql -U "$DB_USER" -d "$DB_NAME" -c "DROP TABLE IF EXISTS query1, query2, query3, query4, query5 CASCADE;" || true

# Create schema
psql -U "$DB_USER" -d "$DB_NAME" -f sql/schema_tables.sql
psql -U "$DB_USER" -d "$DB_NAME" -f sql/schema_constraints.sql

# Load data (datasets not tracked in repo)
psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY authors FROM './authors.csv' CSV HEADER;"
psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY subreddits FROM './subreddits.csv' CSV HEADER;"
psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY submissions FROM './submissions.csv' CSV HEADER;"
psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY comments FROM './comments.csv' CSV HEADER;"

# Run analytics
psql -U "$DB_USER" -d "$DB_NAME" -f sql/analytics_queries.sql

echo "=== Pipeline completed successfully ==="
