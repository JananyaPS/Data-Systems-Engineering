# Relational Query Engineering (PostgreSQL)

## Overview
This project focuses on **relational database design and query optimization**
using PostgreSQL. It demonstrates how careful schema modeling, constraints, and
query planning directly impact performance and correctness in data-intensive
systems.

The work emphasizes practical database engineering concepts rather than
theoretical SQL usage, reflecting how relational systems are designed and
optimized in production environments.

## Objectives
- Design a normalized relational schema from raw datasets
- Enforce data integrity using primary keys, foreign keys, and constraints
- Execute complex analytical SQL queries efficiently
- Understand query execution behavior and optimization trade-offs

## Key Features
- **Schema Design**: Well-structured tables with explicit relationships
- **Data Integrity**: Primary keys, foreign keys, and cascading constraints
- **Query Optimization**: Performance-aware SQL queries over large tables
- **Automation**: Shell script to initialize schema, load data, and run queries

## Tech Stack
- **Database:** PostgreSQL
- **Query Language:** Advanced SQL (CTEs, window functions, correlated subqueries)
- **Performance Optimization:** Indexing (B-Tree, composite indexes), execution plan analysis (`EXPLAIN ANALYZE`)
- **Data Modeling:** Normalized relational schemas (3NF), primary/foreign keys, constraints
- **Automation:** Bash scripting for ingestion + query execution
- **Environment & Versioning:** Linux, Git, GitHub

## Input Format
- **Format:** CSV
- **Data Type:** Multi-table relational datasets with foreign-key relationships
- **Workload:** Join-heavy + aggregation-heavy analytical queries
- **Ingestion:** Batch load into PostgreSQL with schema validation and constraints enforced

## How It Works (High Level)
1. Define a normalized relational schema (tables, PK/FK constraints) to model entities and relationships.
2. Ingest raw CSVs into PostgreSQL with integrity checks and validation.
3. Implement analytical SQL queries to answer key business/system questions.
4. Profile query performance using `EXPLAIN ANALYZE` to identify bottlenecks.
5. Optimize using index strategy + query rewrites (CTE refactors, predicate pushdown, join order improvements).
6. Re-run and compare execution plans and timings to confirm measurable improvements.
7. Validate correctness by ensuring optimized queries return identical results.

## Notes
- Emphasis is on **query performance engineering**, not just correctness.
- Optimization is **execution-plan driven** and reproducible via scripts.
- Designed to reflect **real analytical workloads** and demonstrate **production-grade SQL + systems thinking**.
