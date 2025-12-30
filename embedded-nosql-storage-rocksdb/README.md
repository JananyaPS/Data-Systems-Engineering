# Embedded NoSQL Storage with RocksDB (C++)

## Overview
This project implements an **embedded NoSQL storage layer** using RocksDB in C++.
It demonstrates how a persistent key-value store can be built on top of an
LSM-tree storage engine to support efficient ingestion and retrieval patterns.

The implementation focuses on practical storage-engine usage patterns commonly
found in real systems: batch writes, multi-key reads, iterator scans, and
deletions.

## Key Features
- **Bulk ingestion from CSV** using `WriteBatch` for high-throughput writes
- **Key schema mapping** from tabular rows into key-value pairs:
  - Key format: `<id>_<column_name>`
  - Value: cell content as a string
- **MultiGet** for efficient batched point lookups
- **Range scan** using iterators to traverse key ranges
- **Delete operation** to remove keys from the store

## Tech Stack
- C++
- RocksDB
- CSV parsing library (header-based)

## Project Structure
embedded-nosql-storage-rocksdb
- README.md
- src/
- kv_store_rocksdb.cpp

## How It Works
1. Open or create a RocksDB database at the given path
2. Read a CSV file and extract the header (column names)
3. For each row:
   - Read the `id` field
   - Create one KV entry per column using `<id>_<column>` as the key
4. Persist all entries with a single batch write
5. Support common KV operations for lookup, scan, and delete

## Notes
- Datasets are not included due to size/licensing constraints.
- For local execution, install RocksDB and ensure include/library paths are configured.
- This project is designed to showcase embedded storage fundamentals rather than a full service layer.
