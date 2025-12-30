// ============================================================
// Embedded NoSQL Storage Layer (RocksDB)
// ------------------------------------------------------------
// This module demonstrates a persistent key-value storage layer
// built on RocksDB. It supports:
//  - CSV bulk ingestion via WriteBatch
//  - MultiGet for batched point lookups
//  - Iterator-based scans for ID-range traversal
//  - Key deletion
//
// Key schema:
//   <id>_<column_name>  ->  <cell_value>
// Example:
//   42_display_name -> "datascience"
// ============================================================

#include <iostream>
#include <string>
#include <vector>
#include <memory>

// CSV parsing (header-based)
#include "csv.hpp"

// RocksDB
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>

using std::cerr;
using std::endl;
using std::string;
using std::vector;

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

/**
 * Open (or create) a RocksDB database at `db_path` and bulk-load
 * CSV content from `csv_file_path` using a batched write.
 *
 * Assumptions:
 * - CSV contains a column named "id"
 * - Header names are used as column names for key construction
 */
DB* build_store_from_csv(const string& csv_file_path, const string& db_path) {
    csv::CSVReader reader(csv_file_path);

    const vector<string> headers = reader.get_col_names();
    if (headers.empty()) {
        cerr << "CSV header is empty; cannot ingest data." << endl;
        return nullptr;
    }

    DB* db = nullptr;

    Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;

    Status openStatus = DB::Open(options, db_path, &db);
    if (!openStatus.ok()) {
        cerr << "Failed to open RocksDB at '" << db_path
             << "': " << openStatus.ToString() << endl;
        return nullptr;
    }

    WriteBatch batch;

    for (csv::CSVRow& row : reader) {
        string id_value;
        try {
            id_value = row["id"].get<string>();
        } catch (...) {
            // Skip rows without a valid id column
            continue;
        }

        if (id_value.empty()) continue;

        // For each column, create "<id>_<column>" -> "<value>"
        for (size_t i = 0; i < headers.size(); ++i) {
            const string& col_name = headers[i];
            const string key = id_value + "_" + col_name;

            string value;
            try {
                value = row[i].get<string>();
            } catch (...) {
                value.clear();
            }

            batch.Put(key, value);
        }
    }

    Status writeStatus = db->Write(WriteOptions(), &batch);
    if (!writeStatus.ok()) {
        cerr << "Batch write failed: " << writeStatus.ToString() << endl;
        delete db;
        return nullptr;
    }

    return db;
}

/**
 * Perform a batched lookup (MultiGet). The returned vector is aligned
 * to the input `keys` vector; missing keys return an empty string.
 */
vector<string> multiget_values(DB* db, const vector<string>& keys) {
    vector<string> values;
    if (!db || keys.empty()) return values;

    vector<Slice> key_slices;
    key_slices.reserve(keys.size());
    for (const auto& k : keys) key_slices.emplace_back(k);

    values.resize(keys.size());

    vector<Status> statuses = db->MultiGet(ReadOptions(), key_slices, &values);

    for (size_t i = 0; i < statuses.size(); ++i) {
        if (!statuses[i].ok()) {
            values[i].clear();  // normalize "missing" to empty string
        }
    }

    return values;
}

/**
 * Scan keys for IDs in the inclusive range [start_id, end_id] and
 * return only values whose key ends with "_display_name".
 *
 * Note:
 * - This performs lexicographic comparisons on id prefixes.
 * - If IDs are numeric, consider normalizing/padding IDs or parsing them
 *   as integers before comparison.
 */
vector<string> scan_display_names(DB* db, const string& start_id, const string& end_id) {
    vector<string> result;
    if (!db) return result;

    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ReadOptions()));
    it->Seek(start_id);

    while (it->Valid()) {
        const string key = it->key().ToString();

        const size_t pos = key.find('_');
        if (pos == string::npos) {
            it->Next();
            continue;
        }

        const string id_part = key.substr(0, pos);

        if (id_part > end_id) break;
        if (id_part < start_id) {
            it->Next();
            continue;
        }

        // Include only display_name entries
        if (key.find("_display_name", pos) != string::npos) {
            result.push_back(it->value().ToString());
        }

        it->Next();
    }

    return result;
}

/**
 * Delete a key from the underlying RocksDB store.
 */
Status delete_entry(DB* db, const string& key) {
    if (!db) return Status::InvalidArgument("DB pointer is null");
    return db->Delete(WriteOptions(), key);
}
