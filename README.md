# Rustino

A minimal Rust-based SQL query engine with Iceberg table support.

## Features

- **Iceberg Table Integration**: Reads Iceberg table metadata and manifests
- **SQL Query Engine**: Built on DataFusion for SQL query execution
- **Parquet Support**: Native support for reading Parquet files
- **Minimal Dependencies**: Lightweight implementation with minimal external dependencies

## Architecture

- **Engine**: SQL query execution using DataFusion
- **Iceberg**: Iceberg table metadata reading and file discovery
- **SQL Parser**: Hand-written recursive descent parser for SQL queries
- **Coordinator/Worker**: Distributed query execution framework (foundation)

## Current Status

✅ Reads Iceberg table metadata (`metadata.json`)  
✅ Detects Avro format manifests (with graceful fallback)  
✅ Falls back to directory scanning for tables without metadata  
✅ Executes SQL queries on Parquet data  
✅ Minimal dependency footprint  

🚧 Avro manifest parsing (detected but not yet parsed)  
🚧 Partitioning support  
🚧 Schema evolution  
🚧 Write operations  

## Usage

```rust
use rustino::iceberg::IcebergTable;
use rustino::engine::Engine;

// Load an Iceberg table
let iceberg = IcebergTable::load(
    "example_table",
    "/path/to/table"
)?;

// Get parquet files
let files = iceberg.parquet_files()?;

// Register with engine
let mut engine = Engine::new();
engine.register_parquet_files("example_table", files).await?;

// Query
let batches = engine.query("SELECT * FROM example_table LIMIT 10").await?;
```

## Building

```bash
cargo build
cargo run
```

## Dependencies

- `tokio` - Async runtime
- `anyhow` - Error handling
- `serde` / `serde_json` - JSON parsing
- `datafusion` - SQL query engine

## Roadmap

This project aims to become a Trino-comparable query engine with:
- Full Iceberg support (Avro manifest parsing)
- Distributed query execution
- Advanced SQL features
- Performance optimizations

## License

[Add your license here]
