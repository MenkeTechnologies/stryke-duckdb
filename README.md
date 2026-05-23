```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ d u c k d b ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-duckdb/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-duckdb/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[EMBEDDED DUCKDB SQL ENGINE FOR STRYKE // DIRECT-QUERY PARQUET / CSV / JSON]`

> *"No import step. No schema. Just SQL."*

Embedded DuckDB SQL engine for stryke. Direct-query parquet / CSV / JSON
from disk or URL without loading, persistent `.duckdb` files when you
need them, full standard SQL on top. Opt-in package tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-arrow`](https://github.com/MenkeTechnologies/stryke-arrow) · [`stryke-parquet`](https://github.com/MenkeTechnologies/stryke-parquet) · [`stryke-postgres`](https://github.com/MenkeTechnologies/stryke-postgres) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why this is one of the most useful stryke packages](#0x00-why-this-is-one-of-the-most-useful-stryke-packages)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick start](#0x02-quick-start)
- [\[0x03\] CLI: `duck`](#0x03-cli-duck)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] Helper protocol](#0x05-helper-protocol)
- [\[0x06\] Tests](#0x06-tests)
- [\[0x07\] DuckDB type encoding](#0x07-duckdb-type-encoding)
- [\[0x08\] Dev workflow](#0x08-dev-workflow)
- [\[0x09\] Layout](#0x09-layout)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Why this is one of the most useful stryke packages

DuckDB is an in-process analytical SQL engine. With this package, a
stryke one-liner gets:

```stryke
use DuckDB
p DuckDB::query "SELECT COUNT(*) FROM 's3://my-bucket/events/*.parquet'"
```

No import step, no schema declaration. DuckDB infers everything, vectorizes
the scan, and returns a row count. The same engine that powers Motherduck,
the same SQL surface as PostgreSQL, with the embedded-runtime ergonomics
of SQLite.

Pairs cleanly with [stryke-arrow](../stryke-arrow) (data pipeline) and
[stryke-parquet](../stryke-parquet) (file diagnostics). DuckDB closes the
"now what do I do with this data" loop — runs SQL on the file you just
inspected.

## [0x01] Install

```sh
cd ~/projects/stryke-duckdb
cargo build --release          # first run compiles libduckdb (~3-5 min)
s pkg install -g .             # installs `duck` and `duck-build` CLIs
```

Or:

```sh
make install
```

## [0x02] Quick start

```stryke
use DuckDB

# In-memory queries (default — every invocation starts fresh).
p DuckDB::query "SELECT 1 + 1 AS two, 'hi' AS s"

# Query a parquet file directly — no table to create.
my @rows = DuckDB::query "SELECT id, name FROM 'events.parquet' LIMIT 10"
@rows |> ep

# Aggregate a CSV.
my $count = DuckDB::query_scalar
    "SELECT COUNT(*) FROM read_csv_auto('orders.csv') WHERE total > 100"

# Hit remote files (requires httpfs extension; load it on connect).
my @rows = DuckDB::query
    "SELECT * FROM 'https://example.com/data.parquet' LIMIT 5",
    extensions => ["httpfs"]

# Persistent file db.
DuckDB::execute "CREATE TABLE users (id INT, name VARCHAR, score DOUBLE)",
                db => "app.duckdb"
DuckDB::execute "INSERT INTO users VALUES (?, ?, ?)",
                bind => [42, "alice", 1.5],
                db => "app.duckdb"

# Bulk load a parquet into a table.
my $r = DuckDB::import "events.parquet", "events",
                       db => "app.duckdb",
                       replace => 1
p "loaded $r->{num_rows} rows"

# Dump table back out.
DuckDB::export "events", "events.zstd.parquet",
               db => "app.duckdb",
               kind => "parquet",
               compression => "zstd"

# Stream large results without buffering on the stryke side.
DuckDB::query_stream "SELECT * FROM events",
    db => "app.duckdb",
    callback => sub ($row) { process $row }
```

## [0x03] CLI: `duck`

```sh
duck query    "SELECT * FROM 'events.parquet' LIMIT 10"
duck query    "SELECT ? AS r" --bind='[42]' --columnar
duck execute  "CREATE TABLE t (id INT)" --db=app.duckdb
duck exec     --file=migrate.sql --db=app.duckdb
duck dump     events.parquet --where='ts > now() - INTERVAL 1 DAY' --limit=100
duck import   events.parquet --table=events --kind=parquet --db=app.duckdb --replace
duck export   --table=events events.zstd.parquet --kind=parquet --compression=zstd --db=app.duckdb
duck tables   --db=app.duckdb
duck schema   --table=events --db=app.duckdb
duck inspect  --db=app.duckdb
duck ping
```

Global flags (also via env vars):

```
-D, --db PATH                 path to `.duckdb` file ($DUCKDB_FILE). Default: in-memory.
    --read-only               open the file db read-only
-e, --extension NAME          INSTALL + LOAD a DuckDB extension on connect (repeatable)
-p, --pragma K=V              `SET <k>=<v>;` on connect (repeatable)
```

Common extensions:

```
httpfs    HTTP / HTTPS / S3 file reads
aws       S3 with AWS-SDK auth
iceberg   Iceberg table format
delta     Delta Lake
spatial   geospatial functions
excel     .xlsx reader
```

## [0x04] API reference

### Read paths

```stryke
DuckDB::query         $sql, %opts → @rows | hashref | meta-hashref
DuckDB::query_stream  $sql, %opts → $count             # callback per row
DuckDB::query_one     $sql, %opts → \%row | undef
DuckDB::query_col     $sql, %opts → @values
DuckDB::query_scalar  $sql, %opts → $value | undef
DuckDB::dump          $source, %opts → @rows           # source = table | path | URL
```

`%opts`: `db`, `pragmas` (arrayref), `extensions` (arrayref), `read_only`,
`bind` (arrayref for `?` placeholders), `columnar`, `with_meta`, `limit`,
`callback` (stream only).

### DDL / DML

```stryke
DuckDB::execute   $sql, %opts → { affected_rows }
DuckDB::exec_file $path, %opts → { ok: true }
DuckDB::import    $path, $table, %opts → { table, kind, source, num_rows }
DuckDB::export    $table, $path, %opts → { table, kind, path, file_size }
```

`import` opts: `kind` (`parquet|csv|json|auto`), `replace`, plus connection.
`export` opts: `kind` (`parquet|csv|json`), `compression` (parquet only).

### Metadata

```stryke
DuckDB::tables   %opts → @{ {name, schema}, … }
DuckDB::schema   $table, %opts → { table, num_rows, columns: [...] }
DuckDB::inspect  %opts → { version, file, file_size, databases: [...] }
DuckDB::ping     %opts → 1 | ""
```

### Helper plumbing

```stryke
DuckDB::helper_path()   → $abs_path
DuckDB::ensure_built()  → $abs_path
DuckDB::version()       → "stryke-duckdb-helper 0.1.0"
```

## [0x05] Helper protocol

```sh
stryke-duckdb-helper query "SELECT 1+1"
stryke-duckdb-helper --db app.duckdb execute 'CREATE TABLE t (id INT)'
stryke-duckdb-helper --db app.duckdb import data.parquet --table=t --kind=parquet
stryke-duckdb-helper --db app.duckdb export --table=t out.parquet --kind=parquet
stryke-duckdb-helper -e httpfs query "SELECT COUNT(*) FROM 'https://x.com/y.parquet'"
```

Output:

* `query`, `dump` → NDJSON rows. `--columnar` for one `{columns, num_rows, rows}` object.
* `execute` → `{affected_rows}`
* `import`/`export` → `{table, kind, ...}` summary
* `tables` → NDJSON `{name, schema}`
* `schema`, `inspect`, `ping` → single JSON object / line

## [0x06] Tests

```sh
cargo test                   # compiles, no live calls
s test t/                    # 9 self-contained tests
```

Self-contained — no external service required. Tests cover in-memory
queries, positional binds, columnar output, persistent-file CTAS round
trip, and metadata introspection.

## [0x07] DuckDB type encoding

Output JSON is produced via the Arrow result iterator, so types match
[stryke-arrow](../stryke-arrow):

| DuckDB | JSON |
|---|---|
| `BOOLEAN` | bool |
| `TINYINT`/`SMALLINT`/`INTEGER`/`BIGINT` | number |
| `UTINYINT`/…/`UBIGINT` | number |
| `HUGEINT` | string (precision preserved) |
| `FLOAT`/`DOUBLE` | number |
| `DECIMAL` | string |
| `VARCHAR`/`TEXT` | string |
| `BLOB` | `"base64:…"` string |
| `DATE` | `"YYYY-MM-DD"` |
| `TIMESTAMP`/`TIMESTAMP WITH TIME ZONE` | ISO 8601 string |
| `INTERVAL` | `{months, days, nanos}` |
| `LIST<T>` | JSON array |
| `STRUCT<…>` | JSON object |
| `MAP<K,V>` | JSON object |
| `UUID` | string |
| `NULL` | null |

## [0x08] Dev workflow

```sh
make             # release build (first time: ~3-5 min for libduckdb)
make debug
make test
make install
make clean
```

## [0x09] Layout

```
stryke-duckdb/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # Rust helper crate manifest
  Makefile
  src/main.rs                      # single-file helper, ~600 LOC
  lib/
    DuckDB.stk                     # `use DuckDB`
  bin/
    duck.stk                       # `duck` CLI
    duck-build.stk
  t/
    test_duckdb.stk                # 9-test self-contained round-trip
  examples/
    query_parquet.stk
    aggregate_csv.stk
    parquet_to_db.stk
  .github/workflows/
    ci.yml                         # cargo + 9-test round-trip
    release.yml                    # cross-compile + GH release on tag push
```

## [0xFF] License

MIT.
