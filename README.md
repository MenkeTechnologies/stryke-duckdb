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
- [\[0x03\] Connection options](#0x03-connection-options)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] FFI layer](#0x05-ffi-layer)
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

From a release (no rustc or libduckdb compile on the consumer machine —
the cdylib bundles libduckdb statically):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-duckdb
```

From a local checkout (publisher / contributor workflow — first run
compiles libduckdb ~3-5 min, then installs into
`~/.stryke/store/duckdb@<version>/`):

```sh
cd ~/projects/stryke-duckdb
cargo build --release
s pkg install -g .
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

## [0x03] Connection options

Every `DuckDB::*` op accepts `%opts` as its final argument. Connection
fields the cdylib understands (matching the v1 helper-binary flags):

```
db          → path to a `.duckdb` file. Omit for `:memory:` (default).
session     → name for distinct `:memory:` instances. Defaults to "_default";
              same-session calls share the same in-memory db.
read_only   → 1 to open the file db RO
pragmas     → \@stmts — `SET name=value;` strings to run on connect
extensions  → \@names — `INSTALL <name>; LOAD <name>;` for each on connect
```

Inline:

```stryke
DuckDB::query "SELECT COUNT(*) FROM 'events.parquet'",
    extensions => ["httpfs"]

DuckDB::execute "INSERT INTO users VALUES (?, ?, ?)",
    bind => [42, "alice", 1.5],
    db   => "app.duckdb"
```

The cdylib caches one `duckdb::Connection` per `(db, session, read_only)`
tuple — `:memory:` databases persist across calls (the v1 helper binary
got a fresh empty `:memory:` every fork). Two calls with the same
`db => "app.duckdb"` share the same connection object.

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
DuckDB::execute     $sql, %opts → { affected }
DuckDB::exec_file   $path, %opts → { ok: true }
DuckDB::insert_many $table, $rows_aref, %opts → $inserted_count   # single multi-row INSERT
DuckDB::import      $path, $table, %opts → { table, kind, source, num_rows }
DuckDB::export      $table, $path, %opts → { table, kind, path, file_size }
DuckDB::truncate    $table, %opts → 1                 # DELETE FROM (empties the table)
DuckDB::upsert      $table, $row_href, %opts → $affected | @rows   # INSERT … ON CONFLICT DO UPDATE
```

`insert_many` bulk-inserts an arrayref of hashrefs in one multi-row
INSERT. Columns are inferred from the first row's keys (sorted); every
row must share them. Table and column names are identifier-validated;
values are bound. Returns the inserted-row count.

```stryke
DuckDB::insert_many "events",
    [{ id => 1, kind => "click" },
     { id => 2, kind => "view"  }]
```

`upsert` inserts a single row and, on a unique/PK conflict over the
`conflict` columns, updates the `update` columns from the proposed row
(DuckDB `excluded.*`). The conflict-target columns must carry a UNIQUE or
PRIMARY KEY constraint. Options: `conflict => \@cols` (required); `update
=> \@cols` (defaults to every row column that isn't a conflict target —
an empty list is `DO NOTHING`); `returning => "col,…" | "*"` for the
affected rows instead of a count. Names are identifier-validated; values
are bound.

```stryke
DuckDB::upsert "kv", { id => 1, name => "a", hits => 1 }, conflict => ["id"]
DuckDB::upsert "kv", { id => 1, name => "x", hits => 9 },
               conflict => ["id"], update => ["hits"]   # only bump hits
my @r = DuckDB::upsert "kv", { id => 2, name => "b" },
                       conflict => ["id"], returning => "*"
```

`import` opts: `kind` (`parquet|csv|json|auto`), `replace`, plus connection.
`export` opts: `kind` (`parquet|csv|json`), `compression` (parquet only).

### Transactions

Statements issued with the same `%opts` run on the same cached handle,
so these ride on that affinity (no extra FFI).

```stryke
DuckDB::begin       %opts → 1                    # BEGIN TRANSACTION
DuckDB::commit      %opts → 1                    # COMMIT
DuckDB::rollback    %opts → 1                    # ROLLBACK
DuckDB::transaction $code, %opts → $code_result  # BEGIN; $code->(); COMMIT — or ROLLBACK + re-raise on die
```

### Metadata

```stryke
DuckDB::tables         %opts → @{ {name, schema}, … }
DuckDB::databases      %opts → @names              # attached + system/temp catalogs
DuckDB::schema         $table, %opts → { table, num_rows, columns: [...] }
DuckDB::inspect        %opts → { version, file, file_size, databases: [...] }
DuckDB::server_version %opts → $version_string     # live SELECT version() (e.g. "v1.5.3")
DuckDB::ping           %opts → 1 | ""
DuckDB::count          $table, $where?, %opts → $row_count   # SELECT count(*) [WHERE $where]
DuckDB::table_exists   $name, %opts → 1 | 0                  # $name must be a plain identifier
```

## [0x05] FFI layer

Each `DuckDB::*` wrapper builds a JSON args dict and calls a sibling
`duckdb__*` symbol resolved out of `libstryke_duckdb.{dylib,so}`. The
cdylib is dlopened in-process on first `use DuckDB` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook) and caches one
`duckdb::Connection` per `(db, session, read_only)` tuple in
`OnceCell<Mutex<HashMap>>` for the life of the stryke process.

Wire shape (cdylib responses):

* `query`, `dump` → `{"columns": [...], "rows": [{col: val, ...}, ...]}`
* `execute` → `{"affected": <n>}`
* `exec` → `{"ok": true}`
* `import` → `{"table": ..., "rows": <n>}`
* `export` → `{"path": ..., "kind": ...}`
* `tables` → `{"tables": [...]}`
* `schema` → `{"table": ..., "columns": [{name, type, nullable}, ...]}`
* `inspect`, `ping` → `{...}`
* Errors → `{"error": "<msg>"}` — the wrapper `die`s with it

## [0x06] Tests

```sh
cargo test                   # compiles, no live calls
s test t/                    # self-contained assertion tests
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
  Cargo.toml                       # cdylib crate manifest
  Makefile
  src/lib.rs                       # cdylib — duckdb__* extern "C" exports + persistent conn cache
  lib/
    DuckDB.stk                     # `use DuckDB` — thin wrapper around the FFI symbols
  t/
    test_duckdb.stk                # self-contained assertion round-trip
    test_stryke_duckdb_surface.stk # wrapper-completeness pin
  examples/
    aggregate_csv.stk
    discover.stk
    parquet_to_db.stk
    query_parquet.stk
    window.stk
  .github/workflows/
    ci.yml                         # cargo check/test/clippy + docs lint
    release.yml                    # cross-compile + GH release on tag push
```

## [0xFF] License

MIT.
