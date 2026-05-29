//! `stryke-duckdb-helper` — embedded DuckDB SQL bridge for stryke.
//!
//! DuckDB lives in-process; this binary just exposes a JSON/NDJSON CLI
//! over the standard Rust binding (`duckdb` crate, `bundled` feature so
//! no system library is needed).
//!
//! Two modes:
//!   * `--db PATH` opens a persistent `.duckdb` file.
//!   * default: an in-memory database, perfect for one-shot queries
//!     that go straight against parquet/CSV/JSON files via DuckDB's
//!     direct-file SQL (e.g. `SELECT * FROM 'data.parquet'`).

use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use clap::{Args, Parser, Subcommand};
use duckdb::types::{Value, ValueRef};
use duckdb::{Connection, ToSql};
use serde_json::{json, Map as JMap, Value as JsonValue};

#[derive(Parser, Debug)]
#[command(
    name = "stryke-duckdb-helper",
    version,
    about = "DuckDB embedded SQL bridge for the stryke `duckdb` package"
)]
struct Cli {
    /// Path to a `.duckdb` file. Omit for an in-memory database.
    #[arg(long, short = 'D', env = "DUCKDB_FILE", global = true)]
    db: Option<PathBuf>,

    /// `SET <name>=<value>;` to run on every connection. Repeatable.
    #[arg(long = "pragma", short = 'p', global = true, value_name = "K=V")]
    pragmas: Vec<String>,

    /// Read-only mode (file dbs only).
    #[arg(long, global = true)]
    read_only: bool,

    /// Auto-install + load a DuckDB extension on connect. Repeatable.
    /// Examples: `httpfs`, `aws`, `iceberg`, `excel`, `spatial`.
    #[arg(long = "extension", short = 'e', global = true, value_name = "NAME")]
    extensions: Vec<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run a SELECT (or any statement that yields rows). NDJSON to stdout.
    Query {
        sql: String,
        /// JSON array of positional bind values for `?` placeholders.
        #[arg(long)]
        bind: Option<String>,
        #[arg(long)]
        columnar: bool,
        #[arg(long)]
        with_meta: bool,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Run a non-SELECT statement (DDL/DML). Reports affected rows.
    Execute {
        sql: String,
        #[arg(long)]
        bind: Option<String>,
    },
    /// Run a multi-statement SQL script.
    Exec {
        #[arg(long, short = 'f')]
        file: PathBuf,
    },
    /// `SELECT * FROM <table-or-file> [LIMIT N]` shorthand. Accepts a bare
    /// table name, a `gs://`/`s3://`/`http://`/`https://` URL, or any
    /// path DuckDB can read via its auto-format reader.
    Dump {
        source: String,
        #[arg(long)]
        columns: Option<String>,
        #[arg(long = "where")]
        where_clause: Option<String>,
        #[arg(long)]
        order_by: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Import a file into a new table. `kind`: `parquet|csv|json|auto`.
    Import {
        path: PathBuf,
        #[arg(long, short = 't')]
        table: String,
        #[arg(long, default_value = "auto")]
        kind: String,
        /// Drop the table first if it exists.
        #[arg(long)]
        replace: bool,
    },
    /// Export a table to a file. `kind`: `parquet|csv|json`.
    Export {
        #[arg(long, short = 't')]
        table: String,
        path: PathBuf,
        #[arg(long, default_value = "parquet")]
        kind: String,
        #[arg(long, default_value = "zstd")]
        compression: String,
    },
    /// List tables in the (current schema of the) database.
    Tables,
    /// Column info for one table.
    Schema {
        #[arg(long, short = 't')]
        table: String,
    },
    /// Cardinality, file size, DuckDB version, attached databases.
    Inspect,
    /// `SELECT 1` round-trip.
    Ping,
}

/// Shared global flags-as-args (kept around for tests / sub-helpers).
#[derive(Args, Debug, Clone)]
#[allow(dead_code)]
struct GlobalConn {
    db: Option<PathBuf>,
    pragmas: Vec<String>,
    extensions: Vec<String>,
    read_only: bool,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("stryke-duckdb-helper: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let conn = open_conn(
        cli.db.as_deref(),
        cli.read_only,
        &cli.pragmas,
        &cli.extensions,
    )?;
    match cli.cmd {
        Cmd::Query {
            sql,
            bind,
            columnar,
            with_meta,
            limit,
        } => cmd_query(&conn, &sql, bind.as_deref(), columnar, with_meta, limit),
        Cmd::Execute { sql, bind } => cmd_execute(&conn, &sql, bind.as_deref()),
        Cmd::Exec { file } => cmd_exec_file(&conn, &file),
        Cmd::Dump {
            source,
            columns,
            where_clause,
            order_by,
            limit,
        } => cmd_dump(
            &conn,
            &source,
            columns.as_deref(),
            where_clause.as_deref(),
            order_by.as_deref(),
            limit,
        ),
        Cmd::Import {
            path,
            table,
            kind,
            replace,
        } => cmd_import(&conn, &path, &table, &kind, replace),
        Cmd::Export {
            table,
            path,
            kind,
            compression,
        } => cmd_export(&conn, &table, &path, &kind, &compression),
        Cmd::Tables => cmd_tables(&conn),
        Cmd::Schema { table } => cmd_schema(&conn, &table),
        Cmd::Inspect => cmd_inspect(&conn, cli.db.as_deref()),
        Cmd::Ping => cmd_ping(&conn),
    }
}

/* ------------------------------------------------------------------------- */
/* connection                                                                */
/* ------------------------------------------------------------------------- */

fn open_conn(
    db: Option<&std::path::Path>,
    read_only: bool,
    pragmas: &[String],
    extensions: &[String],
) -> Result<Connection> {
    let conn = match db {
        Some(p) => {
            if read_only {
                Connection::open_with_flags(
                    p,
                    duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?,
                )
                .with_context(|| format!("opening {} (read-only)", p.display()))?
            } else {
                Connection::open(p).with_context(|| format!("opening {}", p.display()))?
            }
        }
        None => Connection::open_in_memory().context("opening :memory:")?,
    };
    for ext in extensions {
        let ext = ext.trim();
        if ext.is_empty() {
            continue;
        }
        conn.execute_batch(&format!("INSTALL {ext}; LOAD {ext};"))
            .with_context(|| format!("loading extension {ext}"))?;
    }
    for kv in pragmas {
        let kv = kv.trim();
        if kv.is_empty() {
            continue;
        }
        conn.execute_batch(&format!("SET {kv};"))
            .with_context(|| format!("applying pragma {kv}"))?;
    }
    Ok(conn)
}

/* ------------------------------------------------------------------------- */
/* binds                                                                     */
/* ------------------------------------------------------------------------- */

fn parse_bind(s: Option<&str>) -> Result<Vec<Value>> {
    let Some(raw) = s else {
        return Ok(Vec::new());
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let v: JsonValue = serde_json::from_str(raw).context("parsing --bind JSON")?;
    match v {
        JsonValue::Array(arr) => Ok(arr.into_iter().map(json_to_duckval).collect()),
        JsonValue::Null => Ok(Vec::new()),
        _ => bail!("--bind must be a JSON array"),
    }
}

fn json_to_duckval(v: JsonValue) -> Value {
    match v {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Boolean(b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::BigInt(i)
            } else if let Some(u) = n.as_u64() {
                Value::UBigInt(u)
            } else if let Some(f) = n.as_f64() {
                Value::Double(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        JsonValue::String(s) => Value::Text(s),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Text(v.to_string()),
    }
}

fn bind_refs(b: &[Value]) -> Vec<&dyn ToSql> {
    b.iter().map(|v| v as &dyn ToSql).collect()
}

/* ------------------------------------------------------------------------- */
/* row → JSON                                                                */
/* ------------------------------------------------------------------------- */

#[allow(dead_code)]
fn valref_to_json(v: ValueRef<'_>) -> JsonValue {
    match v {
        ValueRef::Null => JsonValue::Null,
        ValueRef::Boolean(b) => JsonValue::Bool(b),
        ValueRef::TinyInt(i) => json!(i),
        ValueRef::SmallInt(i) => json!(i),
        ValueRef::Int(i) => json!(i),
        ValueRef::BigInt(i) => json!(i),
        ValueRef::HugeInt(i) => JsonValue::String(i.to_string()),
        ValueRef::UTinyInt(u) => json!(u),
        ValueRef::USmallInt(u) => json!(u),
        ValueRef::UInt(u) => json!(u),
        ValueRef::UBigInt(u) => json!(u),
        ValueRef::Float(f) => json!(f),
        ValueRef::Double(f) => json!(f),
        ValueRef::Decimal(d) => JsonValue::String(d.to_string()),
        ValueRef::Timestamp(_unit, _ts) => JsonValue::String(format!("{:?}", v)),
        ValueRef::Date32(d) => JsonValue::String(format!("date32:{d}")),
        ValueRef::Time64(_unit, _t) => JsonValue::String(format!("{:?}", v)),
        ValueRef::Interval {
            months,
            days,
            nanos,
        } => json!({
            "months": months,
            "days": days,
            "nanos": nanos,
        }),
        ValueRef::Text(s) => JsonValue::String(String::from_utf8_lossy(s).into_owned()),
        ValueRef::Blob(b) => {
            let mut out = String::from("base64:");
            out.push_str(&B64.encode(b));
            JsonValue::String(out)
        }
        // List/Array/Struct/Map/Enum come back via `Value::List(...)` etc.
        // when called through `row.get::<_, Value>(idx)`; ValueRef variants
        // for those aren't trivially traversable, so we fall back to debug.
        other => JsonValue::String(format!("{:?}", other)),
    }
}

#[allow(dead_code)]
fn row_to_json(row: &duckdb::Row<'_>, column_names: &[String]) -> Result<JsonValue> {
    let mut out = JMap::with_capacity(column_names.len());
    for (i, name) in column_names.iter().enumerate() {
        let vr = row.get_ref(i).map_err(|e| anyhow!("col {i}: {e}"))?;
        out.insert(name.clone(), valref_to_json(vr));
    }
    Ok(JsonValue::Object(out))
}

#[allow(dead_code)]
fn row_to_array(row: &duckdb::Row<'_>, ncols: usize) -> Result<Vec<JsonValue>> {
    let mut out = Vec::with_capacity(ncols);
    for i in 0..ncols {
        let vr = row.get_ref(i).map_err(|e| anyhow!("col {i}: {e}"))?;
        out.push(valref_to_json(vr));
    }
    Ok(out)
}

/* ------------------------------------------------------------------------- */
/* commands                                                                  */
/* ------------------------------------------------------------------------- */

fn cmd_query(
    conn: &Connection,
    sql: &str,
    bind: Option<&str>,
    columnar: bool,
    with_meta: bool,
    limit: Option<usize>,
) -> Result<()> {
    // Use DuckDB's Arrow result iterator — robust schema metadata
    // (sidesteps a panic we hit going through `Statement::column_names`
    // on a prepared-but-unexecuted statement in duckdb 1.10502).
    use arrow_json::writer::{LineDelimited, WriterBuilder as JsonWriterBuilder};

    let binds = parse_bind(bind)?;
    let bind_refs = bind_refs(&binds);
    let mut stmt = conn.prepare(sql).context("prepare")?;
    let mut arrow_iter = stmt.query_arrow(&bind_refs[..]).context("query_arrow")?;
    let schema = arrow_iter.get_schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    if columnar {
        let mut emitted_rows: usize = 0;
        let mut payload: Vec<arrow::array::RecordBatch> = Vec::new();
        for batch in arrow_iter.by_ref() {
            let remaining = limit
                .map(|l| l.saturating_sub(emitted_rows))
                .unwrap_or(usize::MAX);
            if remaining == 0 {
                break;
            }
            let batch = if batch.num_rows() > remaining {
                batch.slice(0, remaining)
            } else {
                batch
            };
            emitted_rows += batch.num_rows();
            payload.push(batch);
            if limit.is_some_and(|l| emitted_rows >= l) {
                break;
            }
        }
        let mut row_buf: Vec<u8> = Vec::with_capacity(emitted_rows * 64);
        {
            let mut w = JsonWriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, LineDelimited>(&mut row_buf);
            for b in &payload {
                w.write(b)?;
            }
            w.finish()?;
        }
        let mut rows_arr: Vec<Vec<JsonValue>> = Vec::with_capacity(emitted_rows);
        for line in row_buf.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let mut row: serde_json::Map<String, JsonValue> = serde_json::from_slice(line)?;
            let arr: Vec<JsonValue> = columns
                .iter()
                .map(|c| row.remove(c).unwrap_or(JsonValue::Null))
                .collect();
            rows_arr.push(arr);
        }
        let obj = json!({
            "columns": columns,
            "num_rows": rows_arr.len(),
            "rows": rows_arr,
        });
        serde_json::to_writer(&mut out, &obj)?;
        out.write_all(b"\n")?;
    } else {
        if with_meta {
            let meta = json!({ "meta": { "columns": columns } });
            serde_json::to_writer(&mut out, &meta)?;
            out.write_all(b"\n")?;
        }
        let mut writer = JsonWriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut out);
        let mut emitted: usize = 0;
        for batch in arrow_iter.by_ref() {
            let remaining = limit
                .map(|l| l.saturating_sub(emitted))
                .unwrap_or(usize::MAX);
            if remaining == 0 {
                break;
            }
            let batch = if batch.num_rows() > remaining {
                batch.slice(0, remaining)
            } else {
                batch
            };
            writer.write(&batch)?;
            emitted += batch.num_rows();
            if limit.is_some_and(|l| emitted >= l) {
                break;
            }
        }
        writer.finish()?;
    }
    out.flush()?;
    Ok(())
}

fn cmd_execute(conn: &Connection, sql: &str, bind: Option<&str>) -> Result<()> {
    let binds = parse_bind(bind)?;
    let bind_refs = bind_refs(&binds);
    let n = conn.execute(sql, &bind_refs[..]).context("execute")?;
    emit_json(&json!({ "affected_rows": n }))
}

fn cmd_exec_file(conn: &Connection, file: &PathBuf) -> Result<()> {
    let raw =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    conn.execute_batch(&raw).context("execute_batch")?;
    emit_json(&json!({ "ok": true }))
}

fn cmd_dump(
    conn: &Connection,
    source: &str,
    columns: Option<&str>,
    where_clause: Option<&str>,
    order_by: Option<&str>,
    limit: Option<usize>,
) -> Result<()> {
    let cols = columns.unwrap_or("*");
    let from = if looks_like_path_or_url(source) {
        format!("'{}'", source.replace('\'', "''"))
    } else {
        // Bare identifier. Quote as a DuckDB identifier.
        format!("\"{}\"", source.replace('"', "\"\""))
    };
    let mut sql = format!("SELECT {cols} FROM {from}");
    if let Some(w) = where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(w);
    }
    if let Some(o) = order_by {
        sql.push_str(" ORDER BY ");
        sql.push_str(o);
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    cmd_query(conn, &sql, None, false, false, None)
}

fn looks_like_path_or_url(s: &str) -> bool {
    s.contains('/')
        || s.contains('\\')
        || s.starts_with("http://")
        || s.starts_with("https://")
        || s.starts_with("s3://")
        || s.starts_with("gs://")
        || s.ends_with(".parquet")
        || s.ends_with(".csv")
        || s.ends_with(".tsv")
        || s.ends_with(".json")
        || s.ends_with(".jsonl")
        || s.ends_with(".ndjson")
}

fn cmd_import(
    conn: &Connection,
    path: &Path,
    table: &str,
    kind: &str,
    replace: bool,
) -> Result<()> {
    if replace {
        conn.execute_batch(&format!("DROP TABLE IF EXISTS {};", quote_ident(table)))
            .context("drop table")?;
    }
    let path_lit = format!("'{}'", path.display().to_string().replace('\'', "''"));
    let select_expr = match kind.to_ascii_lowercase().as_str() {
        "parquet" => format!("SELECT * FROM read_parquet({path_lit})"),
        "csv" => format!("SELECT * FROM read_csv_auto({path_lit})"),
        "json" | "ndjson" => format!("SELECT * FROM read_json_auto({path_lit})"),
        "auto" | "" => format!("SELECT * FROM {path_lit}"),
        other => bail!("unknown --kind `{other}` (parquet|csv|json|auto)"),
    };
    let sql = format!("CREATE TABLE {} AS {select_expr};", quote_ident(table),);
    conn.execute_batch(&sql).context("import statement")?;
    let count: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {}", quote_ident(table)),
            [],
            |r| r.get(0),
        )
        .context("count after import")?;
    emit_json(&json!({
        "table": table,
        "kind": kind,
        "source": path.display().to_string(),
        "num_rows": count,
    }))
}

fn cmd_export(
    conn: &Connection,
    table: &str,
    path: &PathBuf,
    kind: &str,
    compression: &str,
) -> Result<()> {
    let path_lit = format!("'{}'", path.display().to_string().replace('\'', "''"));
    let copy_opts = match kind.to_ascii_lowercase().as_str() {
        "parquet" => format!(
            "(FORMAT 'parquet', COMPRESSION '{}')",
            compression.replace('\'', "''")
        ),
        "csv" => "(FORMAT 'csv', HEADER TRUE)".to_string(),
        "json" | "ndjson" => "(FORMAT 'json')".to_string(),
        other => bail!("unknown --kind `{other}` (parquet|csv|json)"),
    };
    let sql = format!("COPY {} TO {path_lit} {copy_opts};", quote_ident(table),);
    conn.execute_batch(&sql).context("export statement")?;
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    emit_json(&json!({
        "table": table,
        "kind": kind,
        "path": path.display().to_string(),
        "file_size": size,
    }))
}

fn cmd_tables(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT table_name, table_schema FROM information_schema.tables \
         WHERE table_schema NOT IN ('pg_catalog','information_schema') \
         ORDER BY table_schema, table_name",
    )?;
    let mut rows = stmt.query([])?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let schema: String = row.get(1)?;
        serde_json::to_writer(&mut out, &json!({ "name": name, "schema": schema }))?;
        out.write_all(b"\n")?;
    }
    Ok(())
}

fn cmd_schema(conn: &Connection, table: &str) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT column_name, data_type, is_nullable, column_default, ordinal_position \
         FROM information_schema.columns \
         WHERE table_name = ? \
         ORDER BY ordinal_position",
    )?;
    let mut rows = stmt.query([table])?;
    let mut cols: Vec<JsonValue> = Vec::new();
    while let Some(row) = rows.next()? {
        cols.push(json!({
            "name": row.get::<_, String>(0)?,
            "type": row.get::<_, String>(1)?,
            "nullable": row.get::<_, String>(2)? == "YES",
            "default": row.get::<_, Option<String>>(3)?,
            "ordinal_position": row.get::<_, i32>(4)?,
        }));
    }
    if cols.is_empty() {
        bail!("table `{table}` not found in current database");
    }
    let row_count: Option<i64> = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {}", quote_ident(table)),
            [],
            |r| r.get(0),
        )
        .ok();
    emit_json(&json!({
        "table": table,
        "num_rows": row_count,
        "columns": cols,
    }))
}

fn cmd_inspect(conn: &Connection, db_path: Option<&std::path::Path>) -> Result<()> {
    let version: String = conn
        .query_row("SELECT version()", [], |r| r.get(0))
        .context("version()")?;
    let db_size = db_path
        .and_then(|p| std::fs::metadata(p).ok().map(|m| m.len()))
        .unwrap_or(0);
    let mut stmt =
        conn.prepare("SELECT database_name, type FROM duckdb_databases() ORDER BY database_name")?;
    let mut rows = stmt.query([])?;
    let mut dbs: Vec<JsonValue> = Vec::new();
    while let Some(row) = rows.next()? {
        dbs.push(json!({
            "name": row.get::<_, String>(0)?,
            "type": row.get::<_, String>(1)?,
        }));
    }
    emit_json(&json!({
        "version": version,
        "file": db_path.map(|p| p.display().to_string()),
        "file_size": db_size,
        "databases": dbs,
    }))
}

fn cmd_ping(conn: &Connection) -> Result<()> {
    let n: i32 = conn
        .query_row("SELECT 1", [], |r| r.get(0))
        .context("SELECT 1")?;
    println!("ok ({n})");
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* helpers                                                                   */
/* ------------------------------------------------------------------------- */

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

#[allow(dead_code)]
fn _force_link() -> BufReader<&'static [u8]> {
    BufReader::new(&[])
}
#[allow(dead_code)]
fn _force_bufread<R: BufRead>(_: R) {}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── parse_bind ──────────────────────────────────────────────────

    #[test]
    fn parse_bind_none_empty() {
        assert!(parse_bind(None).unwrap().is_empty());
    }

    #[test]
    fn parse_bind_blank_string_empty() {
        assert!(parse_bind(Some("")).unwrap().is_empty());
        assert!(parse_bind(Some("   ")).unwrap().is_empty());
    }

    #[test]
    fn parse_bind_null_treated_as_empty() {
        // The impl maps JSON null → empty Vec (defensive against `--bind null`).
        assert!(parse_bind(Some("null")).unwrap().is_empty());
    }

    #[test]
    fn parse_bind_array_of_scalars() {
        let v = parse_bind(Some(r#"[1, "two", true, null]"#)).unwrap();
        assert_eq!(v.len(), 4);
        assert!(matches!(v[0], Value::BigInt(1)));
        assert!(matches!(v[1], Value::Text(ref s) if s == "two"));
        assert!(matches!(v[2], Value::Boolean(true)));
        assert!(matches!(v[3], Value::Null));
    }

    #[test]
    fn parse_bind_non_array_rejected() {
        let err = parse_bind(Some(r#"{"k":1}"#)).unwrap_err();
        assert!(format!("{err}").contains("array"));
    }

    #[test]
    fn parse_bind_invalid_json_errors() {
        let err = parse_bind(Some("not json")).unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("parsing"));
    }

    // ─── json_to_duckval ─────────────────────────────────────────────

    #[test]
    fn json_to_duckval_null() {
        assert!(matches!(json_to_duckval(JsonValue::Null), Value::Null));
    }

    #[test]
    fn json_to_duckval_bool() {
        assert!(matches!(json_to_duckval(json!(true)), Value::Boolean(true)));
        assert!(matches!(
            json_to_duckval(json!(false)),
            Value::Boolean(false)
        ));
    }

    #[test]
    fn json_to_duckval_positive_int_is_bigint() {
        // i64::as_i64 wins for positive ints in safe range.
        assert!(matches!(json_to_duckval(json!(42)), Value::BigInt(42)));
        assert!(matches!(json_to_duckval(json!(-5)), Value::BigInt(-5)));
    }

    #[test]
    fn json_to_duckval_large_unsigned_is_ubigint() {
        // Value above i64::MAX falls through as_i64 → as_u64.
        let big: u64 = i64::MAX as u64 + 1;
        match json_to_duckval(json!(big)) {
            Value::UBigInt(u) => assert_eq!(u, big),
            other => panic!("expected UBigInt, got {other:?}"),
        }
    }

    #[test]
    fn json_to_duckval_float_is_double() {
        match json_to_duckval(json!(2.5)) {
            Value::Double(f) => assert_eq!(f, 2.5),
            other => panic!("expected Double, got {other:?}"),
        }
    }

    #[test]
    fn json_to_duckval_string_is_text() {
        match json_to_duckval(json!("hi")) {
            Value::Text(s) => assert_eq!(s, "hi"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn json_to_duckval_array_serialized_to_text() {
        // Container types get serialized so the bind survives unchanged.
        match json_to_duckval(json!([1, 2, 3])) {
            Value::Text(s) => assert_eq!(s, "[1,2,3]"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn json_to_duckval_object_serialized_to_text() {
        match json_to_duckval(json!({"k": 1})) {
            Value::Text(s) => assert_eq!(s, "{\"k\":1}"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    // ─── looks_like_path_or_url ──────────────────────────────────────

    #[test]
    fn looks_like_path_or_url_positive_cases() {
        assert!(looks_like_path_or_url("./local.parquet"));
        assert!(looks_like_path_or_url("/abs/path.csv"));
        assert!(looks_like_path_or_url("relative/data.json"));
        assert!(looks_like_path_or_url("C:\\windows\\file.tsv"));
        assert!(looks_like_path_or_url("https://x.com/d.parquet"));
        assert!(looks_like_path_or_url("http://x.com/d.csv"));
        assert!(looks_like_path_or_url("s3://bucket/key.csv"));
        assert!(looks_like_path_or_url("gs://bucket/key"));
        assert!(looks_like_path_or_url("foo.ndjson"));
        assert!(looks_like_path_or_url("foo.jsonl"));
    }

    #[test]
    fn looks_like_path_or_url_negative_cases() {
        // Bare table name with no '/', no '\', no extension.
        assert!(!looks_like_path_or_url("my_table"));
        assert!(!looks_like_path_or_url("schema_dot_table"));
        assert!(!looks_like_path_or_url(""));
        assert!(!looks_like_path_or_url("SELECT"));
    }

    // ─── quote_ident ─────────────────────────────────────────────────

    #[test]
    fn quote_ident_wraps_in_double_quotes() {
        assert_eq!(quote_ident("users"), "\"users\"");
    }

    #[test]
    fn quote_ident_doubles_internal_double_quotes() {
        // SQL identifier escaping: " → ""
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn quote_ident_preserves_dots_spaces_unicode() {
        // Identifier quoting doesn't touch anything other than embedded `"`.
        assert_eq!(quote_ident("my.schema"), "\"my.schema\"");
        assert_eq!(quote_ident("with space"), "\"with space\"");
        assert_eq!(quote_ident("日本語"), "\"日本語\"");
    }

    #[test]
    fn quote_ident_empty_string_still_quoted() {
        assert_eq!(quote_ident(""), "\"\"");
    }

    // ─── bind_refs ───────────────────────────────────────────────────

    #[test]
    fn bind_refs_count_matches_input() {
        let binds = vec![Value::BigInt(1), Value::Text("x".into()), Value::Null];
        let refs = bind_refs(&binds);
        assert_eq!(refs.len(), 3);
    }

    // ─── valref_to_json (via in-memory query) ────────────────────────

    #[test]
    fn valref_to_json_scalar_round_trip() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn
            .prepare("SELECT 1::INTEGER, 'hi'::VARCHAR, TRUE, NULL, 2.5::DOUBLE")
            .unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let arr = row_to_array(row, 5).unwrap();
        assert_eq!(arr[0], json!(1));
        assert_eq!(arr[1], json!("hi"));
        assert_eq!(arr[2], json!(true));
        assert_eq!(arr[3], JsonValue::Null);
        assert_eq!(arr[4], json!(2.5));
    }

    #[test]
    fn valref_to_json_bigint_emits_number() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT 9999999999::BIGINT").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let v = valref_to_json(row.get_ref(0).unwrap());
        assert_eq!(v, json!(9999999999i64));
    }

    #[test]
    fn valref_to_json_blob_base64_prefixed() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT 'abc'::BLOB").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let v = valref_to_json(row.get_ref(0).unwrap());
        let s = v.as_str().unwrap();
        assert!(s.starts_with("base64:"));
        let decoded = B64.decode(s.strip_prefix("base64:").unwrap()).unwrap();
        assert_eq!(decoded, b"abc");
    }

    #[test]
    fn row_to_json_named_columns() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT 7 AS a, 'x' AS b").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let names = vec!["a".to_string(), "b".to_string()];
        let v = row_to_json(row, &names).unwrap();
        assert_eq!(v["a"], json!(7));
        assert_eq!(v["b"], json!("x"));
    }

    #[test]
    fn looks_like_path_or_url_parquet_extension() {
        assert!(looks_like_path_or_url("data/file.parquet"));
        assert!(looks_like_path_or_url("./relative.csv"));
    }

    #[test]
    fn looks_like_path_or_url_tsv_extension() {
        assert!(looks_like_path_or_url("sheet.tsv"));
    }

    #[test]
    fn json_to_duckval_zero() {
        assert!(matches!(json_to_duckval(json!(0)), Value::BigInt(0)));
    }

    #[test]
    fn parse_bind_nested_array_serialized() {
        let v = parse_bind(Some("[[1,2]]")).unwrap();
        assert_eq!(v.len(), 1);
        // Inner array element is serialized via json_to_duckval → Text.
        assert!(matches!(&v[0], Value::Text(s) if s == "[1,2]"));
    }

    #[test]
    fn quote_ident_multiple_embedded_quotes() {
        assert_eq!(quote_ident("a\"b\"c"), "\"a\"\"b\"\"c\"");
    }

    #[test]
    fn valref_to_json_boolean_column() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT TRUE AS flag").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(valref_to_json(row.get_ref(0).unwrap()), json!(true));
    }

    #[test]
    fn bind_refs_empty_input() {
        assert!(bind_refs(&[]).is_empty());
    }

    #[test]
    fn valref_to_json_tinyint_and_utinyint() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn
            .prepare("SELECT CAST(5 AS TINYINT) AS t, CAST(6 AS UTINYINT) AS u")
            .unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(valref_to_json(row.get_ref(0).unwrap()), json!(5));
        assert_eq!(valref_to_json(row.get_ref(1).unwrap()), json!(6));
    }

    #[test]
    fn valref_to_json_decimal_as_string() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn
            .prepare("SELECT CAST(1.25 AS DECIMAL(10,2)) AS d")
            .unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let v = valref_to_json(row.get_ref(0).unwrap());
        assert_eq!(v.as_str().unwrap(), "1.25");
    }

    #[test]
    fn valref_to_json_interval_struct() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn
            .prepare("SELECT INTERVAL '1' MONTH + INTERVAL '2' DAY AS iv")
            .unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let v = valref_to_json(row.get_ref(0).unwrap());
        assert_eq!(v["months"], 1);
        assert_eq!(v["days"], 2);
    }

    #[test]
    fn json_to_duckval_negative_int() {
        assert!(matches!(json_to_duckval(json!(-9)), Value::BigInt(-9)));
    }

    #[test]
    fn looks_like_path_or_url_http_scheme() {
        assert!(looks_like_path_or_url("http://example.com/x.csv"));
        // Bare table names without '/', '\', or known extensions are not paths.
        assert!(!looks_like_path_or_url("my_table"));
    }

    #[test]
    fn row_to_array_column_count_matches() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT 1, 2, 3").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let arr = row_to_array(row, 3).unwrap();
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn quote_ident_triple_embedded_quote() {
        assert_eq!(quote_ident("a\"b\"c"), "\"a\"\"b\"\"c\"");
    }

    #[test]
    fn parse_bind_float_becomes_double() {
        let v = parse_bind(Some("[2.5]")).unwrap();
        assert!(matches!(&v[0], Value::Double(f) if *f == 2.5));
    }

    #[test]
    fn valref_to_json_smallint() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT CAST(100 AS SMALLINT) AS s").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(valref_to_json(row.get_ref(0).unwrap()), json!(100));
    }

    #[test]
    fn valref_to_json_varchar_text() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT 'hello'::VARCHAR AS s").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(valref_to_json(row.get_ref(0).unwrap()), json!("hello"));
    }

    #[test]
    fn valref_to_json_null_column() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT NULL::INTEGER AS n").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(valref_to_json(row.get_ref(0).unwrap()), JsonValue::Null);
    }

    #[test]
    fn json_to_duckval_true_false() {
        assert!(matches!(json_to_duckval(json!(true)), Value::Boolean(true)));
        assert!(matches!(
            json_to_duckval(json!(false)),
            Value::Boolean(false)
        ));
    }

    #[test]
    fn looks_like_path_or_url_windows_backslash() {
        assert!(looks_like_path_or_url(r"C:\data\file.csv"));
    }

    #[test]
    fn parse_bind_bool_values() {
        let v = parse_bind(Some("[true, false]")).unwrap();
        assert!(matches!(&v[0], Value::Boolean(true)));
        assert!(matches!(&v[1], Value::Boolean(false)));
    }

    #[test]
    fn row_to_json_multiple_columns() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT 1 AS a, 'x' AS b").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let v = row_to_json(row, &["a".into(), "b".into()]).unwrap();
        assert_eq!(v["a"], json!(1));
        assert_eq!(v["b"], json!("x"));
    }

    #[test]
    fn quote_ident_backslash_not_special() {
        assert_eq!(quote_ident("a\\b"), "\"a\\b\"");
    }

    #[test]
    fn valref_to_json_uint_column() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT CAST(7 AS UINTEGER) AS u").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(valref_to_json(row.get_ref(0).unwrap()), json!(7));
    }

    #[test]
    fn valref_to_json_float_column() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT CAST(1.5 AS FLOAT) AS f").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(valref_to_json(row.get_ref(0).unwrap()), json!(1.5));
    }

    #[test]
    fn looks_like_path_or_url_https() {
        assert!(looks_like_path_or_url("https://cdn.example/data.parquet"));
    }

    #[test]
    fn parse_bind_empty_array() {
        assert!(parse_bind(Some("[]")).unwrap().is_empty());
    }

    #[test]
    fn quote_ident_empty_string() {
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn valref_to_json_date32_prefix() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT DATE '2024-06-15' AS d").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let v = valref_to_json(row.get_ref(0).unwrap());
        assert!(v.as_str().unwrap().starts_with("date32:"));
    }

    #[test]
    fn json_to_duckval_string_becomes_text() {
        assert!(matches!(json_to_duckval(json!("hi")), Value::Text(s) if s == "hi"));
    }

    #[test]
    fn valref_to_json_usmallint() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT CAST(9 AS USMALLINT) AS u").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(valref_to_json(row.get_ref(0).unwrap()), json!(9));
    }

    #[test]
    fn valref_to_json_hugeint_as_string() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn
            .prepare("SELECT CAST(999999999999999999 AS HUGEINT) AS h")
            .unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert!(valref_to_json(row.get_ref(0).unwrap()).is_string());
    }

    #[test]
    fn looks_like_path_or_url_file_scheme() {
        assert!(looks_like_path_or_url("file:///tmp/x.csv"));
    }

    #[test]
    fn quote_ident_dot_in_name() {
        assert_eq!(quote_ident("schema.table"), "\"schema.table\"");
    }

    #[test]
    fn row_to_array_single_column() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT 42").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row_to_array(row, 1).unwrap(), vec![json!(42)]);
    }

    #[test]
    fn json_to_duckval_array_serializes_text() {
        let v = json_to_duckval(json!([1, 2]));
        assert!(matches!(v, Value::Text(s) if s == "[1,2]"));
    }

    #[test]
    fn looks_like_path_or_url_relative_dot_slash() {
        assert!(looks_like_path_or_url("./data.parquet"));
    }

    #[test]
    fn valref_to_json_real_as_float() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT CAST(1.25 AS REAL) AS r").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(valref_to_json(row.get_ref(0).unwrap()), json!(1.25));
    }

    #[test]
    fn json_to_duckval_zero_int() {
        assert!(matches!(json_to_duckval(json!(0)), Value::BigInt(0)));
    }

    #[test]
    fn looks_like_path_or_url_s3_scheme() {
        assert!(looks_like_path_or_url("s3://bucket/key.parquet"));
    }

    #[test]
    fn parse_bind_single_int() {
        assert!(matches!(
            parse_bind(Some("[7]")).unwrap()[0],
            Value::BigInt(7)
        ));
    }

    #[test]
    fn quote_ident_space_in_name() {
        assert_eq!(quote_ident("col name"), "\"col name\"");
    }

    #[test]
    fn row_to_json_single_column() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT 3 AS x").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row_to_json(row, &["x".into()]).unwrap()["x"], json!(3));
    }

    #[test]
    fn bind_refs_single_element() {
        let v = parse_bind(Some("[1]")).unwrap();
        assert_eq!(bind_refs(&v).len(), 1);
    }

    #[test]
    fn looks_like_path_or_url_not_plain_identifier() {
        assert!(!looks_like_path_or_url("my_table"));
    }

    #[test]
    fn json_to_duckval_large_u64_ubigint() {
        assert!(matches!(
            json_to_duckval(json!(u64::MAX)),
            Value::UBigInt(_),
        ));
    }

    #[test]
    fn looks_like_path_or_url_windows_path() {
        assert!(looks_like_path_or_url(r"C:\data\file.parquet"));
    }

    #[test]
    fn parse_bind_two_ints() {
        let v = parse_bind(Some("[1,2]")).unwrap();
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn quote_ident_reserved_keyword() {
        assert_eq!(quote_ident("select"), "\"select\"");
    }

    #[test]
    fn row_to_json_two_columns() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT 1 AS a, 2 AS b").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let j = row_to_json(row, &["a".into(), "b".into()]).unwrap();
        assert_eq!(j["a"], json!(1));
        assert_eq!(j["b"], json!(2));
    }

    #[test]
    fn bind_refs_two_values() {
        let v = parse_bind(Some("[1,2]")).unwrap();
        assert_eq!(bind_refs(&v).len(), 2);
    }

    #[test]
    fn looks_like_path_or_url_csv_extension() {
        assert!(looks_like_path_or_url("/tmp/data.csv"));
    }

    // ─── parse_bind error-message contracts ──────────────────────────
    //
    // The existing tests pin happy-path types and the rejection
    // shape; these pin the user-visible error text on the two
    // structured failure paths so refactors of the message strings
    // don't silently change what scripts see.

    #[test]
    fn parse_bind_string_scalar_rejected_with_array_hint() {
        let err = parse_bind(Some("\"oops\"")).unwrap_err();
        assert!(
            format!("{err}").contains("--bind must be a JSON array"),
            "rejection must hint at the expected shape"
        );
    }

    #[test]
    fn parse_bind_bool_scalar_rejected_with_array_hint() {
        let err = parse_bind(Some("true")).unwrap_err();
        assert!(format!("{err}").contains("--bind must be a JSON array"));
    }

    #[test]
    fn parse_bind_invalid_json_surfaces_context() {
        let err = parse_bind(Some("[1,")).unwrap_err();
        // anyhow `.context("parsing --bind JSON")` must be reachable;
        // chain contains it.
        let chain: Vec<_> = err.chain().map(|c| c.to_string()).collect();
        assert!(
            chain.iter().any(|s| s.contains("parsing --bind JSON")),
            "expected `parsing --bind JSON` in chain; got {chain:?}"
        );
    }

    // ─── clap parsing — Cli top-level + Cmd subcommand routing ───────────
    // Pin the CLI surface: global flags, required positionals, default
    // values. Drift here would silently change which DuckDB SQL fires or
    // which file format is read/written by default.

    fn parse_cli(args: &[&str]) -> Result<Cli, clap::Error> {
        let mut argv = vec!["stryke-duckdb-helper"];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv)
    }

    #[test]
    fn cli_in_memory_default_when_db_flag_absent() {
        // Pin: no --db means in-memory database. A drift to a default file
        // path would silently persist queries that callers expect to be
        // ephemeral (the documented in-memory contract per main.rs:9-11).
        let cli = parse_cli(&["ping"]).expect("parse");
        assert!(cli.db.is_none(), "default must be in-memory (db=None)");
        assert!(matches!(cli.cmd, Cmd::Ping));
    }

    #[test]
    fn cli_query_requires_sql_positional() {
        let err = parse_cli(&["query"]).expect_err("missing sql");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn cli_import_default_kind_is_auto() {
        // Pin: import auto-detects format from extension. A drift to
        // explicit `parquet` would silently break CSV/JSON imports.
        let cli = parse_cli(&["import", "/tmp/data.parquet", "--table", "t"]).expect("parse");
        match cli.cmd {
            Cmd::Import { kind, replace, .. } => {
                assert_eq!(kind, "auto");
                assert!(!replace, "--replace must be opt-in (no silent drop)");
            }
            _ => panic!("expected Import"),
        }
    }

    #[test]
    fn cli_export_defaults_parquet_and_zstd_compression() {
        // Pin parquet+zstd — the high-compression DuckDB default that
        // benchmarks tend to use. Drift here would surprise round-trip
        // size measurements (csv default = far larger files).
        let cli = parse_cli(&["export", "--table", "t", "/tmp/out.parquet"]).expect("parse");
        match cli.cmd {
            Cmd::Export {
                kind, compression, ..
            } => {
                assert_eq!(kind, "parquet");
                assert_eq!(compression, "zstd");
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn cli_global_extensions_and_pragmas_accumulate() {
        // Both --extension and --pragma are repeatable globals; pin the
        // accumulate-into-Vec wiring against accidental last-wins.
        let cli = parse_cli(&[
            "-e",
            "httpfs",
            "-e",
            "aws",
            "-p",
            "memory_limit=4GB",
            "tables",
        ])
        .expect("parse");
        assert_eq!(cli.extensions, vec!["httpfs", "aws"]);
        assert_eq!(cli.pragmas, vec!["memory_limit=4GB"]);
        assert!(matches!(cli.cmd, Cmd::Tables));
    }

    #[test]
    fn cli_schema_requires_table_flag() {
        // --table is the required selector; without it the SELECT would
        // hit no table at all.
        let err = parse_cli(&["schema"]).expect_err("missing --table");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }
}
