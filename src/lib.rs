//! stryke-duckdb — DuckDB cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn duckdb__*` is a JSON-string-in /
//! JSON-string-out wrapper around `duckdb`'s sync API. stryke's FFI bridge
//! (`rust_ffi.rs::load_cdylib`) resolves these symbols at first
//! `use DuckDB`, registers each one as a stryke-callable function, and on
//! each call passes a JSON-encoded args dict and copies the returned JSON
//! into a stryke string. The cdylib's `stryke_free_cstring` export plugs
//! the returned-allocation leak the inline-FFI v1 had.
//!
//! Persistent state: `CONNS` caches one `duckdb::Connection` per
//! `(path, session, read_only)` tuple for the life of the stryke process.
//! The big functional consequence: `:memory:` databases now persist across
//! calls (the predecessor helper binary recreated `:memory:` per fork —
//! every query saw an empty database).

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use duckdb::{types::ValueRef, Connection};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde_json::{json, Map, Value};

// ── connection cache ────────────────────────────────────────────────────────

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct DbKey {
    path: String,
    session: String,
    read_only: bool,
}

type ConnHandle = Arc<Mutex<Connection>>;

static CONNS: OnceCell<Mutex<HashMap<DbKey, ConnHandle>>> = OnceCell::new();

fn conns() -> &'static Mutex<HashMap<DbKey, ConnHandle>> {
    CONNS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn key_from_opts(opts: &Value) -> DbKey {
    let path = opts
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or(":memory:")
        .to_string();
    let session = opts
        .get("session")
        .and_then(|v| v.as_str())
        .unwrap_or("_default")
        .to_string();
    let read_only = opts
        .get("read_only")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    DbKey {
        path,
        session,
        read_only,
    }
}

fn open_conn(key: &DbKey, opts: &Value) -> Result<Connection> {
    let conn = if key.path == ":memory:" {
        Connection::open_in_memory()?
    } else if key.read_only {
        let cfg = duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?;
        Connection::open_with_flags(&key.path, cfg)?
    } else {
        Connection::open(&key.path)?
    };
    apply_conn_opts(&conn, opts)?;
    Ok(conn)
}

/// Apply pragmas + extensions to a (possibly cached) connection. Extracted so
/// `with_conn` can call it on cache hits too — pre-fix the cache reused a
/// connection without re-running pragmas/extensions, so calls with different
/// opts silently shared the FIRST opts state.
fn apply_conn_opts(conn: &Connection, opts: &Value) -> Result<()> {
    // Optional `pragmas`: list of `SET name=value;` strings to run on connect.
    if let Some(arr) = opts.get("pragmas").and_then(|v| v.as_array()) {
        for p in arr {
            if let Some(s) = p.as_str() {
                conn.execute_batch(s)?;
            }
        }
    }
    // Optional `extensions`: list of names to INSTALL + LOAD. Names are
    // whitelisted to ASCII letters/digits/underscore so a caller can't smuggle
    // arbitrary SQL like `httpfs; ATTACH '/etc/passwd' AS p` via the name slot.
    // Pre-fix the name was raw-interpolated, enabling extension injection.
    if let Some(arr) = opts.get("extensions").and_then(|v| v.as_array()) {
        for ext in arr {
            if let Some(name) = ext.as_str() {
                let valid_ext_name =
                    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
                if !valid_ext_name {
                    bail!(
                        "extension name `{name}` contains invalid characters \
                         (must be ASCII alphanumeric / underscore)"
                    );
                }
                conn.execute_batch(&format!("INSTALL {0}; LOAD {0};", name))?;
            }
        }
    }
    Ok(())
}

fn with_conn<F, R>(opts: &Value, f: F) -> Result<R>
where
    F: FnOnce(&mut Connection) -> Result<R>,
{
    let key = key_from_opts(opts);
    let handle = {
        let mut map = conns().lock();
        if let Some(h) = map.get(&key) {
            Arc::clone(h)
        } else {
            let c = open_conn(&key, opts)?;
            let h = Arc::new(Mutex::new(c));
            map.insert(key.clone(), Arc::clone(&h));
            h
        }
    };
    let mut conn = handle.lock();
    // Re-apply pragmas/extensions on cache hits so opts from the CURRENT call
    // take effect. DbKey is (path, session, read_only) only — pragmas and
    // extensions are NOT part of the cache key, so a second call with
    // different pragmas previously silently ignored them.
    apply_conn_opts(&conn, opts)?;
    f(&mut conn)
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-duckdb handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── row → JSON ──────────────────────────────────────────────────────────────

fn value_ref_to_json(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Bool(b),
        ValueRef::TinyInt(n) => json!(n),
        ValueRef::SmallInt(n) => json!(n),
        ValueRef::Int(n) => json!(n),
        ValueRef::BigInt(n) => json!(n),
        ValueRef::UTinyInt(n) => json!(n),
        ValueRef::USmallInt(n) => json!(n),
        ValueRef::UInt(n) => json!(n),
        ValueRef::UBigInt(n) => json!(n),
        ValueRef::Float(n) => json!(n),
        ValueRef::Double(n) => json!(n),
        ValueRef::HugeInt(n) => Value::String(n.to_string()),
        ValueRef::Decimal(d) => Value::String(d.to_string()),
        ValueRef::Text(b) => match std::str::from_utf8(b) {
            Ok(s) => Value::String(s.to_string()),
            Err(_) => Value::String(format!("<binary text {} bytes>", b.len())),
        },
        ValueRef::Blob(b) => Value::String(format!("<blob {} bytes>", b.len())),
        ValueRef::Date32(d) => json!(d),
        ValueRef::Time64(unit, n) => json!({"unit": format!("{:?}", unit), "value": n}),
        ValueRef::Timestamp(unit, n) => json!({"unit": format!("{:?}", unit), "value": n}),
        other => Value::String(format!("{:?}", other)),
    }
}

/// `serde_json::Value` doesn't impl `duckdb::ToSql`. Map each variant to
/// a concrete `Box<dyn ToSql>` so `params_from_iter` accepts it.
fn value_to_tosql(v: &Value) -> Box<dyn duckdb::ToSql> {
    match v {
        Value::Null => Box::new(Option::<i64>::None),
        Value::Bool(b) => Box::new(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Box::new(i)
            } else if let Some(f) = n.as_f64() {
                Box::new(f)
            } else {
                Box::new(n.to_string())
            }
        }
        Value::String(s) => Box::new(s.clone()),
        // Arrays/objects: serialize as a JSON string. DuckDB's JSON type
        // accepts string casts via `::JSON`; the caller can opt in by
        // wrapping in their SQL.
        Value::Array(_) | Value::Object(_) => Box::new(v.to_string()),
    }
}

fn run_query(conn: &mut Connection, sql: &str, params: &[Value]) -> Result<Value> {
    let mut stmt = conn.prepare(sql)?;
    let boxed: Vec<Box<dyn duckdb::ToSql>> = params.iter().map(value_to_tosql).collect();
    let refs: Vec<&dyn duckdb::ToSql> = boxed.iter().map(|b| b.as_ref()).collect();
    // duckdb-1.10503 `column_count`/`column_name` panic ("statement was not
    // executed yet") if called before `stmt.query()` binds the params and
    // resolves the arrow schema. Bind first; introspect column metadata via
    // `rows.as_ref()` once the rowset is live; then iterate.
    let mut rows = stmt.query(duckdb::params_from_iter(refs))?;
    let (col_count, names): (usize, Vec<String>) = {
        let stmt_ref = rows.as_ref().expect("rows backed by a live statement");
        let c = stmt_ref.column_count();
        let n: Vec<String> = (0..c)
            .map(|i| {
                stmt_ref
                    .column_name(i)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "?".to_string())
            })
            .collect();
        (c, n)
    };
    let mut out: Vec<Value> = Vec::new();
    while let Some(row) = rows.next()? {
        let mut obj = Map::new();
        for (i, name) in names.iter().enumerate().take(col_count) {
            obj.insert(name.clone(), value_ref_to_json(row.get_ref(i)?));
        }
        out.push(Value::Object(obj));
    }
    Ok(json!({"columns": names, "rows": out}))
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn duckdb__version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

#[no_mangle]
pub extern "C" fn duckdb__ping(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let mut stmt = c.prepare("SELECT 1")?;
            let mut rows = stmt.query([])?;
            let _ = rows.next()?;
            Ok(json!({"ok": true}))
        })
    })
}

#[no_mangle]
pub extern "C" fn duckdb__inspect(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            // Pull duckdb version + database list + table counts.
            let mut info = Map::new();
            info.insert("path".to_string(), json!(key_from_opts(&v).path));
            // duckdb version
            {
                let mut s = c.prepare("SELECT version()")?;
                let mut r = s.query([])?;
                if let Some(row) = r.next()? {
                    info.insert(
                        "duckdb_version".to_string(),
                        value_ref_to_json(row.get_ref(0)?),
                    );
                }
            }
            // table counts (current schema)
            {
                let mut s =
                    c.prepare("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema()")?;
                let mut r = s.query([])?;
                if let Some(row) = r.next()? {
                    info.insert(
                        "table_count".to_string(),
                        value_ref_to_json(row.get_ref(0)?),
                    );
                }
            }
            Ok(Value::Object(info))
        })
    })
}

#[no_mangle]
pub extern "C" fn duckdb__query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = v["sql"]
            .as_str()
            .ok_or_else(|| anyhow!("missing sql"))?
            .to_string();
        let params: Vec<Value> = v["params"].as_array().cloned().unwrap_or_default();
        with_conn(&v, |c| run_query(c, &sql, &params))
    })
}

#[no_mangle]
pub extern "C" fn duckdb__execute(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = v["sql"]
            .as_str()
            .ok_or_else(|| anyhow!("missing sql"))?
            .to_string();
        let params: Vec<Value> = v["params"].as_array().cloned().unwrap_or_default();
        with_conn(&v, |c| {
            let boxed: Vec<Box<dyn duckdb::ToSql>> = params.iter().map(value_to_tosql).collect();
            let refs: Vec<&dyn duckdb::ToSql> = boxed.iter().map(|b| b.as_ref()).collect();
            let n = c.execute(&sql, duckdb::params_from_iter(refs))?;
            Ok(json!({"affected": n}))
        })
    })
}

#[no_mangle]
pub extern "C" fn duckdb__exec(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = v["sql"]
            .as_str()
            .ok_or_else(|| anyhow!("missing sql"))?
            .to_string();
        with_conn(&v, |c| {
            c.execute_batch(&sql)?;
            Ok(json!({"ok": true}))
        })
    })
}

#[no_mangle]
pub extern "C" fn duckdb__dump(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        // Validate the FROM-clause identifier exactly as import/export do —
        // `source` is interpolated raw into `SELECT * FROM {source}`, so an
        // unvalidated value is the same injection surface validate_identifier
        // closes for the other two ops.
        let source = validate_identifier(
            v["source"]
                .as_str()
                .ok_or_else(|| anyhow!("missing source"))?,
            "source",
        )?;
        let limit = v["limit"].as_i64();
        let sql = match limit {
            Some(n) => format!("SELECT * FROM {} LIMIT {}", source, n),
            None => format!("SELECT * FROM {}", source),
        };
        with_conn(&v, |c| run_query(c, &sql, &[]))
    })
}

#[no_mangle]
pub extern "C" fn duckdb__import(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let path = v["path"]
            .as_str()
            .ok_or_else(|| anyhow!("missing path"))?
            .to_string();
        let table = validate_identifier(
            v["table"]
                .as_str()
                .ok_or_else(|| anyhow!("missing table"))?,
            "table",
        )?;
        let kind = v["kind"].as_str().unwrap_or("auto");
        let reader = match kind {
            "parquet" => format!("read_parquet('{}')", path.replace('\'', "''")),
            "csv" => format!("read_csv_auto('{}')", path.replace('\'', "''")),
            "json" => format!("read_json_auto('{}')", path.replace('\'', "''")),
            _ => format!("'{}'", path.replace('\'', "''")),
        };
        let sql = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM {}",
            table, reader
        );
        with_conn(&v, |c| {
            c.execute_batch(&sql)?;
            let mut s = c.prepare(&format!("SELECT COUNT(*) FROM {}", table))?;
            let mut r = s.query([])?;
            let n: i64 = r.next()?.map(|row| row.get(0).unwrap_or(0i64)).unwrap_or(0);
            Ok(json!({"table": table, "rows": n}))
        })
    })
}

/// Validate a DuckDB identifier for safe `format!()` interpolation.
/// Pre-fix, `duckdb__import` accepted any string in `table` and concatenated
/// it raw into `CREATE OR REPLACE TABLE {table} AS SELECT * FROM ...` —
/// a payload like `users; DROP TABLE users` executed both. DuckDB
/// identifier rules: letter or `_` first; letter, digit, `_`, `$` rest.
/// `.` allowed for schema-qualified `schema.table`.
fn validate_identifier(name: &str, what: &str) -> Result<String> {
    if name.is_empty() {
        bail!("`{what}` must not be empty");
    }
    let valid_start = |c: char| c.is_ascii_alphabetic() || c == '_';
    let valid_rest = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '$';
    for (i, part) in name.split('.').enumerate() {
        if part.is_empty() {
            bail!("`{what}` has empty segment (position {i}) in `{name}`");
        }
        let mut chars = part.chars();
        let first = chars.next().expect("non-empty checked above");
        if !valid_start(first) {
            bail!("`{what}` segment `{part}` must start with letter or underscore");
        }
        for c in chars {
            if !valid_rest(c) {
                bail!("`{what}` segment `{part}` contains invalid character `{c}`");
            }
        }
    }
    Ok(name.to_string())
}

/// Quote an arbitrary string as a single DuckDB identifier — wrap it in double
/// quotes and double any embedded double quote (ANSI SQL / PostgreSQL rules,
/// verified against the engine: `"a""b"` denotes the identifier `a"b`). The
/// safe-embedding companion to `validate_identifier`: where that one accepts
/// only bare-legal names, this lets a name with spaces, keywords, or punctuation
/// go into dynamic SQL unharmed.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[no_mangle]
pub extern "C" fn duckdb__export(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = validate_identifier(
            v["table"]
                .as_str()
                .ok_or_else(|| anyhow!("missing table"))?,
            "table",
        )?;
        let path = v["path"]
            .as_str()
            .ok_or_else(|| anyhow!("missing path"))?
            .to_string();
        let kind = v["kind"].as_str().unwrap_or("parquet");
        let fmt = match kind {
            "parquet" => "FORMAT PARQUET",
            "csv" => "FORMAT CSV, HEADER",
            "json" => "FORMAT JSON",
            other => {
                return Err(anyhow!(
                    "export kind must be parquet|csv|json, got {}",
                    other
                ))
            }
        };
        let sql = format!(
            "COPY (SELECT * FROM {}) TO '{}' ({})",
            table,
            path.replace('\'', "''"),
            fmt
        );
        with_conn(&v, |c| {
            c.execute_batch(&sql)?;
            Ok(json!({"path": path, "kind": kind}))
        })
    })
}

#[no_mangle]
pub extern "C" fn duckdb__tables(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let mut s = c.prepare(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = current_schema() ORDER BY table_name",
            )?;
            let mut r = s.query([])?;
            let mut out: Vec<String> = Vec::new();
            while let Some(row) = r.next()? {
                out.push(row.get(0)?);
            }
            Ok(json!({"tables": out}))
        })
    })
}

#[no_mangle]
pub extern "C" fn duckdb__schema(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = v["table"]
            .as_str()
            .ok_or_else(|| anyhow!("missing table"))?
            .to_string();
        with_conn(&v, |c| {
            let mut s = c.prepare(
                "SELECT column_name, data_type, is_nullable \
                 FROM information_schema.columns WHERE table_name = ? \
                 AND table_schema = current_schema() ORDER BY ordinal_position",
            )?;
            let mut r = s.query([&table])?;
            let mut out: Vec<Value> = Vec::new();
            while let Some(row) = r.next()? {
                out.push(json!({
                    "name": row.get::<_, String>(0)?,
                    "type": row.get::<_, String>(1)?,
                    "nullable": row.get::<_, String>(2)? == "YES",
                }));
            }
            Ok(json!({"table": table, "columns": out}))
        })
    })
}

/// Bulk-load rows into a table via DuckDB's native `Appender` — its fastest
/// ingest path (no SQL parse per row). `rows` is an array of arrays, each a
/// full row in column order. Returns the appended row count.
#[no_mangle]
pub extern "C" fn duckdb__appender(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = validate_identifier(
            v["table"]
                .as_str()
                .ok_or_else(|| anyhow!("missing table"))?,
            "table",
        )?;
        let rows = v["rows"]
            .as_array()
            .ok_or_else(|| anyhow!("missing rows (array of column-ordered arrays)"))?
            .clone();
        with_conn(&v, |c| {
            let mut app = c.appender(&table)?;
            let mut n = 0i64;
            for row in &rows {
                let cells = row
                    .as_array()
                    .ok_or_else(|| anyhow!("each row must be an array of column values"))?;
                let boxed: Vec<Box<dyn duckdb::ToSql>> = cells.iter().map(value_to_tosql).collect();
                let refs: Vec<&dyn duckdb::ToSql> = boxed.iter().map(|b| b.as_ref()).collect();
                app.append_row(duckdb::appender_params_from_iter(refs))?;
                n += 1;
            }
            app.flush()?;
            Ok(json!({"table": table, "appended": n}))
        })
    })
}

/// Return the query plan for `sql`. `analyze => true` runs `EXPLAIN ANALYZE`
/// (executes the query and reports real timings). The plan is collected from
/// DuckDB's `explain_value` column into one text blob.
#[no_mangle]
pub extern "C" fn duckdb__explain(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = v["sql"].as_str().ok_or_else(|| anyhow!("missing sql"))?;
        let keyword = if v["analyze"].as_bool().unwrap_or(false) {
            "EXPLAIN ANALYZE "
        } else {
            "EXPLAIN "
        };
        let explain_sql = format!("{keyword}{sql}");
        with_conn(&v, |c| {
            let mut stmt = c.prepare(&explain_sql)?;
            let mut rows = stmt.query([])?;
            let mut lines = Vec::new();
            // EXPLAIN yields (explain_key, explain_value) rows; the plan text
            // is the second column.
            while let Some(row) = rows.next()? {
                if let Ok(val) = row.get::<_, String>(1) {
                    lines.push(val);
                }
            }
            Ok(json!({"plan": lines.join("\n")}))
        })
    })
}

#[no_mangle]
pub extern "C" fn duckdb__views(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let mut s = c.prepare(
                "SELECT table_name FROM information_schema.views \
                 WHERE table_schema = current_schema() ORDER BY table_name",
            )?;
            let mut r = s.query([])?;
            let mut out: Vec<String> = Vec::new();
            while let Some(row) = r.next()? {
                out.push(row.get(0)?);
            }
            Ok(json!({"views": out}))
        })
    })
}

#[no_mangle]
pub extern "C" fn duckdb__functions(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            let mut s = c.prepare(
                "SELECT DISTINCT function_name FROM duckdb_functions() ORDER BY function_name",
            )?;
            let mut r = s.query([])?;
            let mut out: Vec<String> = Vec::new();
            while let Some(row) = r.next()? {
                out.push(row.get(0)?);
            }
            Ok(json!({"functions": out}))
        })
    })
}

#[no_mangle]
pub extern "C" fn duckdb__settings(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            run_query(
                c,
                "SELECT name, value, description FROM duckdb_settings() ORDER BY name",
                &[],
            )
        })
    })
}

#[no_mangle]
pub extern "C" fn duckdb__extensions(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            run_query(
                c,
                "SELECT extension_name, loaded, installed, description \
                 FROM duckdb_extensions() ORDER BY extension_name",
                &[],
            )
        })
    })
}

#[no_mangle]
pub extern "C" fn duckdb__quote_identifier(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = v["name"].as_str().ok_or_else(|| anyhow!("missing name"))?;
        Ok(json!({ "quoted": quote_identifier(name) }))
    })
}

/// ATTACH a database file under an alias so its tables become visible as
/// `alias.table`. `path` is single-quote escaped (string literal); `alias`
/// is identifier-validated (it is interpolated as a bare name). `read_only`
/// adds `(READ_ONLY)`. Idempotent ATTACH is requested via `if_not_exists`,
/// which adds `IF NOT EXISTS`.
#[no_mangle]
pub extern "C" fn duckdb__attach(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let path = v["attach_path"]
            .as_str()
            .ok_or_else(|| anyhow!("missing attach_path"))?
            .to_string();
        let alias = validate_identifier(
            v["alias"]
                .as_str()
                .ok_or_else(|| anyhow!("missing alias"))?,
            "alias",
        )?;
        let read_only = v["attach_read_only"].as_bool().unwrap_or(false);
        let if_not_exists = v["if_not_exists"].as_bool().unwrap_or(false);
        let ine = if if_not_exists { "IF NOT EXISTS " } else { "" };
        let ro = if read_only { " (READ_ONLY)" } else { "" };
        let sql = format!(
            "ATTACH {}'{}' AS {}{}",
            ine,
            path.replace('\'', "''"),
            alias,
            ro
        );
        with_conn(&v, |c| {
            c.execute_batch(&sql)?;
            Ok(json!({"alias": alias, "attach_path": path, "read_only": read_only}))
        })
    })
}

/// DETACH a previously attached database alias. `alias` is
/// identifier-validated. `if_exists` adds `IF EXISTS` so detaching an
/// absent alias is a no-op instead of an error.
#[no_mangle]
pub extern "C" fn duckdb__detach(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let alias = validate_identifier(
            v["alias"]
                .as_str()
                .ok_or_else(|| anyhow!("missing alias"))?,
            "alias",
        )?;
        let if_exists = v["if_exists"].as_bool().unwrap_or(false);
        let ie = if if_exists { "IF EXISTS " } else { "" };
        let sql = format!("DETACH {}{}", ie, alias);
        with_conn(&v, |c| {
            c.execute_batch(&sql)?;
            Ok(json!({"alias": alias, "detached": true}))
        })
    })
}

/// COPY rows FROM a file into an existing table — DuckDB's bulk file
/// loader. Unlike `import` (which does CREATE TABLE AS), this appends into
/// a table that already exists. `table` is identifier-validated; `path` is
/// single-quote escaped. `kind` selects the format (`csv|parquet|json`,
/// default inferred from the extension by DuckDB when omitted).
#[no_mangle]
pub extern "C" fn duckdb__copy_from(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = validate_identifier(
            v["table"]
                .as_str()
                .ok_or_else(|| anyhow!("missing table"))?,
            "table",
        )?;
        let path = v["file"]
            .as_str()
            .ok_or_else(|| anyhow!("missing file"))?
            .to_string();
        let kind = v["kind"].as_str().unwrap_or("auto");
        let fmt = match kind {
            "csv" => " (FORMAT CSV, HEADER)",
            "parquet" => " (FORMAT PARQUET)",
            "json" => " (FORMAT JSON)",
            "auto" => "",
            other => {
                return Err(anyhow!(
                    "copy_from kind must be csv|parquet|json|auto, got {}",
                    other
                ))
            }
        };
        let sql = format!("COPY {} FROM '{}'{}", table, path.replace('\'', "''"), fmt);
        with_conn(&v, |c| {
            let n = c.execute(&sql, [])?;
            Ok(json!({"table": table, "path": path, "copied": n}))
        })
    })
}

/// CREATE INDEX `name` ON `table` (`columns`). All three are
/// identifier-validated (column list is an array of names). `unique` makes
/// it `CREATE UNIQUE INDEX`; `if_not_exists` adds `IF NOT EXISTS`.
#[no_mangle]
pub extern "C" fn duckdb__create_index(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = validate_identifier(
            v["name"].as_str().ok_or_else(|| anyhow!("missing name"))?,
            "index",
        )?;
        let table = validate_identifier(
            v["table"]
                .as_str()
                .ok_or_else(|| anyhow!("missing table"))?,
            "table",
        )?;
        let cols_json = v["columns"]
            .as_array()
            .ok_or_else(|| anyhow!("missing columns (array of column names)"))?;
        if cols_json.is_empty() {
            bail!("columns must name at least one column");
        }
        let mut cols: Vec<String> = Vec::with_capacity(cols_json.len());
        for c in cols_json {
            let s = c
                .as_str()
                .ok_or_else(|| anyhow!("each column must be a string"))?;
            cols.push(validate_identifier(s, "column")?);
        }
        let unique = if v["unique"].as_bool().unwrap_or(false) {
            "UNIQUE "
        } else {
            ""
        };
        let ine = if v["if_not_exists"].as_bool().unwrap_or(false) {
            "IF NOT EXISTS "
        } else {
            ""
        };
        let sql = format!(
            "CREATE {}INDEX {}{} ON {} ({})",
            unique,
            ine,
            name,
            table,
            cols.join(", ")
        );
        with_conn(&v, |c| {
            c.execute_batch(&sql)?;
            Ok(json!({"index": name, "table": table, "columns": cols}))
        })
    })
}

/// DROP an object of `kind` (`table|view|index`) named `name`. `name` is
/// identifier-validated, `kind` is matched against the allowlist. `cascade`
/// adds `CASCADE`; `if_exists` (default true) adds `IF EXISTS` so dropping a
/// missing object is a no-op.
#[no_mangle]
pub extern "C" fn duckdb__drop(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let kind = v["kind"].as_str().unwrap_or("table");
        let keyword = match kind {
            "table" => "TABLE",
            "view" => "VIEW",
            "index" => "INDEX",
            other => return Err(anyhow!("drop kind must be table|view|index, got {}", other)),
        };
        let name = validate_identifier(
            v["name"].as_str().ok_or_else(|| anyhow!("missing name"))?,
            "name",
        )?;
        let ie = if v["if_exists"].as_bool().unwrap_or(true) {
            "IF EXISTS "
        } else {
            ""
        };
        let cascade = if v["cascade"].as_bool().unwrap_or(false) {
            " CASCADE"
        } else {
            ""
        };
        let sql = format!("DROP {} {}{}{}", keyword, ie, name, cascade);
        with_conn(&v, |c| {
            c.execute_batch(&sql)?;
            Ok(json!({"dropped": name, "kind": kind}))
        })
    })
}

/// List indexes via `duckdb_indexes()` — one row per index with its name,
/// table, schema, uniqueness, and the SQL that created it.
#[no_mangle]
pub extern "C" fn duckdb__indexes(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            run_query(
                c,
                "SELECT index_name, table_name, schema_name, is_unique, sql \
                 FROM duckdb_indexes() ORDER BY schema_name, table_name, index_name",
                &[],
            )
        })
    })
}

/// List table/column constraints via `duckdb_constraints()` — PRIMARY KEY,
/// UNIQUE, CHECK, NOT NULL, FOREIGN KEY — for the current database.
#[no_mangle]
pub extern "C" fn duckdb__constraints(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| {
            run_query(
                c,
                "SELECT schema_name, table_name, constraint_type, constraint_text \
                 FROM duckdb_constraints() \
                 ORDER BY schema_name, table_name, constraint_type",
                &[],
            )
        })
    })
}

/// `PRAGMA table_info('table')` — ordinal, name, type, notnull, default,
/// and primary-key flag per column. Complements `schema` (information_schema
/// based) with the engine's native column view including `pk`. `table` is
/// identifier-validated, then single-quote escaped into the PRAGMA argument.
#[no_mangle]
pub extern "C" fn duckdb__table_info(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = validate_identifier(
            v["table"]
                .as_str()
                .ok_or_else(|| anyhow!("missing table"))?,
            "table",
        )?;
        let sql = format!("PRAGMA table_info('{}')", table.replace('\'', "''"));
        with_conn(&v, |c| run_query(c, &sql, &[]))
    })
}

/// `PRAGMA database_size` — storage stats for the attached databases:
/// database name, block size/count, used/free blocks, WAL size, and the
/// total on-disk size. Returns the rows verbatim.
#[no_mangle]
pub extern "C" fn duckdb__database_size(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        with_conn(&v, |c| run_query(c, "PRAGMA database_size", &[]))
    })
}

/// Bulk-load rows into a SUBSET of a table's columns via DuckDB's native
/// `Appender` with an explicit column list — the unspecified columns take
/// their DEFAULT (or NULL). `columns` is an array of identifier-validated
/// names; each row in `rows` is an array of values in that column order.
/// Returns the appended row count.
#[no_mangle]
pub extern "C" fn duckdb__appender_columns(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = validate_identifier(
            v["table"]
                .as_str()
                .ok_or_else(|| anyhow!("missing table"))?,
            "table",
        )?;
        let cols_json = v["columns"]
            .as_array()
            .ok_or_else(|| anyhow!("missing columns (array of column names)"))?;
        if cols_json.is_empty() {
            bail!("columns must name at least one column");
        }
        let mut cols: Vec<String> = Vec::with_capacity(cols_json.len());
        for c in cols_json {
            let s = c
                .as_str()
                .ok_or_else(|| anyhow!("each column must be a string"))?;
            cols.push(validate_identifier(s, "column")?);
        }
        let rows = v["rows"]
            .as_array()
            .ok_or_else(|| anyhow!("missing rows (array of column-ordered arrays)"))?
            .clone();
        with_conn(&v, |c| {
            let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            let mut app = c.appender_with_columns(&table, &col_refs)?;
            let mut n = 0i64;
            for row in &rows {
                let cells = row
                    .as_array()
                    .ok_or_else(|| anyhow!("each row must be an array of column values"))?;
                if cells.len() != cols.len() {
                    bail!(
                        "row has {} values but {} columns were named",
                        cells.len(),
                        cols.len()
                    );
                }
                let boxed: Vec<Box<dyn duckdb::ToSql>> = cells.iter().map(value_to_tosql).collect();
                let refs: Vec<&dyn duckdb::ToSql> = boxed.iter().map(|b| b.as_ref()).collect();
                app.append_row(duckdb::appender_params_from_iter(refs))?;
                n += 1;
            }
            app.flush()?;
            Ok(json!({"table": table, "columns": cols, "appended": n}))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_from_opts_defaults_to_memory_singleton() {
        let k = key_from_opts(&json!({}));
        assert_eq!(k.path, ":memory:");
        assert_eq!(k.session, "_default");
        assert!(!k.read_only);
    }

    #[test]
    fn key_from_opts_path_override() {
        let k = key_from_opts(&json!({"path": "/tmp/a.duckdb"}));
        assert_eq!(k.path, "/tmp/a.duckdb");
    }

    #[test]
    fn key_from_opts_session_isolates_caches() {
        // Same path + different session = different cache slot. Required
        // so two concurrent `s t` worker threads don't share a connection.
        let a = key_from_opts(&json!({"path": "/tmp/x.duckdb", "session": "worker-1"}));
        let b = key_from_opts(&json!({"path": "/tmp/x.duckdb", "session": "worker-2"}));
        assert_ne!(a, b);
    }

    #[test]
    fn key_from_opts_read_only_flag_round_trips() {
        let k = key_from_opts(&json!({"path": "/tmp/y.duckdb", "read_only": true}));
        assert!(k.read_only);
    }

    #[test]
    fn value_ref_null_and_bool() {
        assert_eq!(value_ref_to_json(ValueRef::Null), Value::Null);
        assert_eq!(value_ref_to_json(ValueRef::Boolean(true)), json!(true));
        assert_eq!(value_ref_to_json(ValueRef::Boolean(false)), json!(false));
    }

    #[test]
    fn value_ref_signed_integers() {
        assert_eq!(value_ref_to_json(ValueRef::TinyInt(-12)), json!(-12));
        assert_eq!(value_ref_to_json(ValueRef::SmallInt(300)), json!(300));
        assert_eq!(value_ref_to_json(ValueRef::Int(100_000)), json!(100_000));
        assert_eq!(
            value_ref_to_json(ValueRef::BigInt(i64::MIN)),
            json!(i64::MIN)
        );
    }

    #[test]
    fn value_ref_unsigned_integers() {
        assert_eq!(value_ref_to_json(ValueRef::UTinyInt(255)), json!(255));
        assert_eq!(value_ref_to_json(ValueRef::USmallInt(60000)), json!(60000));
        assert_eq!(
            value_ref_to_json(ValueRef::UInt(4_000_000_000)),
            json!(4_000_000_000_u32)
        );
        assert_eq!(
            value_ref_to_json(ValueRef::UBigInt(u64::MAX)),
            json!(u64::MAX)
        );
    }

    #[test]
    fn value_ref_floats() {
        assert_eq!(value_ref_to_json(ValueRef::Float(1.5)), json!(1.5));
        assert_eq!(
            value_ref_to_json(ValueRef::Double(std::f64::consts::PI)),
            json!(std::f64::consts::PI)
        );
    }

    #[test]
    fn value_ref_hugeint_stringifies() {
        // HugeInt doesn't fit JSON Number; package returns String. Stable
        // contract so downstream parsers can treat it as a decimal.
        let v = value_ref_to_json(ValueRef::HugeInt(
            170_141_183_460_469_231_731_687_303_715_884_105_727_i128,
        ));
        assert!(matches!(v, Value::String(_)));
        assert_eq!(
            v.as_str().unwrap(),
            "170141183460469231731687303715884105727"
        );
    }

    #[test]
    fn value_ref_text_utf8() {
        let s = b"hello";
        assert_eq!(value_ref_to_json(ValueRef::Text(s)), json!("hello"));
    }

    #[test]
    fn value_ref_text_non_utf8_falls_back_to_marker() {
        let bytes = &[0xFF_u8, 0xFE, 0xFD];
        let v = value_ref_to_json(ValueRef::Text(bytes));
        assert_eq!(v, json!("<binary text 3 bytes>"));
    }

    #[test]
    fn value_ref_blob_marker() {
        let bytes = &[0u8; 1024];
        let v = value_ref_to_json(ValueRef::Blob(bytes));
        assert_eq!(v, json!("<blob 1024 bytes>"));
    }

    // ── value_to_tosql round-trip ──
    // Exercises the JSON → ToSql mapping by binding values into a real
    // in-memory DuckDB and reading them back. End-to-end correctness
    // matters more than introspecting the boxed trait object.

    fn bind_and_read_back(v: &Value) -> Value {
        let conn = Connection::open_in_memory().unwrap();
        let boxed = value_to_tosql(v);
        let mut stmt = conn.prepare("SELECT ?").unwrap();
        let mut rows = stmt.query(duckdb::params![boxed.as_ref()]).unwrap();
        let row = rows.next().unwrap().unwrap();
        value_ref_to_json(row.get_ref(0).unwrap())
    }

    #[test]
    fn value_to_tosql_null_round_trip() {
        assert_eq!(bind_and_read_back(&Value::Null), Value::Null);
    }

    #[test]
    fn value_to_tosql_bool_round_trip() {
        assert_eq!(bind_and_read_back(&json!(true)), json!(true));
        assert_eq!(bind_and_read_back(&json!(false)), json!(false));
    }

    #[test]
    fn value_to_tosql_integer_round_trip() {
        assert_eq!(bind_and_read_back(&json!(42)), json!(42));
        assert_eq!(bind_and_read_back(&json!(-99)), json!(-99));
    }

    #[test]
    fn value_to_tosql_float_round_trip() {
        assert_eq!(bind_and_read_back(&json!(3.5)), json!(3.5));
    }

    #[test]
    fn value_to_tosql_string_round_trip() {
        assert_eq!(bind_and_read_back(&json!("hi")), json!("hi"));
    }

    #[test]
    fn value_to_tosql_array_serializes_as_json_string() {
        // Arrays/objects encode as JSON text — callers cast to JSON in SQL.
        let v = bind_and_read_back(&json!([1, 2, 3]));
        assert_eq!(v.as_str().unwrap(), "[1,2,3]");
    }

    #[test]
    fn value_to_tosql_object_serializes_as_json_string() {
        let v = bind_and_read_back(&json!({"a": 1}));
        assert_eq!(v.as_str().unwrap(), r#"{"a":1}"#);
    }

    // ── hand-crafted bug-class catchers ──

    #[test]
    fn value_ref_double_non_finite_collapses_to_null_silently() {
        // Bug class: silent data loss for non-finite floats. `json!(f64)` routes
        // through `serde_json::Number::from_f64`, which returns `None` for NaN /
        // ±Inf, and the `json!` macro substitutes `Value::Null`. That makes a
        // DuckDB row containing `0.0/0.0` indistinguishable from SQL NULL on
        // the stryke side. This test pins the current contract so any future
        // refactor (e.g. emitting "NaN" / "Infinity" string sentinels, or
        // panicking) is a deliberate, reviewable change — not a silent drift.
        assert_eq!(value_ref_to_json(ValueRef::Double(f64::NAN)), Value::Null);
        assert_eq!(
            value_ref_to_json(ValueRef::Double(f64::INFINITY)),
            Value::Null
        );
        assert_eq!(
            value_ref_to_json(ValueRef::Double(f64::NEG_INFINITY)),
            Value::Null
        );
        assert_eq!(value_ref_to_json(ValueRef::Float(f32::NAN)), Value::Null);
        assert_eq!(
            value_ref_to_json(ValueRef::Float(f32::INFINITY)),
            Value::Null
        );
    }

    #[test]
    fn key_from_opts_empty_string_path_is_not_memory_singleton() {
        // Bug class: missing-key vs empty-string semantic mismatch. The
        // `.unwrap_or(":memory:")` fallback fires ONLY when "path" is absent
        // or non-string — an empty `""` is a valid `&str` and falls through
        // verbatim. That means callers passing `{"path": ""}` (a common
        // foot-gun for code that builds opts from CLI args) get a DbKey with
        // an empty path, NOT the `:memory:` singleton. Pinning this guards
        // against (a) a future "helpful" coercion that silently changes the
        // cache key for existing callers, and (b) the inverse regression that
        // makes empty paths accidentally collide with the `:memory:` slot.
        let k = key_from_opts(&json!({"path": ""}));
        assert_eq!(k.path, "");
        assert_ne!(k.path, ":memory:");
        let mem = key_from_opts(&json!({}));
        assert_ne!(k, mem);
    }

    #[test]
    fn run_query_duplicate_column_names_silently_drop_earlier_row_cells() {
        // Bug class: row-Map key collision silently discards data. `run_query`
        // returns `{"columns": [...], "rows": [{col: val, ...}]}`. The
        // `columns` array is built by index (preserves all names including
        // duplicates), but each row is a `serde_json::Map` keyed by the
        // column name string — so `SELECT 1 AS x, 2 AS x` reports two
        // columns yet ships one `x` field per row, with the LAST column's
        // value winning. Downstream stryke code that iterates rows by
        // `for col in columns: row[col]` will read the wrong value for the
        // earlier `x`. Pinning the current (buggy-but-deterministic) behavior
        // so the boss sees an explicit failure when (and only when) the row
        // shape gains disambiguation (e.g. arrayified rows, suffixed keys).
        let mut conn = Connection::open_in_memory().unwrap();
        let out = run_query(&mut conn, "SELECT 1 AS x, 2 AS x", &[]).unwrap();
        let cols = out["columns"].as_array().expect("columns array");
        assert_eq!(cols.len(), 2, "columns array preserves both duplicates");
        assert_eq!(cols[0], json!("x"));
        assert_eq!(cols[1], json!("x"));
        let rows = out["rows"].as_array().expect("rows array");
        assert_eq!(rows.len(), 1);
        let row = rows[0].as_object().expect("row is object");
        // Map collapsed to one entry — the bug class.
        assert_eq!(
            row.len(),
            1,
            "row map collapses duplicate keys (silent data loss)"
        );
        // serde_json::Map (with preserve_order) keeps the FIRST inserted
        // key but its VALUE is overwritten by the later insert. The
        // surviving value is the SECOND column's (2), not the first (1).
        assert_eq!(
            row.get("x"),
            Some(&json!(2)),
            "last-write-wins on duplicate column name"
        );
    }

    #[test]
    fn run_query_param_count_mismatch_returns_err_not_panic() {
        // Bug class: panic-on-bad-input crossing the FFI boundary. `run_query`
        // is called from `duckdb__query` which is wrapped in `catch_unwind`,
        // but a panic here would still corrupt the cached `Connection` via
        // `Mutex` poisoning (`parking_lot::Mutex` does NOT poison, but a
        // panic still leaves `with_conn`'s held lock dropped mid-operation —
        // depending on duckdb-rs internal invariants the underlying
        // `duckdb::Connection` state could be partially mutated). We want a
        // clean `Result::Err` propagation: caller passes too many params for
        // the SQL's placeholder count → DuckDB errors out, `?` propagates,
        // FFI layer ships `{"error": "..."}`. A future refactor that
        // `.unwrap()`s mid-pipeline would convert this to a panic and break
        // the contract for every stryke caller.
        let mut conn = Connection::open_in_memory().unwrap();
        // 0 placeholders, 2 params supplied.
        let result = run_query(&mut conn, "SELECT 1", &[json!(99), json!("extra")]);
        assert!(
            result.is_err(),
            "param-count mismatch must surface as Err, not Ok/panic; got {:?}",
            result
        );
    }

    #[test]
    fn run_query_ddl_returns_empty_columns_without_panicking_on_expect() {
        // Bug class: `run_query` line 219 has `rows.as_ref().expect(...)`
        // — a panic site. `expect` documents the duckdb-rs contract
        // "Statement is alive while Rows lives" but a DDL/no-result query
        // is the corner case most likely to trip future API drift. If
        // someone upgrades the duckdb crate and `rows.as_ref()` starts
        // returning `None` for statements that produced no rowset, the
        // `expect` panics across the FFI boundary; `catch_unwind` turns it
        // into a generic `"stryke-duckdb handler panicked"` error losing
        // the original SQL context.
        //
        // This test pins the working contract: a CREATE TABLE through the
        // query path completes without panicking. Failure of this test
        // means duckdb-rs broke the `rows.as_ref()` invariant and the
        // `expect` must be replaced with a `Result::Err` mapping BEFORE
        // bumping the crate — surfacing this as a bug-review checkpoint
        // rather than a crash in production. We assert structural shape,
        // not exact column count, because duckdb's DDL surfaces an
        // affected-rows column whose presence is a crate-internal detail
        // that may change across point releases — what matters is no
        // panic crossing the FFI boundary.
        let mut conn = Connection::open_in_memory().unwrap();
        let result = run_query(&mut conn, "CREATE TABLE t_dummy (n INTEGER)", &[]);
        let out = result.expect("DDL through query path must not panic or err");
        assert!(
            out.get("columns").and_then(|c| c.as_array()).is_some(),
            "result shape: columns array always present"
        );
        assert!(
            out.get("rows").and_then(|r| r.as_array()).is_some(),
            "result shape: rows array always present"
        );
    }

    #[test]
    fn value_to_tosql_u64_above_i64_max_loses_precision_round_trip() {
        // Bug class: silent integer precision loss on parameter binding.
        // JSON allows arbitrary-precision integers; `value_to_tosql` tries
        // `as_i64()` first (fails for any u64 > i64::MAX), then falls through
        // to `as_f64()` which silently truncates to ~15-17 significant digits.
        // u64::MAX = 18446744073709551615 round-trips as 1.8446744073709552e19.
        // This corrupts Discord snowflakes, Twitter IDs, Postgres bigserials
        // cast to unsigned, and any other > 2^53 identifier the user binds.
        //
        // The test pins the *current* lossy behavior so the boss sees an
        // explicit failure when (and only when) someone fixes it (e.g. by
        // routing > i64::MAX through `Box::new(n.to_string())` like HugeInt).
        // If `value_to_tosql` is later changed to round-trip u64::MAX
        // exactly, this test will fail and the fix can be reviewed
        // intentionally instead of slipping in silently.
        let v = bind_and_read_back(&json!(u64::MAX));
        // The lossy path lands in DuckDB as a Double; `value_ref_to_json`
        // turns it back into a JSON number. The exact value is NOT u64::MAX.
        let f = v
            .as_f64()
            .expect("u64::MAX is bound as a Double, not preserved as an integer");
        assert!(
            (f - u64::MAX as f64).abs() < 1.0e4,
            "round-trip should land near u64::MAX as a float, got {}",
            f
        );
        assert_ne!(
            v,
            json!(u64::MAX),
            "if this assertion fires, value_to_tosql now preserves u64 precision — \
             review the fix and update this test to assert exact equality"
        );
    }

    /// `validate_identifier` must reject SQL-injection payloads.
    /// Pre-fix, `duckdb__import` interpolated `table` raw into the
    /// `CREATE OR REPLACE TABLE {table} AS SELECT * FROM ...` SQL,
    /// letting a `table` param of `users; DROP TABLE users` execute
    /// both statements via `execute_batch`.
    #[test]
    fn validate_identifier_rejects_semicolon_drop_payload() {
        assert!(validate_identifier("users; DROP TABLE users", "table").is_err());
        assert!(validate_identifier("users -- c", "table").is_err());
        assert!(validate_identifier("\"users\"", "table").is_err());
        assert!(validate_identifier("users'", "table").is_err());
        assert!(validate_identifier("", "table").is_err());
        assert!(
            validate_identifier("1users", "table").is_err(),
            "identifier must start with letter or `_`, not digit"
        );
    }

    /// Schema-qualified names must work — DuckDB supports `schema.table`
    /// and `database.schema.table`. A blanket `.` reject would break the
    /// documented import-into-non-default-schema use case.
    #[test]
    fn validate_identifier_accepts_schema_qualified_name() {
        assert!(validate_identifier("analytics.events", "table").is_ok());
        assert!(validate_identifier("_private.t1", "table").is_ok());
        assert!(validate_identifier("a.b.c", "table").is_ok());
        assert!(validate_identifier(".x", "table").is_err());
        assert!(validate_identifier("x.", "table").is_err());
        assert!(validate_identifier("a..b", "table").is_err());
    }

    #[test]
    fn with_conn_cached_connection_reapplies_pragmas_on_subsequent_calls() {
        // FIXED: pragmas are now re-applied on every `with_conn` call, including
        // cache hits. Pre-fix the cached connection was reused without running
        // the new call's pragmas — callers that toggled session config per
        // query saw the setting silently dropped on every call after the first.
        //
        // Unique session per test invocation so the global CONNS cache from
        // other tests doesn't poison this one.
        let session = format!("test-pragma-drop-{}", std::process::id());
        let threads_a: i64 = with_conn(
            &json!({"session": session, "pragmas": ["SET threads=2"]}),
            |c| {
                let mut s = c.prepare("SELECT current_setting('threads')")?;
                let mut r = s.query([])?;
                let row = r.next()?.expect("one row");
                Ok(row.get::<_, i64>(0)?)
            },
        )
        .expect("first call applies pragma");
        assert_eq!(threads_a, 2);

        let threads_b: i64 = with_conn(
            &json!({"session": session, "pragmas": ["SET threads=7"]}),
            |c| {
                let mut s = c.prepare("SELECT current_setting('threads')")?;
                let mut r = s.query([])?;
                let row = r.next()?.expect("one row");
                Ok(row.get::<_, i64>(0)?)
            },
        )
        .expect("second call applies pragma on cached connection");
        assert_eq!(
            threads_b, 7,
            "second call's pragma must take effect on cached connection (got {threads_b})"
        );
    }

    #[test]
    fn key_from_opts_non_bool_read_only_silently_defaults_to_false() {
        // Bug class: type-coercion footgun crossing the JSON FFI boundary.
        // stryke callers building opts from string-typed CLI args / env vars
        // commonly pass `{"read_only": "true"}` (string, not bool) expecting
        // the value to be respected. `Value::as_bool()` returns `None` for
        // any non-bool JSON node, and `.unwrap_or(false)` then silently
        // routes the connection into READ-WRITE mode. Worse: the resulting
        // `DbKey` collides with the read-write cache slot, so a later call
        // with `{"read_only": false}` reuses the same handle — there's no
        // way to recover to a true read-only handle without changing the
        // session.
        //
        // Pinning the current (silently-permissive) behavior so the boss
        // sees an explicit failure if and only if someone fixes it (e.g.
        // by erroring on non-bool, or by accepting "true"/"1"/"yes").
        let k_string_true = key_from_opts(&json!({"read_only": "true"}));
        assert!(
            !k_string_true.read_only,
            "string 'true' silently coerces to false (bug); should either error or coerce to true"
        );
        let k_int_one = key_from_opts(&json!({"read_only": 1}));
        assert!(!k_int_one.read_only, "integer 1 silently coerces to false");
        let k_null = key_from_opts(&json!({"read_only": null}));
        assert!(!k_null.read_only, "JSON null silently coerces to false");
        // Sanity: same path + different `read_only` interpretation collide
        // on the same cache slot when the user meant read-only but typed
        // the string form.
        let k_meant_ro = key_from_opts(&json!({"path": "/tmp/coll.db", "read_only": "true"}));
        let k_actually_rw = key_from_opts(&json!({"path": "/tmp/coll.db", "read_only": false}));
        assert_eq!(
            k_meant_ro, k_actually_rw,
            "string-true read_only collides with rw cache key — caller cannot tell their request was downgraded"
        );
    }

    /// `apply_conn_opts` must reject extension names containing SQL-meaningful
    /// characters. Pre-fix, `INSTALL {name}; LOAD {name};` was format!-built
    /// from the raw caller string, so a name like `httpfs; ATTACH '/etc/passwd' AS p`
    /// executed three statements via `execute_batch`. Whitelist is ASCII
    /// alphanumeric + underscore — covers every real DuckDB extension name.
    #[test]
    fn apply_conn_opts_rejects_extension_injection_payloads() {
        let conn = Connection::open_in_memory().expect("memdb");
        let err1 = apply_conn_opts(
            &conn,
            &json!({"extensions": ["httpfs; ATTACH '/etc/passwd' AS p"]}),
        )
        .expect_err("semicolon in name must hard-fail");
        assert!(err1.to_string().contains("invalid characters"));

        let err2 = apply_conn_opts(&conn, &json!({"extensions": ["foo bar"]}))
            .expect_err("space in name must hard-fail");
        assert!(err2.to_string().contains("invalid characters"));

        let err3 = apply_conn_opts(&conn, &json!({"extensions": ["--comment"]}))
            .expect_err("comment-leader in name must hard-fail");
        assert!(err3.to_string().contains("invalid characters"));

        // Empty array / missing field is a no-op.
        assert!(apply_conn_opts(&conn, &json!({"extensions": []})).is_ok());
        assert!(apply_conn_opts(&conn, &json!({})).is_ok());
    }

    #[test]
    fn value_ref_temporal_types_emit_lossy_raw_encodings() {
        // Bug class: silent semantic loss on temporal columns. The DuckDB
        // DATE / TIME / TIMESTAMP types do NOT round-trip as ISO strings —
        // `value_ref_to_json` emits the raw arrow backing representation:
        //   * Date32  -> a bare i32 day-count since 1970-01-01 (line 204).
        //     `19723` is 2024-01-01, but on the stryke side it is
        //     indistinguishable from the plain integer 19723. A caller that
        //     does `row["d"] + 1` gets 19724, not "the next day", and a
        //     consumer rendering the field has no signal it is a date.
        //   * Time64 / Timestamp -> a `{"unit": "<TimeUnit Debug>", "value": n}`
        //     object whose `unit` string is the *Rust enum Debug spelling*
        //     ("Microsecond"), not a SQL keyword. Any downstream parser that
        //     matches on the unit name is coupled to that exact casing.
        //
        // Pinning the current contract so a future change to ISO-string
        // emission (or a TimeUnit Debug rename in the duckdb crate) becomes a
        // reviewable, deliberate break instead of silent data drift.
        assert_eq!(value_ref_to_json(ValueRef::Date32(19723)), json!(19723));
        assert_eq!(
            value_ref_to_json(ValueRef::Date32(i32::MIN)),
            json!(i32::MIN),
            "negative (pre-epoch) day counts pass through unclamped"
        );
        assert_eq!(
            value_ref_to_json(ValueRef::Time64(
                duckdb::types::TimeUnit::Microsecond,
                86_399_000_000
            )),
            json!({"unit": "Microsecond", "value": 86_399_000_000_i64})
        );
        assert_eq!(
            value_ref_to_json(ValueRef::Timestamp(duckdb::types::TimeUnit::Nanosecond, -1)),
            json!({"unit": "Nanosecond", "value": -1_i64}),
            "the value field is signed; pre-epoch timestamps stay negative, not wrapped to u64"
        );
    }

    #[test]
    fn value_ref_unknown_variant_stringifies_rather_than_nulling() {
        // Bug class: a catch-all match arm silently swallowing a real value as
        // NULL. `value_ref_to_json` ends in `other => Value::String(format!(
        // "{:?}", other))` (line 207). The dangerous alternative refactor is
        // `other => Value::Null`, which would make every Interval / List /
        // Struct / Map column read back as SQL NULL on the stryke side —
        // total silent data loss with no error. This pins that the fallback
        // produces a NON-null, NON-empty string so such a regression fails
        // loudly. Interval is a stable representative of the `other` arm.
        let v = value_ref_to_json(ValueRef::Interval {
            months: 14,
            days: 3,
            nanos: 500,
        });
        let s = v
            .as_str()
            .expect("unknown variant must stringify, not become Null");
        assert!(
            !s.is_empty(),
            "fallback string must carry the value, not be empty"
        );
        assert_ne!(
            v,
            Value::Null,
            "the catch-all arm must never collapse to NULL"
        );
        // The Debug rendering must actually reflect the value, not a constant
        // placeholder — a regression to `format!("{}", "?")` would also be
        // non-null and non-empty but lose all data. Anchor on a field value.
        assert!(
            s.contains("14"),
            "stringified Interval must include its month count, got {s:?}"
        );
    }

    #[test]
    fn validate_identifier_dollar_sign_position_asymmetry() {
        // Bug class: position-dependent character-class off-by-one. The
        // validator's two predicates differ by exactly the `$` character:
        //   valid_start: ascii_alphabetic | '_'        (line 424)
        //   valid_rest : ascii_alphanumeric | '_' | '$' (line 425)
        // So `$` is legal in the REST of a segment but illegal as its FIRST
        // char — matching DuckDB's own identifier grammar. A refactor that
        // accidentally unifies the two predicates (a tempting "simplification")
        // would either start accepting `$leading` (lets a generated-column
        // name like `$1` through) or stop accepting the valid `col$ext` form.
        // Pin both sides of the asymmetry plus a digit-in-rest sanity check.
        assert!(
            validate_identifier("col$ext", "table").is_ok(),
            "`$` is valid in rest position"
        );
        assert!(
            validate_identifier("_a$b$c", "table").is_ok(),
            "multiple `$` in rest position are valid"
        );
        assert!(
            validate_identifier("$leading", "table").is_err(),
            "`$` must be rejected as the first char of a segment"
        );
        // Schema-qualified: `$` is rest-only per segment, so a segment that
        // *starts* with `$` must fail even when an earlier segment is valid.
        assert!(
            validate_identifier("good.$bad", "table").is_err(),
            "per-segment start rule applies after the dot too"
        );
        assert!(
            validate_identifier("a1$.b2$", "table").is_ok(),
            "digit and `$` both valid in rest of each segment"
        );
    }

    #[test]
    fn quote_identifier_doubles_inner_quotes_and_round_trips_through_engine() {
        // Plain name still gets wrapped (so it survives even if it's a keyword).
        assert_eq!(quote_identifier("users"), "\"users\"");
        // Spaces and punctuation are fine inside the quotes, untouched.
        assert_eq!(quote_identifier("my table"), "\"my table\"");
        // A literal double quote is doubled — the ANSI/DuckDB escape.
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_identifier("\"\""), "\"\"\"\"\"\"");
        // The export returns the same string.
        let out = call_export(duckdb__quote_identifier, &json!({"name": "a\"b"}));
        assert_eq!(out["quoted"], json!("\"a\"\"b\""));
        // End to end: quoting a weird column name produces SQL DuckDB accepts,
        // and the column round-trips back to its raw form.
        let sess = json!({"path": ":memory:", "session": "test-quote-identifier"});
        let col = quote_identifier("a\"b");
        with_conn(&sess, |c| {
            c.execute_batch(&format!("CREATE OR REPLACE TABLE t({col} INTEGER)"))?;
            c.execute_batch("INSERT INTO t VALUES (42)")?;
            Ok(json!({}))
        })
        .unwrap();
        let rows = with_conn(&sess, |c| {
            run_query(c, &format!("SELECT {col} AS v FROM t"), &[])
        })
        .unwrap();
        assert_eq!(
            rows["rows"][0]["v"],
            json!(42),
            "quoted weird identifier is valid SQL"
        );
        // Missing name errors.
        let err = call_export(duckdb__quote_identifier, &json!({}));
        assert!(err.get("error").is_some(), "missing name → error envelope");
    }

    // ── appender + explain functional round-trip (embedded DuckDB) ───────────

    /// Drive a `duckdb__*` export the way stryke's bridge does and reclaim the
    /// returned CString.
    fn call_export(f: extern "C" fn(*const c_char) -> *const c_char, arg: &Value) -> Value {
        let cs = CString::new(arg.to_string()).unwrap();
        let raw = f(cs.as_ptr());
        assert!(!raw.is_null());
        let out = unsafe { CStr::from_ptr(raw) }.to_str().unwrap().to_string();
        unsafe { stryke_free_cstring(raw as *mut c_char) };
        serde_json::from_str(&out).unwrap()
    }

    /// The native Appender must bulk-load every row, and EXPLAIN must return a
    /// non-empty plan — exercised end to end against an isolated in-memory DB.
    #[test]
    fn appender_bulk_loads_and_explain_returns_plan() {
        let sess = json!({"path": ":memory:", "session": "test-appender-roundtrip"});
        with_conn(&sess, |c| {
            c.execute_batch("CREATE OR REPLACE TABLE t(id INTEGER, name VARCHAR)")?;
            Ok(())
        })
        .unwrap();

        let mut arg = sess.clone();
        arg["table"] = json!("t");
        arg["rows"] = json!([[1, "a"], [2, "b"], [3, "c"]]);
        let r = call_export(duckdb__appender, &arg);
        assert_eq!(r["appended"], 3, "appender must report 3 rows; got {r}");

        let n = with_conn(&sess, |c| {
            let mut s = c.prepare("SELECT count(*) FROM t")?;
            let mut rows = s.query([])?;
            Ok(rows.next()?.unwrap().get::<_, i64>(0)?)
        })
        .unwrap();
        assert_eq!(n, 3, "all appended rows must be visible");

        let mut earg = sess.clone();
        earg["sql"] = json!("SELECT * FROM t WHERE id > 1");
        let e = call_export(duckdb__explain, &earg);
        assert!(
            e["plan"].as_str().is_some_and(|p| !p.is_empty()),
            "EXPLAIN must return a non-empty plan; got {e}"
        );
    }

    /// Appender must reject a non-array `rows` and an injection-shaped table
    /// before touching the connection.
    #[test]
    fn appender_validates_args() {
        let v = call_export(duckdb__appender, &json!({"table": "t"}));
        assert!(v["error"].as_str().unwrap().contains("missing rows"));
        let v = call_export(
            duckdb__appender,
            &json!({"table": "t; DROP TABLE t", "rows": []}),
        );
        assert!(v["error"].is_string(), "injection table must error");
    }

    // ── new ops: attach/detach, copy_from, index DDL, drop, native
    //    catalog introspection, column-subset appender ──────────────────

    /// ATTACH a real file-backed db under an alias, write through the alias,
    /// confirm the table is visible as `alias.table`, then DETACH. Exercises
    /// the multi-database path end to end against the engine.
    #[test]
    fn attach_detach_round_trip_through_alias() {
        let dir = std::env::temp_dir();
        let file = dir.join(format!(
            "stryke-duckdb-attach-{}.duckdb",
            std::process::id()
        ));
        let file_s = file.to_str().unwrap().to_string();
        let _ = std::fs::remove_file(&file);
        let sess = json!({"path": ":memory:", "session": "test-attach-detach"});

        let mut a = sess.clone();
        a["attach_path"] = json!(file_s);
        a["alias"] = json!("ext");
        let r = call_export(duckdb__attach, &a);
        assert_eq!(
            r["alias"],
            json!("ext"),
            "attach reports the alias; got {r}"
        );

        // Create + populate a table inside the attached db, read it back.
        with_conn(&sess, |c| {
            c.execute_batch("CREATE TABLE ext.things(id INTEGER)")?;
            c.execute_batch("INSERT INTO ext.things VALUES (1),(2),(3)")?;
            Ok(())
        })
        .unwrap();
        let n = with_conn(&sess, |c| {
            let mut s = c.prepare("SELECT count(*) FROM ext.things")?;
            let mut rows = s.query([])?;
            Ok(rows.next()?.unwrap().get::<_, i64>(0)?)
        })
        .unwrap();
        assert_eq!(n, 3, "rows written through the attached alias are visible");

        let d = call_export(
            duckdb__detach,
            &json!({"path": ":memory:", "session": "test-attach-detach", "alias": "ext"}),
        );
        assert_eq!(
            d["detached"],
            json!(true),
            "detach reports success; got {d}"
        );

        let _ = std::fs::remove_file(&file);
        let _ = std::fs::remove_file(format!("{file_s}.wal"));
    }

    /// attach/detach must reject an injection-shaped alias before issuing SQL.
    #[test]
    fn attach_detach_validate_alias() {
        let v = call_export(
            duckdb__attach,
            &json!({"attach_path": "/tmp/x.db", "alias": "a; DROP TABLE t"}),
        );
        assert!(
            v["error"].is_string(),
            "injection alias must error on attach"
        );
        let v = call_export(duckdb__detach, &json!({"alias": "a\"b"}));
        assert!(
            v["error"].is_string(),
            "injection alias must error on detach"
        );
        let v = call_export(duckdb__attach, &json!({"alias": "ext"}));
        assert!(
            v["error"].as_str().unwrap().contains("missing attach_path"),
            "missing attach_path must error"
        );
    }

    /// CREATE INDEX (incl. UNIQUE), then list it via duckdb_indexes(), then
    /// DROP it — all against the engine. The DROP must remove it from the list.
    #[test]
    fn create_index_lists_then_drops() {
        let sess = json!({"path": ":memory:", "session": "test-index-ddl"});
        with_conn(&sess, |c| {
            c.execute_batch("CREATE OR REPLACE TABLE widgets(id INTEGER, sku VARCHAR)")?;
            Ok(())
        })
        .unwrap();

        let mut ci = sess.clone();
        ci["name"] = json!("idx_widgets_sku");
        ci["table"] = json!("widgets");
        ci["columns"] = json!(["sku"]);
        ci["unique"] = json!(true);
        let r = call_export(duckdb__create_index, &ci);
        assert_eq!(
            r["index"],
            json!("idx_widgets_sku"),
            "create_index reports name; got {r}"
        );

        let idx = call_export(duckdb__indexes, &sess);
        let names: Vec<String> = idx["rows"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|row| row["index_name"].as_str().map(|s| s.to_string()))
            .collect();
        assert!(
            names.iter().any(|n| n == "idx_widgets_sku"),
            "duckdb_indexes() must list the new index; got {names:?}"
        );

        let d = call_export(
            duckdb__drop,
            &json!({"path": ":memory:", "session": "test-index-ddl", "kind": "index", "name": "idx_widgets_sku"}),
        );
        assert_eq!(
            d["dropped"],
            json!("idx_widgets_sku"),
            "drop reports name; got {d}"
        );
        let idx2 = call_export(duckdb__indexes, &sess);
        let names2: Vec<String> = idx2["rows"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|row| row["index_name"].as_str().map(|s| s.to_string()))
            .collect();
        assert!(
            !names2.iter().any(|n| n == "idx_widgets_sku"),
            "dropped index must be gone; got {names2:?}"
        );
    }

    /// create_index validates the column list shape and the index name.
    #[test]
    fn create_index_validates_args() {
        let v = call_export(
            duckdb__create_index,
            &json!({"name": "i", "table": "t", "columns": []}),
        );
        assert!(
            v["error"].as_str().unwrap().contains("at least one column"),
            "empty columns must error; got {v}"
        );
        let v = call_export(
            duckdb__create_index,
            &json!({"name": "i; DROP TABLE t", "table": "t", "columns": ["c"]}),
        );
        assert!(v["error"].is_string(), "injection index name must error");
    }

    /// drop must reject an unknown kind and accept the if_exists default
    /// (dropping an absent table is a no-op, not an error).
    #[test]
    fn drop_unknown_kind_errors_and_if_exists_is_default() {
        let v = call_export(duckdb__drop, &json!({"kind": "schema", "name": "x"}));
        assert!(
            v["error"].as_str().unwrap().contains("drop kind must be"),
            "unknown drop kind must error; got {v}"
        );
        // if_exists defaults to true, so dropping a never-created table is fine.
        let v = call_export(
            duckdb__drop,
            &json!({"path": ":memory:", "session": "test-drop-default", "name": "never_existed"}),
        );
        assert_eq!(
            v["dropped"],
            json!("never_existed"),
            "default if_exists drop is a no-op success; got {v}"
        );
    }

    /// copy_from appends a CSV file's rows into an existing table. Writes a
    /// temp CSV, COPYs it in, verifies the row count, then cleans up.
    #[test]
    fn copy_from_csv_appends_into_existing_table() {
        let dir = std::env::temp_dir();
        let csv = dir.join(format!("stryke-duckdb-copyfrom-{}.csv", std::process::id()));
        let csv_s = csv.to_str().unwrap().to_string();
        std::fs::write(&csv, "id,label\n1,a\n2,b\n3,c\n").unwrap();
        let sess = json!({"path": ":memory:", "session": "test-copy-from"});
        with_conn(&sess, |c| {
            c.execute_batch("CREATE OR REPLACE TABLE loaded(id INTEGER, label VARCHAR)")?;
            Ok(())
        })
        .unwrap();

        let mut cf = sess.clone();
        cf["table"] = json!("loaded");
        cf["file"] = json!(csv_s);
        cf["kind"] = json!("csv");
        let r = call_export(duckdb__copy_from, &cf);
        assert_eq!(r["copied"], json!(3), "copy_from reports 3 rows; got {r}");

        let n = with_conn(&sess, |c| {
            let mut s = c.prepare("SELECT count(*) FROM loaded")?;
            let mut rows = s.query([])?;
            Ok(rows.next()?.unwrap().get::<_, i64>(0)?)
        })
        .unwrap();
        assert_eq!(n, 3, "all CSV rows landed in the table");

        let _ = std::fs::remove_file(&csv);
    }

    /// table_info exposes the engine's native column view including the pk
    /// flag, and database_size returns at least one stats row for :memory:.
    #[test]
    fn table_info_and_database_size_report_engine_state() {
        let sess = json!({"path": ":memory:", "session": "test-table-info"});
        with_conn(&sess, |c| {
            c.execute_batch("CREATE OR REPLACE TABLE ti(id INTEGER PRIMARY KEY, name VARCHAR)")?;
            Ok(())
        })
        .unwrap();

        let mut ti = sess.clone();
        ti["table"] = json!("ti");
        let r = call_export(duckdb__table_info, &ti);
        let rows = r["rows"].as_array().expect("table_info rows");
        assert_eq!(rows.len(), 2, "table_info lists both columns; got {r}");
        // The id column must be flagged pk (DuckDB emits pk as a truthy flag).
        let id_row = rows
            .iter()
            .find(|row| row["name"] == json!("id"))
            .expect("id column present");
        let pk = &id_row["pk"];
        assert!(
            pk == &json!(true) || pk == &json!(1) || pk.as_i64() == Some(1),
            "id column must be flagged primary key; got {pk}"
        );

        let ds = call_export(duckdb__database_size, &sess);
        assert!(
            ds["rows"].as_array().is_some_and(|a| !a.is_empty()),
            "database_size returns at least one stats row; got {ds}"
        );
    }

    /// constraints lists the PRIMARY KEY declared on a table.
    #[test]
    fn constraints_lists_primary_key() {
        let sess = json!({"path": ":memory:", "session": "test-constraints"});
        with_conn(&sess, |c| {
            c.execute_batch("CREATE OR REPLACE TABLE c_tbl(id INTEGER PRIMARY KEY, v INTEGER)")?;
            Ok(())
        })
        .unwrap();
        let r = call_export(duckdb__constraints, &sess);
        let types: Vec<String> = r["rows"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|row| row["constraint_type"].as_str().map(|s| s.to_string()))
            .collect();
        assert!(
            types.iter().any(|t| t.contains("PRIMARY KEY")),
            "constraints must surface the PRIMARY KEY; got {types:?}"
        );
    }

    /// appender_columns loads only a column subset; the unnamed column takes
    /// its DEFAULT. Row-length mismatch against the named columns must error.
    #[test]
    fn appender_columns_subset_load_and_arity_check() {
        let sess = json!({"path": ":memory:", "session": "test-appender-columns"});
        with_conn(&sess, |c| {
            c.execute_batch("CREATE OR REPLACE TABLE ac(id INTEGER, note VARCHAR DEFAULT 'def')")?;
            Ok(())
        })
        .unwrap();

        let mut ap = sess.clone();
        ap["table"] = json!("ac");
        ap["columns"] = json!(["id"]);
        ap["rows"] = json!([[1], [2], [3]]);
        let r = call_export(duckdb__appender_columns, &ap);
        assert_eq!(
            r["appended"],
            json!(3),
            "appender_columns reports 3; got {r}"
        );

        let got = with_conn(&sess, |c| {
            let mut s = c.prepare("SELECT note FROM ac WHERE id = 1")?;
            let mut rows = s.query([])?;
            Ok(rows.next()?.unwrap().get::<_, String>(0)?)
        })
        .unwrap();
        assert_eq!(got, "def", "unnamed column took its DEFAULT");

        // Row arity mismatch: 1 named column, 2-value row → error before append.
        let mut bad = sess.clone();
        bad["table"] = json!("ac");
        bad["columns"] = json!(["id"]);
        bad["rows"] = json!([[1, 2]]);
        let e = call_export(duckdb__appender_columns, &bad);
        assert!(
            e["error"].as_str().unwrap().contains("values but"),
            "row/column arity mismatch must error; got {e}"
        );
    }
}
