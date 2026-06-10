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
        let source = v["source"]
            .as_str()
            .ok_or_else(|| anyhow!("missing source"))?
            .to_string();
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
}
