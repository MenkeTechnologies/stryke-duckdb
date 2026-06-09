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

use anyhow::{anyhow, Result};
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
    // Optional `pragmas`: list of `SET name=value;` strings to run on connect.
    if let Some(arr) = opts.get("pragmas").and_then(|v| v.as_array()) {
        for p in arr {
            if let Some(s) = p.as_str() {
                conn.execute_batch(s)?;
            }
        }
    }
    // Optional `extensions`: list of names to INSTALL + LOAD.
    if let Some(arr) = opts.get("extensions").and_then(|v| v.as_array()) {
        for ext in arr {
            if let Some(name) = ext.as_str() {
                conn.execute_batch(&format!("INSTALL {0}; LOAD {0};", name))?;
            }
        }
    }
    Ok(conn)
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
    let col_count = stmt.column_count();
    let names: Vec<String> = (0..col_count)
        .map(|i| {
            stmt.column_name(i)
                .map(|s| s.to_string())
                .unwrap_or_else(|_| "?".to_string())
        })
        .collect();
    let boxed: Vec<Box<dyn duckdb::ToSql>> = params.iter().map(value_to_tosql).collect();
    let refs: Vec<&dyn duckdb::ToSql> = boxed.iter().map(|b| b.as_ref()).collect();
    let mut rows = stmt.query(duckdb::params_from_iter(refs))?;
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
        let table = v["table"]
            .as_str()
            .ok_or_else(|| anyhow!("missing table"))?
            .to_string();
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

#[no_mangle]
pub extern "C" fn duckdb__export(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = v["table"]
            .as_str()
            .ok_or_else(|| anyhow!("missing table"))?
            .to_string();
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
