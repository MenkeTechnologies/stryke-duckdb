//! Round 4 integration tests pinned the v1 helper-binary CLI contract
//! (`stryke-duckdb-helper --help`, exit codes, subcommand routing).
//!
//! v0.2.0 retired that binary in favor of an in-process cdylib loaded by
//! stryke via dlopen — there is no longer a CLI surface to contract-test.
//! The exports are exercised end-to-end by:
//!   * `t/test_duckdb.stk` — live round-trip against an in-memory db.
//!   * The `DuckDB::*` `.stk` wrappers themselves — calling a missing
//!     export fails loud at `duckdb__<op>(...)` resolution.
//!
//! This file is preserved (per repo convention: never delete test files)
//! and replaced with a single sanity test so `cargo test` stays green.

#[test]
fn cdylib_replacement_for_helper_binary_compiles() {}
