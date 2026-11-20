// The sort queries below come from `benchmarks/src/sort_tpch.rs` of Apache DataFusion, which is
// licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0
// (http://www.apache.org/licenses/LICENSE-2.0). See the NOTICE file of this crate.

//! The sort queries of DataFusion's `sort_tpch` benchmark, run with the sort of DataFusion and with
//! the MultiMerge operator in the same process, one after the other.
//!
//! The queries are those of `benchmarks/src/sort_tpch.rs` of Apache DataFusion 55.3.0, unchanged, over
//! the TPC-H `lineitem` table in Parquet. As there, every run plans the query, executes it and counts
//! the rows of the result, which are then dropped.
//!
//! ```text
//! cargo run --release --example sort_tpch -- --path DIR [--query N] [--iterations N]
//!     [--partitions N] [--batch-size N] [--mem-table] [--modes LIST] [--explain]
//!     [--output FILE.json]
//! ```
//!
//! The data can be generated with `tpchgen-cli` (`pip install tpchgen-cli`):
//! `tpchgen-cli parquet -s 1 --tables=lineitem --parts=24 --output-dir=DIR`.
//!
//! `DIR/lineitem` holds the Parquet files. With `--mem-table` the table is loaded into memory first,
//! split into `--partitions` partitions, so that the time is the time of the sort and not of the scan.
//! `--modes` runs only some of the three plans, as a comma-separated list of `datafusion`, `final`
//! and `whole` (for example to measure the memory of each one in its own process). DataFusion's own
//! options can be set through their `DATAFUSION_*` environment variables.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Instant;

use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{displayable, execute_stream};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use multimerge_datafusion::{Mode, session_state};

/// Sort queries with different characteristics (from DataFusion):
/// - Sort key with fixed length or variable length (VARCHAR)
/// - Sort key with different cardinality
/// - Different number of sort keys
/// - Different number of payload columns (thin: 1 additional column other than sort keys; wide: all
///   columns except sort keys)
///
/// Key columns, with the cardinality at scale factor 1: `l_linenumber` INTEGER, 7; `l_suppkey`
/// BIGINT, 10k; `l_orderkey` BIGINT, 1.5M; `l_comment` VARCHAR, 4.5M (about 26 characters).
const SORT_QUERIES: [(&str, &str); 11] = [
    (
        "1 key (INTEGER, 7 values) + 1 payload column",
        "SELECT l_linenumber, l_partkey FROM lineitem ORDER BY l_linenumber",
    ),
    (
        "1 key (BIGINT, 1.5M values) + 1 payload column",
        "SELECT l_orderkey, l_partkey FROM lineitem ORDER BY l_orderkey",
    ),
    (
        "1 key (VARCHAR, 4.5M values) + 1 payload column",
        "SELECT l_comment, l_partkey FROM lineitem ORDER BY l_comment",
    ),
    (
        "2 keys (BIGINT 1.5M, INTEGER 7) + 1 payload column",
        "SELECT l_orderkey, l_linenumber, l_partkey FROM lineitem ORDER BY l_orderkey, l_linenumber",
    ),
    (
        "3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M), no payload",
        "SELECT l_linenumber, l_suppkey, l_orderkey FROM lineitem ORDER BY l_linenumber, l_suppkey, l_orderkey",
    ),
    (
        "3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 1 payload column",
        "SELECT l_linenumber, l_suppkey, l_orderkey, l_partkey FROM lineitem \
         ORDER BY l_linenumber, l_suppkey, l_orderkey",
    ),
    (
        "3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 12 payload columns",
        "SELECT l_linenumber, l_suppkey, l_orderkey, \
                l_partkey, l_quantity, l_extendedprice, l_discount, l_tax, \
                l_returnflag, l_linestatus, l_shipdate, l_commitdate, \
                l_receiptdate, l_shipinstruct, l_shipmode \
         FROM lineitem ORDER BY l_linenumber, l_suppkey, l_orderkey",
    ),
    (
        "4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M), no payload",
        "SELECT l_orderkey, l_suppkey, l_linenumber, l_comment FROM lineitem \
         ORDER BY l_orderkey, l_suppkey, l_linenumber, l_comment",
    ),
    (
        "4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 1 payload column",
        "SELECT l_orderkey, l_suppkey, l_linenumber, l_comment, l_partkey FROM lineitem \
         ORDER BY l_orderkey, l_suppkey, l_linenumber, l_comment",
    ),
    (
        "4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 12 payload columns",
        "SELECT l_orderkey, l_suppkey, l_linenumber, l_comment, \
                l_partkey, l_quantity, l_extendedprice, l_discount, l_tax, \
                l_returnflag, l_linestatus, l_shipdate, l_commitdate, \
                l_receiptdate, l_shipinstruct, l_shipmode \
         FROM lineitem ORDER BY l_orderkey, l_suppkey, l_linenumber, l_comment",
    ),
    (
        "1 key (VARCHAR, 7 values) + 2 payload columns",
        "SELECT l_shipmode, l_comment, l_partkey FROM lineitem ORDER BY l_shipmode",
    ),
];

struct Options {
    path: String,
    queries: Vec<usize>,
    iterations: usize,
    partitions: usize,
    batch_size: usize,
    mem_table: bool,
    /// Which plans run: 0 = DataFusion's, 1 = final merge, 2 = whole sort.
    modes: Vec<usize>,
    explain: bool,
    output: Option<String>,
}

/// The `--modes` list: a comma-separated subset of `datafusion`, `final` and `whole`.
fn parse_modes(text: &str) -> std::result::Result<Vec<usize>, String> {
    let mut modes = Vec::new();
    for mode in text.split(',') {
        let index = match mode.trim() {
            "datafusion" => 0,
            "final" => 1,
            "whole" => 2,
            other => {
                return Err(format!(
                    "--modes: unknown mode {other} (datafusion, final, whole)"
                ));
            }
        };
        if !modes.contains(&index) {
            modes.push(index);
        }
    }
    modes.sort_unstable();
    Ok(modes)
}

fn options() -> std::result::Result<Options, String> {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let mut options = Options {
        path: String::new(),
        queries: (1..=SORT_QUERIES.len()).collect(),
        iterations: 3,
        partitions: cores,
        batch_size: 8192,
        mem_table: false,
        modes: vec![0, 1, 2],
        explain: false,
        output: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or(format!("{name} needs a value"));
        let number = |text: String, name: &str| {
            text.parse::<usize>()
                .map_err(|_| format!("{name}: not a number: {text}"))
        };
        match arg.as_str() {
            "--path" | "-p" => options.path = value("--path")?,
            "--query" | "-q" => {
                options.queries = vec![number(value("--query")?, "--query")?]
            }
            "--iterations" | "-i" => {
                options.iterations = number(value("--iterations")?, "--iterations")?
            }
            "--partitions" | "-n" => {
                options.partitions = number(value("--partitions")?, "--partitions")?
            }
            "--batch-size" | "-s" => {
                options.batch_size = number(value("--batch-size")?, "--batch-size")?
            }
            "--output" | "-o" => options.output = Some(value("--output")?),
            "--mem-table" | "-m" => options.mem_table = true,
            "--modes" => options.modes = parse_modes(&value("--modes")?)?,
            "--explain" => options.explain = true,
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if options.path.is_empty() {
        return Err(
            "--path DIR is required (DIR/lineitem holds the Parquet files)".to_string(),
        );
    }
    if options
        .queries
        .iter()
        .any(|&q| q == 0 || q > SORT_QUERIES.len())
    {
        return Err(format!("--query goes from 1 to {}", SORT_QUERIES.len()));
    }
    Ok(options)
}

/// Plans and runs `sql`, counting the rows of the result. Returns (milliseconds, rows).
async fn run(ctx: &SessionContext, sql: &str) -> Result<(f64, usize)> {
    let started = Instant::now();
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let mut stream = execute_stream(plan, ctx.task_ctx())?;
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch?.num_rows();
    }
    Ok((started.elapsed().as_secs_f64() * 1000.0, rows))
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    sorted[sorted.len() / 2]
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = options().map_err(DataFusionError::Execution)?;
    // Any DataFusion option can also be set through its DATAFUSION_* environment variable.
    let base = SessionConfig::from_env()?
        .with_target_partitions(options.partitions)
        .with_batch_size(options.batch_size);
    let config = || base.clone();
    let contexts: Vec<(&str, SessionContext)> = [
        ("datafusion", SessionContext::new_with_config(config())),
        (
            "multimerge final merge",
            SessionContext::new_with_state(session_state(config(), Mode::FinalMerge)),
        ),
        (
            "multimerge whole sort",
            SessionContext::new_with_state(session_state(config(), Mode::WholeSort)),
        ),
    ]
    .into_iter()
    .enumerate()
    .filter(|(index, _)| options.modes.contains(index))
    .map(|(_, context)| context)
    .collect();

    let lineitem = format!("{}/lineitem", options.path.trim_end_matches('/'));
    for (_, ctx) in &contexts {
        ctx.register_parquet("lineitem", &lineitem, ParquetReadOptions::default())
            .await?;
    }
    if options.mem_table {
        let ctx = &contexts[0].1;
        let started = Instant::now();
        let provider = ctx.table_provider("lineitem").await?;
        let table = Arc::new(
            MemTable::load(provider, Some(options.partitions), &ctx.state()).await?,
        );
        println!(
            "lineitem loaded into memory in {:.1} s",
            started.elapsed().as_secs_f64()
        );
        for (_, ctx) in &contexts {
            ctx.deregister_table("lineitem")?;
            ctx.register_table("lineitem", Arc::clone(&table) as _)?;
        }
    }
    let (_, rows) = run(&contexts[0].1, "SELECT l_orderkey FROM lineitem").await?;
    println!(
        "lineitem: {rows} rows; {} partitions, batches of {} rows, {} iterations, {}",
        options.partitions,
        options.batch_size,
        options.iterations,
        if options.mem_table {
            "table in memory"
        } else {
            "table read from Parquet in every run"
        }
    );

    let mut json = String::from("{\n \"queries\": [\n");
    for (position, &query) in options.queries.iter().enumerate() {
        let (what, sql) = SORT_QUERIES[query - 1];
        println!("\nQ{query}: {what}");

        let mut replaced = Vec::new();
        for (name, ctx) in &contexts {
            let plan = ctx.sql(sql).await?.create_physical_plan().await?;
            let text = displayable(plan.as_ref()).indent(false).to_string();
            replaced.push(text.contains("MultimergeSortExec"));
            if options.explain {
                println!("--- plan with {name}\n{text}");
            }
        }

        let mut times: Vec<Vec<f64>> = vec![Vec::new(); contexts.len()];
        let mut counted = Vec::new();
        for iteration in 0..options.iterations {
            for (index, (name, ctx)) in contexts.iter().enumerate() {
                let (ms, rows) = run(ctx, sql).await?;
                println!(
                    "   iteration {iteration}, {name:23}: {ms:10.1} ms, {rows} rows"
                );
                times[index].push(ms);
                counted.push(rows);
            }
        }
        if counted.iter().any(|&rows| rows != counted[0]) {
            return Err(DataFusionError::Execution(format!(
                "Q{query}: the runs returned different numbers of rows"
            )));
        }

        let medians: Vec<f64> = times.iter().map(|t| median(t)).collect();
        let reference = contexts
            .iter()
            .position(|(name, _)| *name == "datafusion")
            .map(|index| medians[index]);
        let summary = contexts
            .iter()
            .zip(&medians)
            .map(|((name, _), ms)| {
                let name = name.trim_start_matches("multimerge ");
                match reference {
                    Some(reference) if name != "datafusion" => {
                        format!("{name} {ms:.1} ms ({:.2}x)", reference / ms)
                    }
                    _ => format!("{name} {ms:.1} ms"),
                }
            })
            .collect::<Vec<_>>()
            .join(" | ");
        println!("   median: {summary}");

        let _ = write!(
            json,
            "  {{\"query\": {query}, \"what\": \"{what}\", \"rows\": {}, \"modes\": {{",
            counted[0]
        );
        for (index, (name, _)) in contexts.iter().enumerate() {
            let all = times[index]
                .iter()
                .map(|t| format!("{t:.1}"))
                .collect::<Vec<_>>()
                .join(", ");
            let comma = if index + 1 < contexts.len() { ", " } else { "" };
            let _ = write!(
                json,
                "\"{name}\": {{\"median_ms\": {:.1}, \"ms\": [{all}], \"multimerge_in_plan\": {}}}{comma}",
                medians[index], replaced[index]
            );
        }
        json.push_str(if position + 1 < options.queries.len() {
            "}},\n"
        } else {
            "}}\n"
        });
    }
    let _ = write!(
        json,
        " ],\n \"partitions\": {}, \"batch_size\": {}, \"iterations\": {}, \"mem_table\": {}\n}}\n",
        options.partitions, options.batch_size, options.iterations, options.mem_table
    );
    if let Some(output) = &options.output {
        std::fs::write(output, json)?;
        println!("\nresults in {output}");
    }
    Ok(())
}
