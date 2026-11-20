# multimerge-datafusion

A sort operator for [Apache DataFusion](https://datafusion.apache.org/) backed by the
[Adaptive Parallel MultiMerge Sort](https://github.com/fbcouto/adaptive-parallel-multimerge-sort)
engine, plugged in through a physical optimizer rule: no change to DataFusion itself.

The engine is adaptive. It finds the runs that are already in the data and merges them instead of
sorting from scratch, and on data with no order it sorts the encoded keys by radix. In a database
the input is often partly ordered — a table read in insertion order, partitions that are each
already sorted, a key that follows another — and that is where it pays most.

Status: experimental. Tested with DataFusion 55.3.0.

## Usage

```rust
use datafusion::prelude::{SessionConfig, SessionContext};
use multimerge_datafusion::{session_state, Mode};

let ctx = SessionContext::new_with_state(session_state(SessionConfig::new(), Mode::FinalMerge));
// ctx.sql("SELECT ... ORDER BY ...") now plans full sorts with MultimergeSortExec.
```

Or add the rule to a session you build yourself:

```rust
use datafusion::execution::session_state::SessionStateBuilder;
use multimerge_datafusion::{optimizer_rules, Mode};

let state = SessionStateBuilder::new()
    .with_default_features()
    .with_physical_optimizer_rules(optimizer_rules(Mode::FinalMerge))
    .build();
```

## What the rule replaces

`MultimergeSortRule` rewrites full sorts — sorts without a limit — into `MultimergeSortExec`, which
reads all its input partitions and returns one sorted partition:

| | `Mode::FinalMerge` | `Mode::WholeSort` |
| :--- | :--- | :--- |
| `SortPreservingMergeExec` over `SortExec` on every partition | DataFusion still sorts each partition; the operator replaces the merge | the operator replaces both |
| `SortPreservingMergeExec` over partitions sorted for another reason | the operator merges them | the same |
| `SortExec` over a single partition | left as it is | the operator replaces it (reading through a `CoalescePartitionsExec`) |

A sort with `fetch` (`ORDER BY ... LIMIT`) is left to DataFusion.

## How it sorts

1. **Keys.** A single primitive key column without nulls is read as an integer that preserves the
   order (including the IEEE total order of floats). Anything else goes through Arrow's row format,
   and the engine gets an 8-byte prefix of each row plus a comparator for ties.
2. **Shortcuts before the engine.** Input that is already one run is returned as it is; batches
   that are each sorted and do not overlap much are merged as ranges, by slicing, without moving
   rows; a batch that is only its integer key is sorted as plain values.
3. **The engine** sorts `(key, row)` pairs: run detection and a cache-blocked k-way merge where the
   data has order, radix where it does not.
4. **Output.** The rows are taken column by column, in the sorted order.

## Correctness

`tests/sort_correctness.rs` compares every result with DataFusion's own sort (`SortExec` and
`SortPreservingMergeExec`): the sort keys must be identical row by row, and the set of rows the
same. It covers 4 data shapes (number of rows, partitions, input and output batch sizes) and 30
orderings — ascending and descending, nulls first and last, one to three keys; integers, floats,
decimals, dates, timestamps, booleans, strings and dictionaries — for the whole sort and for the
merge of sorted partitions, and every internal option of the operator. Other tests check that
the rule replaces only full sorts, and that plans with several window functions over conflicting
orderings still pass DataFusion's plan checks.

DataFusion's own SQL test suite (sqllogictest, the 502 test files of DataFusion 55.3.0) was run
with the rule installed in the session of every test file (`scripts/sqllogictest.sh` sets it up):

| | files with a difference | differences | `EXPLAIN` output only | other |
| :--- | ---: | ---: | ---: | :--- |
| without the rule | 0 | 0 | — | — |
| `Mode::FinalMerge` | 33 | 125 | 125 | none |
| `Mode::WholeSort` | 54 | 199 | 189 | 10, below |

The `EXPLAIN` differences show the operator in place of `SortPreservingMergeExec` (and of
`SortExec`, in `WholeSort` mode), and one more line in `EXPLAIN VERBOSE` for the rule. In
`WholeSort` mode, 7 queries return rows with equal sort keys in another order, which `ORDER BY`
leaves open (the test files record the order DataFusion happens to produce); 2 queries in
`pwmj.slt` return the order their `ORDER BY` asks for where DataFusion 55.3.0 does not (fixed in
DataFusion since); and `set_variable.slt` expects the out-of-memory message of `SortExec`, while
the operator fails with its own, as it does not spill.

## Benchmarks

Measured on a Google Colab runtime — AMD EPYC 7B13, 24 vCPUs, 47 GB — with Rust 1.100 and this
crate's release profile. Speedup is DataFusion's time divided by the operator's.

### `sort_tpch`

`examples/sort_tpch.rs` runs the sort queries of DataFusion's `sort_tpch` benchmark
(`benchmarks/src/sort_tpch.rs`) three times in the same process: with DataFusion's own plan, and
with the operator in each mode.

```text
pip install tpchgen-cli
tpchgen-cli parquet -s 1 --tables=lineitem --parts=24 --output-dir=data/tpch_sf1
cargo run --release --example sort_tpch -- --path data/tpch_sf1 --mem-table --iterations 5
```

TPC-H scale factor 1, table loaded into memory first (`--mem-table`):

| Query | Sort keys | DataFusion | `FinalMerge` | `WholeSort` |
| :--- | :--- | ---: | ---: | ---: |
| Q1 | 1 key (INTEGER, 7 values) + 1 payload column | 176.6 ms | 26.6 ms (6.64x) | 90.4 ms (1.95x) |
| Q2 | 1 key (BIGINT, 1.5M values) + 1 payload column | 173.5 ms | 29.6 ms (5.86x) | 84.1 ms (2.06x) |
| Q3 | 1 key (VARCHAR, 4.5M values) + 1 payload column | 1013.7 ms | 431.4 ms (2.35x) | 331.9 ms (3.05x) |
| Q4 | 2 keys (BIGINT 1.5M, INTEGER 7) + 1 payload column | 277.4 ms | 45.5 ms (6.10x) | 90.0 ms (3.08x) |
| Q5 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M), no payload | 619.1 ms | 259.6 ms (2.38x) | 738.9 ms (0.84x) |
| Q6 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 1 payload column | 621.1 ms | 265.8 ms (2.34x) | 737.9 ms (0.84x) |
| Q7 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 12 payload columns | 1082.1 ms | 649.4 ms (1.67x) | 892.0 ms (1.21x) |
| Q8 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M), no payload | 499.1 ms | 104.5 ms (4.78x) | 160.8 ms (3.10x) |
| Q9 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 1 payload column | 500.0 ms | 105.5 ms (4.74x) | 158.5 ms (3.15x) |
| Q10 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 12 payload columns | 706.5 ms | 183.9 ms (3.84x) | 254.9 ms (2.77x) |
| Q11 | 1 key (VARCHAR, 7 values) + 2 payload columns | 310.0 ms | 51.3 ms (6.04x) | 259.1 ms (1.20x) |
| Geometric mean | | | 3.85x | 1.88x |

6,001,215 rows, 24 partitions, batches of 8192 rows, median of 5 runs, table in memory.

The same, reading the Parquet files:

| Query | Sort keys | DataFusion | `FinalMerge` | `WholeSort` |
| :--- | :--- | ---: | ---: | ---: |
| Q1 | 1 key (INTEGER, 7 values) + 1 payload column | 182.3 ms | 37.0 ms (4.93x) | 96.2 ms (1.90x) |
| Q2 | 1 key (BIGINT, 1.5M values) + 1 payload column | 159.9 ms | 34.3 ms (4.66x) | 18.4 ms (8.69x) |
| Q3 | 1 key (VARCHAR, 4.5M values) + 1 payload column | 1020.8 ms | 498.7 ms (2.05x) | 363.1 ms (2.81x) |
| Q4 | 2 keys (BIGINT 1.5M, INTEGER 7) + 1 payload column | 263.0 ms | 49.1 ms (5.36x) | 26.8 ms (9.81x) |
| Q5 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M), no payload | 485.8 ms | 268.3 ms (1.81x) | 771.5 ms (0.63x) |
| Q6 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 1 payload column | 513.8 ms | 281.7 ms (1.82x) | 754.3 ms (0.68x) |
| Q7 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 12 payload columns | 966.7 ms | 608.9 ms (1.59x) | 975.0 ms (0.99x) |
| Q8 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M), no payload | 463.0 ms | 122.0 ms (3.80x) | 187.5 ms (2.47x) |
| Q9 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 1 payload column | 475.2 ms | 128.4 ms (3.70x) | 196.8 ms (2.41x) |
| Q10 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 12 payload columns | 727.1 ms | 251.4 ms (2.89x) | 333.6 ms (2.18x) |
| Q11 | 1 key (VARCHAR, 7 values) + 2 payload columns | 337.1 ms | 85.3 ms (3.95x) | 265.2 ms (1.27x) |
| Geometric mean | | | 3.05x | 2.08x |

6,001,215 rows, 24 partitions, batches of 8192 rows, median of 5 runs, reading Parquet.

TPC-H scale factor 10, the eight queries that completed with every plan:

| Query | Sort keys | DataFusion | `FinalMerge` | `WholeSort` |
| :--- | :--- | ---: | ---: | ---: |
| Q1 | 1 key (INTEGER, 7 values) + 1 payload column | 2381.2 ms | 352.7 ms (6.75x) | 949.5 ms (2.51x) |
| Q2 | 1 key (BIGINT, 1.5M values) + 1 payload column | 1718.4 ms | 405.9 ms (4.23x) | 80.6 ms (21.32x) |
| Q4 | 2 keys (BIGINT 1.5M, INTEGER 7) + 1 payload column | 2930.8 ms | 652.7 ms (4.49x) | 157.5 ms (18.61x) |
| Q5 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M), no payload | 6936.1 ms | 3114.2 ms (2.23x) | 13655.4 ms (0.51x) |
| Q6 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 1 payload column | 6686.4 ms | 3291.5 ms (2.03x) | 13740.2 ms (0.49x) |
| Q8 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M), no payload | 5113.0 ms | 1288.1 ms (3.97x) | 2157.0 ms (2.37x) |
| Q9 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 1 payload column | 5258.5 ms | 1302.0 ms (4.04x) | 2179.8 ms (2.41x) |
| Q11 | 1 key (VARCHAR, 7 values) + 2 payload columns | 4492.1 ms | 666.5 ms (6.74x) | 3354.9 ms (1.34x) |
| Geometric mean | | | 3.98x | 2.57x |

59,986,052 rows, 24 partitions, batches of 8192 rows, median of 3 runs, table in memory. Reading
Parquet, the same eight queries: `FinalMerge` 1.67x to 6.56x (geometric mean 3.71x), `WholeSort`
0.43x to 13.62x (2.17x). The numbers of values in the key descriptions are those of SF1.

Q3, Q7 and Q10 needed more memory than this machine has with the three plans in one process. Each
plan in its own process, reading Parquet (one run; peak resident memory of the process):

| Query | DataFusion | `FinalMerge` | `WholeSort` |
| :--- | ---: | ---: | ---: |
| Q3 | 12.9 s, 5.6 GB | out of memory (over 44.6 GB) | 124.4 s, 8.5 GB |
| Q7 | 26.4 s, 15.8 GB | out of memory (over 44.6 GB) | 121.6 s, 34.4 GB |
| Q10 | 27.3 s, 24.2 GB | 2.6 s, 27.8 GB | 5.3 s, 40.7 GB |

At this scale the operator fails where DataFusion does not: `FinalMerge` runs out of memory on Q3
and Q7, and `WholeSort` takes 5 to 10 times DataFusion's time on them. The same happens with
DataFusion reading strings as `Utf8` instead of string views. The cause is not known yet.

- In `FinalMerge` mode DataFusion still sorts each of the 24 partitions; the operator replaces
  only `SortPreservingMergeExec`, which merges the sorted partitions on one thread. In these
  queries that merge takes most of DataFusion's time.
- `WholeSort` loses on Q5 and Q6, more at SF10 (0.43x to 0.51x): three key columns, the first with
  only 7 values. After the bytes that all rows share, the 8-byte prefix of these keys is mostly
  constant bytes, so most comparisons go to the full rows.
- `WholeSort` times vary by up to 30% between runs of the same binary on this machine (Q11 at SF1:
  from about 200 to 290 ms); that column is less precise than the others.
- Reading Parquet, the rows of Q2 and Q4 arrive already in the order of the first key (the files
  were written in `l_orderkey` order). The operator finds that out and only merges ranges;
  DataFusion does not know it, because the files do not declare an order.

### DataFusion's sort benchmark

`benches/sort.rs` is the sort benchmark of DataFusion (`benches/sort.rs` of DataFusion 55.3.0,
every line unchanged), with cases added for this operator over the same data:
`cargo bench --bench sort`. With `MULTIMERGE_PHASES=1` it prints where the time of the operator
goes (key encoding, preparation, engine, output).

Median of 10 Criterion samples per case. Speedups are geometric means, with the lowest and the
highest in parentheses.

The benchmark's own cases, 8 partitions, 13 data types. "Sort and merge" is DataFusion's
`SortExec` on every partition followed by `SortPreservingMergeExec`, against the operator doing
the whole sort; "merge of sorted partitions" is `SortPreservingMergeExec` alone, against the
operator merging. The operator uses 8 threads. Note that the benchmark generates each partition
already sorted: DataFusion's `SortExec` sorts it anyway, while the engine finds the runs.

| Rows | Sort and merge | Merge of sorted partitions |
| :--- | ---: | ---: |
| 100k | 2.64x (1.94x to 3.47x) | 2.26x (1.57x to 3.32x) |
| 1M | 4.36x (1.76x to 11.53x) | 3.65x (1.54x to 8.85x) |

The benchmark's `sort_axis` group: one partition of 1M rows (i64, utf8 view and a mixed tuple;
low and high cardinality; 0 to 100 extra columns), which DataFusion sorts on one thread:

| Input | Cases | Operator with 1 thread | Operator with 8 threads |
| :--- | ---: | ---: | ---: |
| sorted | 24 | 11.40x (1.99x to 150.69x) | 30.68x (8.44x to 215.99x) |
| nearly sorted | 24 | 1.18x (0.63x to 3.16x) | 5.32x (3.46x to 15.12x) |
| unsorted | 24 | 1.42x (0.80x to 3.54x) | 6.62x (3.19x to 10.84x) |
| all | 72 | 2.67x (0.63x to 150.69x) | 10.26x (3.19x to 215.99x) |

With 1 thread the operator is slower than DataFusion (below 0.95x) in 11 of the 72 cases, 9 of
them nearly sorted; the lowest is 0.63x (mixed tuple, low cardinality, nearly sorted, 5 extra
columns). With 8 threads it is faster in all 72.

## Limitations

- **Memory only.** The operator keeps its whole input in memory and reserves it from the memory
  pool. If the pool cannot grow, the query fails: it does not spill to disk as `SortExec` does, and
  it does not fall back to it.
- **No limit.** Sorts with `fetch` stay with DataFusion's operators.
- **One output partition**, produced after all the input has been read.
- **Scale.** At TPC-H SF10 (60 million rows) on a 47 GB machine, `FinalMerge` ran out of memory on
  two of the eleven `sort_tpch` queries and `WholeSort` was 5 to 10 times slower than DataFusion on
  them (see Benchmarks); not understood yet.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE): the benchmark and the queries of
the `sort_tpch` example come from Apache DataFusion.
