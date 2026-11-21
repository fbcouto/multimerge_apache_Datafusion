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
4. **Output.** The rows are taken column by column, in the sorted order. A taken string view array
   carries the list of every data buffer of the input; when those lists would grow too long (after
   a merge, or with many small batches), the strings are copied into each output batch instead.

## Correctness

`tests/sort_correctness.rs` compares every result with DataFusion's own sort (`SortExec` and
`SortPreservingMergeExec`): the sort keys must be identical row by row, and the set of rows the
same. It covers 4 data shapes (number of rows, partitions, input and output batch sizes) and 30
orderings — ascending and descending, nulls first and last, one to three keys; integers, floats,
decimals, dates, timestamps, booleans, strings and dictionaries — for the whole sort and for the
merge of sorted partitions, and every internal option of the operator. Other tests check that
the rule replaces only full sorts, that plans with several window functions over conflicting
orderings still pass DataFusion's plan checks, and that output batches of string views do not
carry the list of every data buffer of the input.

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
| Q1 | 1 key (INTEGER, 7 values) + 1 payload column | 171.2 ms | 25.7 ms (6.66x) | 84.6 ms (2.02x) |
| Q2 | 1 key (BIGINT, 1.5M values) + 1 payload column | 158.4 ms | 30.1 ms (5.26x) | 85.0 ms (1.86x) |
| Q3 | 1 key (VARCHAR, 4.5M values) + 1 payload column | 1029.6 ms | 335.4 ms (3.07x) | 322.7 ms (3.19x) |
| Q4 | 2 keys (BIGINT 1.5M, INTEGER 7) + 1 payload column | 278.0 ms | 49.3 ms (5.64x) | 90.4 ms (3.08x) |
| Q5 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M), no payload | 604.8 ms | 260.9 ms (2.32x) | 621.6 ms (0.97x) |
| Q6 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 1 payload column | 619.4 ms | 260.9 ms (2.37x) | 651.8 ms (0.95x) |
| Q7 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 12 payload columns | 1110.6 ms | 453.6 ms (2.45x) | 812.6 ms (1.37x) |
| Q8 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M), no payload | 487.2 ms | 101.2 ms (4.81x) | 155.0 ms (3.14x) |
| Q9 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 1 payload column | 492.1 ms | 105.3 ms (4.67x) | 158.4 ms (3.11x) |
| Q10 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 12 payload columns | 694.2 ms | 180.2 ms (3.85x) | 260.5 ms (2.66x) |
| Q11 | 1 key (VARCHAR, 7 values) + 2 payload columns | 294.1 ms | 51.9 ms (5.67x) | 205.4 ms (1.43x) |
| Geometric mean | | | 3.98x | 1.97x |

6,001,215 rows, 24 partitions, batches of 8192 rows, median of 5 runs, table in memory.

The same, reading the Parquet files:

| Query | Sort keys | DataFusion | `FinalMerge` | `WholeSort` |
| :--- | :--- | ---: | ---: | ---: |
| Q1 | 1 key (INTEGER, 7 values) + 1 payload column | 179.6 ms | 36.2 ms (4.96x) | 94.0 ms (1.91x) |
| Q2 | 1 key (BIGINT, 1.5M values) + 1 payload column | 156.0 ms | 33.1 ms (4.71x) | 18.9 ms (8.25x) |
| Q3 | 1 key (VARCHAR, 4.5M values) + 1 payload column | 1016.7 ms | 388.8 ms (2.61x) | 365.8 ms (2.78x) |
| Q4 | 2 keys (BIGINT 1.5M, INTEGER 7) + 1 payload column | 272.6 ms | 49.4 ms (5.52x) | 27.5 ms (9.91x) |
| Q5 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M), no payload | 490.0 ms | 278.3 ms (1.76x) | 735.2 ms (0.67x) |
| Q6 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 1 payload column | 505.8 ms | 286.9 ms (1.76x) | 694.4 ms (0.73x) |
| Q7 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 12 payload columns | 969.9 ms | 494.9 ms (1.96x) | 947.4 ms (1.02x) |
| Q8 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M), no payload | 459.5 ms | 122.8 ms (3.74x) | 190.2 ms (2.42x) |
| Q9 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 1 payload column | 473.0 ms | 129.8 ms (3.64x) | 196.6 ms (2.41x) |
| Q10 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 12 payload columns | 721.1 ms | 247.7 ms (2.91x) | 337.9 ms (2.13x) |
| Q11 | 1 key (VARCHAR, 7 values) + 2 payload columns | 328.5 ms | 83.8 ms (3.92x) | 291.1 ms (1.13x) |
| Geometric mean | | | 3.17x | 2.07x |

6,001,215 rows, 24 partitions, batches of 8192 rows, median of 5 runs, reading Parquet.

TPC-H scale factor 10, each plan in its own process, reading Parquet with the files already in the
page cache, median of 3 runs:

| Query | Sort keys | DataFusion | `FinalMerge` | `WholeSort` |
| :--- | :--- | ---: | ---: | ---: |
| Q1 | 1 key (INTEGER, 7 values) + 1 payload column | 2206.8 ms | 408.1 ms (5.41x) | 1042.4 ms (2.12x) |
| Q2 | 1 key (BIGINT, 1.5M values) + 1 payload column | 1891.4 ms | 393.8 ms (4.80x) | 175.7 ms (10.76x) |
| Q3 | 1 key (VARCHAR, 4.5M values) + 1 payload column | 13137.8 ms | 4168.6 ms (3.15x) | 5842.5 ms (2.25x) |
| Q4 | 2 keys (BIGINT 1.5M, INTEGER 7) + 1 payload column | 3430.6 ms | 629.6 ms (5.45x) | 265.9 ms (12.90x) |
| Q5 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M), no payload | 4911.4 ms | 2904.6 ms (1.69x) | 11021.9 ms (0.45x) |
| Q6 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 1 payload column | 5260.1 ms | 3107.9 ms (1.69x) | 11116.0 ms (0.47x) |
| Q7 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 12 payload columns | 14207.0 ms | see below | 14295.8 ms (0.99x) |
| Q8 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M), no payload | 4755.7 ms | 1410.7 ms (3.37x) | 1802.0 ms (2.64x) |
| Q9 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 1 payload column | 4939.4 ms | 1462.3 ms (3.38x) | 1877.6 ms (2.63x) |
| Q10 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 12 payload columns | 8376.9 ms | 2377.7 ms (3.52x) | 3068.2 ms (2.73x) |
| Q11 | 1 key (VARCHAR, 7 values) + 2 payload columns | 4621.6 ms | 914.2 ms (5.06x) | 2984.0 ms (1.55x) |
| Geometric mean | | | 3.48x | 2.13x |

59,986,052 rows, 24 partitions, batches of 8192 rows. The numbers of values in the key
descriptions are those of SF1. The geometric mean of `FinalMerge` leaves Q7 out.

Peak resident memory of each of those processes, in GB:

| Query | DataFusion | `FinalMerge` | `WholeSort` |
| :--- | ---: | ---: | ---: |
| Q1 | 1.0 | 1.5 | 3.0 |
| Q2 | 1.3 | 1.8 | 1.6 |
| Q3 | 5.8 | 11.4 | 7.9 |
| Q4 | 2.5 | 4.7 | 2.6 |
| Q5 | 3.1 | 7.5 | 4.5 |
| Q6 | 3.6 | 8.4 | 5.8 |
| Q7 | 11.7 | 38.8 in one run; stopped at 41.4 in three | 27.1 |
| Q8 | 8.1 | 13.6 | 10.1 |
| Q9 | 8.6 | 14.4 | 11.2 |
| Q10 | 16.6 | 28.7 | 32.4 |
| Q11 | 4.4 | 7.2 | 9.3 |

The operator needs more memory than DataFusion's plan, up to about three times as much: it keeps
its whole input until the output is built, keeps a concatenated copy of the columns while it builds
the output (string views whose rows are copied excepted), and builds the output in full before
returning the first batch (DataFusion streams it). Q7 with `FinalMerge` (12 payload columns, after DataFusion's own
per-partition sorts) is at the limit of this 47 GB machine: a run on its own took 6.2 s and peaked
at 38.8 GB; with three runs in one process the first took 6.3 s and the second was stopped when
less than 3 GB of memory were left.

- In `FinalMerge` mode DataFusion still sorts each of the 24 partitions; the operator replaces
  only `SortPreservingMergeExec`, which merges the sorted partitions on one thread. In these
  queries that merge takes most of DataFusion's time.
- `WholeSort` loses on Q5 and Q6, more at SF10 (0.45x and 0.47x), and only ties on Q7: three key
  columns, the first with only 7 values. After the bytes that all rows share, the 8-byte prefix of
  these keys is mostly constant bytes, so most comparisons go to the full rows.
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
- **Memory use.** Besides its input, the operator holds the sort keys and, while it builds the
  output, a concatenated copy of the columns; the output is built in full before it is returned. At TPC-H SF10 its peak was up to about three times that of
  DataFusion's plan, and on one query (Q7, `FinalMerge`) close to the 47 GB of the machine used for
  the benchmarks (see Benchmarks).

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE): the benchmark and the queries of
the `sort_tpch` example come from Apache DataFusion.
