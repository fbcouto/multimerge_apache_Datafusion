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
4. **Output.** The rows are taken column by column, in the sorted order. String view columns are
   copied instead: each output batch holds its own strings, rather than a list of every data buffer
   of the input.

## Correctness

`tests/sort_correctness.rs` compares every result with DataFusion's own sort (`SortExec` and
`SortPreservingMergeExec`): the sort keys must be identical row by row, and the set of rows the
same. It covers 4 data shapes (number of rows, partitions, input and output batch sizes) and 30
orderings — ascending and descending, nulls first and last, one to three keys; integers, floats,
decimals, dates, timestamps, booleans, strings and dictionaries — for the whole sort and for the
merge of sorted partitions, and every internal option of the operator. Other tests check that
the rule replaces only full sorts, that plans with several window functions over conflicting
orderings still pass DataFusion's plan checks, and that output batches of string views hold only
their own strings.

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
| Q1 | 1 key (INTEGER, 7 values) + 1 payload column | 173.2 ms | 25.9 ms (6.69x) | 81.2 ms (2.13x) |
| Q2 | 1 key (BIGINT, 1.5M values) + 1 payload column | 160.0 ms | 30.1 ms (5.32x) | 82.6 ms (1.94x) |
| Q3 | 1 key (VARCHAR, 4.5M values) + 1 payload column | 1010.1 ms | 337.6 ms (2.99x) | 354.1 ms (2.85x) |
| Q4 | 2 keys (BIGINT 1.5M, INTEGER 7) + 1 payload column | 277.3 ms | 49.6 ms (5.59x) | 89.6 ms (3.09x) |
| Q5 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M), no payload | 626.0 ms | 255.6 ms (2.45x) | 765.0 ms (0.82x) |
| Q6 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 1 payload column | 630.2 ms | 266.7 ms (2.36x) | 756.5 ms (0.83x) |
| Q7 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 12 payload columns | 1105.8 ms | 440.9 ms (2.51x) | 938.0 ms (1.18x) |
| Q8 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M), no payload | 488.5 ms | 106.7 ms (4.58x) | 141.6 ms (3.45x) |
| Q9 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 1 payload column | 496.1 ms | 103.5 ms (4.79x) | 145.7 ms (3.40x) |
| Q10 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 12 payload columns | 697.0 ms | 177.5 ms (3.93x) | 235.7 ms (2.96x) |
| Q11 | 1 key (VARCHAR, 7 values) + 2 payload columns | 303.1 ms | 56.1 ms (5.40x) | 184.7 ms (1.64x) |
| Geometric mean | | | 3.98x | 1.96x |

6,001,215 rows, 24 partitions, batches of 8192 rows, median of 5 runs, table in memory.

The same, reading the Parquet files:

| Query | Sort keys | DataFusion | `FinalMerge` | `WholeSort` |
| :--- | :--- | ---: | ---: | ---: |
| Q1 | 1 key (INTEGER, 7 values) + 1 payload column | 178.2 ms | 38.2 ms (4.66x) | 97.1 ms (1.84x) |
| Q2 | 1 key (BIGINT, 1.5M values) + 1 payload column | 158.9 ms | 32.6 ms (4.87x) | 18.3 ms (8.68x) |
| Q3 | 1 key (VARCHAR, 4.5M values) + 1 payload column | 1026.5 ms | 392.9 ms (2.61x) | 375.2 ms (2.74x) |
| Q4 | 2 keys (BIGINT 1.5M, INTEGER 7) + 1 payload column | 276.7 ms | 47.9 ms (5.78x) | 26.5 ms (10.44x) |
| Q5 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M), no payload | 484.9 ms | 273.9 ms (1.77x) | 753.5 ms (0.64x) |
| Q6 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 1 payload column | 499.9 ms | 287.0 ms (1.74x) | 648.7 ms (0.77x) |
| Q7 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 12 payload columns | 956.3 ms | 491.8 ms (1.94x) | 950.2 ms (1.01x) |
| Q8 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M), no payload | 468.0 ms | 126.5 ms (3.70x) | 171.2 ms (2.73x) |
| Q9 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 1 payload column | 484.2 ms | 136.4 ms (3.55x) | 183.7 ms (2.64x) |
| Q10 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 12 payload columns | 716.8 ms | 227.8 ms (3.15x) | 315.2 ms (2.27x) |
| Q11 | 1 key (VARCHAR, 7 values) + 2 payload columns | 363.1 ms | 83.7 ms (4.34x) | 282.3 ms (1.29x) |
| Geometric mean | | | 3.21x | 2.16x |

6,001,215 rows, 24 partitions, batches of 8192 rows, median of 5 runs, reading Parquet.

TPC-H scale factor 10, each plan in its own process, reading Parquet with the files already in the
page cache, median of 3 runs:

| Query | Sort keys | DataFusion | `FinalMerge` | `WholeSort` |
| :--- | :--- | ---: | ---: | ---: |
| Q1 | 1 key (INTEGER, 7 values) + 1 payload column | 2300.6 ms | 372.1 ms (6.18x) | 962.5 ms (2.39x) |
| Q2 | 1 key (BIGINT, 1.5M values) + 1 payload column | 1878.4 ms | 366.0 ms (5.13x) | 162.3 ms (11.57x) |
| Q3 | 1 key (VARCHAR, 4.5M values) + 1 payload column | 13097.0 ms | 4174.8 ms (3.14x) | 5852.1 ms (2.24x) |
| Q4 | 2 keys (BIGINT 1.5M, INTEGER 7) + 1 payload column | 3425.4 ms | 600.9 ms (5.70x) | 249.7 ms (13.72x) |
| Q5 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M), no payload | 4921.5 ms | 2898.7 ms (1.70x) | 10719.5 ms (0.46x) |
| Q6 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 1 payload column | 5225.9 ms | 3010.4 ms (1.74x) | 10963.4 ms (0.48x) |
| Q7 | 3 keys (INTEGER 7, BIGINT 10k, BIGINT 1.5M) + 12 payload columns | 14152.3 ms | see below | 14591.1 ms (0.97x) |
| Q8 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M), no payload | 4704.1 ms | 1455.3 ms (3.23x) | 1714.0 ms (2.74x) |
| Q9 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 1 payload column | 4935.8 ms | 1473.4 ms (3.35x) | 1886.5 ms (2.62x) |
| Q10 | 4 keys (BIGINT 1.5M, BIGINT 10k, INTEGER 7, VARCHAR 4.5M) + 12 payload columns | 8361.1 ms | 2407.1 ms (3.47x) | 3086.4 ms (2.71x) |
| Q11 | 1 key (VARCHAR, 7 values) + 2 payload columns | 4620.5 ms | 903.7 ms (5.11x) | 2275.5 ms (2.03x) |
| Geometric mean | | | 3.55x | 2.24x |

59,986,052 rows, 24 partitions, batches of 8192 rows. The numbers of values in the key
descriptions are those of SF1. The geometric mean of `FinalMerge` leaves Q7 out.

Peak resident memory of each of those processes, in GB:

| Query | DataFusion | `FinalMerge` | `WholeSort` |
| :--- | ---: | ---: | ---: |
| Q1 | 1.0 | 1.4 | 3.0 |
| Q2 | 1.4 | 1.9 | 1.6 |
| Q3 | 6.0 | 11.6 | 8.0 |
| Q4 | 2.6 | 4.7 | 2.6 |
| Q5 | 3.1 | 7.6 | 4.6 |
| Q6 | 3.6 | 8.6 | 5.8 |
| Q7 | 11.7 | 36.4 in one run; stopped at 41.3 in three | 27.1 |
| Q8 | 8.2 | 13.6 | 10.0 |
| Q9 | 8.8 | 14.4 | 11.3 |
| Q10 | 16.6 | 28.6 | 32.2 |
| Q11 | 4.5 | 7.2 | 9.4 |

The operator needs more memory than DataFusion's plan, up to about three times as much: it keeps
its whole input until the output is built, keeps a concatenated copy of every column that is not a
string view while it builds the output, and builds the output in full before returning the first
batch (DataFusion streams it). Q7 with `FinalMerge` (12 payload columns, after DataFusion's own
per-partition sorts) is at the limit of this 47 GB machine: a run on its own took 5.8 s and peaked
at 36.4 GB; with three runs in one process the first took 6.7 s, the second 24.2 s, and the third
was stopped when less than 3 GB of memory were left.

- In `FinalMerge` mode DataFusion still sorts each of the 24 partitions; the operator replaces
  only `SortPreservingMergeExec`, which merges the sorted partitions on one thread. In these
  queries that merge takes most of DataFusion's time.
- `WholeSort` loses on Q5 and Q6, more at SF10 (0.46x and 0.48x), and only ties on Q7: three key
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
  output, a concatenated copy of the columns that are not string views; the output is built in
  full before it is returned. At TPC-H SF10 its peak was up to about three times that of
  DataFusion's plan, and on one query (Q7, `FinalMerge`) close to the 47 GB of the machine used for
  the benchmarks (see Benchmarks).

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE): the benchmark and the queries of
the `sort_tpch` example come from Apache DataFusion.
