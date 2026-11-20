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
merge of sorted partitions, and every internal option of the operator. Another test checks that
the rule replaces only full sorts.

## Benchmarks

`benches/sort.rs` is DataFusion's own sort benchmark (`benches/sort.rs` of DataFusion 55.3.0) with
cases added for this operator over the same data: `cargo bench --bench sort`. With
`MULTIMERGE_PHASES=1` it prints where the time of the operator goes (key encoding, preparation,
engine, output).

`examples/sort_tpch.rs` runs the queries of DataFusion's `sort_tpch` benchmark with DataFusion's
sort and with the operator in both modes:

```text
pip install tpchgen-cli
tpchgen-cli parquet -s 1 --tables=lineitem --parts=24 --output-dir=data/tpch_sf1
cargo run --release --example sort_tpch -- --path data/tpch_sf1 --mem-table --iterations 5
```

Results: in preparation.

## Limitations

- **Memory only.** The operator keeps its whole input in memory and reserves it from the memory
  pool. If the pool cannot grow, the query fails: it does not spill to disk as `SortExec` does, and
  it does not fall back to it.
- **No limit.** Sorts with `fetch` stay with DataFusion's operators.
- **One output partition**, produced after all the input has been read.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE): the benchmark and the queries of
the `sort_tpch` example come from Apache DataFusion.
