//! The MultiMerge operator must return what the sort of DataFusion returns: the same sequence of
//! sort keys and the same set of rows. Rows with equal keys may come in another order.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, DictionaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, StringArray, StringViewArray,
    TimestampNanosecondArray, UInt16Array,
};
use datafusion::arrow::compute::{SortOptions, concat_batches};
use datafusion::arrow::datatypes::{Int32Type, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::Result;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource::memory::MemorySourceConfig;
use multimerge_datafusion::{
    Gather, Mode, MultimergeSortExec, SortPath, Tuning, session_state, sort_batches,
    sort_batches_with,
};

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// `rows` rows in `partitions` partitions, with batches of uneven sizes up to `batch` rows.
/// `id` grows along the batches, which are dealt to the partitions in turn: read partition after
/// partition, an input that is in order comes as runs interleaved in blocks.
fn table(
    rows: usize,
    partitions: usize,
    batch: usize,
) -> (SchemaRef, Vec<Vec<RecordBatch>>) {
    let mut random = Random(0x9E37_79B9_7F4A_7C15);
    let mut output: Vec<Vec<RecordBatch>> = vec![Vec::new(); partitions];
    let mut schema = None;
    let (mut begin, mut turn) = (0usize, 0usize);
    while begin < rows {
        let size = (batch / 2 + (random.next() as usize) % (batch / 2 + 1))
            .min(rows - begin)
            .max(1);
        let ids: Vec<i64> = (begin..begin + size).map(|i| i as i64).collect();
        let mut i64s = Vec::with_capacity(size);
        let mut small = Vec::with_capacity(size);
        let mut floats = Vec::with_capacity(size);
        let mut strings = Vec::with_capacity(size);
        let mut urls = Vec::with_capacity(size);
        let mut views = Vec::with_capacity(size);
        let mut dict = Vec::with_capacity(size);
        let mut decimals = Vec::with_capacity(size);
        let mut stamps = Vec::with_capacity(size);
        let mut flags = Vec::with_capacity(size);
        let mut whole_floats = Vec::with_capacity(size);
        let mut single_floats = Vec::with_capacity(size);
        let mut shorts = Vec::with_capacity(size);
        let mut days = Vec::with_capacity(size);
        for &id in &ids {
            let r = random.next();
            i64s.push(r as i64);
            // Columns without nulls: their values are read as the key, without encoding.
            whole_floats.push(match id % 97 {
                1 => f64::NAN,
                2 => -0.0,
                3 => 0.0,
                4 => f64::INFINITY,
                5 => f64::NEG_INFINITY,
                6 => -f64::NAN,
                _ => ((r % 4_000_001) as f64 - 2_000_000.0) / 16.0,
            });
            single_floats.push(if id % 89 == 1 {
                f32::NAN
            } else {
                ((r % 20_001) as f32 - 10_000.0) / 4.0
            });
            shorts.push((r % 300) as u16);
            days.push((r % 40_000) as i32 - 20_000);
            small.push(if id % 13 == 0 {
                None
            } else {
                Some((r % 1000) as i32 - 500)
            });
            floats.push(match id % 101 {
                0 => None,
                1 => Some(f64::NAN),
                2 => Some(-0.0),
                3 => Some(0.0),
                4 => Some(f64::INFINITY),
                5 => Some(f64::NEG_INFINITY),
                _ => Some(((r % 2_000_001) as f64 - 1_000_000.0) / 8.0),
            });
            strings.push(match id % 17 {
                0 => None,
                1 => Some(String::new()),
                _ => Some(format!("{:x}", r % 50_000)),
            });
            let tail = if id % 5 == 0 { "" } else { "/details" };
            urls.push(format!(
                "https://example.com/catalog/item/{}{}",
                r % 300_000,
                tail
            ));
            views.push(format!("view-{:020}", r % 70_000));
            dict.push(format!("group-{:02}", r % 50));
            decimals.push((r as i128 % 2_000_000_000_001) - 1_000_000_000_000);
            stamps.push((r % 4_000_000_000_000_000_000) as i64);
            flags.push(if id % 19 == 0 {
                None
            } else {
                Some(r.is_multiple_of(2))
            });
        }
        let dictionary: DictionaryArray<Int32Type> =
            dict.iter().map(|s| s.as_str()).collect();
        let decimals = Decimal128Array::from(decimals)
            .with_precision_and_scale(20, 3)
            .unwrap();
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("id", Arc::new(Int64Array::from(ids))),
            ("i64", Arc::new(Int64Array::from(i64s))),
            ("small", Arc::new(Int32Array::from(small))),
            ("f64", Arc::new(Float64Array::from(floats))),
            ("s", Arc::new(StringArray::from(strings))),
            ("url", Arc::new(StringArray::from(urls))),
            (
                "view",
                Arc::new(StringViewArray::from_iter_values(views.iter())),
            ),
            ("dict", Arc::new(dictionary)),
            ("dec", Arc::new(decimals)),
            ("ts", Arc::new(TimestampNanosecondArray::from(stamps))),
            ("flag", Arc::new(BooleanArray::from(flags))),
            ("f64nn", Arc::new(Float64Array::from(whole_floats))),
            ("f32nn", Arc::new(Float32Array::from(single_floats))),
            ("u16", Arc::new(UInt16Array::from(shorts))),
            ("day", Arc::new(Date32Array::from(days))),
        ];
        let record = RecordBatch::try_from_iter(columns).unwrap();
        schema.get_or_insert_with(|| record.schema());
        output[turn % partitions].push(record);
        begin += size;
        turn += 1;
    }
    (schema.unwrap(), output)
}

/// (column, descending, nulls first)
type Key = (&'static str, bool, bool);

fn ordering(schema: &SchemaRef, keys: &[Key]) -> LexOrdering {
    LexOrdering::new(keys.iter().map(|&(name, descending, nulls_first)| {
        PhysicalSortExpr::new(
            col(name, schema).unwrap(),
            SortOptions {
                descending,
                nulls_first,
            },
        )
    }))
    .unwrap()
}

const ORDERINGS: &[&[Key]] = &[
    &[("i64", false, false)],
    &[("i64", true, false)],
    &[("small", false, true)],
    &[("small", true, false)],
    &[("f64", false, false)],
    &[("f64", true, true)],
    &[("s", false, false)],
    &[("s", true, true)],
    &[("url", true, false)],
    &[("view", false, false)],
    &[("dict", false, false)],
    &[("dec", true, false)],
    &[("ts", false, false)],
    &[("flag", false, true)],
    &[("id", false, false)],
    &[("id", true, false)],
    &[("f64nn", false, false)],
    &[("f64nn", true, true)],
    &[("f32nn", false, false)],
    &[("f32nn", true, false)],
    &[("u16", false, false)],
    &[("u16", true, false)],
    &[("day", false, false)],
    &[("day", true, true)],
    &[("small", false, false), ("i64", true, false)],
    &[("dict", false, false), ("s", false, true)],
    &[
        ("flag", true, false),
        ("f64", false, false),
        ("id", false, false),
    ],
    &[("url", false, false), ("small", false, false)],
    &[("s", true, true), ("dec", false, false)],
    &[
        ("small", false, true),
        ("dict", true, false),
        ("ts", true, false),
    ],
];

async fn run(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Result<RecordBatch> {
    let schema = plan.schema();
    let batches = collect(plan, ctx.task_ctx()).await?;
    Ok(concat_batches(&schema, &batches)?)
}

/// The rows of `batch`, as bytes: only the sort keys in their order, or every column.
fn encoded(batch: &RecordBatch, keys: Option<&[Key]>) -> Vec<Vec<u8>> {
    let schema = batch.schema();
    let (fields, columns): (Vec<SortField>, Vec<ArrayRef>) = match keys {
        Some(keys) => keys
            .iter()
            .map(|&(name, descending, nulls_first)| {
                let index = schema.index_of(name).unwrap();
                let options = SortOptions {
                    descending,
                    nulls_first,
                };
                let field = SortField::new_with_options(
                    schema.field(index).data_type().clone(),
                    options,
                );
                (field, batch.column(index).clone())
            })
            .unzip(),
        None => schema
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, column)| {
                (SortField::new(field.data_type().clone()), column.clone())
            })
            .unzip(),
    };
    let rows = RowConverter::new(fields)
        .unwrap()
        .convert_columns(&columns)
        .unwrap();
    rows.iter().map(|row| row.data().to_vec()).collect()
}

fn assert_same_sort(
    expected: &RecordBatch,
    actual: &RecordBatch,
    keys: &[Key],
    what: &str,
) {
    assert_eq!(
        expected.num_rows(),
        actual.num_rows(),
        "{what}: number of rows"
    );
    let (expected_keys, actual_keys) =
        (encoded(expected, Some(keys)), encoded(actual, Some(keys)));
    if let Some(row) =
        (0..expected_keys.len()).find(|&i| expected_keys[i] != actual_keys[i])
    {
        panic!("{what}: the sort keys differ at row {row}");
    }
    let (mut expected_rows, mut actual_rows) =
        (encoded(expected, None), encoded(actual, None));
    expected_rows.sort_unstable();
    actual_rows.sort_unstable();
    assert!(
        expected_rows == actual_rows,
        "{what}: the sets of rows differ"
    );
}

fn context(batch_size: usize, partitions: usize) -> SessionContext {
    let config = SessionConfig::new()
        .with_batch_size(batch_size)
        .with_target_partitions(partitions);
    SessionContext::new_with_config(config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_result_as_datafusion_sort() -> Result<()> {
    // (rows, partitions, largest input batch, output batch size)
    let shapes: &[(usize, usize, usize, usize)] = &[
        (60_000, 8, 4096, 8192),
        (20_000, 3, 500, 1024),
        (5_000, 1, 5000, 100),
        (40_000, 16, 64, 333),
    ];
    for &(rows, partitions, batch, batch_size) in shapes {
        let (schema, data) = table(rows, partitions, batch);
        let ctx = context(batch_size, partitions);
        for keys in ORDERINGS {
            let sort = ordering(&schema, keys);
            let source = || {
                MemorySourceConfig::try_new_exec(&data, Arc::clone(&schema), None)
                    .unwrap()
            };
            let sorted_partitions = || {
                let sorted = SortExec::new(sort.clone(), source())
                    .with_preserve_partitioning(true);
                Arc::new(sorted) as Arc<dyn ExecutionPlan>
            };

            let reference = Arc::new(SortPreservingMergeExec::new(
                sort.clone(),
                sorted_partitions(),
            ));
            let expected = run(reference, &ctx).await?;
            let what = format!("{keys:?}, {rows} rows in {partitions} partitions");

            let whole = run(
                Arc::new(MultimergeSortExec::new(sort.clone(), source(), false)),
                &ctx,
            )
            .await?;
            assert_same_sort(&expected, &whole, keys, &format!("whole sort, {what}"));

            let merge = run(
                Arc::new(MultimergeSortExec::new(
                    sort.clone(),
                    sorted_partitions(),
                    true,
                )),
                &ctx,
            )
            .await?;
            assert_same_sort(
                &expected,
                &merge,
                keys,
                &format!("merge of sorted partitions, {what}"),
            );
        }
    }
    Ok(())
}

/// A batch that is only its key column goes through the bare-key path; the same sort with the
/// byte keys, the other way of building the output and the stable engine must agree with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bare_keys_and_every_tuning_agree_with_datafusion() -> Result<()> {
    let (schema, data) = table(70_000, 8, 4096);
    let ctx = context(4096, 8);
    let default = Tuning::default();
    let tunings = [
        ("default", default),
        (
            "byte keys",
            Tuning {
                exact_keys: false,
                bare_keys: false,
                ..default
            },
        ),
        (
            "no bare keys",
            Tuning {
                bare_keys: false,
                ..default
            },
        ),
        (
            "interleave",
            Tuning {
                gather: Gather::Interleave,
                ..default
            },
        ),
        (
            "stable engine",
            Tuning {
                stable_engine: true,
                ..default
            },
        ),
    ];

    // One column alone.
    for name in ["i64", "f64nn", "f32nn", "u16", "day", "ts", "small", "s"] {
        let index = schema.index_of(name).unwrap();
        let alone: Vec<Vec<RecordBatch>> = data
            .iter()
            .map(|part| {
                part.iter()
                    .map(|batch| batch.project(&[index]).unwrap())
                    .collect()
            })
            .collect();
        let alone_schema = alone[0][0].schema();
        for descending in [false, true] {
            let keys: &[Key] = &[(name, descending, descending)];
            let sort = ordering(&alone_schema, keys);
            let source =
                MemorySourceConfig::try_new_exec(&alone, Arc::clone(&alone_schema), None)
                    .unwrap();
            let sorted =
                SortExec::new(sort.clone(), source).with_preserve_partitioning(true);
            let expected = run(
                Arc::new(SortPreservingMergeExec::new(sort.clone(), Arc::new(sorted))),
                &ctx,
            )
            .await?;
            for (label, tuning) in &tunings {
                let (batches, stats) =
                    sort_batches_with(alone.clone(), &sort, 4096, 4, false, tuning)?;
                let actual = concat_batches(&alone_schema, &batches)?;
                assert_same_sort(
                    &expected,
                    &actual,
                    keys,
                    &format!("{name} alone, descending={descending}, {label}"),
                );
                let bare = tuning.bare_keys
                    && tuning.exact_keys
                    && !matches!(name, "small" | "s");
                assert_eq!(
                    stats.path == SortPath::BareKeys,
                    bare,
                    "{name} alone, {label}: path {:?}",
                    stats.path
                );
            }
        }
    }

    // The whole table, by one key and by several.
    let orderings: &[&[Key]] = &[
        &[("i64", false, false)],
        &[("f64nn", true, false)],
        &[("dict", false, false), ("s", true, true)],
        &[("u16", false, false), ("url", false, false)],
    ];
    for keys in orderings {
        let sort = ordering(&schema, keys);
        let source =
            MemorySourceConfig::try_new_exec(&data, Arc::clone(&schema), None).unwrap();
        let sorted = SortExec::new(sort.clone(), source).with_preserve_partitioning(true);
        let expected = run(
            Arc::new(SortPreservingMergeExec::new(sort.clone(), Arc::new(sorted))),
            &ctx,
        )
        .await?;
        for (label, tuning) in &tunings {
            let (batches, stats) =
                sort_batches_with(data.clone(), &sort, 4096, 4, false, tuning)?;
            let actual = concat_batches(&schema, &batches)?;
            assert_same_sort(&expected, &actual, keys, &format!("{keys:?}, {label}"));
            assert_eq!(stats.path, SortPath::Engine, "{keys:?}, {label}");
        }
    }
    Ok(())
}

#[test]
fn takes_the_path_of_the_input() -> Result<()> {
    let (schema, data) = table(50_000, 8, 4096);
    let path = |keys: &[Key], data: Vec<Vec<RecordBatch>>| -> Result<(SortPath, usize)> {
        let (batches, stats) =
            sort_batches(data, &ordering(&schema, keys), 8192, 4, false)?;
        Ok((stats.path, batches.iter().map(|b| b.num_rows()).sum()))
    };

    // Random keys go through the engine.
    assert_eq!(
        path(&[("i64", false, false)], data.clone())?,
        (SortPath::Engine, 50_000)
    );
    assert_eq!(
        path(&[("s", false, false), ("small", true, false)], data.clone())?,
        (SortPath::Engine, 50_000)
    );
    // Input in order, or in the opposite order: the batches are only put one after the other.
    assert_eq!(
        path(&[("id", false, false)], data.clone())?,
        (SortPath::OrderedRanges, 50_000)
    );
    assert_eq!(
        path(&[("id", true, false)], data.clone())?,
        (SortPath::OrderedRanges, 50_000)
    );
    // Nothing to sort.
    assert_eq!(
        path(&[("id", false, false)], vec![Vec::new(), Vec::new()])?,
        (SortPath::Empty, 0)
    );

    // One sorted partition is already the answer.
    let one = vec![data.iter().flatten().cloned().collect::<Vec<_>>()];
    let (_, stats) = sort_batches(
        one,
        &ordering(&schema, &[("id", false, false)]),
        8192,
        4,
        true,
    )?;
    assert_eq!(stats.path, SortPath::SingleRun);
    Ok(())
}

async fn plan_text(ctx: &SessionContext, sql: &str) -> Result<String> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let text = displayable(plan.as_ref()).indent(false).to_string();
    Ok(text)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_rule_replaces_only_full_sorts() -> Result<()> {
    let (schema, data) = table(30_000, 8, 2048);
    let config = || {
        SessionConfig::new()
            .with_target_partitions(8)
            .with_batch_size(4096)
    };
    let default = SessionContext::new_with_config(config());
    let whole = SessionContext::new_with_state(session_state(config(), Mode::WholeSort));
    let merge = SessionContext::new_with_state(session_state(config(), Mode::FinalMerge));
    for ctx in [&default, &whole, &merge] {
        let table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), data.clone())?;
        ctx.register_table("t", Arc::new(table))?;
    }

    let full = "SELECT small, i64, s FROM t ORDER BY small, i64 DESC";
    let text = plan_text(&default, full).await?;
    assert!(
        !text.contains("MultimergeSortExec"),
        "default plan:\n{text}"
    );
    assert!(
        text.contains("SortPreservingMergeExec") && text.contains("SortExec"),
        "default plan:\n{text}"
    );

    let text = plan_text(&whole, full).await?;
    assert!(
        text.contains("MultimergeSortExec") && text.contains("sorted_input=false"),
        "whole sort plan:\n{text}"
    );
    assert!(
        !text.contains("SortPreservingMergeExec") && !text.contains(" SortExec"),
        "whole sort plan:\n{text}"
    );

    let text = plan_text(&merge, full).await?;
    assert!(
        text.contains("MultimergeSortExec") && text.contains("sorted_input=true"),
        "final merge plan:\n{text}"
    );
    assert!(
        text.contains(" SortExec") && !text.contains("SortPreservingMergeExec"),
        "final merge plan:\n{text}"
    );

    // A sort with a limit keeps the operators of DataFusion.
    let text = plan_text(
        &whole,
        "SELECT small, i64 FROM t ORDER BY small, i64 DESC LIMIT 10",
    )
    .await?;
    assert!(
        !text.contains("MultimergeSortExec"),
        "plan with limit:\n{text}"
    );

    // The same answers through SQL, with the rule and without it.
    let queries: &[(&str, &[Key])] = &[
        (full, &[("small", false, false), ("i64", true, false)]),
        (
            "SELECT url, dict, f64 FROM t WHERE small > 0 ORDER BY f64 DESC NULLS FIRST, url",
            &[("f64", true, true), ("url", false, false)],
        ),
        (
            "SELECT dict, count(*) AS n, sum(small) AS total FROM t GROUP BY dict ORDER BY dict",
            &[("dict", false, false)],
        ),
        ("SELECT id FROM t ORDER BY id", &[("id", false, false)]),
    ];
    for &(sql, keys) in queries {
        let expected = default.sql(sql).await?.collect().await?;
        let expected = concat_batches(&expected[0].schema(), &expected)?;
        for (name, ctx) in [("whole sort", &whole), ("final merge", &merge)] {
            let actual = ctx.sql(sql).await?.collect().await?;
            let actual = concat_batches(&actual[0].schema(), &actual)?;
            assert_same_sort(&expected, &actual, keys, &format!("{name}: {sql}"));
        }
    }
    Ok(())
}
