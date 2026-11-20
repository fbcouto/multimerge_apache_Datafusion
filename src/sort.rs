//! In-memory sort of record batches with the MultiMerge engine.
//!
//! The sort keys of every row become something that orders by plain comparison:
//!
//! - one primitive column without nulls is read as it is, as an integer in the sort order
//!   ([`Keys::Exact`]);
//! - anything else is encoded with Arrow's row format, which turns any combination of columns,
//!   directions and null placements into one byte string per row whose byte order is the sort order
//!   ([`Keys::Bytes`]).
//!
//! From there on:
//!
//! 1. Batches that already come in order are merged by ranges of rows and returned as slices of the
//!    input, without a copy ([`SortPath::OrderedRanges`]).
//! 2. A batch that is only its integer key is sorted as bare key values, the form in which the engine
//!    has its paths for narrow ranges, and the sorted values are the output ([`SortPath::BareKeys`]).
//! 3. Otherwise one small element per row is built, with eight bytes of the key as an integer prefix
//!    plus the identity of the row. The engine sorts the elements, comparing the remaining bytes only
//!    for rows whose prefixes are equal, and the columns are then gathered in that order
//!    ([`SortPath::Engine`]).

use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, RecordBatchOptions, UInt32Array, UInt64Array, make_array,
};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::compute::{
    concat, interleave_record_batch, take, take_record_batch,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, Rows, SortField};
use datafusion::common::{Result, exec_err};
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use rayon::prelude::*;

use crate::engine;

/// What the engine sorts: eight bytes of the key and the row they belong to.
#[derive(Clone, Copy)]
#[repr(C)]
struct Element {
    key: u64,
    /// Batch in the high 32 bits, row inside the batch in the low 32 bits.
    id: u64,
}

const ROW_BITS: u32 = 32;
const ROW_MASK: u64 = (1 << ROW_BITS) - 1;

/// How the result was produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortPath {
    /// There were no rows.
    Empty,
    /// Only one sorted partition had rows: it is the answer.
    SingleRun,
    /// The batches were already in order: they were merged by ranges of rows, without a copy
    /// (batches in the exact opposite order were turned around first).
    OrderedRanges,
    /// The batches were only their integer key: the key values themselves were sorted.
    BareKeys,
    /// The engine sorted one element per row and the columns were gathered in that order.
    Engine,
}

/// Where the time of one sort went.
#[derive(Clone, Copy, Debug)]
pub struct SortStats {
    pub path: SortPath,
    pub rows: usize,
    /// Reading or encoding the sort keys of every row.
    pub encode: Duration,
    /// Looking for order in the input and building the elements.
    pub prepare: Duration,
    /// Inside the engine.
    pub engine: Duration,
    /// Building the output batches.
    pub gather: Duration,
}

impl SortStats {
    fn new(path: SortPath, rows: usize) -> Self {
        Self {
            path,
            rows,
            encode: Duration::ZERO,
            prepare: Duration::ZERO,
            engine: Duration::ZERO,
            gather: Duration::ZERO,
        }
    }
}

/// How the output batches are built from the sorted elements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gather {
    /// Every column is first put in one array and the rows are then taken from it by position.
    /// Dictionaries are merged once, and a taken dictionary or string view is only its keys or views.
    Take,
    /// Rows are picked straight from the input batches, one output batch at a time.
    Interleave,
}

/// Choices that do not change the result, only how it is produced. They exist for measurements.
#[derive(Clone, Copy, Debug)]
pub struct Tuning {
    /// Read the key of a single primitive column without nulls as an integer, instead of encoding bytes.
    pub exact_keys: bool,
    /// Sort a batch that is only its integer key by the key values themselves.
    pub bare_keys: bool,
    /// Use the stable entries of the engine.
    pub stable_engine: bool,
    pub gather: Gather,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            exact_keys: true,
            bare_keys: true,
            stable_engine: false,
            gather: Gather::Take,
        }
    }
}

/// One Rayon pool per thread count, shared by all sorts.
fn pool(threads: usize) -> Result<Arc<rayon::ThreadPool>> {
    static POOLS: Mutex<Vec<(usize, Arc<rayon::ThreadPool>)>> = Mutex::new(Vec::new());
    let threads = threads.max(1);
    let mut pools = POOLS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((_, pool)) = pools.iter().find(|(t, _)| *t == threads) {
        return Ok(Arc::clone(pool));
    }
    let built = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|i| format!("multimerge-sort-{i}"))
        .build();
    match built {
        Ok(pool) => {
            let pool = Arc::new(pool);
            pools.push((threads, Arc::clone(&pool)));
            Ok(pool)
        }
        Err(e) => exec_err!("MultiMerge sort: cannot create the thread pool: {e}"),
    }
}

/// Sorts all the rows of `partitions` by `ordering` and returns them in batches.
///
/// With `sorted_partitions` every partition is already sorted and only the merge is left.
/// `threads` is how many threads the sort may use. Blocks the calling thread until it is done.
pub fn sort_batches(
    partitions: Vec<Vec<RecordBatch>>,
    ordering: &LexOrdering,
    batch_size: usize,
    threads: usize,
    sorted_partitions: bool,
) -> Result<(Vec<RecordBatch>, SortStats)> {
    sort_batches_with(
        partitions,
        ordering,
        batch_size,
        threads,
        sorted_partitions,
        &Tuning::default(),
    )
}

/// [`sort_batches`] with the choices of `tuning`.
pub fn sort_batches_with(
    partitions: Vec<Vec<RecordBatch>>,
    ordering: &LexOrdering,
    batch_size: usize,
    threads: usize,
    sorted_partitions: bool,
    tuning: &Tuning,
) -> Result<(Vec<RecordBatch>, SortStats)> {
    pool(threads)?.install(|| {
        sort_in_pool(
            partitions,
            ordering,
            batch_size.max(1),
            sorted_partitions,
            tuning,
        )
    })
}

/// How the values of a primitive column map to integers in the same order.
#[derive(Clone, Copy)]
enum Kind {
    Unsigned,
    Signed,
    /// The total order of IEEE 754, which is how Arrow sorts floating point numbers.
    Float,
}

/// One primitive sort column whose values, read as integers, are the whole key.
#[derive(Clone, Copy)]
struct ExactKey {
    kind: Kind,
    /// Bytes of one value.
    width: usize,
    descending: bool,
}

impl ExactKey {
    fn of(data_type: &DataType, descending: bool) -> Option<Self> {
        use DataType::*;
        let (kind, width) = match data_type {
            UInt8 => (Kind::Unsigned, 1),
            UInt16 => (Kind::Unsigned, 2),
            UInt32 => (Kind::Unsigned, 4),
            UInt64 => (Kind::Unsigned, 8),
            Int8 => (Kind::Signed, 1),
            Int16 => (Kind::Signed, 2),
            Int32 | Date32 | Time32(_) => (Kind::Signed, 4),
            Int64 | Date64 | Time64(_) | Timestamp(_, _) | Duration(_) => {
                (Kind::Signed, 8)
            }
            Float32 => (Kind::Float, 4),
            Float64 => (Kind::Float, 8),
            _ => return None,
        };
        Some(Self {
            kind,
            width,
            descending,
        })
    }

    fn mask(&self) -> u64 {
        if self.width == 8 {
            u64::MAX
        } else {
            (1u64 << (8 * self.width)) - 1
        }
    }

    /// The key of a value given by its bits, zero-extended.
    #[inline]
    fn key(&self, bits: u64) -> u64 {
        let sign = 1u64 << (8 * self.width - 1);
        let ascending = match self.kind {
            Kind::Unsigned => bits,
            Kind::Signed => bits ^ sign,
            Kind::Float if bits & sign != 0 => !bits & self.mask(),
            Kind::Float => bits | sign,
        };
        if self.descending {
            !ascending
        } else {
            ascending
        }
    }

    /// The bits of the value that has this key: the inverse of [`Self::key`].
    #[inline]
    fn bits(&self, key: u64) -> u64 {
        let sign = 1u64 << (8 * self.width - 1);
        let ascending = if self.descending { !key } else { key };
        match self.kind {
            Kind::Unsigned => ascending,
            Kind::Signed => ascending ^ sign,
            Kind::Float if ascending & sign != 0 => ascending ^ sign,
            Kind::Float => !ascending & self.mask(),
        }
    }

    /// The keys of all the values of a primitive array without nulls.
    fn keys_of(&self, array: &dyn Array) -> Vec<u64> {
        let data = array.to_data();
        let (start, end) = (
            data.offset() * self.width,
            (data.offset() + data.len()) * self.width,
        );
        let bytes = &data.buffers()[0].as_slice()[start..end];
        match self.width {
            1 => bytes.iter().map(|&b| self.key(b as u64)).collect(),
            2 => bytes
                .as_chunks::<2>()
                .0
                .iter()
                .map(|c| self.key(u16::from_le_bytes(*c) as u64))
                .collect(),
            4 => bytes
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| self.key(u32::from_le_bytes(*c) as u64))
                .collect(),
            _ => bytes
                .as_chunks::<8>()
                .0
                .iter()
                .map(|c| self.key(u64::from_le_bytes(*c)))
                .collect(),
        }
    }

    /// An array of `data_type` with the values of `keys`, in that order.
    fn array_of(&self, keys: &[u64], data_type: &DataType) -> Result<ArrayRef> {
        let buffer = match self.width {
            1 => Buffer::from_vec(
                keys.iter()
                    .map(|&k| self.bits(k) as u8)
                    .collect::<Vec<u8>>(),
            ),
            2 => Buffer::from_vec(
                keys.iter()
                    .map(|&k| self.bits(k) as u16)
                    .collect::<Vec<u16>>(),
            ),
            4 => Buffer::from_vec(
                keys.iter()
                    .map(|&k| self.bits(k) as u32)
                    .collect::<Vec<u32>>(),
            ),
            _ => {
                Buffer::from_vec(keys.iter().map(|&k| self.bits(k)).collect::<Vec<u64>>())
            }
        };
        let data = ArrayData::builder(data_type.clone())
            .len(keys.len())
            .add_buffer(buffer)
            .build()?;
        Ok(make_array(data))
    }
}

/// The sort keys of every row, batch by batch, in a form that orders by plain comparison.
enum Keys {
    /// The whole key as an integer: one primitive column without nulls.
    Exact(Vec<Vec<u64>>),
    /// Bytes from Arrow's row format: any types, any number of columns.
    Bytes(Vec<Rows>),
}

impl Keys {
    /// The order of two rows, each given by (batch, row).
    #[inline]
    fn compare(&self, a: (usize, usize), b: (usize, usize)) -> Ordering {
        match self {
            Keys::Exact(keys) => keys[a.0][a.1].cmp(&keys[b.0][b.1]),
            Keys::Bytes(rows) => rows[a.0].row(a.1).cmp(&rows[b.0].row(b.1)),
        }
    }

    /// `Some(backwards)` when the batch is in order, read forwards or backwards. `None` when it is in
    /// no order, or when `stop` says that another batch already is not.
    fn direction(&self, batch: usize, stop: &AtomicBool) -> Option<bool> {
        let (mut ascending, mut descending) = (true, true);
        match self {
            Keys::Exact(keys) => {
                let keys = &keys[batch];
                let mut start = 1;
                while start < keys.len() {
                    let end = (start + 1024).min(keys.len());
                    for row in start..end {
                        ascending &= keys[row - 1] <= keys[row];
                        descending &= keys[row - 1] >= keys[row];
                    }
                    if (!ascending && !descending) || stop.load(AtomicOrdering::Relaxed) {
                        return None;
                    }
                    start = end;
                }
            }
            Keys::Bytes(rows) => {
                let rows = &rows[batch];
                for row in 1..rows.num_rows() {
                    match rows.row(row - 1).cmp(&rows.row(row)) {
                        Ordering::Less => descending = false,
                        Ordering::Greater => ascending = false,
                        Ordering::Equal => {}
                    }
                    if (!ascending && !descending)
                        || (row & 1023 == 0 && stop.load(AtomicOrdering::Relaxed))
                    {
                        return None;
                    }
                }
            }
        }
        Some(!ascending)
    }
}

fn sort_in_pool(
    partitions: Vec<Vec<RecordBatch>>,
    ordering: &LexOrdering,
    batch_size: usize,
    sorted_partitions: bool,
    tuning: &Tuning,
) -> Result<(Vec<RecordBatch>, SortStats)> {
    let started = Instant::now();

    let mut runs_with_rows = 0;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for partition in partitions {
        let before = batches.len();
        batches.extend(partition.into_iter().filter(|batch| batch.num_rows() > 0));
        runs_with_rows += usize::from(batches.len() > before);
    }
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();

    if batches.is_empty() {
        return Ok((batches, SortStats::new(SortPath::Empty, 0)));
    }
    if sorted_partitions && runs_with_rows == 1 {
        return Ok((batches, SortStats::new(SortPath::SingleRun, total_rows)));
    }
    if batches.len() as u64 > ROW_MASK
        || batches
            .iter()
            .any(|batch| batch.num_rows() as u64 > ROW_MASK)
    {
        return exec_err!(
            "MultiMerge sort: too many batches, or a batch with too many rows"
        );
    }
    let schema = batches[0].schema();

    // The sort columns of every batch.
    let key_columns: Vec<Vec<ArrayRef>> = batches
        .par_iter()
        .map(|batch| {
            ordering
                .iter()
                .map(|sort| sort.expr.evaluate(batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<ArrayRef>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    let first = ordering.iter().next().expect("a sort has at least one key");
    let exact = (tuning.exact_keys && ordering.len() == 1)
        .then(|| ExactKey::of(key_columns[0][0].data_type(), first.options.descending))
        .flatten()
        .filter(|_| {
            key_columns
                .iter()
                .all(|columns| columns[0].null_count() == 0)
        });

    let keys = match exact {
        Some(exact) => Keys::Exact(
            key_columns
                .par_iter()
                .map(|columns| exact.keys_of(columns[0].as_ref()))
                .collect(),
        ),
        None => {
            let fields = ordering
                .iter()
                .zip(&key_columns[0])
                .map(|(sort, column)| {
                    SortField::new_with_options(column.data_type().clone(), sort.options)
                })
                .collect();
            let converter = RowConverter::new(fields)?;
            let rows = key_columns
                .par_iter()
                .map(|columns| converter.convert_columns(columns))
                .collect::<std::result::Result<Vec<Rows>, ArrowError>>()?;
            Keys::Bytes(rows)
        }
    };
    let encode = started.elapsed();

    if let Some(output) = merge_ordered_ranges(&batches, &keys, sorted_partitions)? {
        let mut stats = SortStats::new(SortPath::OrderedRanges, total_rows);
        stats.encode = encode;
        stats.prepare = started.elapsed() - encode;
        return Ok((output, stats));
    }

    // A batch that is only its integer key: the key values themselves are sorted, and they are the output.
    if let (Some(exact), Keys::Exact(per_batch), true) = (exact, &keys, tuning.bare_keys)
    {
        let only_the_key = schema.fields().len() == 1
            && batches
                .iter()
                .zip(&key_columns)
                .all(|(batch, columns)| Arc::ptr_eq(batch.column(0), &columns[0]));
        if only_the_key {
            let mut values = Vec::with_capacity(total_rows);
            for keys in per_batch {
                values.extend_from_slice(keys);
            }
            let prepare = started.elapsed() - encode;

            let sorting = Instant::now();
            engine::multi_merge_sort_radix(&mut values);
            let engine_time = sorting.elapsed();

            let gathering = Instant::now();
            let data_type = schema.field(0).data_type();
            let output = values
                .par_chunks(batch_size)
                .map(|chunk| {
                    Ok(RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![exact.array_of(chunk, data_type)?],
                    )?)
                })
                .collect::<Result<Vec<_>>>()?;
            let stats = SortStats {
                path: SortPath::BareKeys,
                rows: total_rows,
                encode,
                prepare,
                engine: engine_time,
                gather: gathering.elapsed(),
            };
            return Ok((output, stats));
        }
    }
    drop(key_columns);

    let mut elements = vec![Element { key: 0, id: 0 }; total_rows];
    let mut slices = Vec::with_capacity(batches.len());
    let mut rest = elements.as_mut_slice();
    for batch in &batches {
        let (head, tail) = rest.split_at_mut(batch.num_rows());
        slices.push(head);
        rest = tail;
    }

    // `tail` is how many bytes of a key the prefix covers when it does not cover the whole key.
    let tail = match &keys {
        Keys::Exact(per_batch) => {
            slices
                .into_par_iter()
                .zip(per_batch.par_iter())
                .enumerate()
                .for_each(|(batch, (slice, keys))| {
                    let base = (batch as u64) << ROW_BITS;
                    for (row, (element, &key)) in slice.iter_mut().zip(keys).enumerate() {
                        *element = Element {
                            key,
                            id: base | row as u64,
                        };
                    }
                });
            None
        }
        Keys::Bytes(rows) => {
            // The bytes that all keys share at the beginning do not help to order them: the prefix takes
            // the eight bytes that come right after. When no key is longer than that, the prefix is the
            // whole key.
            let reference = rows[0].row(0).data();
            let (common, longest) = rows
                .par_iter()
                .map(|rows| {
                    let mut common = reference.len();
                    let mut longest = 0;
                    for row in rows.iter() {
                        let data = row.data();
                        longest = longest.max(data.len());
                        if data.len() < common || data[..common] != reference[..common] {
                            common = common_prefix(data, &reference[..common]);
                        }
                    }
                    (common, longest)
                })
                .reduce(|| (reference.len(), 0), |a, b| (a.0.min(b.0), a.1.max(b.1)));

            slices
                .into_par_iter()
                .zip(rows.par_iter())
                .enumerate()
                .for_each(|(batch, (slice, rows))| {
                    let base = (batch as u64) << ROW_BITS;
                    for (row, element) in slice.iter_mut().enumerate() {
                        *element = Element {
                            key: prefix_key(rows.row(row).data(), common),
                            id: base | row as u64,
                        };
                    }
                });
            (longest > common + 8).then_some(common + 8)
        }
    };
    let prepare = started.elapsed() - encode;

    let sorting = Instant::now();
    match (tail, &keys) {
        (Some(tail), Keys::Bytes(rows)) => {
            let rest = |id: u64| {
                let data = rows[(id >> ROW_BITS) as usize]
                    .row((id & ROW_MASK) as usize)
                    .data();
                &data[data.len().min(tail)..]
            };
            let compare = |a: &Element, b: &Element| {
                a.key.cmp(&b.key).then_with(|| rest(a.id).cmp(rest(b.id)))
            };
            if tuning.stable_engine {
                engine::multi_merge_sort_stable_by_prefix_key(
                    &mut elements,
                    |e| e.key,
                    compare,
                )
            } else {
                engine::multi_merge_sort_by_prefix_key(&mut elements, |e| e.key, compare)
            }
        }
        _ => {
            if tuning.stable_engine {
                engine::multi_merge_sort_stable_by_radix_key(&mut elements, |e| e.key)
            } else {
                engine::multi_merge_sort_by_radix_key(&mut elements, |e| e.key)
            }
        }
    }
    let engine_time = sorting.elapsed();
    drop(keys);

    let gathering = Instant::now();
    let output = match tuning.gather {
        Gather::Take => gather_by_take(batches, &schema, &elements, batch_size)?,
        Gather::Interleave => {
            let sources: Vec<&RecordBatch> = batches.iter().collect();
            elements
                .par_chunks(batch_size)
                .map(|chunk| {
                    let indices: Vec<(usize, usize)> = chunk
                        .iter()
                        .map(|e| {
                            ((e.id >> ROW_BITS) as usize, (e.id & ROW_MASK) as usize)
                        })
                        .collect();
                    interleave_record_batch(&sources, &indices)
                })
                .collect::<std::result::Result<Vec<_>, ArrowError>>()?
        }
    };

    let stats = SortStats {
        path: SortPath::Engine,
        rows: total_rows,
        encode,
        prepare,
        engine: engine_time,
        gather: gathering.elapsed(),
    };
    Ok((output, stats))
}

/// Builds the output by position: every column of the input becomes one array, and each output
/// batch takes its rows from those arrays.
fn gather_by_take(
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
    elements: &[Element],
    batch_size: usize,
) -> Result<Vec<RecordBatch>> {
    let mut offsets = Vec::with_capacity(batches.len());
    let mut total = 0u64;
    for batch in &batches {
        offsets.push(total);
        total += batch.num_rows() as u64;
    }

    let columns: Vec<ArrayRef> = if batches.len() == 1 {
        batches[0].columns().to_vec()
    } else {
        (0..schema.fields().len())
            .into_par_iter()
            .map(|column| {
                let parts: Vec<&dyn Array> = batches
                    .iter()
                    .map(|batch| batch.column(column).as_ref())
                    .collect();
                concat(&parts)
            })
            .collect::<std::result::Result<Vec<_>, ArrowError>>()?
    };
    // The input is not needed any more: its rows are in `columns`.
    drop(batches);

    let position = |element: &Element| {
        offsets[(element.id >> ROW_BITS) as usize] + (element.id & ROW_MASK)
    };
    let narrow = total <= u32::MAX as u64;
    elements
        .par_chunks(batch_size)
        .map(|chunk| {
            let taken = if narrow {
                let indices = UInt32Array::from_iter_values(
                    chunk.iter().map(|e| position(e) as u32),
                );
                columns
                    .iter()
                    .map(|column| take(column.as_ref(), &indices, None))
                    .collect::<std::result::Result<Vec<_>, _>>()
            } else {
                let indices = UInt64Array::from_iter_values(chunk.iter().map(position));
                columns
                    .iter()
                    .map(|column| take(column.as_ref(), &indices, None))
                    .collect::<std::result::Result<Vec<_>, _>>()
            }?;
            let options = RecordBatchOptions::new().with_row_count(Some(chunk.len()));
            Ok(RecordBatch::try_new_with_options(
                Arc::clone(schema),
                taken,
                &options,
            )?)
        })
        .collect()
}

/// How many bytes `a` and `b` have in common at the beginning.
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// Eight bytes of `data`, `skip` bytes after its beginning, as a big-endian number padded with zeros.
#[inline]
fn prefix_key(data: &[u8], skip: usize) -> u64 {
    let rest = &data[data.len().min(skip)..];
    if rest.len() >= 8 {
        u64::from_be_bytes([
            rest[0], rest[1], rest[2], rest[3], rest[4], rest[5], rest[6], rest[7],
        ])
    } else {
        let mut bytes = [0u8; 8];
        bytes[..rest.len()].copy_from_slice(rest);
        u64::from_be_bytes(bytes)
    }
}

/// Rows that already come in order are not sorted again and, as far as possible, not copied.
///
/// When every batch is in order (a batch in the exact opposite order counts, read backwards), the
/// batches are merged by ranges: each step takes, from the batch with the least row, every row that
/// goes before the first row of the next batch. Batches that do not overlap come out whole and a
/// batch that overlaps its neighbours is sliced where they meet; a slice is a view, not a copy.
/// With rows of equal keys the batch that arrived first goes first.
///
/// Returns `None`, with nothing done, when a batch is in no order (seen in its first rows) or when
/// the batches are so interleaved that the ranges would be short.
fn merge_ordered_ranges(
    batches: &[RecordBatch],
    keys: &Keys,
    sorted_partitions: bool,
) -> Result<Option<Vec<RecordBatch>>> {
    let count = batches.len();

    // Sorted partitions are made of sorted batches: only batches that arrive as they were read are examined.
    let mut backwards = vec![false; count];
    if !sorted_partitions {
        let unordered = AtomicBool::new(false);
        backwards = (0..count)
            .into_par_iter()
            .map(|batch| match keys.direction(batch, &unordered) {
                Some(backwards) => backwards,
                None => {
                    unordered.store(true, AtomicOrdering::Relaxed);
                    false
                }
            })
            .collect();
        if unordered.load(AtomicOrdering::Relaxed) {
            return Ok(None);
        }
    }

    // `at(batch, row)` is a row of a batch read in its sorted direction.
    let at = |batch: usize, row: usize| {
        (
            batch,
            if backwards[batch] {
                batches[batch].num_rows() - 1 - row
            } else {
                row
            },
        )
    };

    // Each batch stands at its first row not taken yet. `goes_after` keeps on top of the heap the
    // batch with the least row and, among equal rows, the one that arrived first.
    let mut position = vec![0usize; count];
    let goes_after = |position: &[usize], a: usize, b: usize| match keys
        .compare(at(a, position[a]), at(b, position[b]))
    {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => a > b,
    };

    let mut heap: Vec<usize> = (0..count).collect();
    for start in (0..count / 2).rev() {
        sift_down(&mut heap, start, &|a, b| goes_after(&position, a, b));
    }

    let max_ranges = 4 * count + 16;
    let mut ranges: Vec<(usize, usize, usize)> = Vec::with_capacity(count);
    while let Some(&current) = heap.first() {
        let last = heap.len() - 1;
        heap.swap(0, last);
        heap.pop();
        sift_down(&mut heap, 0, &|a, b| goes_after(&position, a, b));

        let mut end = batches[current].num_rows();
        if let Some(&next) = heap.first() {
            // The first row of `current` that goes after the first row of the next batch.
            let bound = at(next, position[next]);
            let mut low = position[current] + 1;
            while low < end {
                let middle = low + (end - low) / 2;
                match keys.compare(at(current, middle), bound) {
                    Ordering::Less => low = middle + 1,
                    Ordering::Equal if current < next => low = middle + 1,
                    _ => end = middle,
                }
            }
        }

        ranges.push((current, position[current], end));
        if ranges.len() > max_ranges {
            return Ok(None);
        }

        position[current] = end;
        if end < batches[current].num_rows() {
            heap.push(current);
            sift_up(&mut heap, &|a, b| goes_after(&position, a, b));
        }
    }

    // A batch read backwards is turned around once; the ranges are then slices of it.
    let sources = batches
        .par_iter()
        .zip(backwards.par_iter())
        .map(|(batch, &backwards)| {
            if backwards && batch.num_rows() > 1 {
                let indices =
                    UInt32Array::from_iter_values((0..batch.num_rows() as u32).rev());
                take_record_batch(batch, &indices)
            } else {
                Ok(batch.clone())
            }
        })
        .collect::<std::result::Result<Vec<_>, ArrowError>>()?;

    let output = ranges
        .into_iter()
        .map(|(batch, begin, end)| sources[batch].slice(begin, end - begin))
        .collect();
    Ok(Some(output))
}

/// Binary heap over `heap`, with `goes_after(a, b)` true when `a` must come out after `b`.
fn sift_down(
    heap: &mut [usize],
    mut node: usize,
    goes_after: &dyn Fn(usize, usize) -> bool,
) {
    loop {
        let left = 2 * node + 1;
        if left >= heap.len() {
            return;
        }
        let right = left + 1;
        let child = if right < heap.len() && goes_after(heap[left], heap[right]) {
            right
        } else {
            left
        };
        if !goes_after(heap[node], heap[child]) {
            return;
        }
        heap.swap(node, child);
        node = child;
    }
}

fn sift_up(heap: &mut [usize], goes_after: &dyn Fn(usize, usize) -> bool) {
    let mut node = heap.len() - 1;
    while node > 0 {
        let parent = (node - 1) / 2;
        if !goes_after(heap[parent], heap[node]) {
            return;
        }
        heap.swap(parent, node);
        node = parent;
    }
}
