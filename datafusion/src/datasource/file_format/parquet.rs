// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Parquet format abstractions

use std::io::Read;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream::StreamExt;
use parquet::arrow::ArrowReader;
use parquet::arrow::ParquetFileArrowReader;
use parquet::errors::ParquetError;
use parquet::errors::Result as ParquetResult;
use parquet::file::reader::ChunkReader;
use parquet::file::reader::Length;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::statistics::Statistics as ParquetStatistics;

use super::FileFormat;
use super::PartitionedFile;
use super::{create_max_min_accs, get_col_stats};
use crate::arrow::datatypes::{DataType, Field};
use crate::datasource::object_store::ObjectReader;
use crate::datasource::object_store::ObjectStoreRegistry;
use crate::datasource::object_store::SizedFile;
use crate::datasource::object_store::SizedFileStream;
use crate::error::DataFusionError;
use crate::error::Result;
use crate::logical_plan::combine_filters;
use crate::logical_plan::Expr;
use crate::physical_plan::expressions::{MaxAccumulator, MinAccumulator};
use crate::physical_plan::file_format::ParquetExec;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::{Accumulator, Statistics};
use crate::scalar::ScalarValue;

/// The Apache Parquet `FileFormat` implementation
pub struct ParquetFormat {
    object_store_registry: Arc<ObjectStoreRegistry>,
    enable_pruning: bool,
}

impl Default for ParquetFormat {
    fn default() -> Self {
        Self {
            enable_pruning: true,
            object_store_registry: Arc::new(ObjectStoreRegistry::new()),
        }
    }
}

impl ParquetFormat {
    /// Create Parquet with the given object store and default values
    pub fn new(object_store_registry: Arc<ObjectStoreRegistry>) -> Self {
        Self {
            object_store_registry,
            ..Default::default()
        }
    }

    /// Activate statistics based row group level pruning
    /// - defaults to true
    pub fn with_enable_pruning(&mut self, enable: bool) -> &mut Self {
        self.enable_pruning = enable;
        self
    }
}

#[async_trait]
impl FileFormat for ParquetFormat {
    async fn infer_schema(&self, mut paths: SizedFileStream) -> Result<SchemaRef> {
        // We currently get the schema information from the first file rather than do
        // schema merging and this is a limitation.
        // See https://issues.apache.org/jira/browse/ARROW-11017
        let first_file = paths
            .next()
            .await
            .ok_or_else(|| DataFusionError::Plan("No data file found".to_owned()))??;
        let (schema, _) = fetch_metadata(&self.object_store_registry, first_file)?;
        Ok(Arc::new(schema))
    }

    async fn infer_stats(&self, path: SizedFile) -> Result<Statistics> {
        let (_, stats) = fetch_metadata(&self.object_store_registry, path)?;
        Ok(stats)
    }

    async fn create_physical_plan(
        &self,
        schema: SchemaRef,
        files: Vec<Vec<PartitionedFile>>,
        statistics: Statistics,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // If enable pruning then combine the filters to build the predicate.
        // If disable pruning then set the predicate to None, thus readers
        // will not prune data based on the statistics.
        let predicate = if self.enable_pruning {
            combine_filters(filters)
        } else {
            None
        };

        Ok(Arc::new(ParquetExec::new(
            Arc::clone(&self.object_store_registry),
            files,
            statistics,
            schema,
            projection.clone(),
            predicate,
            batch_size,
            limit,
        )))
    }

    fn object_store_registry(&self) -> &Arc<ObjectStoreRegistry> {
        &self.object_store_registry
    }
}

fn summarize_min_max(
    max_values: &mut Vec<Option<MaxAccumulator>>,
    min_values: &mut Vec<Option<MinAccumulator>>,
    fields: &[Field],
    i: usize,
    stat: &ParquetStatistics,
) {
    match stat {
        ParquetStatistics::Boolean(s) => {
            if let DataType::Boolean = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update(&[ScalarValue::Boolean(Some(*s.max()))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update(&[ScalarValue::Boolean(Some(*s.min()))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                }
            }
        }
        ParquetStatistics::Int32(s) => {
            if let DataType::Int32 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update(&[ScalarValue::Int32(Some(*s.max()))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update(&[ScalarValue::Int32(Some(*s.min()))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                }
            }
        }
        ParquetStatistics::Int64(s) => {
            if let DataType::Int64 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update(&[ScalarValue::Int64(Some(*s.max()))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update(&[ScalarValue::Int64(Some(*s.min()))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                }
            }
        }
        ParquetStatistics::Float(s) => {
            if let DataType::Float32 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update(&[ScalarValue::Float32(Some(*s.max()))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update(&[ScalarValue::Float32(Some(*s.min()))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                }
            }
        }
        ParquetStatistics::Double(s) => {
            if let DataType::Float64 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update(&[ScalarValue::Float64(Some(*s.max()))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update(&[ScalarValue::Float64(Some(*s.min()))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

/// Read and parse the metadata of the Parquet file at location `path`
fn fetch_metadata(
    object_store_registry: &ObjectStoreRegistry,
    fmeta: SizedFile,
) -> Result<(Schema, Statistics)> {
    let object_store = object_store_registry.get_by_uri(&fmeta.path)?;
    let obj_reader = ChunkObjectReader(object_store.file_reader(fmeta)?);
    let file_reader = Arc::new(SerializedFileReader::new(obj_reader)?);
    let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
    let schema = arrow_reader.get_schema()?;
    let num_fields = schema.fields().len();
    let fields = schema.fields().to_vec();
    let meta_data = arrow_reader.get_metadata();

    let mut num_rows = 0;
    let mut total_byte_size = 0;
    let mut null_counts = vec![0; num_fields];
    let mut has_statistics = false;

    let (mut max_values, mut min_values) = create_max_min_accs(&schema);

    for row_group_meta in meta_data.row_groups() {
        num_rows += row_group_meta.num_rows();
        total_byte_size += row_group_meta.total_byte_size();

        let columns_null_counts = row_group_meta
            .columns()
            .iter()
            .flat_map(|c| c.statistics().map(|stats| stats.null_count()));

        for (i, cnt) in columns_null_counts.enumerate() {
            null_counts[i] += cnt as usize
        }

        for (i, column) in row_group_meta.columns().iter().enumerate() {
            if let Some(stat) = column.statistics() {
                has_statistics = true;
                summarize_min_max(&mut max_values, &mut min_values, &fields, i, stat)
            }
        }
    }

    let column_stats = if has_statistics {
        Some(get_col_stats(
            &schema,
            null_counts,
            &mut max_values,
            &mut min_values,
        ))
    } else {
        None
    };

    let statistics = Statistics {
        num_rows: Some(num_rows as usize),
        total_byte_size: Some(total_byte_size as usize),
        column_statistics: column_stats,
        is_exact: true,
    };

    Ok((schema, statistics))
}

/// A wrapper around the object reader to make it implement `ChunkReader`
pub struct ChunkObjectReader(pub Arc<dyn ObjectReader>);

impl Length for ChunkObjectReader {
    fn len(&self) -> u64 {
        self.0.length()
    }
}

impl ChunkReader for ChunkObjectReader {
    type T = Box<dyn Read + Send + Sync>;

    fn get_read(&self, start: u64, length: usize) -> ParquetResult<Self::T> {
        self.0
            .sync_chunk_reader(start, length)
            .map_err(|e| ParquetError::ArrowError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        datasource::object_store::local::{local_sized_file, local_sized_file_stream},
        physical_plan::collect,
    };

    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array,
        TimestampNanosecondArray,
    };
    use futures::StreamExt;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let projection = None;
        let exec = get_exec("alltypes_plain.parquet", &projection, 2, None).await?;
        let stream = exec.execute(0).await?;

        let tt_batches = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 4 /* 8/2 */);

        // test metadata
        assert_eq!(exec.statistics().num_rows, Some(8));
        assert_eq!(exec.statistics().total_byte_size, Some(671));

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let projection = None;
        let exec = get_exec("alltypes_plain.parquet", &projection, 1024, Some(1)).await?;

        // note: even if the limit is set, the executor rounds up to the batch size
        assert_eq!(exec.statistics().num_rows, Some(8));
        assert_eq!(exec.statistics().total_byte_size, Some(671));
        assert!(exec.statistics().is_exact);
        let batches = collect(exec).await?;
        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_alltypes_plain_parquet() -> Result<()> {
        let projection = None;
        let exec = get_exec("alltypes_plain.parquet", &projection, 1024, None).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        let y = x.join("\n");
        assert_eq!(
            "id: Int32\n\
             bool_col: Boolean\n\
             tinyint_col: Int32\n\
             smallint_col: Int32\n\
             int_col: Int32\n\
             bigint_col: Int64\n\
             float_col: Float32\n\
             double_col: Float64\n\
             date_string_col: Binary\n\
             string_col: Binary\n\
             timestamp_col: Timestamp(Nanosecond, None)",
            y
        );

        let batches = collect(exec).await?;

        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_bool_alltypes_plain_parquet() -> Result<()> {
        let projection = Some(vec![1]);
        let exec = get_exec("alltypes_plain.parquet", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let mut values: Vec<bool> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_i32_alltypes_plain_parquet() -> Result<()> {
        let projection = Some(vec![0]);
        let exec = get_exec("alltypes_plain.parquet", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut values: Vec<i32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{:?}", values));

        Ok(())
    }

    #[tokio::test]
    async fn read_i96_alltypes_plain_parquet() -> Result<()> {
        let projection = Some(vec![10]);
        let exec = get_exec("alltypes_plain.parquet", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let mut values: Vec<i64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1235865600000000000, 1235865660000000000, 1238544000000000000, 1238544060000000000, 1233446400000000000, 1233446460000000000, 1230768000000000000, 1230768060000000000]", format!("{:?}", values));

        Ok(())
    }

    #[tokio::test]
    async fn read_f32_alltypes_plain_parquet() -> Result<()> {
        let projection = Some(vec![6]);
        let exec = get_exec("alltypes_plain.parquet", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let mut values: Vec<f32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_f64_alltypes_plain_parquet() -> Result<()> {
        let projection = Some(vec![7]);
        let exec = get_exec("alltypes_plain.parquet", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut values: Vec<f64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_binary_alltypes_plain_parquet() -> Result<()> {
        let projection = Some(vec![9]);
        let exec = get_exec("alltypes_plain.parquet", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut values: Vec<&str> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{:?}", values)
        );

        Ok(())
    }

    async fn get_exec(
        file_name: &str,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, file_name);
        let format = ParquetFormat::default();
        let schema = format
            .infer_schema(local_sized_file_stream(vec![filename.clone()]))
            .await
            .expect("Schema inference");
        let stats = format
            .infer_stats(local_sized_file(filename.clone()))
            .await
            .expect("Stats inference");
        let files = vec![vec![PartitionedFile {
            file: local_sized_file(filename.clone()),
        }]];
        let exec = format
            .create_physical_plan(
                schema,
                files,
                stats,
                projection,
                batch_size,
                &[],
                limit,
            )
            .await?;
        Ok(exec)
    }
}
