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

//! Execution plan for reading CSV files

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use arrow::csv;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use futures::Stream;
use std::any::Any;
use std::fs::File;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;

/// Execution plan for scanning a CSV file
#[derive(Debug, Clone)]
pub struct CsvExec {
    /// List of data files
    files: Vec<String>,
    /// Schema representing the CSV file
    schema: SchemaRef,
    /// Provided statistics
    statistics: Statistics,
    /// Does the CSV file have a header?
    has_header: bool,
    /// An optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Schema after the projection has been applied
    projected_schema: SchemaRef,
    /// Batch size
    batch_size: usize,
    /// Limit in nr. of rows
    limit: Option<usize>,
}

impl CsvExec {
    /// Create a new CSV reader execution plan provided file list and schema
    /// TODO: support partitiondd file list (Vec<Vec<PartitionedFile>>)
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        files: Vec<String>,
        statistics: Statistics,
        schema: SchemaRef,
        has_header: bool,
        delimiter: u8,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        let projected_schema = match &projection {
            None => Arc::clone(&schema),
            Some(p) => Arc::new(Schema::new(
                p.iter().map(|i| schema.field(*i).clone()).collect(),
            )),
        };

        Ok(Self {
            files,
            schema,
            statistics,
            has_header,
            delimiter: Some(delimiter),
            projection,
            projected_schema,
            batch_size,
            limit,
        })
    }
}

#[async_trait]
impl ExecutionPlan for CsvExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.files.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(CsvStream::try_new(
            &self.files[partition],
            self.schema.clone(),
            self.has_header,
            self.delimiter,
            &self.projection,
            self.batch_size,
            self.limit,
        )?))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "CsvExec: has_header={}, batch_size={}, limit={:?}, partitions=[{}]",
                    self.has_header,
                    self.batch_size,
                    self.limit,
                    self.files.join(", ")
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
    }
}

/// Iterator over batches
struct CsvStream<R: Read> {
    /// Arrow CSV reader
    reader: csv::Reader<R>,
}
impl CsvStream<File> {
    /// Create an iterator for a CSV file
    pub fn try_new(
        filename: &str,
        schema: SchemaRef,
        has_header: bool,
        delimiter: Option<u8>,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        let file = File::open(filename)?;
        Self::try_new_from_reader(
            file, schema, has_header, delimiter, projection, batch_size, limit,
        )
    }
}
impl<R: Read> CsvStream<R> {
    /// Create an iterator for a reader
    pub fn try_new_from_reader(
        reader: R,
        schema: SchemaRef,
        has_header: bool,
        delimiter: Option<u8>,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<CsvStream<R>> {
        let start_line = if has_header { 1 } else { 0 };
        let bounds = limit.map(|x| (0, x + start_line));

        let reader = csv::Reader::new(
            reader,
            schema,
            has_header,
            delimiter,
            batch_size,
            bounds,
            projection.clone(),
        );

        Ok(Self { reader })
    }
}

impl<R: Read + Unpin> Stream for CsvStream<R> {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.reader.next())
    }
}

impl<R: Read + Unpin> RecordBatchStream for CsvStream<R> {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::aggr_test_schema;
    use futures::StreamExt;

    #[tokio::test]
    async fn csv_exec_with_projection() -> Result<()> {
        let schema = aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv = CsvExec::try_new(
            vec![path],
            Statistics::default(),
            schema,
            true,
            b',',
            Some(vec![0, 2, 4]),
            1024,
            None,
        )?;
        assert_eq!(13, csv.schema.fields().len());
        assert_eq!(3, csv.projected_schema.fields().len());
        assert_eq!(3, csv.schema().fields().len());
        let mut stream = csv.execute(0).await?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        let batch_schema = batch.schema();
        assert_eq!(3, batch_schema.fields().len());
        assert_eq!("c1", batch_schema.field(0).name());
        assert_eq!("c3", batch_schema.field(1).name());
        assert_eq!("c5", batch_schema.field(2).name());
        Ok(())
    }

    #[tokio::test]
    async fn csv_exec_without_projection() -> Result<()> {
        let schema = aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv = CsvExec::try_new(
            vec![path],
            Statistics::default(),
            schema,
            true,
            b',',
            None,
            1024,
            None,
        )?;
        assert_eq!(13, csv.schema.fields().len());
        assert_eq!(13, csv.projected_schema.fields().len());
        assert_eq!(13, csv.schema().fields().len());
        let mut it = csv.execute(0).await?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(13, batch.num_columns());
        let batch_schema = batch.schema();
        assert_eq!(13, batch_schema.fields().len());
        assert_eq!("c1", batch_schema.field(0).name());
        assert_eq!("c2", batch_schema.field(1).name());
        assert_eq!("c3", batch_schema.field(2).name());
        Ok(())
    }
}