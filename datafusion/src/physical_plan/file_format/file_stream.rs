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

//! A generic stream over file format readers that can be used by
//! any file format that read its files from start to end.

use crate::{
    datasource::{object_store::ObjectStore, PartitionedFile},
    error::Result as DataFusionResult,
};
use arrow::{
    datatypes::SchemaRef,
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use futures::Stream;
use std::{
    io::Read,
    iter,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use super::RecordBatchStream;

pub type FileIter =
    Box<dyn Iterator<Item = DataFusionResult<Box<dyn Read + Send + Sync>>> + Send + Sync>;
pub type BatchIter = Box<dyn Iterator<Item = ArrowResult<RecordBatch>> + Send + Sync>;

/// A stream that iterates record batch by record batch, file over file.
pub struct FileStream<F>
where
    F: Fn(Box<dyn Read + Send + Sync>, &Option<usize>) -> BatchIter
        + Send
        + Sync
        + Unpin
        + 'static,
{
    /// An iterator over record batches of the last file returned by file_iter
    batch_iter: BatchIter,
    /// An iterator over input files
    file_iter: FileIter,
    /// The stream schema (file schema after projection)
    schema: SchemaRef,
    /// The remaining number of records to parse, None if no limit
    remain: Option<usize>,
    /// A closure that takes a reader and an optional remaining number of lines
    /// (before reaching the limit) and returns a batch iterator. If the file reader
    /// is not capable of limiting the number of records in the last batch, the file
    /// stream will take care of truncating it.
    file_reader: F,
}

impl<F> FileStream<F>
where
    F: Fn(Box<dyn Read + Send + Sync>, &Option<usize>) -> BatchIter
        + Send
        + Sync
        + Unpin
        + 'static,
{
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        files: Vec<PartitionedFile>,
        file_reader: F,
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> Self {
        let read_iter = files.into_iter().map(move |f| -> DataFusionResult<_> {
            object_store
                .file_reader(f.file_meta.sized_file)?
                .sync_reader()
        });

        Self {
            file_iter: Box::new(read_iter),
            batch_iter: Box::new(iter::empty()),
            remain: limit,
            schema,
            file_reader,
        }
    }

    /// Acts as a flat_map of record batches over files.
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>> {
        match self.batch_iter.next() {
            Some(batch) => Some(batch),
            None => match self.file_iter.next() {
                Some(Ok(f)) => {
                    self.batch_iter = (self.file_reader)(f, &self.remain);
                    self.next_batch()
                }
                Some(Err(e)) => Some(Err(ArrowError::ExternalError(Box::new(e)))),
                None => None,
            },
        }
    }
}

impl<F> Stream for FileStream<F>
where
    F: Fn(Box<dyn Read + Send + Sync>, &Option<usize>) -> BatchIter
        + Send
        + Sync
        + Unpin
        + 'static,
{
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // check if finished or no limit
        match self.remain {
            Some(r) if r == 0 => return Poll::Ready(None),
            None => return Poll::Ready(self.get_mut().next_batch()),
            Some(r) => r,
        };

        Poll::Ready(match self.as_mut().next_batch() {
            Some(Ok(item)) => {
                if let Some(remain) = self.remain.as_mut() {
                    if *remain >= item.num_rows() {
                        *remain -= item.num_rows();
                        Some(Ok(item))
                    } else {
                        let len = *remain;
                        *remain = 0;
                        Some(Ok(RecordBatch::try_new(
                            item.schema(),
                            item.columns()
                                .iter()
                                .map(|column| column.slice(0, len))
                                .collect(),
                        )?))
                    }
                } else {
                    Some(Ok(item))
                }
            }
            other => other,
        })
    }
}

impl<F> RecordBatchStream for FileStream<F>
where
    F: Fn(Box<dyn Read + Send + Sync>, &Option<usize>) -> BatchIter
        + Send
        + Sync
        + Unpin
        + 'static,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
