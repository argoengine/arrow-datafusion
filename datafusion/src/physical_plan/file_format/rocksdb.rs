use crate::physical_plan::{ExecutionPlan, Partitioning, Distribution, SendableRecordBatchStream, DisplayFormatType, Statistics};
use std::any::Any;
use std::sync::Arc;
use crate::execution::runtime_env::RuntimeEnv;
use crate::physical_plan::metrics::MetricsSet;
use std::fmt::{Formatter, Debug};
use futures::future::OkInto;
use async_trait::async_trait;
use crate::physical_plan::file_format::FileScanConfig;
use crate::error::DataFusionError;
use super::file_stream::{BatchIter, FileStream};
use std::io::Read;
use arrow::record_batch::RecordBatch;
use arrow::error::Result;
use arrow::datatypes::*;
use crate::arrow::array::{ArrayRef, BooleanBuilder, Array};
use crate::physical_plan::expressions::PhysicalSortExpr;

#[derive(Debug, Clone)]
pub struct RocksdbExec{
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
}

/// Execution plan for scanning one or more Parquet partitions

impl RocksdbExec{
    /// Create a new Rocksdb reader execution plan provided file list and schema.
    pub fn new(base_config: FileScanConfig) -> Self{
        let (projected_schema, projected_statistics) = base_config.project();
        Self{
            base_config,
            projected_statistics,
            projected_schema,
        }
    }
}

#[async_trait]
impl ExecutionPlan for RocksdbExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        // 输出分区的个数
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // 叶子节点没有child
        vec![]
    }

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> crate::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize, runtime: Arc<RuntimeEnv>) -> crate::error::Result<SendableRecordBatchStream> {

        let batch_size = runtime.batch_size;
        let file_schema = Arc::clone(&self.base_config.file_schema);
        let projection = self.base_config.file_column_projection_indices();
        let fun = move |file, remaining: &Option<usize>| {
            Box::new(RocksdbReader::new(
                Arc::clone(&file_schema),
                batch_size,
                projection.clone(),
            )) as BatchIter
        };

        Ok(Box::pin(FileStream::new(
            Arc::clone(&self.base_config.object_store),
            self.base_config.file_groups[partition].clone(),
            fun,
            Arc::clone(&self.projected_schema),
            self.base_config.limit,
            self.base_config.table_partition_cols.clone(),
        )))
    }


    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "RocksdbExec:  limit={:?}, files={}",
                    self.base_config.limit,
                    super::FileGroupsDisplay(&self.base_config.file_groups),
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

pub struct RocksdbReader {
    /// Explicit schema for the rockdb
    schema: SchemaRef,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<usize>>,
    /// Number of records per batch
    batch_size: usize,
    last_id:Option<i64>,
}

impl RocksdbReader {
    pub fn new(
        schema: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<usize>>,) -> Self{
        Self{
            schema,
            projection,
            batch_size,
            last_id: Option::Some(std::i64::MIN),
        }
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        match &self.projection {
            Some(projection) => {
                let fields = self.schema.fields();
                let projected_fields: Vec<Field> =
                    projection.iter().map(|i| fields[*i].clone()).collect();

                Arc::new(Schema::new(projected_fields))
            }
            None => self.schema.clone(),
        }
    }
}

impl Iterator for RocksdbReader {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {

        if self.last_id == Some(std::i64::MAX)  {
            return None;
        }

        // 返回数据
        let fields = self.schema.fields();
        let projection: Vec<usize> = match self.projection {
            Some(ref v) => v.clone(),
            None => fields.iter().enumerate().map(|(i, _)| i).collect(),
        };

        let projected_fields: Vec<Field> = projection
            .iter()
            .map(|i| fields[*i].clone())
            .collect();
        let metadata = Some(self.schema.metadata().clone());
        let projected_schema = Arc::new(match metadata {
            None => Schema::new(projected_fields),
            Some(metadata) => Schema::new_with_metadata(projected_fields, metadata),
        });


        let mut builder = BooleanBuilder::new(10);
        builder.append_value(true);
        builder.append_value(true);
        builder.append_value(true);
        builder.append_value(true);
        builder.append_value(true);
        builder.append_value(false);
        builder.append_value(false);
        builder.append_value(false);
        builder.append_value(false);
        builder.append_value(false);

        let arc:ArrayRef= Arc::new(builder.finish());

        let arr = vec![arc];

        // 现在demo中未实现真正读取rocksdb
        let result = RecordBatch::try_new(projected_schema,arr);

        // 更新last_id 标识上次读取的位置  分批次读取
        self.last_id = Option::Some(std::i64::MAX);
        Some(result)
    }
}