use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::{self, datatypes::SchemaRef};
use async_trait::async_trait;
use futures::StreamExt;

use super::FileFormat;
use crate::datasource::object_store::{ObjectReader, ObjectReaderStream};
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::file_format::{RocksdbExec, FileScanConfig};
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Statistics;
use std::collections::HashMap;
use crate::arrow::datatypes::{DataType, Field};

#[derive(Debug)]
pub struct RocksdbFormat {
    schema:  Schema,
}

impl RocksdbFormat {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema
        }
    }
}

#[async_trait]
impl FileFormat for RocksdbFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(&self, mut readers: ObjectReaderStream) -> Result<SchemaRef> {
        Ok(Arc::new(self.schema.to_owned()))
    }

    async fn infer_stats(&self, _reader: Arc<dyn ObjectReader>) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = RocksdbExec::new(conf);
        Ok(Arc::new(exec))
    }
}