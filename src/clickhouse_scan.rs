use std::{error::Error, sync::Arc};
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId, Inserter},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
    Connection, Result,
};
use clickhouse_rs::{Pool, types::SqlType};
use tokio::runtime::Runtime;

#[repr(C)]
struct ClickHouseScanBindData {
    url: String,
    user: String,
    password: String,
    query: String,
}

impl Drop for ClickHouseScanBindData {
    fn drop(&mut self) {}
}

impl Free for ClickHouseScanBindData {
    fn free(&mut self) {}
}

#[repr(C)]
struct ClickHouseScanInitData {
    runtime: Option<Arc<Runtime>>,
    block_data: Option<Vec<Vec<String>>>,
    column_types: Option<Vec<LogicalTypeId>>,
    column_names: Option<Vec<String>>,
    current_row: usize,
    total_rows: usize,
    done: bool,
}

impl Drop for ClickHouseScanInitData {
    fn drop(&mut self) {
        self.done = true;
        self.runtime.take();
        self.block_data.take();
        self.column_types.take();
        self.column_names.take();
    }
}

impl Free for ClickHouseScanInitData {
    fn free(&mut self) {
        self.done = true;
        self.current_row = self.total_rows;
        self.runtime.take();
    }
}

fn map_clickhouse_type(sql_type: SqlType) -> LogicalTypeId {
    match sql_type {
        SqlType::Int8 | SqlType::Int16 | SqlType::Int32 => LogicalTypeId::Integer,
        SqlType::Int64 => LogicalTypeId::Bigint,
        SqlType::UInt8 | SqlType::UInt16 | SqlType::UInt32 => LogicalTypeId::UInteger,
        SqlType::UInt64 => LogicalTypeId::UBigint,
        SqlType::Float32 => LogicalTypeId::Float,
        SqlType::Float64 => LogicalTypeId::Double,
        SqlType::String | SqlType::FixedString(_) => LogicalTypeId::Varchar,
        SqlType::Date => LogicalTypeId::Date,
        SqlType::DateTime(_) => LogicalTypeId::Timestamp,
        SqlType::Bool => LogicalTypeId::Boolean,
        _ => LogicalTypeId::Varchar,
    }
}

struct ClickHouseScanVTab;

impl VTab for ClickHouseScanVTab {
    type InitData = ClickHouseScanInitData;
    type BindData = ClickHouseScanBindData;

    unsafe fn bind(bind: &BindInfo, data: *mut Self::BindData) -> Result<(), Box<dyn Error>> {
        if data.is_null() {
            return Err("Invalid bind data pointer".into());
        }

        let query = bind.get_parameter(0).to_string();
        let url = bind.get_named_parameter("url")
            .map(|v| v.to_string())
            .unwrap_or_else(|| std::env::var("CLICKHOUSE_URL")
                .unwrap_or_else(|_| "tcp://localhost:9000".to_string()));
        let user = bind.get_named_parameter("user")
            .map(|v| v.to_string())
            .unwrap_or_else(|| std::env::var("CLICKHOUSE_USER")
                .unwrap_or_else(|_| "default".to_string()));
        let password = bind.get_named_parameter("password")
            .map(|v| v.to_string())
            .unwrap_or_else(|| std::env::var("CLICKHOUSE_PASSWORD")
                .unwrap_or_default());

        println!("Parameters - URL: {}, User: {}, Query: {}", url, user, query);

        unsafe {
            (*data) = ClickHouseScanBindData {
                url,
                user,
                password,
                query,
            };
        }

        bind.add_result_column("version", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        
        Ok(())
    }

    unsafe fn init(info: &InitInfo, data: *mut Self::InitData) -> Result<(), Box<dyn Error>> {
        if data.is_null() {
            return Err("Invalid init data pointer".into());
        }

        let bind_data = info.get_bind_data::<ClickHouseScanBindData>();
        if bind_data.is_null() {
            return Err("Invalid bind data".into());
        }

        let runtime = Arc::new(Runtime::new()?);
        let runtime_clone = runtime.clone();

        let result = runtime.block_on(async {
            let pool = Pool::new((*bind_data).url.clone());
            let mut client = pool.get_handle().await?;
            let block = client.query(&(*bind_data).query).fetch_all().await?;
            
            let columns = block.columns();
            let mut names = Vec::new();
            let mut types = Vec::new();
            let mut data: Vec<Vec<String>> = Vec::new();

            for col in columns {
                names.push(col.name().to_string());
                types.push(map_clickhouse_type(col.sql_type()));
                data.push(Vec::new());
            }

            let mut row_count = 0;
            for row in block.rows() {
                for (col_idx, col) in columns.iter().enumerate() {
                    let value = match col.sql_type() {
                        SqlType::Int8 | SqlType::Int16 | SqlType::Int32 => {
                            match row.get::<i32, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => "NULL".to_string()
                            }
                        },
                        SqlType::Int64 => {
                            match row.get::<i64, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => "NULL".to_string()
                            }
                        },
                        SqlType::UInt8 | SqlType::UInt16 | SqlType::UInt32 => {
                            match row.get::<u32, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => "NULL".to_string()
                            }
                        },
                        SqlType::UInt64 => {
                            match row.get::<u64, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => "NULL".to_string()
                            }
                        },
                        SqlType::Float32 => {
                            match row.get::<f32, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => "NULL".to_string()
                            }
                        },
                        SqlType::Float64 => {
                            match row.get::<f64, &str>(col.name()) {
                                Ok(val) => val.to_string(),
                                Err(_) => "NULL".to_string()
                            }
                        },
                        _ => {
                            match row.get::<String, &str>(col.name()) {
                                Ok(val) => val,
                                Err(_) => "NULL".to_string()
                            }
                        }
                    };
                    data[col_idx].push(value);
                }
                row_count += 1;
            }

            Ok::<(Vec<Vec<String>>, Vec<String>, Vec<LogicalTypeId>, usize), Box<dyn Error>>((data, names, types, row_count))
        })?;

        let (block_data, column_names, column_types, total_rows) = result;

        unsafe {
            (*data) = ClickHouseScanInitData {
                runtime: Some(runtime_clone),
                block_data: Some(block_data),
                column_types: Some(column_types),
                column_names: Some(column_names),
                current_row: 0,
                total_rows,
                done: false,
            };
        }

        Ok(())
    }

    unsafe fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data::<ClickHouseScanInitData>();
        
        if init_data.is_null() {
            return Err("Invalid init data pointer".into());
        }

        unsafe {
            if (*init_data).done || (*init_data).current_row >= (*init_data).total_rows {
                output.set_len(0);
                (*init_data).done = true;
                return Ok(());
            }

            let block_data = match (*init_data).block_data.as_ref() {
                Some(data) => data,
                None => return Err("Block data is not available".into()),
            };

            let column_types = match (*init_data).column_types.as_ref() {
                Some(types) => types,
                None => return Err("Column types are not available".into()),
            };

            let batch_size = 1024.min((*init_data).total_rows - (*init_data).current_row);

            for col_idx in 0..column_types.len() {
                let mut vector = output.flat_vector(col_idx);
                let type_id = &column_types[col_idx];

                match type_id {
                    LogicalTypeId::Integer | LogicalTypeId::UInteger => {
                        let slice = vector.as_mut_slice::<i32>();
                        for row_offset in 0..batch_size {
                            let row_idx = (*init_data).current_row + row_offset;
                            if let Ok(val) = block_data[col_idx][row_idx].parse::<i32>() {
                                slice[row_offset] = val;
                            } else {
                                slice[row_offset] = 0;
                            }
                        }
                    },
                    _ => {
                        for row_offset in 0..batch_size {
                            let row_idx = (*init_data).current_row + row_offset;
                            let val = block_data[col_idx][row_idx].as_str();
                            Inserter::insert(&mut vector, row_offset, val);
                        }
                    }
                }
            }

            (*init_data).current_row += batch_size;
            output.set_len(batch_size);
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

pub fn register_clickhouse_scan(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ClickHouseScanVTab>("clickhouse_scan")?;
    Ok(())
}
