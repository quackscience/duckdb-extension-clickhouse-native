use std::{error::Error, sync::Arc};
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId, Inserter},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
    Connection, Result,
};
use clickhouse_rs::{Pool, types::SqlType};
use tokio::runtime::Runtime;
use std::ptr;

#[repr(C)]
struct ClickHouseScanBindData {
    url: String,
    user: String,
    password: String,
    query: String,
    column_names: Vec<String>,
    column_types: Vec<LogicalTypeId>,
}

impl Drop for ClickHouseScanBindData {
    fn drop(&mut self) {
        self.column_names.clear();
        self.column_types.clear();
    }
}

impl Free for ClickHouseScanBindData {
    fn free(&mut self) {
        // Explicitly clear vectors to ensure proper cleanup
        self.column_names.clear();
        self.column_types.clear();
    }
}

#[repr(C)]
struct ClickHouseScanInitData {
    runtime: Option<Arc<Runtime>>,
    block_data: Option<Vec<Vec<String>>>,
    column_types: Vec<LogicalTypeId>,
    column_names: Vec<String>,
    current_row: usize,
    total_rows: usize,
    done: bool,
}

impl Drop for ClickHouseScanInitData {
    fn drop(&mut self) {
        self.done = true;
        self.runtime.take();
        self.block_data.take();
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
    // println!("Mapping SQL type: {:?}", sql_type);  // Debug print
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
        // Default to Integer for numeric literals
        _ => LogicalTypeId::Integer
    }
}

struct ClickHouseScanVTab;

impl VTab for ClickHouseScanVTab {
    type InitData = ClickHouseScanInitData;
    type BindData = ClickHouseScanBindData;

    // patch
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

    // println!("Parameters - URL: {}, User: {}, Query: {}", url, user, query);

    let runtime = Arc::new(Runtime::new()?);

    let result = runtime.block_on(async {
        let pool = Pool::new(url.clone());
        let mut client = pool.get_handle().await?;
        let block = client.query(&query).fetch_all().await?;

        let columns = block.columns();
        let mut names = Vec::new();
        let mut types = Vec::new();

        for col in columns {
            names.push(col.name().to_string());
            types.push(map_clickhouse_type(col.sql_type()));
        }

        Ok::<(Vec<String>, Vec<LogicalTypeId>), Box<dyn Error>>((names, types))
    })?;

    let (names, types) = result;
    
    // Create a new vector by recreating LogicalTypeId values
    let types_for_iteration: Vec<LogicalTypeId> = types.iter().map(|type_id| {
        match type_id {
            LogicalTypeId::Integer => LogicalTypeId::Integer,
            LogicalTypeId::Bigint => LogicalTypeId::Bigint,
            LogicalTypeId::UInteger => LogicalTypeId::UInteger,
            LogicalTypeId::UBigint => LogicalTypeId::UBigint,
            LogicalTypeId::Float => LogicalTypeId::Float,
            LogicalTypeId::Double => LogicalTypeId::Double,
            LogicalTypeId::Varchar => LogicalTypeId::Varchar,
            LogicalTypeId::Date => LogicalTypeId::Date,
            LogicalTypeId::Timestamp => LogicalTypeId::Timestamp,
            LogicalTypeId::Boolean => LogicalTypeId::Boolean,
            _ => LogicalTypeId::Varchar,
        }
    }).collect();

    // Create bind data
    let bind_data = ClickHouseScanBindData {
        url,
        user,
        password,
        query,
        column_names: names.clone(),
        column_types: types,
    };

    // Add result columns before storing the bind data
    for (name, type_id) in names.iter().zip(types_for_iteration.iter()) {
        let type_handle = LogicalTypeHandle::from(match type_id {
            LogicalTypeId::Integer => LogicalTypeId::Integer,
            LogicalTypeId::Bigint => LogicalTypeId::Bigint,
            LogicalTypeId::UInteger => LogicalTypeId::UInteger,
            LogicalTypeId::UBigint => LogicalTypeId::UBigint,
            LogicalTypeId::Float => LogicalTypeId::Float,
            LogicalTypeId::Double => LogicalTypeId::Double,
            LogicalTypeId::Varchar => LogicalTypeId::Varchar,
            LogicalTypeId::Date => LogicalTypeId::Date,
            LogicalTypeId::Timestamp => LogicalTypeId::Timestamp,
            LogicalTypeId::Boolean => LogicalTypeId::Boolean,
            _ => LogicalTypeId::Varchar,
        });
        bind.add_result_column(name, type_handle);
    }

    // Store the bind data after adding columns
    unsafe {
        ptr::write(data, bind_data);
    }

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

    let result = runtime.block_on(async {
        let pool = Pool::new((*bind_data).url.clone());
        let mut client = pool.get_handle().await?;
        let block = client.query(&(*bind_data).query).fetch_all().await?;

        let columns = block.columns();
        let mut data: Vec<Vec<String>> = Vec::new();

        for _ in columns {
            data.push(Vec::new());
        }

        let mut row_count = 0;
        for row in block.rows() {
            for (col_idx, col) in columns.iter().enumerate() {
                let value = match col.sql_type() {
                    SqlType::UInt8 => {
                        match row.get::<u8, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "0".to_string()
                        }
                    },
                    SqlType::UInt16 => {
                        match row.get::<u16, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "0".to_string()
                        }
                    },
                    SqlType::UInt32 => {
                        match row.get::<u32, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "0".to_string()
                        }
                    },
                    SqlType::UInt64 => {
                        match row.get::<u64, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "0".to_string()
                        }
                    },
                    SqlType::Int8 => {
                        match row.get::<i8, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "0".to_string()
                        }
                    },
                    SqlType::Int16 => {
                        match row.get::<i16, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "0".to_string()
                        }
                    },
                    SqlType::Int32 => {
                        match row.get::<i32, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "0".to_string()
                        }
                    },
                    SqlType::Int64 => {
                        match row.get::<i64, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "0".to_string()
                        }
                    },
                    SqlType::Float32 => {
                        match row.get::<f32, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "0.0".to_string()
                        }
                    },
                    SqlType::Float64 => {
                        match row.get::<f64, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "0.0".to_string()
                        }
                    },
                    SqlType::String | SqlType::FixedString(_) => {
                        match row.get::<String, &str>(col.name()) {
                            Ok(val) => val,
                            Err(_) => String::new()
                        }
                    },
                    SqlType::Bool => {
                        match row.get::<bool, &str>(col.name()) {
                            Ok(val) => val.to_string(),
                            Err(_) => "false".to_string()
                        }
                    },
                    SqlType::Date => {
                        match row.get::<String, &str>(col.name()) {
                            Ok(val) => val,
                            Err(_) => "1970-01-01".to_string()
                        }
                    },
                    SqlType::DateTime(_) => {
                        match row.get::<String, &str>(col.name()) {
                            Ok(val) => val,
                            Err(_) => "1970-01-01 00:00:00".to_string()
                        }
                    },
                    _ => {
                        match row.get::<String, &str>(col.name()) {
                            Ok(val) => val,
                            Err(_) => "0".to_string()
                        }
                    }
                };
                data[col_idx].push(value);
            }
            row_count += 1;
        }

        Ok::<(Vec<Vec<String>>, usize), Box<dyn Error>>((data, row_count))
    })?;

    let (block_data, total_rows) = result;

    // Create new vectors by mapping over references
    let column_types = unsafe {
        (*bind_data).column_types.iter().map(|type_id| {
            match type_id {
                LogicalTypeId::Integer => LogicalTypeId::Integer,
                LogicalTypeId::Bigint => LogicalTypeId::Bigint,
                LogicalTypeId::UInteger => LogicalTypeId::UInteger,
                LogicalTypeId::UBigint => LogicalTypeId::UBigint,
                LogicalTypeId::Float => LogicalTypeId::Float,
                LogicalTypeId::Double => LogicalTypeId::Double,
                LogicalTypeId::Varchar => LogicalTypeId::Varchar,
                LogicalTypeId::Date => LogicalTypeId::Date,
                LogicalTypeId::Timestamp => LogicalTypeId::Timestamp,
                LogicalTypeId::Boolean => LogicalTypeId::Boolean,
                _ => LogicalTypeId::Varchar,
            }
        }).collect::<Vec<_>>()
    };

    let column_names = unsafe { (*bind_data).column_names.clone() };

    // Create init data using ptr::write
    unsafe {
        ptr::write(data, ClickHouseScanInitData {
            runtime: Some(runtime),
            block_data: Some(block_data),
            column_types,
            column_names,
            current_row: 0,
            total_rows,
            done: false,
        });
    }

    Ok(())
}

    // end patch

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

        let column_types = &(*init_data).column_types;

        let batch_size = 1024.min((*init_data).total_rows - (*init_data).current_row);

        for col_idx in 0..column_types.len() {
            let mut vector = output.flat_vector(col_idx);
            let type_id = &column_types[col_idx];

            match type_id {
		LogicalTypeId::Integer | LogicalTypeId::UInteger => {
	                let slice = vector.as_mut_slice::<i32>();
	                for row_offset in 0..batch_size {
	                    let row_idx = (*init_data).current_row + row_offset;
	                    let val_str = &block_data[col_idx][row_idx];
	                    // println!("Parsing value: {}", val_str); // Debug print
	                    
	                    // Try parsing with different number bases
	                    let val = if let Ok(v) = val_str.parse::<i32>() {
	                        v
	                    } else if let Ok(v) = val_str.parse::<u32>() {
	                        v as i32
	                    } else if let Ok(v) = i32::from_str_radix(val_str.trim(), 10) {
	                        v
	                    } else {
	                        println!("Failed to parse: {}", val_str); // Debug print
	                        0
	                    };
	                    slice[row_offset] = val;
	                }
	        },
                LogicalTypeId::UInteger => {
                    let slice = vector.as_mut_slice::<i32>();
                    for row_offset in 0..batch_size {
                        let row_idx = (*init_data).current_row + row_offset;
                        // Try parsing as different unsigned integer types
                        let val = if let Ok(v) = block_data[col_idx][row_idx].parse::<u32>() {
                            v as i32
                        } else if let Ok(v) = block_data[col_idx][row_idx].parse::<u16>() {
                            v as i32
                        } else if let Ok(v) = block_data[col_idx][row_idx].parse::<u8>() {
                            v as i32
                        } else {
                            0
                        };
                        slice[row_offset] = val;
                    }
                },
                LogicalTypeId::Bigint => {
                    let slice = vector.as_mut_slice::<i64>();
                    for row_offset in 0..batch_size {
                        let row_idx = (*init_data).current_row + row_offset;
                        if let Ok(val) = block_data[col_idx][row_idx].parse::<i64>() {
                            slice[row_offset] = val;
                        } else {
                            slice[row_offset] = 0;
                        }
                    }
                },
                LogicalTypeId::UBigint => {
                    let slice = vector.as_mut_slice::<i64>();
                    for row_offset in 0..batch_size {
                        let row_idx = (*init_data).current_row + row_offset;
                        if let Ok(val) = block_data[col_idx][row_idx].parse::<u64>() {
                            slice[row_offset] = val as i64;
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
    // end func

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

pub fn register_clickhouse_scan(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ClickHouseScanVTab>("clickhouse_scan")?;
    Ok(())
}
