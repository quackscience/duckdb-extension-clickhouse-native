use std::{error::Error, ffi::{c_char, CStr, CString}, fs::File, io::{self, Read, BufReader, Seek}};
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId, Inserter},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use byteorder::{ReadBytesExt, LittleEndian};

// All original type definitions remain exactly the same
#[allow(dead_code)]
#[derive(Debug)]
enum ColumnType {
    String, UInt8, UInt64, Int, Enum8, Unsupported(String),
}

#[derive(Debug)]
enum ColumnData {
    String(String), UInt8(u8), UInt64(u64), Int(i32), Null,
}

#[derive(Debug)]
struct Column {
    name: String,
    type_: ColumnType,
    type_params: Option<String>,
    data: Vec<ColumnData>,
}

#[repr(C)]
struct ClickHouseBindData {
    filepath: *mut c_char,
}

#[repr(C)]
struct ClickHouseInitData {
    columns: Vec<Column>,
    current_row: usize,
    total_rows: usize,
    done: bool,
}

impl Free for ClickHouseBindData {
    fn free(&mut self) {
        unsafe { if !self.filepath.is_null() { drop(CString::from_raw(self.filepath)); } }
    }
}

impl Free for ClickHouseInitData {
    fn free(&mut self) {
        self.columns.clear();
    }
}

// All original functions remain exactly the same
fn read_string(reader: &mut impl Read) -> io::Result<String> {
    let len = reader.read_u8()?;
    let mut buffer = vec![0; len as usize];
    reader.read_exact(&mut buffer)?;
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

fn parse_column_type(type_str: &str) -> (ColumnType, Option<String>) {
    let params_start = type_str.find('(');
    let base_type = match params_start {
        Some(idx) => &type_str[..idx],
        None => type_str,
    };

    let params = params_start.map(|idx| {
        if type_str.ends_with(')') {
            type_str[idx..].to_string()
        } else {
            String::new()
        }
    });

    let column_type = match base_type {
        "String" => ColumnType::String,
        "UInt8" => ColumnType::UInt8,
        "UInt64" => ColumnType::UInt64,
        "Int" => ColumnType::Int,
        "Enum8" => ColumnType::Enum8,
        other => ColumnType::Unsupported(other.to_string()),
    };

    (column_type, params)
}

fn read_column_data(reader: &mut impl Read, column_type: &ColumnType, rows: u64) -> io::Result<Vec<ColumnData>> {
    let mut data = Vec::with_capacity(rows as usize);
    for _ in 0..rows {
        let value = match column_type {
            ColumnType::String => ColumnData::String(read_string(reader)?),
            ColumnType::UInt8 | ColumnType::Enum8 => ColumnData::UInt8(reader.read_u8()?),
            ColumnType::UInt64 => ColumnData::UInt64(reader.read_u64::<LittleEndian>()?),
            ColumnType::Int => ColumnData::Int(reader.read_i32::<LittleEndian>()?),
            ColumnType::Unsupported(_) => ColumnData::Null,
        };
        data.push(value);
    }
    Ok(data)
}

fn read_var_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut x = 0u64;
    for i in 0..10 {
        let byte = reader.read_u8()?;
        x |= ((byte & 0x7F) as u64) << (7 * i);
        if byte & 0x80 == 0 {
            return Ok(x);
        }
    }
    Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid VarUInt"))
}

fn read_native_format(reader: &mut BufReader<File>) -> io::Result<Vec<Column>> {
    let num_columns = read_var_u64(reader)?;
    let mut columns: Vec<Column> = Vec::new();
    let mut is_first_block = true;

    loop {
        let num_rows = read_var_u64(reader)?;

        if is_first_block {
            for _ in 0..num_columns {
                let name = read_string(reader)?;
                let type_str = read_string(reader)?;
                let (column_type, type_params) = parse_column_type(&type_str);
                let data = read_column_data(reader, &column_type, num_rows)?;
                columns.push(Column { name, type_: column_type, type_params, data });
            }
            is_first_block = false;
        } else {
            for col in &mut columns {
                let data = read_column_data(reader, &col.type_, num_rows)?;
                col.data.extend(data);
            }
        }

        if num_rows < 65409 {
            break;
        }
    }

    Ok(columns)
}

struct ClickHouseVTab;

impl VTab for ClickHouseVTab {
    type InitData = ClickHouseInitData;
    type BindData = ClickHouseBindData;

    unsafe fn bind(bind: &BindInfo, data: *mut ClickHouseBindData) -> Result<(), Box<dyn Error>> {
        let filepath = bind.get_parameter(0).to_string();
        
        let file = File::open(&filepath)?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);
        let columns = read_native_format(&mut reader)?;
        
        for column in &columns {
            let logical_type = match &column.type_ {
                ColumnType::String => LogicalTypeId::Varchar,
                ColumnType::UInt8 => LogicalTypeId::Integer,
                ColumnType::UInt64 => LogicalTypeId::Integer,
                ColumnType::Int => LogicalTypeId::Integer,
                ColumnType::Enum8 => LogicalTypeId::Integer,
                ColumnType::Unsupported(_) => LogicalTypeId::Varchar,
            };
            bind.add_result_column(&column.name, LogicalTypeHandle::from(logical_type));
        }
        
        // Convert and store filepath
        let c_filepath = CString::new(filepath)?;
        unsafe {
            (*data).filepath = c_filepath.into_raw();
        }
        
        Ok(())
    }

    unsafe fn init(info: &InitInfo, data: *mut ClickHouseInitData) -> Result<(), Box<dyn Error>> {
        let bind_data = info.get_bind_data::<ClickHouseBindData>();
        let filepath = unsafe { CStr::from_ptr((*bind_data).filepath).to_str()? };
    
        let file = File::open(filepath)?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);
    
        // Move columns directly into the data structure without intermediate assignment
        let read_result = read_native_format(&mut reader)?;
        let total_rows = if read_result.is_empty() { 0 } else { read_result[0].data.len() };
    
        unsafe {
            std::ptr::write(&mut (*data).columns, read_result);
            (*data).current_row = 0;
            (*data).total_rows = total_rows;
            (*data).done = false;
        }
    
        Ok(())
    }


    unsafe fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data::<ClickHouseInitData>();
        
        unsafe {
            if (*init_data).done || (*init_data).current_row >= (*init_data).total_rows {
                output.set_len(0);
                (*init_data).done = true;
                return Ok(());
            }

            let batch_size = 1024.min((*init_data).total_rows - (*init_data).current_row);
            
            for col_idx in 0..(*init_data).columns.len() {
                let column = &(*init_data).columns[col_idx];
                let mut vector = output.flat_vector(col_idx);

                match &column.type_ {
                    ColumnType::String => {
                        for row in 0..batch_size {
                            let data_idx = (*init_data).current_row + row;
                            if let ColumnData::String(s) = &column.data[data_idx] {
                                vector.insert(row, s.as_str());
                            }
                        }
                    },
                    ColumnType::UInt8 | ColumnType::Enum8 => {
                        let slice = vector.as_mut_slice::<i32>();
                        for row in 0..batch_size {
                            let data_idx = (*init_data).current_row + row;
                            if let ColumnData::UInt8(v) = column.data[data_idx] {
                                slice[row] = v as i32;
                            }
                        }
                    },
                    ColumnType::UInt64 => {
                        let slice = vector.as_mut_slice::<i32>();
                        for row in 0..batch_size {
                            let data_idx = (*init_data).current_row + row;
                            if let ColumnData::UInt64(v) = column.data[data_idx] {
                                slice[row] = v as i32;
                            }
                        }
                    },
                    ColumnType::Int => {
                        let slice = vector.as_mut_slice::<i32>();
                        for row in 0..batch_size {
                            let data_idx = (*init_data).current_row + row;
                            if let ColumnData::Int(v) = column.data[data_idx] {
                                slice[row] = v;
                            }
                        }
                    },
                    ColumnType::Unsupported(_) => {
                        for row in 0..batch_size {
                            vector.insert(row, "NULL");
                        }
                    },
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

#[duckdb_entrypoint_c_api(ext_name = "clickhouse_native", min_duckdb_version = "v0.0.1")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ClickHouseVTab>("clickhouse_native")?;
    Ok(())
}
