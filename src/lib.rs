use std::{error::Error, ffi::{c_char, CStr, CString}, fs::File, io::{self, Read, BufReader, Seek}};
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId, Inserter},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use byteorder::{ReadBytesExt, LittleEndian};

#[allow(dead_code)]
#[derive(Debug)]
enum ColumnType {
    String,
    UInt8,
    UInt64,
    Int,
    Enum8(EnumType),  // Changed from unit variant to tuple variant
    Unsupported(String),
}

#[derive(Debug)]
enum ColumnData {
    String(String),
    UInt8(u8),
    UInt64(u64),
    Int(i32),
    Enum8(String),
    Null,
}

#[derive(Debug)]
struct EnumValue {
    name: String,
    value: i8,
}

#[derive(Debug)]
struct EnumType {
    values: Vec<EnumValue>,
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
    _filepath_holder: Option<CString>,
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
        if self.filepath.is_null() {
            return;
        }
        self._filepath_holder.take();
        self.filepath = std::ptr::null_mut();
    }
}

impl Free for ClickHouseInitData {
    fn free(&mut self) {
        if self.columns.is_empty() {
            return;
        }
        self.columns.clear();
    }
}


fn read_string(reader: &mut impl Read) -> io::Result<String> {
    let len = reader.read_u8()?;
    let mut buffer = vec![0; len as usize];
    reader.read_exact(&mut buffer)?;
    Ok(String::from_utf8_lossy(&buffer)
        .replace('\0', "")
        .replace('\u{FFFD}', "")
        .to_string())
}

fn parse_enum_values(params: &str) -> Option<EnumType> {
    // Remove outer parentheses and trim whitespace
    let inner = params.trim_matches(|c| c == '(' || c == ')').trim();
    
    // If there's no content after trimming, return None
    if inner.is_empty() {
        return None;
    }
    
    let mut values = Vec::new();
    for pair in inner.split(',') {
        let parts: Vec<&str> = pair.split('=').collect();
        if parts.len() != 2 {
            continue;
        }
        
        // Parse the string value (removing quotes)
        let name = parts[0]
            .trim()
            .trim_matches('\'')
            .to_string();
            
        // Parse the numeric value
        if let Ok(value) = parts[1].trim().parse::<i8>() {
            values.push(EnumValue { name, value });
        }
    }
    
    if values.is_empty() {
        None
    } else {
        Some(EnumType { values })
    }
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
        "Enum8" => {
            if let Some(ref p) = params {
                if let Some(enum_type) = parse_enum_values(p) {
                    ColumnType::Enum8(enum_type)
                } else {
                    ColumnType::Unsupported("Invalid Enum8".to_string())
                }
            } else {
                ColumnType::Unsupported("Invalid Enum8".to_string())
            }
        },
        other => ColumnType::Unsupported(other.to_string()),
    };

    (column_type, params)
}

fn read_column_data(reader: &mut impl Read, column_type: &ColumnType, rows: u64) -> io::Result<Vec<ColumnData>> {
    let mut data = Vec::with_capacity(rows as usize);
    for _ in 0..rows {
        let value = match column_type {
            ColumnType::UInt64 => {
                let val = reader.read_u64::<LittleEndian>()?;
                ColumnData::UInt64(val)
            },
            ColumnType::String => ColumnData::String(read_string(reader)?),
            ColumnType::UInt8 => ColumnData::UInt8(reader.read_u8()?),
            ColumnType::Enum8(enum_type) => {
                let val = reader.read_u8()?;
                let enum_str = enum_type.values
                    .iter()
                    .find(|ev| ev.value == val as i8)
                    .map(|ev| ev.name.clone())
                    .unwrap_or_else(|| format!("Unknown({})", val));
                ColumnData::Enum8(enum_str)
            },
            ColumnType::Int => ColumnData::Int(reader.read_i32::<LittleEndian>()?),
            ColumnType::Unsupported(type_name) => {
                ColumnData::String(format!("<unsupported:{}>", type_name))
            }
        };
        data.push(value);
    }
    Ok(data)
}

fn read_var_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut x = 0u64;
    let mut shift = 0;

    for _ in 0..10 {
        let byte = reader.read_u8()?;
        x |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return Ok(x);
        }
    }
    
    Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid VarUInt"))
}

fn skip_block_header(reader: &mut BufReader<File>) -> io::Result<()> {
    let mut marker = [0u8; 4];
    reader.read_exact(&mut marker)?;
    
    for _ in 0..2 {
        let str_len = reader.read_u8()? as u64;
        reader.seek_relative(str_len as i64)?;
    }
    
    Ok(())
}

fn read_native_format(reader: &mut BufReader<File>) -> io::Result<Vec<Column>> {
    let num_columns = read_var_u64(reader)?;
    let mut columns = Vec::new();
    let num_rows = read_var_u64(reader)?;

    for _ in 0..num_columns {
        let name = read_string(reader)?;
        let type_str = read_string(reader)?;        
        let (column_type, type_params) = parse_column_type(&type_str);
        let data = read_column_data(reader, &column_type, num_rows)?;
        columns.push(Column { name, type_: column_type, type_params, data });
    }

    loop {
        let _pos = reader.stream_position()?;
        let block_columns = match read_var_u64(reader) {
            Ok(cols) => cols,
            Err(_) => break,
        };

        let block_rows = read_var_u64(reader)?;

        if block_rows == 0 {
            break;
        }

        for _ in 0..block_columns {
            let _ = read_string(reader)?;
            let _ = read_string(reader)?;
        }

        for col in &mut columns {
            let mut new_data = read_column_data(reader, &col.type_, block_rows)?;
            col.data.append(&mut new_data);
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
                ColumnType::Enum8(_) => LogicalTypeId::Varchar,  // Store enums as strings
                ColumnType::Unsupported(_) => LogicalTypeId::Varchar,
            };
            bind.add_result_column(&column.name, LogicalTypeHandle::from(logical_type));
        }
        
        let filepath_cstring = CString::new(filepath)?;
        let raw_ptr = filepath_cstring.as_ptr() as *mut c_char;
        
        unsafe {
            (*data).filepath = raw_ptr;
            (*data)._filepath_holder = Some(filepath_cstring);
        }
        
        Ok(())
    }

    unsafe fn init(info: &InitInfo, data: *mut ClickHouseInitData) -> Result<(), Box<dyn Error>> {
        let bind_data = info.get_bind_data::<ClickHouseBindData>();
        let filepath = unsafe { CStr::from_ptr((*bind_data).filepath).to_str()? };
    
        let file = File::open(filepath)?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);
    
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
                    ColumnType::String | ColumnType::Unsupported(_) => {
                        for row in 0..batch_size {
                            let data_idx = (*init_data).current_row + row;
                            match &column.data[data_idx] {
                                ColumnData::String(s) => {
                                    let cleaned = s.replace('\0', "")
                                                 .replace('\u{FFFD}', "");
                                    vector.insert(row, cleaned.as_str())
                                },
                                _ => vector.insert(row, "<invalid>"),
                            }
                        }
                    },
                    ColumnType::UInt8 => {
                        let slice = vector.as_mut_slice::<i32>();
                        for row in 0..batch_size {
                        let data_idx = (*init_data).current_row + row;
                            if let ColumnData::UInt8(v) = column.data[data_idx] {
                                slice[row] = v as i32;
                            }
                        }
                    },
                    ColumnType::Enum8(_) => {
                        for row in 0..batch_size {
                            let data_idx = (*init_data).current_row + row;
                            if let ColumnData::Enum8(ref s) = column.data[data_idx] {
                                vector.insert(row, s.as_str());
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
