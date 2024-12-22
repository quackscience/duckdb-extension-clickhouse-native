use std::{error::Error, ffi::{c_char, CStr, CString}, fs::File, io::{self, Read, BufReader}, path::Path};
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId, Inserter},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
};
use byteorder::{ReadBytesExt, LittleEndian};

#[derive(Debug)]
struct ColumnDefinition {
    name: String,
    type_str: String,
}

#[derive(Debug)]
enum ColumnType {
    String, UInt8, UInt64, Int, Enum8, 
    DateTime, Date,
    Unsupported(String),
}

#[derive(Debug)]
enum ColumnData {
    String(String), UInt8(u8), UInt64(u64), Int(i32), 
    DateTime(u32), Date(u16),
    Null,
}

#[derive(Debug)]
struct Column {
    name: String,
    type_: ColumnType,
    type_params: Option<String>,
    data: Vec<ColumnData>,
}

#[repr(C)]
pub struct ClickHouseFolderBindData {
    dirpath: *mut c_char,
    _dirpath_holder: Option<CString>,
}

#[repr(C)]
pub struct ClickHouseFolderInitData {
    columns: Vec<Column>,
    current_row: usize,
    total_rows: usize,
    done: bool,
}

impl Free for ClickHouseFolderBindData {
    fn free(&mut self) {
        self._dirpath_holder.take();
        self.dirpath = std::ptr::null_mut();
    }
}

impl Free for ClickHouseFolderInitData {
    fn free(&mut self) {
        self.columns.clear();
    }
}

fn read_string(reader: &mut impl Read) -> io::Result<String> {
    let len = reader.read_u8()?;
    let mut buffer = vec![0; len as usize];
    reader.read_exact(&mut buffer)?;
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

fn parse_columns_file(path: &Path) -> io::Result<Vec<ColumnDefinition>> {
    let content = std::fs::read_to_string(path)?;
    let mut lines = content.lines();
    
    // Skip "columns format version: 1"
    lines.next();
    
    // Skip "N columns:" line
    lines.next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing columns count"))?;
    
    let mut columns = Vec::new();
    
    for line in lines {
        if line.is_empty() { continue; }
        
        // Remove backticks and split by space
        let line = line.trim_start_matches('`').trim_end_matches('`');
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        
        if parts.len() != 2 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid column format"));
        }
        
        columns.push(ColumnDefinition {
            name: parts[0].to_string(),
            type_str: parts[1].to_string(),
        });
    }
    
    Ok(columns)
}

fn read_count_file(path: &Path) -> io::Result<u64> {
    let content = std::fs::read_to_string(path)?;
    content.trim().parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
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
        "DateTime" => ColumnType::DateTime,
        "Date" => ColumnType::Date,
        other => ColumnType::Unsupported(other.to_string()),
    };

    (column_type, params)
}

fn read_column_data(reader: &mut impl Read, column_type: &ColumnType, rows: u64) -> io::Result<Vec<ColumnData>> {
    let mut data = Vec::with_capacity(rows as usize);
    println!("Reading {} rows for column type {:?}", rows, column_type);
    
    for row_idx in 0..rows {
        let value = match column_type {
            ColumnType::UInt64 => {
                let val = reader.read_u64::<LittleEndian>()?;
                if row_idx < 5 {  // Print first few values
                    println!("UInt64 value at row {}: {}", row_idx, val);
                }
                ColumnData::UInt64(val)
            },
            ColumnType::DateTime => {
                let val = reader.read_u32::<LittleEndian>()?;
                if row_idx < 5 {
                    println!("DateTime value at row {}: {}", row_idx, val);
                }
                ColumnData::DateTime(val)
            },
            ColumnType::Date => {
                let val = reader.read_u16::<LittleEndian>()?;
                if row_idx < 5 {
                    println!("Date value at row {}: {}", row_idx, val);
                }
                ColumnData::Date(val)
            },
            // ... other types ...
            _ => ColumnData::Null,
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

pub struct ClickHouseFolderVTab;

impl VTab for ClickHouseFolderVTab {
    type InitData = ClickHouseFolderInitData;
    type BindData = ClickHouseFolderBindData;

    unsafe fn bind(bind: &BindInfo, data: *mut ClickHouseFolderBindData) -> Result<(), Box<dyn Error>> {
        let dirpath = bind.get_parameter(0).to_string();
        let dir_path = Path::new(&dirpath);

        // Read columns.txt
        let columns_path = dir_path.join("columns.txt");
        let column_defs = parse_columns_file(&columns_path)?;
        
        // Add columns to DuckDB
        for col in &column_defs {
            let logical_type = match col.type_str.as_str() {
                "UInt64" => LogicalTypeId::Bigint,  // Fixed: BigInt -> Bigint
                "DateTime" => LogicalTypeId::Timestamp,
                "Date" => LogicalTypeId::Date,
                _ => LogicalTypeId::Varchar,
            };
            bind.add_result_column(&col.name, LogicalTypeHandle::from(logical_type));
        }
        
        // Store directory path
        let dirpath_cstring = CString::new(dirpath)?;
        let raw_ptr = dirpath_cstring.as_ptr() as *mut c_char;
        
        unsafe {
            (*data).dirpath = raw_ptr;
            (*data)._dirpath_holder = Some(dirpath_cstring);
        }
        
        Ok(())
    }

    unsafe fn init(info: &InitInfo, data: *mut ClickHouseFolderInitData) -> Result<(), Box<dyn Error>> {
    let bind_data = info.get_bind_data::<ClickHouseFolderBindData>();
    let dirpath = unsafe { CStr::from_ptr((*bind_data).dirpath).to_str()? };
    let dir_path = Path::new(dirpath);

    // Read count.txt first to know how many rows we have
    let count_path = dir_path.join("count.txt");
    let num_rows = read_count_file(&count_path)?;
    println!("Number of rows from count.txt: {}", num_rows);

    // Read column definitions
    let columns_path = dir_path.join("columns.txt");
    let column_defs = parse_columns_file(&columns_path)?;
    println!("Found {} columns in columns.txt", column_defs.len());

    // Read data.bin - contains just raw column data
    let data_path = dir_path.join("data.bin");
    let file = File::open(data_path)?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);

    // Initialize columns based on definitions
    let mut columns = Vec::new();

    for def in column_defs {
        let (column_type, type_params) = parse_column_type(&def.type_str);
        let data = read_column_data(&mut reader, &column_type, num_rows)?;
        columns.push(Column {
            name: def.name,
            type_: column_type,
            type_params,
            data,
        });
    }

    let total_rows = if columns.is_empty() { 0 } else { columns[0].data.len() };
    
    unsafe {
        std::ptr::write(&mut (*data).columns, columns);
        (*data).current_row = 0;
        (*data).total_rows = total_rows;
        (*data).done = false;
    }

    Ok(())
    }


    unsafe fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data::<ClickHouseFolderInitData>();
        
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
                        let slice = vector.as_mut_slice::<i64>();
                        for row in 0..batch_size {
                            let data_idx = (*init_data).current_row + row;
                            if let ColumnData::UInt64(v) = column.data[data_idx] {
                                slice[row] = (v as i64);
                            }
                        }
                    },
                    ColumnType::Int => {
                        let slice = vector.as_mut_slice::<i32>();
                        for row in 0..batch_size {
                            let data_idx = (*init_data).current_row + row;
                            if let ColumnData::Int(v) = column.data[data_idx] {
                                slice[row] = v as i32;
                            }
                        }
                    },
                    ColumnType::DateTime => {
                        let slice = vector.as_mut_slice::<i64>();
                        for row in 0..batch_size {
                            let data_idx = (*init_data).current_row + row;
                            if let ColumnData::DateTime(v) = column.data[data_idx] {
                                slice[row] = (v as i64) * 1_000_000;  // * 1000 Convert to milliseconds
                            }
                        }
                    },
                    ColumnType::Date => {
                        let slice = vector.as_mut_slice::<i32>();
                        for row in 0..batch_size {
                            let data_idx = (*init_data).current_row + row;
                            if let ColumnData::Date(v) = column.data[data_idx] {
                                slice[row] = v as i32;
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
