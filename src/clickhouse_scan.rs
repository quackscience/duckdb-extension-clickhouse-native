use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use std::{
    error::Error,
    fs::File,
    io::{self, BufReader, Read},
    sync::{atomic::{AtomicBool, Ordering}, Mutex},
};

#[derive(Debug)]
struct ClickHouseScanBindData {
    filepath: String,
    #[allow(dead_code)]
    columns: Vec<String>,
}

#[derive(Debug)]
struct ClickHouseScanInitData {
    reader: Mutex<BufReader<File>>,
    done: AtomicBool,
}

struct ClickHouseScanVTab;

impl VTab for ClickHouseScanVTab {
    type InitData = ClickHouseScanInitData;
    type BindData = ClickHouseScanBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let filepath = bind.get_parameter(0).to_string();
        let columns_str = bind.get_parameter(1).to_string();
        let columns: Vec<String> = columns_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        // Add result columns
        for column_name in columns.iter() {
            bind.add_result_column(column_name, LogicalTypeHandle::from(LogicalTypeId::Varchar));
        }

        Ok(ClickHouseScanBindData { filepath, columns })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = info.get_bind_data::<ClickHouseScanBindData>();
        let filepath = unsafe { &(*bind_data).filepath };
        let file = File::open(filepath)?;
        let reader = BufReader::with_capacity(64 * 1024, file);

        Ok(ClickHouseScanInitData {
            reader: Mutex::new(reader),
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let _bind_data = func.get_bind_data();

        if init_data.done.load(Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        let mut buffer = String::new();
        let read_result = {
            let mut reader = init_data.reader.lock().map_err(|e| io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to lock reader: {}", e)
            ))?;
            reader.read_to_string(&mut buffer)
        };

        match read_result {
            Ok(0) => {
                output.set_len(0);
                init_data.done.store(true, Ordering::Relaxed);
            }
            Ok(_) => {
                // Process the data and fill the output chunk
                let vector = output.flat_vector(0);
                vector.insert(0, buffer.as_str());
                output.set_len(1);
            }
            Err(e) => return Err(Box::new(e)),
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // filepath
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // columns
        ])
    }
}

pub fn register_clickhouse_scan(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ClickHouseScanVTab>("clickhouse_scan")?;
    Ok(())
}
