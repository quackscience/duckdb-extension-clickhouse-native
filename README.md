<img src="https://github.com/user-attachments/assets/46a5c546-7e9b-42c7-87f4-bc8defe674e0" width=250 />

# DuckDB Clickhouse Native File reader
This experimental rust extension allows reading ClickHouse Native Format database files.

> Experimental: USE AT YOUR OWN RISK!

### Status
- [x] Basic Fomat Reading
- [x] Column Names & Types 
- [x] Blocks Parser & Iterator

<!--
### 📦 Installation
```sql
INSTALL clickhouse_native FROM community;
LOAD clickhouse_native;
```
-->

### Input
Generate some files with `clickhouse-local` or `clickhouse-server`

```sql
--- simple w/ one row, two columns
SELECT version(), number FROM numbers(1) INTO OUTFILE '/tmp/numbers.clickhouse' FORMAT Native;
--- simple w/ one column, five rows
SELECT number FROM numbers(5) INTO OUTFILE '/tmp/data.clickhouse' FORMAT Native;
--- complex w/ multiple types
SELECT * FROM system.functions LIMIT 10 INTO OUTFILE '/tmp/functions.clickhouse' FORMAT Native;
```

### Usage
Read ClickHouse Native files with DuckDB. _Unoptimized full-file reading._
```sql
D SELECT * FROM clickhouse_native('/tmp/numbers.clickhouse');
┌──────────────┬─────────┐
│  version()   │ number  │
│   varchar    │  int32  │
├──────────────┼─────────┤
│ 24.12.1.1273 │ 0       │
└──────────────┴─────────┘
```
