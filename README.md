<img src="https://github.com/user-attachments/assets/46a5c546-7e9b-42c7-87f4-bc8defe674e0" width=250 />

# DuckDB Clickhouse Native File reader
This experimental rust extension allows reading ClickHouse Native Format database files.

> Experimental: USE AT YOUR OWN RISK!

### Status
- [x] Basic Fomat Reading
- [x] Column Extraction
- [x] Blocks Parser & Iterator
- [ ] Type Mapping WIP
  - [x] Strings
  - [x] Integers
  - [x] Enums
  - [ ] ???
- [ ] Compression support

<!--
### 📦 Installation
```sql
INSTALL clickhouse_native FROM community;
LOAD clickhouse_native;
```
-->

### Input
Generate some native files with `clickhouse-local` or `clickhouse-server`

```sql
--- simple w/ one row, two columns
SELECT version(), number FROM numbers(1) INTO OUTFILE '/tmp/numbers.clickhouse' FORMAT Native;
--- simple w/ one column, 100 rows
SELECT number FROM numbers(100) INTO OUTFILE '/tmp/100.clickhouse' FORMAT Native;
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
```sql
D SELECT count(*), max(number) FROM clickhouse_native('/tmp/100000.clickhouse');
┌──────────────┬─────────────┐
│ count_star() │ max(number) │
│    int64     │    int32    │
├──────────────┼─────────────┤
│       100000 │       99999 │
└──────────────┴─────────────┘
```
