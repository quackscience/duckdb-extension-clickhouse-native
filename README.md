<img src="https://github.com/user-attachments/assets/46a5c546-7e9b-42c7-87f4-bc8defe674e0" width=250 />

# DuckDB Clickhouse Native File reader
This experimental rust extension allows reading ClickHouse Native Format database files.

> Experimental: USE AT YOUR OWN RISK!

### Status
- [x] Basic Fomat Reading
- [x] Column Extraction
- [x] Blocks Parser & Iterator
- [x] Type Mapping WIP
  - [x] Strings
  - [x] Integers
  - [x] Enums
  - [ ] ??? as String
- [ ] Compression support
- [ ] Filter / Range support

<br>

### 📦 Installation
```sql
INSTALL chsql_native FROM community;
LOAD chsql_native;
```


### Input
Generate some native files with `clickhouse-local` or `clickhouse-server`

```sql
--- simple w/ one row, two columns
SELECT version(), number FROM numbers(1) INTO OUTFILE '/tmp/numbers.clickhouse' FORMAT Native;
--- simple w/ one column, 100000 rows
SELECT number FROM numbers(100000) INTO OUTFILE '/tmp/100000.clickhouse' FORMAT Native;
--- complex w/ multiple types
SELECT * FROM system.functions LIMIT 10 INTO OUTFILE '/tmp/functions.clickhouse' FORMAT Native;
```

### Usage
Read ClickHouse Native files with DuckDB. Reads are full-scans at this time.

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
```sql
D SELECT * FROM clickhouse_native('/tmp/functions.clickhouse') WHERE alias_to != '' LIMIT 10;
┌────────────────────┬──────────────┬──────────────────┬──────────────────────┬──────────────┬─────────┬───┬─────────┬───────────┬────────────────┬──────────┬────────────┐
│        name        │ is_aggregate │ case_insensitive │       alias_to       │ create_query │ origin  │ … │ syntax  │ arguments │ returned_value │ examples │ categories │
│      varchar       │    int32     │      int32       │       varchar        │   varchar    │ varchar │   │ varchar │  varchar  │    varchar     │ varchar  │  varchar   │
├────────────────────┼──────────────┼──────────────────┼──────────────────────┼──────────────┼─────────┼───┼─────────┼───────────┼────────────────┼──────────┼────────────┤
│ connection_id      │            0 │                1 │ connectionID         │              │ System  │ … │         │           │                │          │            │
│ rand32             │            0 │                0 │ rand                 │              │ System  │ … │         │           │                │          │            │
│ INET6_ATON         │            0 │                1 │ IPv6StringToNum      │              │ System  │ … │         │           │                │          │            │
│ INET_ATON          │            0 │                1 │ IPv4StringToNum      │              │ System  │ … │         │           │                │          │            │
│ truncate           │            0 │                1 │ trunc                │              │ System  │ … │         │           │                │          │            │
│ ceiling            │            0 │                1 │ ceil                 │              │ System  │ … │         │           │                │          │            │
│ replace            │            0 │                1 │ replaceAll           │              │ System  │ … │         │           │                │          │            │
│ from_utc_timestamp │            0 │                1 │ fromUTCTimestamp     │              │ System  │ … │         │           │                │          │            │
│ mapFromString      │            0 │                0 │ extractKeyValuePairs │              │ System  │ … │         │           │                │          │            │
│ str_to_map         │            0 │                1 │ extractKeyValuePairs │              │ System  │ … │         │           │                │          │            │
├────────────────────┴──────────────┴──────────────────┴──────────────────────┴──────────────┴─────────┴───┴─────────┴───────────┴────────────────┴──────────┴────────────┤
│ 10 rows                                                                                                                                           12 columns (11 shown) │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Performance
Simple CLI _cold start_ count() test using `duckdb` vs. `clickhouse-local` and 1M rows
#### DuckDB
```sql
# time duckdb -c "LOAD chsql_native; SELECT count(*) FROM clickhouse_native('/tmp/1M.clickhouse');"
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│      1000000 │
└──────────────┘

real	0m0.095s
user	0m0.077s
sys	0m0.029s
```
#### clickhouse-local
```sql
# time clickhouse local "SELECT count(*) FROM '/tmp/1M.clickhouse'";
1000000

real	0m0.141s
user	0m0.086s
sys	0m0.043s
```

<br>


### Dev Build
You can easily modify the code and build a local extension for testing and development.

#### Requirements
- Rust

1) Clone and Compile the extension on your system

```bash
cd /usr/src
git clone --recurse-submodules https://github.com/quackscience/duckdb-extension-clickhouse-native
cd duckdb-extension-clickhouse-native
make configure && make
```

2) Download and Run DuckDB with -unsigned
```
wget https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip && unzip duckdb_cli-linux-amd64.zip
./duckdb -unsigned
```

3) Load your local extension build
```sql
D LOAD '/usr/src/duckdb-extension-clickhouse-native/build/debug/clickhouse_native.duckdb_extension';
```
