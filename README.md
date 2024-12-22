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
Read ClickHouse Native files with DuckDB. 
> _⚠️ Unoptimized full-scan file reading_

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
D SELECT * FROM clickhouse_native('/tmp/manyfunctions.clickhouse') WHERE alias_to != '' LIMIT 10;
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

<br>


### Build
The extension is not yet distributed via community repository. To test it you'll have to build locally

#### Requirements
- Rust
- Build Essentials

Clone and Compile the extension on your system

```bash
cd /usr/src
git clone --recurse-submodules https://github.com/quackscience/duckdb-extension-clickhouse-native
cd duckdb-extension-clickhouse-native
make configure && make
```

Download and Run DuckDB with -unsigned
```
wget https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip && unzip duckdb_cli-linux-amd64.zip
./duckdb -unsigned
```

Load your local extension build
```sql
D LOAD '/usr/src/duckdb-extension-clickhouse-native/build/debug/clickhouse_native.duckdb_extension';
```
