require 'mkmf'

dir_config('duckdb')
have_library('duckdb')
have_func('duckdb_bind_null', 'duckdb.h')
create_makefile('duckdb/duckdb_native')
