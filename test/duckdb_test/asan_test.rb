# frozen_string_literal: true

require 'english'
require 'duckdb'

begin
  DuckDB::Database.open('not_exist_dir/not_exist_file')
rescue StandardError
  puts "Error: #{$ERROR_INFO}"
end
