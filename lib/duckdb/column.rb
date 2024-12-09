# frozen_string_literal: true

module DuckDB
  class Column
    # returns column type symbol
    # `:unknown` means that the column type is unknown/unsupported by ruby-duckdb.
    # `:invalid` means that the column type is invalid in duckdb.
    #
    #   require 'duckdb'
    #   db = DuckDB::Database.open
    #   con = db.connect
    #   con.query('CREATE TABLE users (id INTEGER, name VARCHAR(30))')
    #
    #   users = con.query('SELECT * FROM users')
    #   columns = users.columns
    #   columns.first.type #=> :integer
    def type
      type_id = _type
      DuckDB::Converter::IntToSym.type_to_sym(type_id)
    end

    def logical_type
      logical_type_id = _logical_type
      if logical_type_id.is_a?(Integer)
        DuckDB::Converter::IntToSym.type_to_sym(logical_type_id)
      else
        logical_type_id
      end
    end
  end
end
