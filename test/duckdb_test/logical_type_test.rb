require 'test_helper'

module DuckDBTest
  class LogicalTypeTest < Minitest::Test
    CREATE_TABLE_SQL = <<~SQL
      CREATE TABLE table1(
        decimal_col DECIMAL
      )
    SQL

    INSERT_SQL = <<~SQL
      INSERT INTO table1 VALUES(
        123.456789,
      )
    SQL

    SELECT_SQL = 'SELECT * FROM table1'

    def setup
      @db = DuckDB::Database.open
      @con = @db.connect
      create_data(@con)
      result = @con.query(SELECT_SQL)
      @columns = result.columns
    end

    def test_decimal_logical_type
      decimal_col = @columns.detect { |c| c.type == :decimal }
      decimal_logical_type = decimal_col.logical_type

      assert_equal 18, decimal_logical_type.width
      assert_equal 3, decimal_logical_type.scale
    end

    private

    def create_data(con)
      con.query(CREATE_TABLE_SQL)
      con.query(INSERT_SQL)
    end
  end
end
