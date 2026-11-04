require "spec_helper"

RSpec.describe PgHaMigrations::ColumnType do
  let(:connection) { ActiveRecord::Base.connection }

  def relfilenode(relation)
    connection.select_value("SELECT relfilenode FROM pg_class WHERE oid = '#{relation}'::regclass")
  end

  # Runs the ALTER against a real table and reports what Postgres actually did,
  # so every rule in ColumnType is checked against the server rather than
  # against our own reading of the Postgres source.
  def postgres_behavior(from_sql, to_sql)
    connection.execute("CREATE TABLE type_change_probe (value #{from_sql})")
    connection.execute("CREATE INDEX type_change_probe_value ON type_change_probe (value)")

    table_before = relfilenode("type_change_probe")
    index_before = relfilenode("type_change_probe_value")

    connection.execute("ALTER TABLE type_change_probe ALTER COLUMN value TYPE #{to_sql}")

    {
      table_rewritten: relfilenode("type_change_probe") != table_before,
      index_rebuilt: relfilenode("type_change_probe_value") != index_before,
    }
  ensure
    connection.execute("DROP TABLE IF EXISTS type_change_probe")
  end

  def column_type(sql_type)
    connection.execute("CREATE TABLE type_change_source (value #{sql_type})")
    described_class.from_column(PgHaMigrations::Table.from_table_name(:type_change_source), :value)
  ensure
    connection.execute("DROP TABLE IF EXISTS type_change_source")
  end

  SAFE_CHANGES = [
    ["varchar(10)", "varchar(20)"],
    ["varchar(10)", "varchar"],
    ["varchar(10)", "text"],
    ["varchar", "text"],
    ["text", "varchar"],
    ["numeric(10,2)", "numeric(12,2)"],
    ["numeric(10,2)", "numeric"],
    ["numeric(10,0)", "numeric(20,0)"],
    ["bit varying(10)", "bit varying(20)"],
    ["bit varying(10)", "bit varying"],
    ["timestamp(3)", "timestamp(6)"],
    ["timestamp(3)", "timestamp"],
    ["timestamp", "timestamp(6)"],
    ["timestamp(6)", "timestamp"],
    ["timestamptz(0)", "timestamptz(3)"],
    ["time(3)", "time(6)"],
    ["time(3)", "time"],
    ["timetz(3)", "timetz"],
    ["interval(3)", "interval(6)"],
    ["interval(3)", "interval"],
    ["interval", "interval(6)"],
    ["cidr", "inet"],
  ].freeze

  UNSAFE_CHANGES = [
    ["varchar(20)", "varchar(10)"],
    ["varchar", "varchar(10)"],
    ["text", "varchar(10)"],
    ["numeric(10,2)", "numeric(8,2)"],
    ["numeric(10,2)", "numeric(12,3)"],
    ["numeric", "numeric(10,2)"],
    ["bit varying(20)", "bit varying(10)"],
    ["timestamp", "timestamp(3)"],
    ["timestamp(6)", "timestamp(3)"],
    ["time", "time(3)"],
    ["interval", "interval(3)"],
    ["inet", "cidr"],
    ["char(10)", "char(20)"],
    ["char(10)", "text"],
    ["integer", "bigint"],
    ["smallint", "integer"],
    ["real", "double precision"],
    ["date", "timestamp"],
    ["integer", "numeric"],
    ["varchar(10)", "char(20)"],
    ["varchar(10)[]", "varchar(20)[]"],
  ].freeze

  SAFE_CHANGES.each do |from_sql, to_sql|
    it "allows #{from_sql} -> #{to_sql}, which Postgres applies without a rewrite or index rebuild" do
      from_type = column_type(from_sql)
      to_type = described_class.from_sql_type(to_sql)

      expect(from_type.rewrite_free_change_to?(to_type, utc_session_time_zone: true)).to eq(true)
      expect(postgres_behavior(from_sql, to_sql)).to eq(table_rewritten: false, index_rebuilt: false)
    end
  end

  UNSAFE_CHANGES.each do |from_sql, to_sql|
    it "rejects #{from_sql} -> #{to_sql}, which Postgres applies with a table rewrite" do
      from_type = column_type(from_sql)
      to_type = described_class.from_sql_type(to_sql)

      expect(from_type.rewrite_free_change_to?(to_type, utc_session_time_zone: true)).to eq(false)
      expect(postgres_behavior(from_sql, to_sql)).to include(table_rewritten: true)
    end
  end

  it "conservatively rejects interval changes involving a fields restriction" do
    from_type = column_type("interval day to second")
    to_type = described_class.from_sql_type("interval")

    expect(from_type.rewrite_free_change_to?(to_type, utc_session_time_zone: true)).to eq(false)
  end

  describe "timestamp <-> timestamptz" do
    [["timestamp", "timestamptz"], ["timestamp(3)", "timestamptz(6)"], ["timestamptz", "timestamp"]].each do |from_sql, to_sql|
      it "allows #{from_sql} -> #{to_sql} in a UTC session; Postgres skips the rewrite but rebuilds indexes" do
        from_type = column_type(from_sql)
        to_type = described_class.from_sql_type(to_sql)

        expect(from_type.time_zone_change?(to_type)).to eq(true)
        expect(from_type.rewrite_free_change_to?(to_type, utc_session_time_zone: true)).to eq(true)
        expect(postgres_behavior(from_sql, to_sql)).to eq(table_rewritten: false, index_rebuilt: true)
      end
    end

    it "rejects timestamp -> timestamptz outside of a UTC session, where Postgres rewrites the table" do
      from_type = column_type("timestamp")
      to_type = described_class.from_sql_type("timestamptz")

      expect(from_type.rewrite_free_change_to?(to_type, utc_session_time_zone: false)).to eq(false)

      original_time_zone = connection.select_value("SHOW TimeZone")
      connection.execute("SET TimeZone = 'America/New_York'")
      begin
        expect(described_class.utc_session_time_zone?).to eq(false)
        expect(postgres_behavior("timestamp", "timestamptz")).to include(table_rewritten: true)
      ensure
        connection.execute("SET TimeZone = #{connection.quote(original_time_zone)}")
      end
    end

    it "rejects decreasing precision while converting" do
      from_type = column_type("timestamp(6)")
      to_type = described_class.from_sql_type("timestamptz(3)")

      expect(from_type.rewrite_free_change_to?(to_type, utc_session_time_zone: true)).to eq(false)
    end
  end

  describe ".utc_session_time_zone?" do
    it "recognizes UTC aliases" do
      original_time_zone = connection.select_value("SHOW TimeZone")
      begin
        %w[UTC Etc/UTC GMT].each do |zone|
          connection.execute("SET TimeZone = '#{zone}'")
          expect(described_class.utc_session_time_zone?).to eq(true), "expected #{zone} to be treated as UTC"
        end
      ensure
        connection.execute("SET TimeZone = #{connection.quote(original_time_zone)}")
      end
    end
  end

  describe ".from_sql_type" do
    it "resolves the type modifier the same way Postgres does for a column" do
      %w[varchar(42) numeric(9,3) timestamp(2) interval(4) text].each do |sql_type|
        expect(described_class.from_sql_type(sql_type)).to eq(column_type(sql_type))
      end
    end
  end
end
