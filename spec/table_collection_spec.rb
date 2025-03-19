require "spec_helper"

RSpec.describe PgHaMigrations::TableCollection do
  let(:table_a) { PgHaMigrations::Table.new("a", "public") }
  let(:table_b) { PgHaMigrations::Table.new("b", "public") }
  let(:table_c) { PgHaMigrations::Table.new("c", "public", :exclusive) }
  let(:table_d) { PgHaMigrations::Table.new("d", "public", :exclusive) }
  let(:table_e) { PgHaMigrations::Table.new("e", "public", :access_exclusive) }

  describe ".from_table_names" do
    it "initializes a collection from table names" do
      %w[a b].each do |table|
        ActiveRecord::Base.connection.execute("CREATE TABLE #{table}(pk SERIAL)")
      end

      expect(described_class.from_table_names(["a", "b"])).to contain_exactly(table_a, table_b)
    end

    it "initializes a collection from table names and lock mode" do
      %w[c d].each do |table|
        ActiveRecord::Base.connection.execute("CREATE TABLE #{table}(pk SERIAL)")
      end

      expect(described_class.from_table_names(["c", "d"], :exclusive)).to contain_exactly(table_c, table_d)
    end
  end

  describe ".new" do
    it "initializes single item collection" do
      expect(described_class.new([table_a])).to contain_exactly(table_a)
    end

    it "initializes multi-element collection" do
      expect(described_class.new([table_a, table_b])).to contain_exactly(table_a, table_b)
    end

    it "initializes multi-element collection with lock modes" do
      expect(described_class.new([table_c, table_d])).to contain_exactly(table_c, table_d)
    end

    it "initializes multi-element collection without duplicates" do
      expect(described_class.new([table_a, table_a, table_b])).to contain_exactly(table_a, table_b)
    end

    it "raises error if collection is empty" do
      expect do
        described_class.new([])
      end.to raise_error(ArgumentError, "Expected a non-empty list of tables")
    end

    it "raises error if collection contains a mix of nil and non-nil lock modes" do
      expect do
        described_class.new([table_a, table_c])
      end.to raise_error(ArgumentError, "Expected all tables in collection to have the same lock mode")
    end

    it "raises error if collection contains a mix of different non-nil lock modes" do
      expect do
        described_class.new([table_d, table_e])
      end.to raise_error(ArgumentError, "Expected all tables in collection to have the same lock mode")
    end
  end

  describe "#subset?" do
    it "returns true when collections are the same" do
      source = described_class.new([table_a, table_b])
      target = described_class.new([table_a, table_b])

      expect(source.subset?(target)).to eq(true)
    end

    it "returns true when source contains some elements in the target" do
      source = described_class.new([table_a])
      target = described_class.new([table_a, table_b])

      expect(source.subset?(target)).to eq(true)
    end

    it "returns false when source contains an element that the target does not have" do
      source = described_class.new([table_a, table_b])
      target = described_class.new([table_a])

      expect(source.subset?(target)).to eq(false)
    end
  end

  describe "#to_sql" do
    it "returns the fully qualified name of the table in a single element collection" do
      expect(described_class.new([table_a]).to_sql).to eq("\"public\".\"a\"")
    end

    it "returns a comma separated list of fully qualified names in a multi-element collection" do
      expect(described_class.new([table_a, table_b]).to_sql).to eq("\"public\".\"a\", \"public\".\"b\"")
    end
  end

  describe "#with_partitions" do
    let(:table_c_part) { PgHaMigrations::Table.new("c_part", "public", :exclusive) }
    let(:table_d_part) { PgHaMigrations::Table.new("d_part", "public", :exclusive) }

    it "initializes an identical collection if no partitions present" do
      expect(described_class.new([table_c, table_d]).with_partitions).to contain_exactly(table_c, table_d)
    end

    it "initializes a new collection with parent tables and child partitions when present" do
      %w[c d].each do |table|
        ActiveRecord::Base.connection.execute(<<~SQL)
          CREATE TABLE #{table}(pk SERIAL) PARTITION BY RANGE (pk);
          CREATE TABLE #{table}_part PARTITION OF #{table} FOR VALUES FROM (1) TO (2);
        SQL
      end

      expect(described_class.new([table_c, table_d]).with_partitions).to contain_exactly(table_c, table_c_part, table_d, table_d_part)
    end
  end
end
