require "spec_helper"

RSpec.describe PgHaMigrations::Index do
  describe ".from_table_and_parent_index" do
    let(:parent_table) { PgHaMigrations::Table.new("events", "public") }
    let(:child_table) { PgHaMigrations::Table.new("events_child", "public") }

    it "generates stable, distinct names for sibling partitions" do
      parent_index = described_class.new("custom_idx", parent_table)
      first = described_class.from_table_and_parent_index(child_table, parent_index)
      sibling = PgHaMigrations::Table.new("events_other", "public")

      expect(first.name).to match(/\Acustom_idx_[0-9a-f]{10}\z/)
      expect(described_class.from_table_and_parent_index(child_table, parent_index).name).to eq(first.name)
      expect(described_class.from_table_and_parent_index(sibling, parent_index).name).not_to eq(first.name)
    end

    it "distinguishes equally named parent indexes in different schemas" do
      first = described_class.new("custom_idx", parent_table)
      second = described_class.new("custom_idx", PgHaMigrations::Table.new("events", "other"))

      expect(described_class.from_table_and_parent_index(child_table, first).name).not_to eq(
        described_class.from_table_and_parent_index(child_table, second).name
      )
    end

    it "distinguishes long custom names even when their visible prefixes match" do
      names = %w[a b].map do |ending|
        parent_index = described_class.new("x" * 62 + ending, parent_table)
        described_class.from_table_and_parent_index(child_table, parent_index).name
      end

      expect(names.uniq.size).to eq(2)
      expect(names.map(&:bytesize)).to eq([63, 63])
    end

    it "distinguishes equally named child tables in different schemas" do
      parent_index = described_class.new("custom_idx", parent_table)
      other_child = PgHaMigrations::Table.new(child_table.name, "other")

      expect(described_class.from_table_and_parent_index(child_table, parent_index).name).not_to eq(
        described_class.from_table_and_parent_index(other_child, parent_index).name
      )
    end

    it "truncates multibyte names without splitting characters" do
      parent_index = described_class.new("界" * 21, parent_table)
      child_index = described_class.from_table_and_parent_index(child_table, parent_index)

      expect(child_index.name).to be_valid_encoding
      expect(child_index.name.bytesize).to be <= 63
      expect(child_index.name).to match(/\A界+_[0-9a-f]{10}\z/)
    end
  end
end
