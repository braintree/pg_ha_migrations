require "spec_helper"

RSpec.describe PgHaMigrations::MigrationModifier do
  describe "#modify!" do
    {
      create_table: <<-MIGRATION,
class Test < ActiveRecord::Migration[4.2]
  def up
    safe_create_table :foo, id: :bigserial do |t|
      t.text :bar, null: false
    end
  end
end
MIGRATION
      add_column: <<-MIGRATION,
class Test < ActiveRecord::Migration[4.2]
  def up
    safe_add_column :foo, :column_a, :string
  end
end
MIGRATION
      add_index: <<-MIGRATION,
class Test < ActiveRecord::Migration[4.2]
  def up
    safe_add_concurrent_index :foo, :column_a
  end
end
MIGRATION
    }.each do |fixture_name, expected_migration|
      it "changes #{fixture_name} to be safe" do
        Tempfile.create(fixture_name.to_s, "spec/tmp") do |f|
          f.write(File.read("spec/fixtures/#{fixture_name}.rb"))
          f.rewind

          PgHaMigrations::MigrationModifier.new(File.dirname(f.path)).modify!

          expect(f.read).to eq(expected_migration)
        end
      end
    end
  end
end
