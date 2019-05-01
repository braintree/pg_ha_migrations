require "spec_helper"

RSpec.describe PgHaMigrations::UnrunMigrations do
  let(:migrations_path) { File.absolute_path("spec/data/migrations") }

  describe "self.unrun_migrations", :test_isolation_strategy => :truncation do
    it "returns migrations that have not been run against the database" do
      migration = Class.new(ActiveRecord::Migration::Current) do
        def up
          safe_create_table :foos1 do |t|
            t.timestamps :null => false
            t.text :text_column
          end
          safe_create_table :foos2 do |t|
            t.timestamps :null => false
            t.text :text_column
          end
        end
      end

      migration.version = 24000
      migration.name = "240_00"

      migration.suppress_messages do
        ActiveRecord::Migrator.new(:up, [migration]).migrate
      end

      unrun_migrations = PgHaMigrations::UnrunMigrations.unrun_migrations(migrations_path)

      expect(unrun_migrations).to include({:version => "924201"})
      expect(unrun_migrations).to_not include({:version => "24000"})
    end

    it "informs that there are no unrun migrations when none exist" do
      migration = Class.new(ActiveRecord::Migration::Current) do
        def up
          safe_create_table :foos1 do |t|
            t.timestamps :null => false
            t.text :text_column
          end
          safe_create_table :foos2 do |t|
            t.timestamps :null => false
            t.text :text_column
          end
        end
      end

      migration.version = 924201

      migration.suppress_messages do
        ActiveRecord::Migrator.new(:up, [migration]).migrate
      end


      migrations = PgHaMigrations::UnrunMigrations.unrun_migrations(migrations_path)
      expect(migrations).to be_empty
    end
  end

  describe "self.report" do
    it "returns migrations that have not been run against the database" do
      migration = Class.new(ActiveRecord::Migration::Current) do
        def up
          safe_create_table :foos1 do |t|
            t.timestamps :null => false
            t.text :text_column
          end
          safe_create_table :foos2 do |t|
            t.timestamps :null => false
            t.text :text_column
          end
        end
      end

      migration.version = 24000
      migration.name = "240_00"

      migration.suppress_messages do
        ActiveRecord::Migrator.new(:up, [migration]).migrate
      end

      report = PgHaMigrations::UnrunMigrations.report(migrations_path)
      expect(report).to eq("Unrun migrations:\n924201")
    end
  end

  describe "self._migration_files" do
    it "knows about migration" do
      migrations_path = File.absolute_path("spec/data/migrations")
      migration_files = PgHaMigrations::UnrunMigrations._migration_files(migrations_path)
      expect(migration_files.size).to eq(1)
    end
  end
end
