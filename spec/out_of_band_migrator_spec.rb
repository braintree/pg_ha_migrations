require "spec_helper"

RSpec.describe PgHaMigrations::OutOfBandMigrator do
  describe "run" do
    it "prints instructions and waits for prompt" do
      stdin = StringIO.new
      stdout = StringIO.new
      stdin.puts "exit"
      stdin.rewind
      migrator = PgHaMigrations::OutOfBandMigrator.new("_oob", stdin, stdout)
      migrator.run

      expect(stdout.string).to match(/migrations_state/)
      expect(stdout.string).to match(/blocking_database_transactions/)
      expect(stdout.string).to match(/migrate/)
      expect(stdout.string).to match(/exit/)
    end

    it "prints the migrations state" do
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

      allow(PgHaMigrations::UnrunMigrations).to receive(:_migration_files).with("_oob").and_return(
        [
          "db/migrate_oob/924201_release_9242_01.rb",
          "db/migrate_oob/924202_release_9242_01.rb",
          "db/migrate_oob/924203_release_9242_01.rb",
          "db/migrate_oob/924204_release_9242_01.rb",
          "db/migrate_oob/24000_release_240_00.rb",
        ]
      )

      stdin = StringIO.new
      stdout = StringIO.new
      stdin.puts "migrations_state"
      stdin.puts "exit"
      stdin.rewind
      migrator = PgHaMigrations::OutOfBandMigrator.new("_oob", stdin, stdout)
      migrator.run

      expect(stdout.string).to match(/Out Of Band Migrations To Be Run/)
      expect(stdout.string).to match(/Unrun migrations:/)
      expect(stdout.string).to match(/924201/)
      expect(stdout.string).to match(/924202/)
      expect(stdout.string).to match(/924203/)
      expect(stdout.string).to match(/924204/)
    end
  end

  describe "instructions" do
    it "has instructions" do
      migrator = PgHaMigrations::OutOfBandMigrator.new("_oob")
      instructions = migrator.instructions
      expect(instructions).to match(/migrations_state/)
      expect(instructions).to match(/blocking_database_transactions/)
      expect(instructions).to match(/migrate/)
      expect(instructions).to match(/exit/)
    end
  end

  describe "migrations state" do
    it "includes the migration report" do
    end
  end
end
