require "spec_helper"

RSpec.describe "migrations" do
  PgHaMigrations::AllowedVersions::ALLOWED_VERSIONS.each do |migration_klass|
    describe migration_klass do
      let(:schema_migration) do
        ActiveRecord::Base.connection.schema_migration if ActiveRecord::Base.connection.respond_to?(:schema_migration)
      end

      it "can be used as a migration class" do
        expect do
          Class.new(migration_klass)
        end.not_to raise_error
      end

      it "is configured to run migrations non-transactionally by default" do
        migration_dir = Dir.mktmpdir

        File.write("#{migration_dir}/0_create_foos.rb", <<~RUBY)
          class CreateFoos < #{migration_klass}
            def up
              safe_create_table :foos

              raise "boom"
            end
          end
        RUBY

        aggregate_failures do
          expect do
            migration_klass.suppress_messages do
              # This exercises the logic in ActiveRecord::Migrator
              # to optionally wrap migrations in a transaction.
              #
              # Our other tests simply run test_migration.migrate(:up)
              # which completely bypasses this logic.
              ActiveRecord::MigrationContext.new(
                migration_dir,
                schema_migration,
              ).migrate
            end
          end.to raise_error(StandardError, /An error has occurred, all later migrations canceled/)

          expect(ActiveRecord::Base.connection.table_exists?(:foos)).to eq(true)
        end
      ensure
        FileUtils.remove_entry(migration_dir)
        Object.send(:remove_const, :CreateFoos) if defined?(CreateFoos)
      end

      it "can be configured to run migrations transactionally" do
        migration_dir = Dir.mktmpdir

        File.write("#{migration_dir}/0_create_foos.rb", <<~RUBY)
          class CreateFoos < #{migration_klass}
            self.disable_ddl_transaction = false

            def up
              safe_create_table :foos

              raise "boom"
            end
          end
        RUBY

        aggregate_failures do
          expect do
            migration_klass.suppress_messages do
              # This exercises the logic in ActiveRecord::Migrator
              # to optionally wrap migrations in a transaction.
              #
              # Our other tests simply run test_migration.migrate(:up)
              # which completely bypasses this logic.
              ActiveRecord::MigrationContext.new(
                migration_dir,
                schema_migration,
              ).migrate
            end
          end.to raise_error(StandardError, /An error has occurred, this and all later migrations canceled/)

          expect(ActiveRecord::Base.connection.table_exists?(:foos)).to eq(false)
        end
      ensure
        FileUtils.remove_entry(migration_dir)
        Object.send(:remove_const, :CreateFoos) if defined?(CreateFoos)
      end

      it "raises when database adapter is not PostgreSQL" do
        allow(ActiveRecord::Base.connection).to receive(:adapter_name).and_return("SQLite")

        test_migration = Class.new(migration_klass) do
          def up
            unsafe_create_table :foos
          end
        end

        expect do
          test_migration.suppress_messages { test_migration.migrate(:up) }
        end.to raise_error(PgHaMigrations::UnsupportedAdapter, "This gem only works with the PostgreSQL adapter, found SQLite instead")
      end
    end
  end
end
