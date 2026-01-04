require "spec_helper"

RSpec.describe PgHaMigrations::SafeStatements, "utility methods" do
  let(:migration_klass) { ActiveRecord::Migration::Current }

  describe "#safe_set_maintenance_work_mem_gb" do
    it "sets the maintenance work memory for building indexes" do
      begin
        migration = Class.new(migration_klass) do
          def up
            safe_set_maintenance_work_mem_gb 1
          end
        end

        migration.suppress_messages { migration.migrate(:up) }

        expect(ActiveRecord::Base.connection.select_value("SHOW maintenance_work_mem")).to eq("1GB")
      ensure
        ActiveRecord::Base.connection.execute("RESET maintenance_work_mem")
      end
    end
  end

  describe "#adjust_lock_timeout" do
    let(:table_name) { "bogus_table" }
    let(:migration) { Class.new(migration_klass).new }

    before(:each) do
      ActiveRecord::Base.connection.execute("CREATE TABLE #{table_name}(pk SERIAL, i INTEGER)")
    end

    around(:each) do |example|
      @original_timeout_in_milliseconds = ActiveRecord::Base.value_from_sql(
        "SELECT setting::integer FROM pg_settings WHERE name = 'lock_timeout'"
      )
      begin
        example.run
      ensure
        ActiveRecord::Base.connection.execute("SET lock_timeout = #{@original_timeout_in_milliseconds};")
      end
    end

    it "runs the block" do
      expect do |block|
        migration.adjust_lock_timeout(5, &block)
      end.to yield_control
    end

    it "changes the lock_timeout to the requested value in seconds" do
      seconds = (@original_timeout_in_milliseconds / 1000) + 5
      migration.adjust_lock_timeout(seconds) do
        expect(ActiveRecord::Base.value_from_sql("SHOW lock_timeout")).to eq("#{seconds}s")
      end
    end

    it "resets the lock_timeout to the original values even after an exception" do
      seconds = (@original_timeout_in_milliseconds / 1000) + 5
      expect do
        migration.adjust_lock_timeout(seconds) do
          raise "bogus error"
        end
      end.to raise_error("bogus error")

      expect(ActiveRecord::Base.value_from_sql(
        "SELECT setting::integer FROM pg_settings WHERE name = 'lock_timeout'"
      )).to eq(@original_timeout_in_milliseconds)
    end

    it "resets the lock_timeout to the original values even after a SQL failure in a transaction" do
      seconds = (@original_timeout_in_milliseconds / 1000) + 5
      expect do
        migration.connection.transaction do
          migration.adjust_lock_timeout(seconds) do
            ActiveRecord::Base.connection.execute("select bogus;")
          end
        end
      end.to raise_error(ActiveRecord::StatementInvalid, /PG::UndefinedColumn/)

      expect(ActiveRecord::Base.value_from_sql(
        "SELECT setting::integer FROM pg_settings WHERE name = 'lock_timeout'"
      )).to eq(@original_timeout_in_milliseconds)
    end

    it "correctly restores timeout when nested and original was in minutes" do
      # Set timeout to 5 minutes (300000ms)
      ActiveRecord::Base.connection.execute("SET lock_timeout = 300000;")

      migration.adjust_lock_timeout(1) do
        expect(ActiveRecord::Base.value_from_sql(
          "SELECT setting::integer FROM pg_settings WHERE name = 'lock_timeout'"
        )).to eq(1000)
      end

      expect(ActiveRecord::Base.value_from_sql(
        "SELECT setting::integer FROM pg_settings WHERE name = 'lock_timeout'"
      )).to eq(300000)
    end
  end

  describe "#adjust_statement_timeout" do
    let(:table_name) { "bogus_table" }
    let(:migration) { Class.new(migration_klass).new }

    before(:each) do
      ActiveRecord::Base.connection.execute("CREATE TABLE #{table_name}(pk SERIAL, i INTEGER)")
    end

    around(:each) do |example|
      @original_timeout_in_milliseconds = ActiveRecord::Base.value_from_sql(
        "SELECT setting::integer FROM pg_settings WHERE name = 'statement_timeout'"
      )
      begin
        example.run
      ensure
        ActiveRecord::Base.connection.execute("SET statement_timeout = #{@original_timeout_in_milliseconds};")
      end
    end

    it "runs the block" do
      expect do |block|
        migration.adjust_statement_timeout(5, &block)
      end.to yield_control
    end

    it "changes the statement_timeout to the requested value in seconds" do
      seconds = (@original_timeout_in_milliseconds / 1000) + 5
      migration.adjust_statement_timeout(seconds) do
        expect(ActiveRecord::Base.value_from_sql("SHOW statement_timeout")).to eq("#{seconds}s")
      end
    end

    it "resets the statement_timeout to the original values even after an exception" do
      seconds = (@original_timeout_in_milliseconds / 1000) + 5
      expect do
        migration.adjust_statement_timeout(seconds) do
          raise "bogus error"
        end
      end.to raise_error("bogus error")

      expect(ActiveRecord::Base.value_from_sql(
        "SELECT setting::integer FROM pg_settings WHERE name = 'statement_timeout'"
      )).to eq(@original_timeout_in_milliseconds)
    end

    it "resets the statement_timeout to the original values even after a SQL failure in a transaction" do
      seconds = (@original_timeout_in_milliseconds / 1000) + 5
      expect do
        migration.connection.transaction do
          migration.adjust_statement_timeout(seconds) do
            migration.connection.execute("select bogus;")
          end
        end
      end.to raise_error(ActiveRecord::StatementInvalid, /PG::UndefinedColumn/)

      expect(ActiveRecord::Base.value_from_sql(
        "SELECT setting::integer FROM pg_settings WHERE name = 'statement_timeout'"
      )).to eq(@original_timeout_in_milliseconds)
    end

    it "correctly restores timeout when nested and original was in minutes" do
      # Set timeout to 5 minutes (300000ms)
      ActiveRecord::Base.connection.execute("SET statement_timeout = 300000;")

      migration.adjust_statement_timeout(1) do
        expect(ActiveRecord::Base.value_from_sql(
          "SELECT setting::integer FROM pg_settings WHERE name = 'statement_timeout'"
        )).to eq(1000)
      end

      expect(ActiveRecord::Base.value_from_sql(
        "SELECT setting::integer FROM pg_settings WHERE name = 'statement_timeout'"
      )).to eq(300000)
    end

    it "correctly restores timeout when nested and original was in hours" do
      # Set timeout to 1 hour (3600000ms)
      ActiveRecord::Base.connection.execute("SET statement_timeout = 3600000;")

      migration.adjust_statement_timeout(1) do
        expect(ActiveRecord::Base.value_from_sql(
          "SELECT setting::integer FROM pg_settings WHERE name = 'statement_timeout'"
        )).to eq(1000)
      end

      expect(ActiveRecord::Base.value_from_sql(
        "SELECT setting::integer FROM pg_settings WHERE name = 'statement_timeout'"
      )).to eq(3600000)
    end
  end

  describe "#ensure_small_table!" do
    it "does not raise error when empty: false and table is below threshold and has rows" do
      setup_migration = Class.new(migration_klass) do
        def up
          safe_create_table :foos

          unsafe_execute "INSERT INTO foos DEFAULT VALUES"
        end
      end

      setup_migration.suppress_messages { setup_migration.migrate(:up) }

      test_migration = Class.new(migration_klass) do
        def up
          ensure_small_table! :foos
        end
      end

      allow(ActiveRecord::Base.connection).to receive(:select_value).and_call_original
      expect(ActiveRecord::Base.connection).to_not receive(:select_value).with(/SELECT EXISTS/)
      expect(ActiveRecord::Base.connection).to receive(:select_value).with(/pg_total_relation_size/).once.and_call_original

      expect do
        test_migration.suppress_messages { test_migration.migrate(:up) }
      end.to_not raise_error
    end

    it "does not raise error when empty: true and table is below threshold and is empty" do
      setup_migration = Class.new(migration_klass) do
        def up
          safe_create_table :foos
        end
      end

      setup_migration.suppress_messages { setup_migration.migrate(:up) }

      test_migration = Class.new(migration_klass) do
        def up
          ensure_small_table! :foos, empty: true
        end
      end

      allow(ActiveRecord::Base.connection).to receive(:select_value).and_call_original
      expect(ActiveRecord::Base.connection).to receive(:select_value).with(/SELECT EXISTS/).once.and_call_original
      expect(ActiveRecord::Base.connection).to receive(:select_value).with(/pg_total_relation_size/).once.and_call_original

      expect do
        test_migration.suppress_messages { test_migration.migrate(:up) }
      end.to_not raise_error
    end

    it "raises error when empty: true and table has rows" do
      setup_migration = Class.new(migration_klass) do
        def up
          safe_create_table :foos

          unsafe_execute "INSERT INTO foos DEFAULT VALUES"
        end
      end

      setup_migration.suppress_messages { setup_migration.migrate(:up) }

      test_migration = Class.new(migration_klass) do
        def up
          ensure_small_table! :foos, empty: true
        end
      end

      allow(ActiveRecord::Base.connection).to receive(:select_value).and_call_original
      expect(ActiveRecord::Base.connection).to receive(:select_value).with(/SELECT EXISTS/).once.and_call_original
      expect(ActiveRecord::Base.connection).to_not receive(:select_value).with(/pg_total_relation_size/)

      expect do
        test_migration.suppress_messages { test_migration.migrate(:up) }
      end.to raise_error(PgHaMigrations::InvalidMigrationError, "Table \"foos\" has rows")
    end

    it "raises error when empty: true and table is above threshold and is empty" do
      setup_migration = Class.new(migration_klass) do
        def up
          safe_create_table :foos
        end
      end

      setup_migration.suppress_messages { setup_migration.migrate(:up) }

      test_migration = Class.new(migration_klass) do
        def up
          ensure_small_table! :foos, empty: true, threshold: 1.kilobyte
        end
      end

      allow(ActiveRecord::Base.connection).to receive(:select_value).and_call_original
      expect(ActiveRecord::Base.connection).to receive(:select_value).with(/SELECT EXISTS/).once.and_call_original
      expect(ActiveRecord::Base.connection).to receive(:select_value).with(/pg_total_relation_size/).once.and_call_original

      expect do
        test_migration.suppress_messages { test_migration.migrate(:up) }
      end.to raise_error(PgHaMigrations::InvalidMigrationError, "Table \"foos\" is larger than 1024 bytes")
    end

    it "raises error when empty: false and table is above threshold and has rows" do
      setup_migration = Class.new(migration_klass) do
        def up
          safe_create_table :foos

          unsafe_execute "INSERT INTO foos DEFAULT VALUES"
        end
      end

      setup_migration.suppress_messages { setup_migration.migrate(:up) }

      test_migration = Class.new(migration_klass) do
        def up
          ensure_small_table! :foos, threshold: 1.kilobyte
        end
      end

      allow(ActiveRecord::Base.connection).to receive(:select_value).and_call_original
      expect(ActiveRecord::Base.connection).to_not receive(:select_value).with(/SELECT EXISTS/)
      expect(ActiveRecord::Base.connection).to receive(:select_value).with(/pg_total_relation_size/).once.and_call_original

      expect do
        test_migration.suppress_messages { test_migration.migrate(:up) }
      end.to raise_error(PgHaMigrations::InvalidMigrationError, "Table \"foos\" is larger than 1024 bytes")
    end
  end
end
