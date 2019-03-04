require "spec_helper"

RSpec.describe PgHaMigrations::SafeStatements do
  PgHaMigrations::AllowedVersions::ALLOWED_VERSIONS.each do |migration_klass|
    describe migration_klass do
      it "can be used as a migration class" do
        expect do
          Class.new(migration_klass)
        end.not_to raise_error
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

      describe PgHaMigrations::UnsafeStatements do
        it "outputs the operation" do
          original_stdout = $stdout
          begin
            $stdout = StringIO.new

            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
              end
            end

            migration.migrate(:up)

            expect($stdout.string).to match(/migrating.*create_table\(:foos[^\)]*\).*migrated \([0-9.]+s\)/m)
          ensure
            $stdout = original_stdout
          end
        end

        it "raises when using default create_table method" do
          migration = Class.new(migration_klass) do
            def up
              create_table :foos
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:create_table is NOT SAFE!/)
        end

        it "raises when using default add_column method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              add_column :foos, :bar, :text
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:add_column is NOT SAFE!/)
        end

        it "raises when using default change_table method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              change_table(:foos) { }
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:change_table is NOT SAFE!/)
        end

        it "raises when using default drop_table method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              drop_table :foos
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:drop_table is NOT SAFE!/)
        end

        it "raises when using default rename_table method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              rename_table :foos, :bars
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:rename_table is NOT SAFE!/)
        end

        it "raises when using default rename_column method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
              rename_column :foos, :bar, :baz
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:rename_column is NOT SAFE!/)
        end

        it "raises when using default change_column method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
              change_column :foos, :bar, :string
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:change_column is NOT SAFE!/)
        end

        it "raises when using default change_column_nullable method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
              change_column_null :foos, :bar, false
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:change_column_null is NOT .+ SAFE!/)
        end

        it "raises when using default remove_column method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
              remove_column :foos, :bar, :text
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:remove_column is NOT SAFE!/)
        end

        it "raises when using default add_index method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
              add_index :foos, :bar
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:add_index is NOT SAFE!/)
        end

        it "raises when using default add_foreign_key method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_create_table :bars
              safe_add_column :foos, :bar_fk, :integer
              add_foreign_key :foos, :bars, :foreign_key => :bar_fk
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:add_foreign_key is NOT SAFE!/)
        end

        it "raises when using default execute method" do
          migration = Class.new(migration_klass) do
            def up
              execute "SELECT current_date"
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UnsafeMigrationError, /:execute is NOT SAFE!/)
        end
      end

      describe PgHaMigrations::SafeStatements do
        describe "safe_create_table" do
          # This test is also particularly helpful for exposing issues
          # with inheritance from the compatibilty hierarchy, but we
          # have targeted tests for that also.
          it "uses the right pk datatype" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos3 do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            columns = ActiveRecord::Base.connection.columns("foos3")
            id_column = columns.find { |column| column.name == "id" }

            expect(id_column.sql_type).to eq(
              if [ActiveRecord::Migration[4.2], ActiveRecord::Migration[5.0]].include?(migration_klass)
                "integer"
              else
                "bigint"
              end
            )
          end

          it "creates the table with columns of the right type" do
            expect(ActiveRecord::Base.connection.tables).not_to include("foos3")

            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos3 do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            columns = ActiveRecord::Base.connection.columns("foos3")
            column_names = columns.map(&:name)

            updated_at_column = columns.find { |column| column.name == "updated_at" }
            text_column = columns.find { |column| column.name == "text_column" }

            expect(ActiveRecord::Base.connection.tables).to include("foos3")

            expect(updated_at_column.sql_type).to eq("timestamp without time zone")
            expect(text_column.sql_type).to eq("text")
          end
        end

        describe "safe_add_column" do
          it "forbids setting a default" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                safe_add_column :foos, :bar, :text, :default => ""
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error PgHaMigrations::UnsafeMigrationError
          end

          it "forbids setting null => false" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                safe_add_column :foos, :bar, :text, :null => false
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error PgHaMigrations::UnsafeMigrationError
          end

          it "add column of default is not set" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                safe_add_column :foos, :bar, :text
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.columns("foos").map(&:name)).to include("bar")
          end
        end

        describe "safe_change_column_default" do
          it "sets default value of a text column" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                safe_add_column :foos, :bar, :text
                safe_change_column_default :foos, :bar, "baz"
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.default).to eq("baz")

            ActiveRecord::Base.connection.execute("INSERT INTO foos SELECT FROM (VALUES (1)) t")
            expect(ActiveRecord::Base.connection.select_value("SELECT bar FROM foos")).to eq("baz")
          end

          it "sets default value of an integer column" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                safe_add_column :foos, :bar, :integer
                safe_change_column_default :foos, :bar, 5
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.default).to eq("5")

            ActiveRecord::Base.connection.execute("INSERT INTO foos SELECT FROM (VALUES (1)) t")
            expect(ActiveRecord::Base.connection.select_value("SELECT bar FROM foos")).to eq(5)
          end

          it "sets default value to the result of an expression when the expression is a string" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                safe_add_column :foos, :bar, :timestamp
                safe_change_column_default :foos, :bar, 'NOW()'
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.default).to match(/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/)

            foo_class = Class.new(ActiveRecord::Base) do
              self.table_name = "foos"
            end
            ActiveRecord::Base.connection.execute("INSERT INTO foos SELECT FROM (VALUES (1)) t")
            expect(foo_class.first.bar).to be_kind_of(Time)
          end

          it "sets default value to the result of an expression when the expression is a Proc" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                safe_add_column :foos, :bar, :timestamp
                safe_change_column_default :foos, :bar, -> { 'NOW()' }
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            default_value = ActiveRecord::Base.connection.select_value <<-SQL
              SELECT adsrc
              FROM pg_attrdef
              JOIN pg_attribute ON adnum = attnum AND adrelid = attrelid
              WHERE attname = 'bar' AND attrelid = 'foos'::regclass
            SQL
            expect(default_value).to match(/\ANOW\(\)\Z/i)

            foo_class = Class.new(ActiveRecord::Base) do
              self.table_name = "foos"
            end
            ActiveRecord::Base.connection.execute("INSERT INTO foos SELECT FROM (VALUES (1)) t")
            expect(foo_class.first.bar).to be_kind_of(Time)
          end
        end

        describe "safe_make_column_nullable" do
          it "removes the not null constraint from the column" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos do |t|
                  t.text :bar, :null => false
                end
                safe_make_column_nullable :foos, :bar
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.null).to eq(true)
          end
        end

        describe "safe_set_maintenance_work_mem_gb" do
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

        describe "enums" do
          def _select_enum_names_and_values
            statement = <<-SQL
          SELECT pg_type.typname AS name,
                 pg_enum.enumlabel AS value
           FROM pg_type
           JOIN pg_enum ON pg_enum.enumtypid = pg_type.oid;
            SQL

            ActiveRecord::Base.connection.execute(statement).to_a
          end

          describe "safe_create_enum_type" do
            it "creates a new enum type" do
              migration = Class.new(migration_klass) do
                def up
                  safe_create_enum_type :bt_foo_enum, ["one", "two", "three"]
                end
              end

              migration.suppress_messages { migration.migrate(:up) }

              result = _select_enum_names_and_values
              expect(result).to eq([
                {"name" => "bt_foo_enum", "value" => "one"},
                {"name" => "bt_foo_enum", "value" => "two"},
                {"name" => "bt_foo_enum", "value" => "three"},
              ])
            end

            it "can create a new enum type with symbols for values" do
              migration = Class.new(migration_klass) do
                def up
                  safe_create_enum_type :bt_foo_enum, [:one, :two, :three]
                end
              end

              migration.suppress_messages { migration.migrate(:up) }

              result = _select_enum_names_and_values
              expect(result).to eq([
                {"name" => "bt_foo_enum", "value" => "one"},
                {"name" => "bt_foo_enum", "value" => "two"},
                {"name" => "bt_foo_enum", "value" => "three"},
              ])
            end

            it "can create a new enum type with no values" do
              migration = Class.new(migration_klass) do
                def up
                  safe_create_enum_type :bt_foo_enum, []
                end
              end

              migration.suppress_messages { migration.migrate(:up) }

              result = _select_enum_names_and_values
              expect(result).to eq([])
            end

            it "raises helpfully if no values argument is passed" do
              migration = Class.new(migration_klass) do
                def up
                  safe_create_enum_type :bt_foo_enum
                end
              end

              expect do
                migration.suppress_messages { migration.migrate(:up) }
              end.to raise_error(ArgumentError, /empty array/)
            end
          end

          describe "safe_add_enum_value" do
            it "creates a new enum value" do
              migration = Class.new(migration_klass) do
                def up
                  unsafe_execute("CREATE TYPE bt_foo_enum AS ENUM ('one', 'two', 'three')")
                  safe_add_enum_value :bt_foo_enum, "four"
                end
              end

              migration.suppress_messages { migration.migrate(:up) }

              result = _select_enum_names_and_values
              expect(result).to eq([
                {"name" => "bt_foo_enum", "value" => "one"},
                {"name" => "bt_foo_enum", "value" => "two"},
                {"name" => "bt_foo_enum", "value" => "three"},
                {"name" => "bt_foo_enum", "value" => "four"},
              ])
            end
          end
        end

        describe "unsafe_make_column_not_nullable" do
          it "make the column not nullable which will cause the table to be locked" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                safe_add_column :foos, :bar, :text
                unsafe_make_column_not_nullable :foos, :bar, :estimated_rows => 0
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.null).to eq(false)
          end
        end

        describe "safe_add_concurrent_index" do
          it "creates an index using the concurrent algorithm" do
            setup_migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :text
              end
            end
            setup_migration.suppress_messages { setup_migration.migrate(:up) }

            test_migration = Class.new(migration_klass) do
              def up
                safe_add_concurrent_index :foos, [:bar]
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to make_database_queries(matching: /CREATE +INDEX CONCURRENTLY/, count: 1)

            indexes = ActiveRecord::Base.connection.indexes("foos")
            expect(indexes.size).to eq(1)
            expect(indexes.first).to have_attributes(:table => "foos", :name => "index_foos_on_bar", :columns => ["bar"])
          end
        end

        describe "safe_remove_concurrent_index" do
          it "removes an index using the concurrent algorithm" do
            setup_migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :text
                unsafe_add_index :foos, [:bar]
              end
            end
            setup_migration.suppress_messages { setup_migration.migrate(:up) }
            expect(ActiveRecord::Base.connection.indexes("foos").size).to eq(1)

            test_migration = Class.new(migration_klass) do
              def up
                safe_remove_concurrent_index :foos, :name => "index_foos_on_bar"
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to make_database_queries(matching: /DROP INDEX CONCURRENTLY "index_foos_on_bar"/, count: 1)
                   .and(make_database_queries(matching: /pg_relation_size/, count: 1))

            expect(ActiveRecord::Base.connection.indexes("foos")).to be_empty
          end

          it "raises if connecting to Postgres 9.1 databases" do
            allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(90112)

            test_migration = Class.new(migration_klass) do
              def up
                safe_remove_concurrent_index :foos, :name => "index_foos_on_bar"
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::InvalidMigrationError, "Removing an index concurrently is not supported on Postgres 9.1 databases")
          end

          it "outputs the index size" do
            original_stdout = $stdout
            begin
              $stdout = StringIO.new

              setup_migration = Class.new(migration_klass) do
                def up
                  unsafe_create_table :foos
                  unsafe_add_column :foos, :bar, :text
                  unsafe_add_index :foos, [:bar]
                end
              end
              setup_migration.suppress_messages { setup_migration.migrate(:up) }

              test_migration = Class.new(migration_klass) do
                def up
                  safe_remove_concurrent_index :foos, :name => "index_foos_on_bar"
                end
              end

              test_migration.migrate(:up)

              expect($stdout.string).to match(/index index_foos_on_bar which is \d+ bytes/)
            ensure
              $stdout = original_stdout
            end
          end

          it "raises a nice error if options isn't a hash" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_remove_concurrent_index :foos, [:column]
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected safe_remove_concurrent_index to be called with arguments (table_name, :name => ...)")
          end

          it "raises a nice error if options is a hash with a :name key" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_remove_concurrent_index :foos, :blah => :foo
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected safe_remove_concurrent_index to be called with arguments (table_name, :name => ...)")
          end
        end

        describe "#adjust_lock_timeout" do
          let(:table_name) { "bogus_table" }
          let(:migration) { Class.new(migration_klass).new }

          before(:each) do
            skip "Only relevant on Postgres 9.3+" unless ActiveRecord::Base.connection.postgresql_version >= 90300

            ActiveRecord::Base.connection.execute("CREATE TABLE #{table_name}(pk SERIAL, i INTEGER)")
          end

          around(:each) do |example|
            @original_timeout_raw_value = ActiveRecord::Base.value_from_sql("SHOW lock_timeout")
            @original_timeout_in_milliseconds = @original_timeout_raw_value.sub(/s\Z/, '').to_i * 1000
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

            expect(ActiveRecord::Base.value_from_sql("SHOW lock_timeout")).to eq(@original_timeout_raw_value)
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

            expect(ActiveRecord::Base.value_from_sql("SHOW lock_timeout")).to eq(@original_timeout_raw_value)
          end
        end

        describe "#adjust_statement_timeout" do
          let(:table_name) { "bogus_table" }
          let(:migration) { Class.new(migration_klass).new }

          before(:each) do
            ActiveRecord::Base.connection.execute("CREATE TABLE #{table_name}(pk SERIAL, i INTEGER)")
          end

          around(:each) do |example|
            @original_timeout_raw_value = ActiveRecord::Base.value_from_sql("SHOW statement_timeout")
            @original_timeout_in_milliseconds = @original_timeout_raw_value.sub(/s\Z/, '').to_i * 1000
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

            expect(ActiveRecord::Base.value_from_sql("SHOW statement_timeout")).to eq(@original_timeout_raw_value)
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

            expect(ActiveRecord::Base.value_from_sql("SHOW statement_timeout")).to eq(@original_timeout_raw_value)
          end
        end

        ["bogus_table", :bogus_table].each do |table_name|
          describe "#safely_acquire_lock_for_table with table_name of type #{table_name.class.name}" do
            let(:alternate_connection_pool) do
              ActiveRecord::ConnectionAdapters::ConnectionPool.new(ActiveRecord::Base.connection_pool.spec)
            end
            let(:alternate_connection) do
              alternate_connection_pool.connection
            end
            let(:migration) { Class.new(migration_klass).new }
            let(:table_lock_struct) { Struct.new(:table, :lock_type) }

            before(:each) do
              ActiveRecord::Base.connection.execute("CREATE TABLE #{table_name}(pk SERIAL, i INTEGER)")
            end

            after(:each) do
              alternate_connection_pool.disconnect!
            end

            def locks_for_table(table, connection:)
              values = connection.execute(<<-SQL)
                SELECT pg_class.relname AS table, pg_locks.mode AS lock_type
                FROM pg_locks
                JOIN pg_class ON pg_locks.relation = pg_class.oid
                WHERE pid IS DISTINCT FROM pg_backend_pid()
                  AND pg_class.relkind = 'r'
                  AND pg_class.relname = '#{table}'
              SQL
              values.to_a.map do |hash|
                table_lock_struct.new(hash["table"], hash["lock_type"])
              end
            end

            it "executes the block" do
              expect do |block|
                migration.safely_acquire_lock_for_table(table_name, &block)
              end.to yield_control
            end

            it "acquires an exclusive lock on the table" do
              migration.safely_acquire_lock_for_table(table_name) do
                expect(locks_for_table(table_name, connection: alternate_connection)).to eq([table_lock_struct.new(table_name.to_s, "AccessExclusiveLock")])
              end
            end

            it "releases the lock (even after an exception)" do
              begin
                migration.safely_acquire_lock_for_table(table_name) do
                  raise "bogus error"
                end
              rescue
                # Throw away error.
              end
              expect(locks_for_table(table_name, connection: alternate_connection)).to be_empty
            end

            it "waits to acquire a lock if the table is already blocked" do
              block_call_count = 0
              expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions).exactly(3).times do |*args|
                # Verify that the method under test hasn't taken out a lock.
                expect(locks_for_table(table_name, connection: alternate_connection)).to be_empty

                block_call_count += 1
                if block_call_count < 3
                  [PgHaMigrations::BlockingDatabaseTransactions::LongRunningTransaction.new("", "", 5, [table_name.to_s])]
                else
                  []
                end
              end

              migration.suppress_messages do
                migration.safely_acquire_lock_for_table(table_name) do
                  expect(locks_for_table(table_name, connection: alternate_connection)).not_to be_empty
                end
              end
            end

            it "fails lock acquisition quickly if Postgres doesn't grant an exclusive lock but then retries" do
              stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)

              expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions).exactly(2).times.and_return([])

              alternate_connection.execute("BEGIN; LOCK #{table_name};")

              lock_call_count = 0
              time_before_lock_calls = Time.now

              allow(ActiveRecord::Base.connection).to receive(:execute).at_least(:once).and_call_original
              expect(ActiveRecord::Base.connection).to receive(:execute).with("LOCK \"#{table_name}\";").exactly(2).times.and_wrap_original do |m, *args|
                lock_call_count += 1

                if lock_call_count == 2
                  # Get rid of the lock we were holding.
                  alternate_connection.execute("ROLLBACK;")
                end

                return_value = nil
                exception = nil
                begin
                  return_value = m.call(*args)
                rescue => e
                  exception = e
                end

                if lock_call_count == 1
                  # First lock attempt should fail fast.
                  expect(Time.now - time_before_lock_calls).to be >= 1.seconds
                  expect(Time.now - time_before_lock_calls).to be < 5.seconds
                  expect(locks_for_table(table_name, connection: alternate_connection)).to be_empty

                  expect(migration).to receive(:sleep).with(1 * PgHaMigrations::LOCK_FAILURE_RETRY_DELAY_MULTLIPLIER) # Stubbed seconds times multiplier
                else
                  # Second lock attempt should succeed.
                  expect(exception).not_to be_present
                  expect(locks_for_table(table_name, connection: alternate_connection)).not_to be_empty
                end

                if exception
                  raise exception
                else
                  return_value
                end
              end

              expect do
                migration.safely_acquire_lock_for_table(table_name) { }
              end.to output(/Timed out trying to acquire an exclusive lock.+#{table_name}/m).to_stdout
            end

            it "doesn't kill a long running query inside of the lock" do
              stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)

              migration.safely_acquire_lock_for_table(table_name) do
                time_before_select_call = Time.now
                expect do
                  ActiveRecord::Base.connection.execute("SELECT pg_sleep(3)")
                end.not_to raise_error
                time_after_select_call = Time.now

                expect(time_after_select_call - time_before_select_call).to be >= 3.seconds
              end
            end

            it "prints out helpful information when waiting for a lock" do
              blocking_queries_calls = 0
              expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions).exactly(2).times do |*args|
                blocking_queries_calls += 1
                if blocking_queries_calls == 1
                  [PgHaMigrations::BlockingDatabaseTransactions::LongRunningTransaction.new("", "some_sql_query", 5, [table_name.to_s])]
                else
                  []
                end
              end

              expect do
                migration = Class.new(migration_klass) do
                  def up
                    safely_acquire_lock_for_table("bogus_table") { }
                  end
                end

                migration.migrate(:up)
              end.to output(/blocking transactions.+tables.+bogus_table.+some_sql_query/m).to_stdout
            end

            it "allows re-entrancy" do
              migration.safely_acquire_lock_for_table(table_name) do
                migration.safely_acquire_lock_for_table(table_name) do
                  expect(locks_for_table(table_name, connection: alternate_connection)).not_to be_empty
                end
                expect(locks_for_table(table_name, connection: alternate_connection)).not_to be_empty
              end
              expect(locks_for_table(table_name, connection: alternate_connection)).to be_empty
            end

            it "uses statement_timeout instead of lock_timeout when on Postgres 9.1" do
              allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(90112)
              expect do
                migration.safely_acquire_lock_for_table(table_name) do
                  expect(locks_for_table(table_name, connection: alternate_connection)).not_to be_empty
                end
                expect(locks_for_table(table_name, connection: alternate_connection)).to be_empty
              end.not_to make_database_queries(matching: /lock_timeout/i)
            end
          end
        end

        describe "unsafe transformations" do
          it "renames create_table to unsafe_create_table" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.tables).to include("foos")
          end

          it "renames drop_table to unsafe_drop_table" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_drop_table :foos
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.tables).not_to include("foos")
          end

          it "renames add_column to unsafe_add_column" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :integer
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.tables).to include("foos")
            expect(ActiveRecord::Base.connection.columns("foos").map(&:name)).to include("bar")
          end

          it "renames change_table to unsafe_change_table" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_change_table :foos do |t|
                  t.string :bar
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.columns("foos").map(&:name)).to include("bar")
          end

          it "renames rename_table to unsafe_rename_table" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_rename_table :foos, :bars
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.tables).not_to include("foos")
            expect(ActiveRecord::Base.connection.tables).to include("bars")
          end

          it "renames rename_column to unsafe_rename_column" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table(:foos) { |t| t.string :bar }
                unsafe_rename_column :foos, :bar, :baz
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.tables).to include("foos")
            expect(ActiveRecord::Base.connection.columns("foos").map(&:name)).not_to include("bar")
            expect(ActiveRecord::Base.connection.columns("foos").map(&:name)).to include("baz")
          end

          it "renames change_column to unsafe_change_column" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table(:foos) { |t| t.string :bar }
                unsafe_change_column :foos, :bar, :text
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.columns("foos").detect { |c| c.name == "bar" }.type).to eq(:text)
          end

          it "renames remove_column to unsafe_remove_column" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table(:foos) { |t| t.string :bar }
                unsafe_remove_column :foos, :bar
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.columns("foos").map(&:name)).not_to include("bar")
          end

          it "renames add_index to unsafe_add_index" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table(:foos) { |t| t.string :bar }
                unsafe_add_index :foos, :bar
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.indexes("foos").map(&:columns)).to include(["bar"])
          end

          it "renames execute to unsafe_execute" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_execute "CREATE TABLE foos ( pk serial )"
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.tables).to include("foos")
          end

          it "renames remove_index to unsafe_remove_index" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table(:foos) { |t| t.string :bar }
                unsafe_add_index :foos, :bar
              end
            end
            migration.suppress_messages { migration.migrate(:up) }
            expect(ActiveRecord::Base.connection.indexes("foos").map(&:columns)).to include(["bar"])

            migration = Class.new(migration_klass) do
              def up
                unsafe_remove_index :foos, :bar
              end
            end
            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.indexes("foos").map(&:columns)).not_to include(["bar"])
          end
        end
      end
    end
  end
end
