require "spec_helper"

RSpec.describe PgHaMigrations::SafeStatements do
  TableLock = Struct.new(:table, :lock_type, :granted)
  def locks_for_table(table, connection:)
    values = connection.execute(<<-SQL)
      SELECT pg_class.relname AS table, pg_locks.mode AS lock_type, granted
      FROM pg_locks
      JOIN pg_class ON pg_locks.relation = pg_class.oid
      WHERE pid IS DISTINCT FROM pg_backend_pid()
        AND pg_class.relkind = 'r'
        AND pg_class.relname = '#{table}'
    SQL
    values.to_a.map do |hash|
      TableLock.new(hash["table"], hash["lock_type"], hash["granted"])
    end
  end

  def pool_config
    if ActiveRecord.gem_version >= Gem::Version.new("7.0")
      ActiveRecord::ConnectionAdapters::PoolConfig.new(
        ActiveRecord::Base,
        ActiveRecord::Base.connection_pool.db_config,
        ActiveRecord::Base.current_role,
        ActiveRecord::Base.current_shard
      )
    elsif ActiveRecord.gem_version >= Gem::Version.new("6.1")
      ActiveRecord::ConnectionAdapters::PoolConfig.new(ActiveRecord::Base, ActiveRecord::Base.connection_pool.db_config)
    else
      ActiveRecord::Base.connection_pool.spec
    end
  end

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

        context "when configured to disable default migration methods" do
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

        context "when not configured to disable default migration methods" do
          before(:each) do
            allow(PgHaMigrations.config)
              .to receive(:disable_default_migration_methods)
              .and_return(false)
          end

          it "does not raise when using default create_table method" do
            migration = Class.new(migration_klass) do
              def up
                create_table :foos
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end

          it "does not raise when using default add_column method" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos
                add_column :foos, :bar, :text
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end

          it "does not raise when using default change_table method" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos
                change_table(:foos) { }
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end

          it "does not raise when using default drop_table method" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos
                drop_table :foos
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end

          it "does not raise when using default rename_table method" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos
                rename_table :foos, :bars
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end

          it "does not raise when using default rename_column method" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos
                safe_add_column :foos, :bar, :text
                rename_column :foos, :bar, :baz
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end

          it "does not raise when using default change_column method" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos
                safe_add_column :foos, :bar, :text
                change_column :foos, :bar, :string
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end

          it "does not raise when using default change_column_null method" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos
                safe_add_column :foos, :bar, :text
                change_column_null :foos, :bar, false
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end

          it "does not raise when using default remove_column method" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos
                safe_add_column :foos, :bar, :text
                remove_column :foos, :bar, :text
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end

          it "does not raise when using default add_index method" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos
                safe_add_column :foos, :bar, :text
                add_index :foos, :bar
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end

          it "does not raise when using default add_foreign_key method" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos
                safe_create_table :bars
                safe_add_column :foos, :bar_id, :integer
                add_foreign_key :foos, :bars, :foreign_key => :bar_id
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end

          it "does not raise when using default execute method" do
            migration = Class.new(migration_klass) do
              def up
                execute "SELECT current_date"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error
          end
        end

        describe "disabling `force: true`" do
          it "is allowed when config.allow_force_create_table = true" do
            allow(PgHaMigrations.config)
              .to receive(:allow_force_create_table)
              .and_return(true)

            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :items, :force => true do |t|
                  # Empty.
                end
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to_not raise_error

            expect(ActiveRecord::Base.connection.tables).to include("items")
          end

          it "raises when config.allow_force_create_table = false" do
            allow(PgHaMigrations.config)
              .to receive(:allow_force_create_table)
              .and_return(false)

            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :items do |t|
                  t.integer :original_column
                end
                unsafe_create_table :items, :force => true do |t|
                  t.integer :new_column
                end
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::UnsafeMigrationError, /force is not safe/i)

            columns = ActiveRecord::Base.connection.columns("items")
            column_names = columns.map(&:name)
            expect(column_names).to include("original_column")
            expect(column_names).not_to include("new_column")
          end
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

            expect(updated_at_column.sql_type).to match(/timestamp(\(6\))? without time zone/)
            expect(text_column.sql_type).to eq("text")
          end

          it "disallows `force: true` regardless of config.allow_force_create_table" do
            aggregate_failures do
              [true, false].each do |config_value|
                allow(PgHaMigrations.config)
                  .to receive(:allow_force_create_table)
                  .and_return(config_value)

                migration = Class.new(migration_klass) do
                  def up
                    unsafe_execute "DROP TABLE IF EXISTS items" # We're in a loop in the test.
                    safe_create_table :items do |t|
                      t.integer :original_column
                    end
                    safe_create_table :items, :force => true do |t|
                      t.integer :new_column
                    end
                  end
                end

                expect do
                  migration.suppress_messages { migration.migrate(:up) }
                end.to raise_error(PgHaMigrations::UnsafeMigrationError, /force is not safe/i)

                columns = ActiveRecord::Base.connection.columns("items")
                column_names = columns.map(&:name)
                expect(column_names).to include("original_column")
                expect(column_names).not_to include("new_column")
              end
            end
          end

          it "doesn't output say_with_time for adapter_name" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_create_table :items
              end
            end

            expect do
              test_migration.migrate(:up)
            end.to_not output(/adapter_name/m).to_stdout
          end
        end

        describe "unsafe_add_column" do
          it "calls safely_acquire_lock_for_table" do
            migration = Class.new(migration_klass).new

            expect(migration).to receive(:safely_acquire_lock_for_table).with(:foos)
            migration.unsafe_add_column(:foos, :bar, :text)
          end
        end

        describe "safe_add_column" do
          it "allows setting a default on Postgres 11+" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                ActiveRecord::Base.connection.execute("INSERT INTO foos DEFAULT VALUES")
                safe_add_column :foos, :bar, :text, :default => "baz"
              end
            end

            migration.suppress_messages { migration.migrate(:up) }
            expect(ActiveRecord::Base.connection.select_value("SELECT bar FROM foos")).to eq("baz")
          end

          it "raises error setting a default on Postgres < 11" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                ActiveRecord::Base.connection.execute("INSERT INTO foos DEFAULT VALUES")
                safe_add_column :foos, :bar, :text, :default => "baz"
              end
            end

            allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(10_00_00)

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error PgHaMigrations::UnsafeMigrationError
          end

          [:string, :text, :enum, :binary].each do |type|
            it "allows a default value that looks like an expression for the #{type.inspect} type on Postgres 11+" do
              migration = Class.new(migration_klass) do
                define_method(:up) do
                  unsafe_create_table :foos
                  # Add an existing value so we trigger backfilling values
                  # on the new column.
                  ActiveRecord::Base.connection.execute("INSERT INTO foos DEFAULT VALUES")
                  if type == :enum
                    safe_create_enum_type :bt_foo_enum, ["NOW()"]
                    safe_add_column :foos, :bar, :bt_foo_enum, :default => 'NOW()'
                  else
                    safe_add_column :foos, :bar, type, :default => 'NOW()'
                  end
                end
              end

              if ActiveRecord::Base.connection.postgresql_version >= 11_00_00
                migration.suppress_messages { migration.migrate(:up) }

                # Handle binary columns being transported, but not stored, as hex.
                expected_value = type == :binary ? "\\x4e4f572829" : "NOW()"
                expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.default).to eq(expected_value)

                ActiveRecord::Base.connection.execute("INSERT INTO foos DEFAULT VALUES")
                expect(ActiveRecord::Base.connection.select_values("SELECT bar FROM foos")).to all(eq(expected_value))
              else
                expect do
                  migration.suppress_messages { migration.migrate(:up) }
                end.to raise_error PgHaMigrations::UnsafeMigrationError
              end
            end
          end

          it "does not allow setting a default value with a proc" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                safe_add_column :foos, :bar, :text, :default => -> { 'NOW()' }
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error PgHaMigrations::UnsafeMigrationError
          end

          it "forbids setting null => false (when no default is provided)" do
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

          it "allows setting null => false (with a default) on Postgres 11+ and forbids it otherwise" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                ActiveRecord::Base.connection.execute("INSERT INTO foos DEFAULT VALUES")
                safe_add_column :foos, :bar, :text, :null => false, :default => "baz"
              end
            end

            if ActiveRecord::Base.connection.postgresql_version >= 11_00_00
              migration.suppress_messages { migration.migrate(:up) }
              aggregate_failures do
                expect(ActiveRecord::Base.connection.select_value("SELECT bar FROM foos")).to eq("baz")
                expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.null).to eq(false)
              end
            else
              expect do
                migration.suppress_messages { migration.migrate(:up) }
              end.to raise_error PgHaMigrations::UnsafeMigrationError
            end
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
                unsafe_add_column :foos, :bar, :text
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
                unsafe_add_column :foos, :bar, :integer
                safe_change_column_default :foos, :bar, 5
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.default).to eq("5")

            ActiveRecord::Base.connection.execute("INSERT INTO foos SELECT FROM (VALUES (1)) t")
            expect(ActiveRecord::Base.connection.select_value("SELECT bar FROM foos")).to eq(5)
          end

          it "calls safely_acquire_lock_for_table" do
            setup_migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :integer
              end
            end

            setup_migration.suppress_messages { setup_migration.migrate(:up) }

            migration = Class.new(migration_klass).new

            expect(migration).to receive(:safely_acquire_lock_for_table).with(:foos)
            migration.safe_change_column_default(:foos, :bar, 5)
          end

          [:string, :text, :enum, :binary].each do |type|
            it "allows a value that looks like an expression for the #{type.inspect} type" do
              migration = Class.new(migration_klass) do
                define_method(:up) do
                  unsafe_create_table :foos
                  if type == :enum
                    safe_create_enum_type :bt_foo_enum, ["NOW()"]
                    unsafe_add_column :foos, :bar, :bt_foo_enum
                  else
                    unsafe_add_column :foos, :bar, type
                  end
                  safe_change_column_default :foos, :bar, 'NOW()'
                end
              end

              migration.suppress_messages { migration.migrate(:up) }

              # Handle binary columns being transported, but not stored, as hex.
              expected_value = type == :binary ? "\\x4e4f572829" : "NOW()"
              expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.default).to eq(expected_value)

              ActiveRecord::Base.connection.execute("INSERT INTO foos SELECT FROM (VALUES (1)) t")
              expect(ActiveRecord::Base.connection.select_value("SELECT bar FROM foos")).to eq(expected_value)
            end
          end

          it "raises a helpful error if the default expression passed as a string results in setting the default to NULL" do
            setup_migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :timestamp
              end
            end
            setup_migration.suppress_messages { setup_migration.migrate(:up) }

            migration = Class.new(migration_klass) do
              def up
                safe_change_column_default :foos, :bar, 'NOW()'
              end
            end

            # Only run the expectations if the version of Rails we're running
            # against is vulnerable to this particular edge case confusion
            # (we want to guard based on behavior not version, since this
            # behavior seems to be incidental and could in theory appear in
            # an arbitrary set of versions).
            ActiveRecord::Base.connection.change_column_default(:foos, :bar, 'NOW()')
            default_value = ActiveRecord::Base.value_from_sql <<~SQL
              SELECT pg_get_expr(adbin, adrelid)
              FROM pg_attrdef
              WHERE adrelid = 'foos'::regclass AND adnum = (
                SELECT attnum
                FROM pg_attribute
                WHERE attrelid = 'foos'::regclass AND attname = 'bar'
              )
            SQL
            if default_value.nil? || (ActiveRecord::VERSION::MAJOR == 5 && ActiveRecord::VERSION::MINOR == 2)
              expect do
                migration.suppress_messages { migration.migrate(:up) }
              end.to raise_error(PgHaMigrations::InvalidMigrationError, /expression using a string literal is ambiguous/)
            end
          end

          it "raises a helpful error if the default expression passed as a string results in setting the default to the constant result of evaluating the expression" do
            setup_migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :timestamp
              end
            end
            setup_migration.suppress_messages { setup_migration.migrate(:up) }

            migration = Class.new(migration_klass) do
              def up
                safe_change_column_default :foos, :bar, 'NOW()'
              end
            end

            # Only run the expectations if the version of Rails we're running
            # against is vulnerable to this particular edge case confusion
            # (we want to guard based on behavior not version, since this
            # behavior seems to be incidental and could in theory appear in
            # an arbitrary set of versions).
            ActiveRecord::Base.connection.change_column_default(:foos, :bar, 'NOW()')
            default_value = ActiveRecord::Base.value_from_sql <<~SQL
              SELECT pg_get_expr(adbin, adrelid)
              FROM pg_attrdef
              WHERE adrelid = 'foos'::regclass AND adnum = (
                SELECT attnum
                FROM pg_attribute
                WHERE attrelid = 'foos'::regclass AND attname = 'bar'
              )
            SQL
            if default_value =~ /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ || (ActiveRecord::VERSION::MAJOR == 5 && ActiveRecord::VERSION::MINOR <= 1)
              expect do
                migration.suppress_messages { migration.migrate(:up) }
              end.to raise_error(PgHaMigrations::InvalidMigrationError, /expression using a string literal is ambiguous/)
            end
          end

          it "sets default value to the result of an expression when a Proc resolves to a quoted string" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :timestamp
                safe_change_column_default :foos, :bar, -> { "'NOW()'" }
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

          it "sets default value to an expression when a Proc resolves to an expression string" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :timestamp
                safe_change_column_default :foos, :bar, -> { 'NOW()' }
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            default_value = ActiveRecord::Base.connection.select_value <<-SQL
              SELECT pg_get_expr(adbin, adrelid)
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

          it "drops the default if changing to nil" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :timestamp, default: Time.now
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            # Verify setup.
            expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.default).to be_present

            migration = Class.new(migration_klass) do
              def up
                safe_change_column_default :foos, :bar, nil
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            default_value = ActiveRecord::Base.connection.select_value <<-SQL
              SELECT pg_get_expr(adbin, adrelid)
              FROM pg_attrdef
              JOIN pg_attribute ON adnum = attnum AND adrelid = attrelid
              WHERE attname = 'bar' AND attrelid = 'foos'::regclass
            SQL
            expect(default_value).to be_nil
          end

          it "doesn't output say_with_time for quote_default_expression" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :text
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            test_migration = Class.new(migration_klass) do
              def up
                safe_change_column_default :foos, :bar, "test"
              end
            end

            expect do
              test_migration.migrate(:up)
            end.to_not output(/quote_default_expression/m).to_stdout
          end

          it "allows setting a constant _default value when the column was added in a previous migration" do
            migration_1 = Class.new(migration_klass) do
              define_method(:up) do
                unsafe_create_table :foos
                safe_add_column :foos, :bar, :text
              end
            end
            migration_2 = Class.new(migration_klass) do
              define_method(:up) do
                safe_change_column_default :foos, :bar, "bogus"
              end
            end

            migration_1.suppress_messages { migration_1.migrate(:up) }

            expect do
              migration_2.suppress_messages { migration_2.migrate(:up) }
            end.not_to raise_error
          end

          describe "when not configured to disallow two-step new column and adding default" do
            it "allows setting a constant default value on Postgres 11+ when the column was added in the same migration" do
              migration = Class.new(migration_klass) do
                define_method(:up) do
                  unsafe_create_table :foos
                  safe_add_column :foos, :bar, :text
                  safe_change_column_default :foos, :bar, "bogus"
                end
              end

              expect do
                migration.suppress_messages { migration.migrate(:up) }
              end.not_to raise_error
            end
          end

          describe "when configured to disallow two-step new column and adding default" do
            before(:each) do
              allow(PgHaMigrations.config)
                .to receive(:prefer_single_step_column_addition_with_default)
                .and_return(true)
            end

            it "disallows setting a constant default value on Postgres 11+ when the column was added in the same migration" do
              migration = Class.new(migration_klass) do
                define_method(:up) do
                  unsafe_create_table :foos
                  safe_add_column :foos, :bar, :text
                  safe_change_column_default :foos, :bar, "bogus"
                end
              end

              if ActiveRecord::Base.connection.postgresql_version >= 11_00_00
                expect do
                  migration.suppress_messages { migration.migrate(:up) }
                end.to raise_error PgHaMigrations::BestPracticeError
              else
                expect do
                  migration.suppress_messages { migration.migrate(:up) }
                end.not_to raise_error
              end
            end
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

          it "calls safely_acquire_lock_for_table" do
            migration = Class.new(migration_klass).new

            expect(migration).to receive(:safely_acquire_lock_for_table).with(:foos)
            migration.safe_make_column_nullable(:foos, :bar)
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
              expect(result).to contain_exactly(
                {"name" => "bt_foo_enum", "value" => "one"},
                {"name" => "bt_foo_enum", "value" => "two"},
                {"name" => "bt_foo_enum", "value" => "three"},
              )
            end

            it "can create a new enum type with symbols for values" do
              migration = Class.new(migration_klass) do
                def up
                  safe_create_enum_type :bt_foo_enum, [:one, :two, :three]
                end
              end

              migration.suppress_messages { migration.migrate(:up) }

              result = _select_enum_names_and_values
              expect(result).to contain_exactly(
                {"name" => "bt_foo_enum", "value" => "one"},
                {"name" => "bt_foo_enum", "value" => "two"},
                {"name" => "bt_foo_enum", "value" => "three"},
              )
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
              expect(result).to contain_exactly(
                {"name" => "bt_foo_enum", "value" => "one"},
                {"name" => "bt_foo_enum", "value" => "two"},
                {"name" => "bt_foo_enum", "value" => "three"},
                {"name" => "bt_foo_enum", "value" => "four"},
              )
            end
          end

          describe "unsafe_rename_enum_value" do
            it "renames a enum value on 10+" do
              migration = Class.new(migration_klass) do
                def up
                  unsafe_execute("CREATE TYPE bt_foo_enum AS ENUM ('one', 'two', 'three')")
                  unsafe_rename_enum_value :bt_foo_enum, "three", "updated"
                end
              end

              migration.suppress_messages { migration.migrate(:up) }

              expect(_select_enum_names_and_values).to contain_exactly(
                {"name" => "bt_foo_enum", "value" => "one"},
                {"name" => "bt_foo_enum", "value" => "two"},
                {"name" => "bt_foo_enum", "value" => "updated"},
              )
            end

            it "raises a helpful error on 9.6" do
              allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(9_06_00)

              migration = Class.new(migration_klass) do
                def up
                  unsafe_execute("CREATE TYPE bt_foo_enum AS ENUM ('one', 'two', 'three')")
                  unsafe_rename_enum_value :bt_foo_enum, "three", "updated"
                end
              end

              expect do
                migration.suppress_messages { migration.migrate(:up) }
              end.to raise_error(PgHaMigrations::InvalidMigrationError, /not supported.+version/)
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

          it "calls safely_acquire_lock_for_table" do
            migration = Class.new(migration_klass).new

            expect(migration).to receive(:safely_acquire_lock_for_table).with(:foos)
            migration.unsafe_make_column_not_nullable(:foos, :bar, :estimated_rows => 0)
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

        describe  "unsafe_add_index" do
          it "raises a helper warning when ActiveRecord is going to swallow per-column options" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos do |t|
                  t.integer :bar, :limit => 4
                  t.integer :baz, :limit => 4
                end
                unsafe_add_index :foos, "bar, baz", :opclass => :int4_ops
              end
            end

            error_matcher_args = if (ActiveRecord::VERSION::MAJOR == 5 && ActiveRecord::VERSION::MINOR <= 1) || ActiveRecord::VERSION::MAJOR == 4
              [ArgumentError, /Unknown key: :opclass/]
            else
              [PgHaMigrations::InvalidMigrationError, /ActiveRecord drops the :opclass option/]
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(*error_matcher_args)
          end

          it "demonstrates ActiveRecord still throws away per-column options when passed string" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos do |t|
                  t.integer :bar, :limit => 4
                  t.integer :baz, :limit => 4
                end
                execute_ancestor_statement(:add_index, :foos, "bar, baz", **{:opclass => {:bar => :int4_ops}})
              end
            end
            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.not_to make_database_queries(matching: /int4_ops/)
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
            allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(9_01_12)

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

        describe "#safe_add_unvalidated_check_constraint" do
          before(:each) do
            setup_migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :text
              end
            end
            setup_migration.suppress_messages { setup_migration.migrate(:up) }
          end

          it "adds a CHECK constraint" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_add_unvalidated_check_constraint :foos, "bar IS NOT NULL", :name => :constraint_foo_bar_is_not_null
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to make_database_queries(matching: /ALTER TABLE .+ ADD CONSTRAINT/, count: 1)

            constraint_name, constraint_validated, constraint_expression = ActiveRecord::Base.tuple_from_sql <<~SQL
              SELECT conname, convalidated, pg_get_constraintdef(oid)
              FROM pg_constraint
              WHERE conrelid = 'foos'::regclass AND contype != 'p'
            SQL

            expect(constraint_name).to eq("constraint_foo_bar_is_not_null")
            expect(constraint_expression).to eq("CHECK ((bar IS NOT NULL)) NOT VALID")
          end

          it "raises a helpful error if a name is not passed" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_add_unvalidated_check_constraint :foos, "bar IS NOT NULL", :name => nil
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected <name> to be present")
          end

          it "does not validate the constraint" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_add_unvalidated_check_constraint :foos, "bar IS NOT NULL", :name => :constraint_foo_bar_is_not_null
              end
            end

            test_migration.suppress_messages { test_migration.migrate(:up) }

            constraint_validated = ActiveRecord::Base.value_from_sql <<~SQL
              SELECT convalidated
              FROM pg_constraint
              WHERE conname = 'constraint_foo_bar_is_not_null'
            SQL

            expect(constraint_validated).to eq(false)
          end

          it "outputs the operation" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_add_unvalidated_check_constraint :foos, "bar IS NOT NULL", :name => :constraint_foo_bar_is_not_null
              end
            end

            expect do
              test_migration.migrate(:up)
            end.to output(/add_check_constraint\(:foos, "bar IS NOT NULL", name: :constraint_foo_bar_is_not_null, validate: false\)/m).to_stdout
          end
        end

        describe "#unsafe_add_check_constraint" do
          before(:each) do
            setup_migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :text
              end
            end
            setup_migration.suppress_messages { setup_migration.migrate(:up) }
          end

          it "calls safely_acquire_lock_for_table" do
            migration = Class.new(migration_klass).new

            expect(migration).to receive(:safely_acquire_lock_for_table).with(:foos)
            migration.unsafe_add_check_constraint(:foos, "bar IS NOT NULL", :name => :constraint_foo_bar_is_not_null)
          end

          it "adds a CHECK constraint" do
            test_migration = Class.new(migration_klass) do
              def up
                unsafe_add_check_constraint :foos, "bar IS NOT NULL", :name => :constraint_foo_bar_is_not_null
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to make_database_queries(matching: /ALTER TABLE .+ ADD CONSTRAINT/, count: 1)

            constraint_name, constraint_validated, constraint_expression = ActiveRecord::Base.tuple_from_sql <<~SQL
              SELECT conname, convalidated, pg_get_constraintdef(oid)
              FROM pg_constraint
              WHERE conrelid = 'foos'::regclass AND contype != 'p'
            SQL

            expect(constraint_name).to eq("constraint_foo_bar_is_not_null")
            expect(constraint_expression).to eq("CHECK ((bar IS NOT NULL))")
          end

          it "raises a helpful error if a name is not passed" do
            test_migration = Class.new(migration_klass) do
              def up
                unsafe_add_check_constraint :foos, "bar IS NOT NULL", :name => nil
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected <name> to be present")
          end

          it "defaults to validating the constraint" do
            test_migration = Class.new(migration_klass) do
              def up
                unsafe_add_check_constraint :foos, "bar IS NOT NULL", :name => :constraint_foo_bar_is_not_null
              end
            end

            test_migration.suppress_messages { test_migration.migrate(:up) }

            constraint_validated = ActiveRecord::Base.value_from_sql <<~SQL
              SELECT convalidated
              FROM pg_constraint
              WHERE conname = 'constraint_foo_bar_is_not_null'
            SQL

            expect(constraint_validated).to eq(true)
          end

          it "optionally creates the constraint as NOT VALID" do
            test_migration = Class.new(migration_klass) do
              def up
                unsafe_add_check_constraint :foos, "bar IS NOT NULL", :name => :constraint_foo_bar_is_not_null, :validate => false
              end
            end

            test_migration.suppress_messages { test_migration.migrate(:up) }

            constraint_validated = ActiveRecord::Base.value_from_sql <<~SQL
              SELECT convalidated
              FROM pg_constraint
              WHERE conname = 'constraint_foo_bar_is_not_null'
            SQL

            expect(constraint_validated).to eq(false)
          end

          it "outputs the operation" do
            test_migration = Class.new(migration_klass) do
              def up
                unsafe_add_check_constraint :foos, "bar IS NOT NULL", :name => :constraint_foo_bar_is_not_null
              end
            end

            expect do
              test_migration.migrate(:up)
            end.to output(/add_check_constraint\(:foos, "bar IS NOT NULL", name: :constraint_foo_bar_is_not_null, validate: true\)/m).to_stdout
          end
        end

        describe "#safe_validate_check_constraint" do
          before(:each) do
            setup_migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :text
                safe_add_unvalidated_check_constraint :foos, "bar IS NOT NULL", :name => :constraint_foo_bar_is_not_null
              end
            end
            setup_migration.suppress_messages { setup_migration.migrate(:up) }
          end

          it "validates an existing CHECK constraint" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_validate_check_constraint :foos, :name => :constraint_foo_bar_is_not_null
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to make_database_queries(matching: /ALTER TABLE .+ VALIDATE CONSTRAINT/, count: 1)
              .and(change do
                ActiveRecord::Base.value_from_sql <<~SQL
                  SELECT convalidated
                  FROM pg_constraint
                  WHERE conname = 'constraint_foo_bar_is_not_null'
                SQL
              end.from(false).to(true))
          end

          it "raises a helpful error if a name is not passed" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_validate_check_constraint :foos, :name => nil
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected <name> to be present")
          end

          it "doesn't acquire a lock which prevents concurrent reads and writes" do
            alternate_connection_pool = ActiveRecord::ConnectionAdapters::ConnectionPool.new(pool_config)
            alternate_connection = alternate_connection_pool.connection

            alternate_connection.execute("BEGIN")
            alternate_connection.execute("LOCK TABLE foos")

            begin
              test_migration = Class.new(migration_klass) do
                def up
                  safe_validate_check_constraint :foos, :name => :constraint_foo_bar_is_not_null
                end
              end

              migration_thread = Thread.new do
                test_migration.suppress_messages { test_migration.migrate(:up) }
              end

              waiting_locks = []
              sleep_counter = 0
              until waiting_locks.present? || sleep_counter >= 5
                waiting_locks = locks_for_table(:foos, connection: alternate_connection).select { |l| !l.granted }
                sleep 1
              end

              alternate_connection.execute("ROLLBACK")

              expect(waiting_locks.size).to eq(1)
              # According to https://www.postgresql.org/docs/current/explicit-locking.html
              # ALTER TABLE VALIDATE CONSTRAINT... should aquire a SHARE UPDATE EXCLUSIVE
              # lock type which does not conflict, for example, with ROW EXCLUSIVE which
              # is generally acquired by anything modifying data in a table.
              expect(waiting_locks[0].lock_type).to eq("ShareUpdateExclusiveLock")

              migration_thread.join
            ensure
              alternate_connection_pool.disconnect!
            end
          end

          it "outputs the operation" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_validate_check_constraint :foos, :name => :constraint_foo_bar_is_not_null
              end
            end

            expect do
              test_migration.migrate(:up)
            end.to output(/validate_check_constraint\(:foos, name: :constraint_foo_bar_is_not_null\)/m).to_stdout
          end
        end

        describe "#safe_rename_constraint" do
          before(:each) do
            setup_migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :text
                unsafe_add_check_constraint :foos, "bar IS NOT NULL", :name => :constraint_foo_bar_is_not_null
              end
            end
            setup_migration.suppress_messages { setup_migration.migrate(:up) }
          end

          it "renames the constraint" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_rename_constraint :foos, :from => :constraint_foo_bar_is_not_null, :to => :other_comstraint
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to make_database_queries(matching: /ALTER TABLE .+ RENAME CONSTRAINT/, count: 1)
              .and(
                change do
                  ActiveRecord::Base.value_from_sql <<~SQL
                    SELECT conname
                    FROM pg_constraint
                    WHERE conrelid = 'foos'::regclass AND contype != 'p'
                  SQL
                end.from("constraint_foo_bar_is_not_null").to("other_comstraint")
              )
          end

          it "raises a helpful error if a from name is not passed" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_rename_constraint :foos, :from => nil, :to => :other_comstraint
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected <from> to be present")
          end

          it "raises a helpful error if a to name is not passed" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_rename_constraint :foos, :from => :constraint_foo_bar_is_not_null, :to => nil
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected <to> to be present")
          end

          it "outputs the operation" do
            test_migration = Class.new(migration_klass) do
              def up
                safe_rename_constraint :foos, :from => :constraint_foo_bar_is_not_null, :to => :other_constraint
              end
            end

            expect do
              test_migration.migrate(:up)
            end.to output(/rename_constraint\(:foos, from: :constraint_foo_bar_is_not_null, to: :other_constraint\)/m).to_stdout
          end

          it "calls safely_acquire_lock_for_table" do
            migration = Class.new(migration_klass).new

            expect(migration).to receive(:safely_acquire_lock_for_table).with(:foos)
            migration.safe_rename_constraint(:foos, :from => :constraint_foo_bar_is_not_null, :to => :other_constraint)
          end
        end

        describe "#unsafe_remove_constraint" do
          before(:each) do
            setup_migration = Class.new(migration_klass) do
              def up
                unsafe_create_table :foos
                unsafe_add_column :foos, :bar, :text
                unsafe_add_check_constraint :foos, "bar IS NOT NULL", :name => :constraint_foo_bar_is_not_null
              end
            end
            setup_migration.suppress_messages { setup_migration.migrate(:up) }
          end

          it "calls safely_acquire_lock_for_table" do
            migration = Class.new(migration_klass).new

            expect(migration).to receive(:safely_acquire_lock_for_table).with(:foos)
            migration.unsafe_remove_constraint(:foos, :name => :constraint_foo_bar_is_not_null)
          end

          it "drop the constraint" do
            test_migration = Class.new(migration_klass) do
              def up
                unsafe_remove_constraint :foos, :name => :constraint_foo_bar_is_not_null
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to make_database_queries(matching: /ALTER TABLE .+ DROP CONSTRAINT/, count: 1)
              .and(
                change do
                  ActiveRecord::Base.pluck_from_sql <<~SQL
                    SELECT conname
                    FROM pg_constraint
                    WHERE conrelid = 'foos'::regclass AND contype != 'p'
                  SQL
                end.from(["constraint_foo_bar_is_not_null"]).to([])
              )
          end

          it "raises a helpful error if a name is not passed" do
            test_migration = Class.new(migration_klass) do
              def up
                unsafe_remove_constraint :foos, :name => nil
              end
            end

            expect do
              test_migration.suppress_messages { test_migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected <name> to be present")
          end

          it "outputs the operation" do
            test_migration = Class.new(migration_klass) do
              def up
                unsafe_remove_constraint :foos, :name => :constraint_foo_bar_is_not_null
              end
            end

            expect do
              test_migration.migrate(:up)
            end.to output(/remove_constraint\(:foos, name: :constraint_foo_bar_is_not_null\)/m).to_stdout
          end
        end

        describe "safe_create_partitioned_table" do
          it "creates range partition on supported versions" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: :created_at do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            partition_strategy = ActiveRecord::Base.connection.select_value(<<~SQL)
              SELECT partstrat
              FROM pg_partitioned_table
              JOIN pg_class on pg_partitioned_table.partrelid = pg_class.oid
              WHERE pg_class.relname = 'foos3'
            SQL

            expect(partition_strategy).to eq("r")
          end

          it "raises error creating range partition on Postgres < 10" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: :created_at do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(9_06_00)

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::InvalidMigrationError, "Native partitioning not supported on Postgres databases before version 10")
          end

          it "creates list partition on supported versions" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :list, key: :created_at do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            partition_strategy = ActiveRecord::Base.connection.select_value(<<~SQL)
              SELECT partstrat
              FROM pg_partitioned_table
              JOIN pg_class on pg_partitioned_table.partrelid = pg_class.oid
              WHERE pg_class.relname = 'foos3'
            SQL

            expect(partition_strategy).to eq("l")
          end

          it "raises error creating list partition on Postgres < 10" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :list, key: :created_at do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(9_06_00)

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::InvalidMigrationError, "Native partitioning not supported on Postgres databases before version 10")
          end

          it "creates hash partition on supported versions" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :hash, key: :created_at do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            partition_strategy = ActiveRecord::Base.connection.select_value(<<~SQL)
              SELECT partstrat
              FROM pg_partitioned_table
              JOIN pg_class on pg_partitioned_table.partrelid = pg_class.oid
              WHERE pg_class.relname = 'foos3'
            SQL

            expect(partition_strategy).to eq("h")
          end

          it "raises error creating hash partition on Postgres < 11" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :hash, key: :created_at do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(10_00_00)

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::InvalidMigrationError, "Hash partitioning not supported on Postgres databases before version 11")
          end

          it "infers pk with defaults and simple key" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: :created_at do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            pk = ActiveRecord::Base.connection.primary_keys("foos3")

            expect(pk).to eq(["id", "created_at"])
          end

          it "infers pk with different name and simple key" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: :created_at, primary_key: :pk do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            pk = ActiveRecord::Base.connection.primary_keys("foos3")

            expect(pk).to eq(["pk", "created_at"])
          end

          it "infers single column pk when used as the partition key" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: :pk, primary_key: :pk do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            pk = ActiveRecord::Base.connection.primary_keys("foos3")

            expect(pk).to eq(["pk"])
          end

          it "does not create pk with defaults and complex key" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: ->{ "(created_at::date)" } do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            pk = ActiveRecord::Base.connection.primary_keys("foos3")

            expect(pk).to be_empty
          end

          it "infers pk with defaults and composite key" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: [:created_at, :text_column] do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            pk = ActiveRecord::Base.connection.primary_keys("foos3")

            expect(pk).to eq(["id", "created_at", "text_column"])
          end

          it "does not create pk when infer_primary_key is false" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, infer_primary_key: false, key: [:created_at, :text_column] do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            pk = ActiveRecord::Base.connection.primary_keys("foos3")

            expect(pk).to be_empty
          end

          it "does not create pk when infer_primary_key_on_partitioned_tables is false" do
            allow(PgHaMigrations.config)
              .to receive(:infer_primary_key_on_partitioned_tables)
              .and_return(false)

            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: [:created_at, :text_column] do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            pk = ActiveRecord::Base.connection.primary_keys("foos3")

            expect(pk).to be_empty
          end

          it "does not create pk when id is false" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, id: false, key: :created_at do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            pk = ActiveRecord::Base.connection.primary_keys("foos3")

            expect(pk).to be_empty
          end

          it "defaults to bigint pk" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: :created_at do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            columns = ActiveRecord::Base.connection.columns("foos3")
            id_column = columns.find { |column| column.name == "id" }

            expect(id_column.sql_type).to eq("bigint")
          end

          it "can override pk type" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: :created_at, id: :serial do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            columns = ActiveRecord::Base.connection.columns("foos3")
            id_column = columns.find { |column| column.name == "id" }

            expect(id_column.sql_type).to eq("integer")
          end

          it "does not create pk on Postgres 10" do
            allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(10_00_00)

            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: :created_at do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            pk = ActiveRecord::Base.connection.primary_keys("foos3")

            expect(pk).to be_empty
          end

          it "raises when partition type is invalid" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :garbage, key: :created_at do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected <type> to be symbol in [:range, :list, :hash]")
          end

          it "raises when partition key is not present" do
            migration = Class.new(migration_klass) do
              def up
                safe_create_partitioned_table :foos3, type: :range, key: nil do |t|
                  t.timestamps :null => false
                  t.text :text_column
                end
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected <key> to be present")
          end
        end

        describe "#adjust_lock_timeout" do
          let(:table_name) { "bogus_table" }
          let(:migration) { Class.new(migration_klass).new }

          before(:each) do
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
              ActiveRecord::ConnectionAdapters::ConnectionPool.new(pool_config)
            end
            let(:alternate_connection) do
              alternate_connection_pool.connection
            end
            let(:migration) { Class.new(migration_klass).new }

            before(:each) do
              ActiveRecord::Base.connection.execute("CREATE TABLE #{table_name}(pk SERIAL, i INTEGER)")
            end

            after(:each) do
              alternate_connection_pool.disconnect!
            end

            it "executes the block" do
              expect do |block|
                migration.safely_acquire_lock_for_table(table_name, &block)
              end.to yield_control
            end

            it "acquires an exclusive lock on the table" do
              migration.safely_acquire_lock_for_table(table_name) do
                expect(locks_for_table(table_name, connection: alternate_connection)).to eq([TableLock.new(table_name.to_s, "AccessExclusiveLock", true)])
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
                  [PgHaMigrations::BlockingDatabaseTransactions::LongRunningTransaction.new("", "", 5, "active", [table_name.to_s])]
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
                  [PgHaMigrations::BlockingDatabaseTransactions::LongRunningTransaction.new("", "some_sql_query", "active", 5, [table_name.to_s])]
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
              allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_wrap_original do |m, *args|
                if caller.detect { |line| line =~ /lib\/pg_ha_migrations\/blocking_database_transactions\.rb/ }
                  # The long-running transactions check needs to know the actual
                  # Postgres version to use the proper columns, so we don't want
                  # to mock any calls from it.
                  m.call(*args)
                else
                  9_01_12
                end
              end

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
