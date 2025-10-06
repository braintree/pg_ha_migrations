require "spec_helper"

RSpec.describe PgHaMigrations::SafeStatements do
  PgHaMigrations::AllowedVersions::ALLOWED_VERSIONS.each do |migration_klass|
    describe migration_klass do
      describe "#safe_create_table" do
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

      describe "#safe_add_column" do
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
          it "allows a default value that looks like an expression for the #{type.inspect} type" do
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

            migration.suppress_messages { migration.migrate(:up) }

            # Handle binary columns being transported, but not stored, as hex.
            expected_value = type == :binary ? "\\x4e4f572829" : "NOW()"
            expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.default).to eq(expected_value)

            ActiveRecord::Base.connection.execute("INSERT INTO foos DEFAULT VALUES")
            expect(ActiveRecord::Base.connection.select_values("SELECT bar FROM foos")).to all(eq(expected_value))
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

        it "allows setting null => false (with a default)" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :foos
              ActiveRecord::Base.connection.execute("INSERT INTO foos DEFAULT VALUES")
              safe_add_column :foos, :bar, :text, :null => false, :default => "baz"
            end
          end

          migration.suppress_messages { migration.migrate(:up) }
          aggregate_failures do
            expect(ActiveRecord::Base.connection.select_value("SELECT bar FROM foos")).to eq("baz")
            expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.null).to eq(false)
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

      describe "#safe_change_column_default" do
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
          before(:each) do
            allow(PgHaMigrations.config)
              .to receive(:prefer_single_step_column_addition_with_default)
              .and_return(false)
          end

          it "allows setting a constant default value when the column was added in the same migration" do
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
          it "disallows setting a constant default value when the column was added in the same migration" do
            migration = Class.new(migration_klass) do
              define_method(:up) do
                unsafe_create_table :foos
                safe_add_column :foos, :bar, :text
                safe_change_column_default :foos, :bar, "bogus"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error PgHaMigrations::BestPracticeError
          end
        end
      end

      describe "#safe_make_column_nullable" do
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

      describe "#safe_make_column_not_nullable" do
        let(:migration) { Class.new(ActiveRecord::Migration::Current).new }

        before(:each) do
          migration.suppress_messages do
            migration.unsafe_create_table :test_table do |t|
              t.integer :column_to_check
            end
          end
        end

        it "adds the not null constraint to the column" do
          migration = Class.new(migration_klass) do
            def up
              safe_make_column_not_nullable :test_table, :column_to_check
            end
          end

          expect(ActiveRecord::Base.connection.columns("test_table").detect { |column| column.name == "column_to_check" }.null).to eq(true)

          migration.suppress_messages { migration.migrate(:up) }

          expect(ActiveRecord::Base.connection.columns("test_table").detect { |column| column.name == "column_to_check" }.null).to eq(false)

          expect(ActiveRecord::Base.connection.select_values("SELECT conname FROM pg_constraint WHERE conname like 'tmp%'"))
            .to be_empty
        end

        it "uses an existing validated constraint with matching definition instead of creating a new one" do
          # Add and validate a constraint that matches what safe_make_column_not_nullable would create
          migration.suppress_messages do
            migration.safe_add_unvalidated_check_constraint(:test_table, "column_to_check IS NOT NULL", name: "existing_not_null_check")
            migration.safe_validate_check_constraint(:test_table, name: "existing_not_null_check")
          end

          # Set up spies to ensure methods aren't called unnecessarily
          expect(migration).to receive(:unsafe_make_column_not_nullable).with(:test_table, :column_to_check).and_call_original
          expect(migration).to receive(:unsafe_remove_constraint).with(:test_table, name: "existing_not_null_check").and_call_original
          expect(migration).not_to receive(:safe_add_unvalidated_check_constraint)
          expect(migration).not_to receive(:safe_validate_check_constraint)

          # Call the method that should use the existing constraint
          expect do
            migration.suppress_messages do
              migration.safe_make_column_not_nullable(:test_table, :column_to_check)
            end
          end.to change {
            # Verify the constraint was dropped
            ActiveRecord::Base.connection.select_value("SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'test_table'::regclass AND contype = 'c'")
          }.from(1).to(0)
            .and change {
              # Verify the column is now not nullable
              ActiveRecord::Base.connection.columns(:test_table).find { |c| c.name == "column_to_check" }.null
            }.from(true).to(false)
        end

        it "validates an existing unvalidated constraint with matching definition instead of creating a new one" do
          # Add an UNVALIDATED constraint that matches what safe_make_column_not_nullable would create
          migration.suppress_messages do
            migration.safe_add_unvalidated_check_constraint(:test_table, "column_to_check IS NOT NULL", name: "existing_not_null_check")
          end

          # Set up spies to ensure methods are called correctly
          expect(migration).to receive(:unsafe_make_column_not_nullable).with(:test_table, :column_to_check).and_call_original
          expect(migration).to receive(:unsafe_remove_constraint).with(:test_table, name: "existing_not_null_check").and_call_original
          expect(migration).to receive(:safe_validate_check_constraint).with(:test_table, name: "existing_not_null_check").and_call_original
          expect(migration).not_to receive(:safe_add_unvalidated_check_constraint)

          # Call the method that should use the existing constraint
          expect do
            migration.suppress_messages do
              migration.safe_make_column_not_nullable(:test_table, :column_to_check)
            end
          end.to change {
            # Verify the constraint was dropped
            ActiveRecord::Base.connection.select_value("SELECT COUNT(*) FROM pg_constraint WHERE conrelid = 'test_table'::regclass AND contype = 'c'")
          }.from(1).to(0)
            .and change {
              # Verify the column is now not nullable
              ActiveRecord::Base.connection.columns(:test_table).find { |c| c.name == "column_to_check" }.null
            }.from(true).to(false)
        end

        describe "doesn't use an existing CHECK constraint that does not enforce non-null values" do
          it "with an entirely different condition" do
            migration.suppress_messages do
              migration.safe_add_unvalidated_check_constraint(:test_table , "column_to_check > 0", name: :check_positive)
              migration.safe_validate_check_constraint(:test_table, name: :check_positive)
            end

            expect(migration).to receive(:safe_add_unvalidated_check_constraint)
              .with(:test_table, '"column_to_check" IS NOT NULL', name: match(/\Atmp_not_null_constraint_/))
              .and_call_original
            migration.suppress_messages do
              migration.safe_make_column_not_nullable(:test_table, :column_to_check)
            end
          end

          it "with a prefixed condition" do
            migration.suppress_messages do
              migration.safe_add_column :test_table, :other_column, :integer
              migration.safe_add_unvalidated_check_constraint(:test_table, "column_to_check IS NOT NULL OR other_column IS NOT NULL", name: :check_a_or_b_not_null)
              migration.safe_validate_check_constraint(:test_table, name: :check_a_or_b_not_null)
            end

            expect(migration).to receive(:safe_add_unvalidated_check_constraint)
              .with(:test_table, '"column_to_check" IS NOT NULL', name: match(/\Atmp_not_null_constraint_/))
              .and_call_original
            migration.suppress_messages do
              migration.safe_make_column_not_nullable(:test_table, :column_to_check)
            end
          end

          it "with a suffixed condition" do
            migration.suppress_messages do
              migration.safe_add_column :test_table, :other_column, :integer
              migration.safe_add_unvalidated_check_constraint(:test_table, "other_column IS NOT NULL OR column_to_check IS NOT NULL", name: :check_a_or_b_not_null)
              migration.safe_validate_check_constraint(:test_table, name: :check_a_or_b_not_null)
            end

            expect(migration).to receive(:safe_add_unvalidated_check_constraint)
              .with(:test_table, '"column_to_check" IS NOT NULL', name: match(/\Atmp_not_null_constraint_/))
              .and_call_original
            migration.suppress_messages do
              migration.safe_make_column_not_nullable(:test_table, :column_to_check)
            end
          end
        end
      end

      describe "#safe_make_column_not_nullable_from_check_constraint" do
        let(:migration) { Class.new(ActiveRecord::Migration::Current).new }

        before(:each) do
          migration.suppress_messages do
            migration.safe_create_table(:test_table) do |t|
              t.integer :column_to_check
            end
            migration.safe_add_unvalidated_check_constraint(:test_table, "column_to_check IS NOT NULL", name: :test_check_constraint)
          end
        end

        it "raises an error if the constraint does not exist" do
          expect do
            migration.suppress_messages do
              migration.safe_make_column_not_nullable_from_check_constraint(:test_table, :column_to_check, constraint_name: :non_existent_constraint)
            end
          end.to raise_error(PgHaMigrations::InvalidMigrationError, /The provided constraint does not exist/)
        end

        it "raises an error if the constraint is not validated" do
          expect do
            migration.suppress_messages do
              migration.safe_make_column_not_nullable_from_check_constraint(:test_table, :column_to_check, constraint_name: :test_check_constraint)
            end
          end.to raise_error(PgHaMigrations::InvalidMigrationError, /The provided constraint is not validated/)
        end

        describe "raises an error if the CHECK constraint does not enforce non-null values" do
          it "with an entirely different condition" do
            migration.suppress_messages do
              migration.safe_add_unvalidated_check_constraint(:test_table, "column_to_check > 0", name: :check_positive)
              migration.safe_validate_check_constraint(:test_table, name: :check_positive)
            end

            expect do
              migration.suppress_messages do
                migration.safe_make_column_not_nullable_from_check_constraint(:test_table, :column_to_check, constraint_name: "check_positive")
              end
            end.to raise_error(PgHaMigrations::InvalidMigrationError, /does not enforce non-null values/)
          end

          it "with a prefixed condition" do
            migration.suppress_messages do
              migration.safe_add_column :test_table, :other_column, :integer
              migration.safe_add_unvalidated_check_constraint(:test_table, "column_to_check IS NOT NULL OR other_column IS NOT NULL", name: :check_a_or_b_not_null)
              migration.safe_validate_check_constraint(:test_table, name: :check_a_or_b_not_null)
            end

            expect do
              migration.safe_make_column_not_nullable_from_check_constraint(:test_table, :a, constraint_name: :check_a_or_b_not_null)
            end.to raise_error(PgHaMigrations::InvalidMigrationError, /The provided constraint does not enforce non-null values for the column/)
          end

          it "with a suffixed condition" do
            migration.suppress_messages do
              migration.safe_add_column :test_table, :other_column, :integer
              migration.safe_add_unvalidated_check_constraint(:test_table, "other_column IS NOT NULL OR column_to_check IS NOT NULL", name: :check_a_or_b_not_null)
              migration.safe_validate_check_constraint(:test_table, name: :check_a_or_b_not_null)
            end

            expect do
              migration.safe_make_column_not_nullable_from_check_constraint(:test_table, :a, constraint_name: :check_a_or_b_not_null)
            end.to raise_error(PgHaMigrations::InvalidMigrationError, /The provided constraint does not enforce non-null values for the column/)
          end
        end

        it "makes the column NOT NULL if the constraint is validated" do
          migration.suppress_messages do
            migration.safe_validate_check_constraint(:test_table, name: :test_check_constraint)
          end

          expect do
            migration.suppress_messages do
              migration.safe_make_column_not_nullable_from_check_constraint(:test_table, :column_to_check, constraint_name: :test_check_constraint)
            end
          end.not_to raise_error

          column_details = ActiveRecord::Base.connection.columns(:test_table).find { |col| col.name == "column_to_check" }
          expect(column_details.null).to be(false)
        end

        it "makes a column that needs quoting NOT NULL if the constraint is validated" do
          migration.suppress_messages do
            migration.unsafe_rename_column(:test_table, :column_to_check, "other column")
            migration.safe_validate_check_constraint(:test_table, name: :test_check_constraint)
          end

          expect do
            migration.suppress_messages do
              migration.safe_make_column_not_nullable_from_check_constraint(:test_table, "other column", constraint_name: :test_check_constraint)
            end
          end.not_to raise_error

          column_details = ActiveRecord::Base.connection.columns(:test_table).find { |col| col.name == "other column" }
          expect(column_details.null).to be(false)
        end

        it "drops the constraint by default using only a single table lock" do
          migration.suppress_messages do
            migration.safe_validate_check_constraint(:test_table, name: :test_check_constraint)
          end

          expect do
            expect do
              migration.suppress_messages do
                migration.safe_make_column_not_nullable_from_check_constraint(:test_table, :column_to_check, constraint_name: :test_check_constraint)
              end
            end.to make_database_queries(matching: /LOCK "public"\."test_table" IN ACCESS EXCLUSIVE MODE/, count: 1)
          end.to change {
            ActiveRecord::Base.connection.select_value(<<~SQL)
              SELECT COUNT(*)
              FROM pg_constraint
              WHERE conname = 'test_check_constraint'
            SQL
          }.from(1).to(0)
        end

        it "doesn't drop the constraint if the drop_constraint argument is false" do
          migration.suppress_messages do
            migration.safe_validate_check_constraint(:test_table, name: :test_check_constraint)
          end

          expect do
            migration.suppress_messages do
              migration.safe_make_column_not_nullable_from_check_constraint(:test_table, :column_to_check, constraint_name: :test_check_constraint, drop_constraint: false)
            end
          end.not_to change {
            ActiveRecord::Base.connection.select_value(<<~SQL)
              SELECT COUNT(*)
              FROM pg_constraint
              WHERE conname = 'test_check_constraint'
            SQL
          }
        end
      end

      describe "#safe_create_enum_type" do
        it "creates a new enum type" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_enum_type :bt_foo_enum, ["one", "two", "three"]
            end
          end

          migration.suppress_messages { migration.migrate(:up) }

          expect(TestHelpers.enum_names_and_values).to contain_exactly(
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

          expect(TestHelpers.enum_names_and_values).to contain_exactly(
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

          expect(TestHelpers.enum_names_and_values).to eq([])
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

      describe "#safe_add_enum_value" do
        it "creates a new enum value" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_execute("CREATE TYPE bt_foo_enum AS ENUM ('one', 'two', 'three')")
              safe_add_enum_value :bt_foo_enum, "four"
            end
          end

          migration.suppress_messages { migration.migrate(:up) }

          expect(TestHelpers.enum_names_and_values).to contain_exactly(
            {"name" => "bt_foo_enum", "value" => "one"},
            {"name" => "bt_foo_enum", "value" => "two"},
            {"name" => "bt_foo_enum", "value" => "three"},
            {"name" => "bt_foo_enum", "value" => "four"},
          )
        end
      end

      describe "#safe_add_index_on_empty_table" do
        it "creates index when table is empty" do
          setup_migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :foos
              unsafe_add_column :foos, :bar, :text
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_index_on_empty_table :foos, [:bar]
            end
          end

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN SHARE MODE/, count: 1)
            .and(make_database_queries(matching: /CREATE INDEX "index_foos_on_bar"/, count: 1))

          indexes = ActiveRecord::Base.connection.indexes("foos")
          expect(indexes.size).to eq(1)
          expect(indexes.first).to have_attributes(
            table: "foos",
            name: "index_foos_on_bar",
            columns: ["bar"],
          )
        end

        it "raises error when :algorithm => :concurrently provided" do
          test_migration = Class.new(migration_klass) do
            def up
              safe_add_index_on_empty_table :foos, [:bar], algorithm: :concurrently
            end
          end

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(ArgumentError, "Cannot call safe_add_index_on_empty_table with :algorithm => :concurrently")
        end

        it "raises error when table contains rows" do
          setup_migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :foos
              unsafe_add_column :foos, :bar, :text

              unsafe_execute("INSERT INTO foos DEFAULT VALUES")
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_index_on_empty_table :foos, [:bar]
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:select_value).and_call_original
          expect(ActiveRecord::Base.connection).to receive(:select_value).with(/SELECT EXISTS/).once.and_call_original
          expect(ActiveRecord::Base.connection).to_not receive(:select_value).with(/pg_total_relation_size/)

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::InvalidMigrationError, "Table \"foos\" has rows")

          indexes = ActiveRecord::Base.connection.indexes("foos")
          expect(indexes).to be_empty
        end

        it "raises error when table is larger than small table threshold" do
          setup_migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :foos
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_index_on_empty_table :foos, [:bar]
            end
          end

          stub_const("PgHaMigrations::SMALL_TABLE_THRESHOLD_BYTES", 1.kilobyte)

          allow(ActiveRecord::Base.connection).to receive(:select_value).and_call_original
          expect(ActiveRecord::Base.connection).to receive(:select_value).with(/SELECT EXISTS/).once.and_call_original
          expect(ActiveRecord::Base.connection).to receive(:select_value).with(/pg_total_relation_size/).once.and_call_original

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::InvalidMigrationError, "Table \"foos\" is larger than 1024 bytes")

          indexes = ActiveRecord::Base.connection.indexes("foos")
          expect(indexes).to be_empty
        end

        it "raises error when table receives writes immediately after the first check" do
          setup_migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :foos
              unsafe_add_column :foos, :bar, :text
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_index_on_empty_table :foos, [:bar]
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:select_value).and_call_original
          expect(ActiveRecord::Base.connection).to receive(:select_value).with(/pg_total_relation_size/).once.and_call_original
          expect(ActiveRecord::Base.connection).to receive(:select_value).with(/SELECT EXISTS/).twice.and_wrap_original do |m, *args|
            m.call(*args).tap do
              ActiveRecord::Base.connection.execute("INSERT INTO foos DEFAULT VALUES")
            end
          end

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::InvalidMigrationError, "Table \"foos\" has rows")

          indexes = ActiveRecord::Base.connection.indexes("foos")
          expect(indexes).to be_empty
        end

        it "raises error when nulls_not_distinct is provided but PostgreSQL < 15" do
          setup_migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_index_on_empty_table :foos, :bar, nulls_not_distinct: true
            end
          end

          # Temporarily mock the PostgreSQL version to be < 15
          allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(14_00_00)

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(
            PgHaMigrations::InvalidMigrationError,
            "nulls_not_distinct option requires PostgreSQL 15 or higher"
          )
        end
      end

      describe "#safe_add_concurrent_index" do
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
          expect(indexes.first).to have_attributes(
            table: "foos",
            name: "index_foos_on_bar",
            columns: ["bar"],
          )
        end

        it "generates index name with hashed identifier when default index name is too large" do
          setup_migration = Class.new(migration_klass) do
            def up
              unsafe_create_table "x" * 51
              unsafe_add_column "x" * 51, :bar, :text
            end
          end
          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_index "x" * 51, [:bar]
            end
          end

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to make_database_queries(matching: /CREATE +INDEX CONCURRENTLY/, count: 1)

          indexes = ActiveRecord::Base.connection.indexes("x" * 51)
          expect(indexes.size).to eq(1)
          expect(indexes.first).to have_attributes(
            table: "x" * 51,
            name: "idx_on_bar_d7a594ad66",
            columns: ["bar"],
          )
        end

        it "raises error when nulls_not_distinct is provided but PostgreSQL < 15" do
          setup_migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_index :foos, :bar, nulls_not_distinct: true
            end
          end

          # Temporarily mock the PostgreSQL version to be < 15
          allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(14_00_00)

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(
            PgHaMigrations::InvalidMigrationError,
            "nulls_not_distinct option requires PostgreSQL 15 or higher"
          )
        end
      end

      describe "#safe_add_concurrent_partitioned_index" do
        before do
          TestHelpers.install_partman(schema: "partman")
        end

        it "creates valid index when there are no child partitions" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "index_foos3_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to_not receive(:execute).with(/CREATE INDEX CONCURRENTLY/)
            expect(ActiveRecord::Base.connection).to_not receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/)
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }

          indexes = ActiveRecord::Base.connection.indexes(:foos3)

          expect(indexes.size).to eq(1)
          expect(indexes.first).to have_attributes(
            table: :foos3,
            name: "index_foos3_on_updated_at",
            columns: ["updated_at"],
            using: :btree,
          )
        end

        it "creates valid index with comment and custom name when multiple child partitions exist" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at, comment: "this is an index", name: "foos3_idx"
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "foos3_idx" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/).exactly(10).times.ordered
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }

          aggregate_failures do
            tables_with_indexes = TestHelpers.partitions_for_table(:foos3).append(:foos3)

            expect(tables_with_indexes.size).to eq(11)

            tables_with_indexes.each do |table|
              indexes = ActiveRecord::Base.connection.indexes(table)

              expected_comment = table == :foos3 ? "this is an index" : nil
              expected_name = table == :foos3 ? "foos3_idx" : "index_#{table}_on_updated_at"

              expect(indexes.size).to eq(1)
              expect(indexes.first).to have_attributes(
                table: table,
                name: expected_name,
                columns: ["updated_at"],
                using: :btree,
                comment: expected_comment,
              )
            end
          end
        end

        it "creates valid hash index when multiple child partitions exist" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at, using: :hash
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "index_foos3_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/).exactly(10).times.ordered
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }

          aggregate_failures do
            tables_with_indexes = TestHelpers.partitions_for_table(:foos3).append(:foos3)

            expect(tables_with_indexes.size).to eq(11)

            tables_with_indexes.each do |table|
              indexes = ActiveRecord::Base.connection.indexes(table)

              expect(indexes.size).to eq(1)
              expect(indexes.first).to have_attributes(
                table: table,
                name: "index_#{table}_on_updated_at",
                columns: ["updated_at"],
                using: :hash,
              )
            end
          end
        end

        it "creates valid unique index when multiple child partitions exist" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, [:created_at, :updated_at], unique: true
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE UNIQUE INDEX "index_foos3_on_created_at_and_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE UNIQUE INDEX CONCURRENTLY/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/).exactly(10).times.ordered
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }

          aggregate_failures do
            tables_with_indexes = TestHelpers.partitions_for_table(:foos3).append(:foos3)

            expect(tables_with_indexes.size).to eq(11)

            tables_with_indexes.each do |table|
              indexes = ActiveRecord::Base.connection.indexes(table)

              expect(indexes.size).to eq(1)
              expect(indexes.first).to have_attributes(
                table: table,
                name: "index_#{table}_on_created_at_and_updated_at",
                columns: ["created_at", "updated_at"],
                unique: true,
                using: :btree,
              )
            end
          end
        end

        it "creates valid partial index when multiple child partitions exist" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at, where: "text_column IS NOT NULL"
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "index_foos3_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/).exactly(10).times.ordered
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }

          aggregate_failures do
            tables_with_indexes = TestHelpers.partitions_for_table(:foos3).append(:foos3)

            expect(tables_with_indexes.size).to eq(11)

            tables_with_indexes.each do |table|
              indexes = ActiveRecord::Base.connection.indexes(table)

              expect(indexes.size).to eq(1)
              expect(indexes.first).to have_attributes(
                table: table,
                name: "index_#{table}_on_updated_at",
                columns: ["updated_at"],
                where: "(text_column IS NOT NULL)",
                using: :btree,
              )
            end
          end
        end

        it "creates valid index using expression when multiple child partitions exist" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, "lower(text_column)"
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "index_foos3_on_lower_text_column" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/).exactly(10).times.ordered
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }

          aggregate_failures do
            tables_with_indexes = TestHelpers.partitions_for_table(:foos3).append(:foos3)

            expect(tables_with_indexes.size).to eq(11)

            tables_with_indexes.each do |table|
              indexes = ActiveRecord::Base.connection.indexes(table)

              expect(indexes.size).to eq(1)
              expect(indexes.first).to have_attributes(
                table: table,
                name: "index_#{table}_on_lower_text_column",
                columns: "lower(text_column)",
                using: :btree,
              )
            end
          end
        end

        it "creates valid index when sub-partition exists with no child partitions" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass)
          TestHelpers.create_range_partitioned_table(:foos3_sub, migration_klass)

          ActiveRecord::Base.connection.execute(<<~SQL)
            ALTER TABLE foos3
            ATTACH PARTITION foos3_sub
            FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
          SQL

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "index_foos3_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3_sub" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "index_foos3_sub_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/).once.ordered
            expect(ActiveRecord::Base.connection).to_not receive(:execute).with(/CREATE INDEX CONCURRENTLY/)
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }

          aggregate_failures do
            %i[foos3 foos3_sub].each do |table|
              indexes = ActiveRecord::Base.connection.indexes(table)

              expect(indexes.size).to eq(1)
              expect(indexes.first).to have_attributes(
                table: table,
                name: "index_#{table}_on_updated_at",
                columns: ["updated_at"],
                using: :btree,
              )
            end
          end
        end

        it "creates valid index when multiple child partitions and child sub-partitions exist" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)
          TestHelpers.create_range_partitioned_table(:foos3_sub, migration_klass, with_partman: true)

          ActiveRecord::Base.connection.execute(<<~SQL)
            ALTER TABLE foos3
            ATTACH PARTITION foos3_sub
            FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
          SQL

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "index_foos3_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3_sub" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "index_foos3_sub_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX "public"."index_foos3_sub_on_updated_at"\nATTACH PARTITION/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX "public"."index_foos3_on_updated_at"\nATTACH PARTITION/).exactly(11).times.ordered
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }

          aggregate_failures do
            tables_with_indexes = TestHelpers.partitions_for_table(:foos3)
              .concat(TestHelpers.partitions_for_table(:foos3_sub))
              .append(:foos3)

            expect(tables_with_indexes.size).to eq(22)

            tables_with_indexes.each do |table|
              indexes = ActiveRecord::Base.connection.indexes(table)

              expect(indexes.size).to eq(1)
              expect(indexes.first).to have_attributes(
                table: table,
                name: "index_#{table}_on_updated_at",
                columns: ["updated_at"],
                using: :btree,
              )
            end
          end
        end

        it "short-circuits when index already valid and if_not_exists is true" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)

          setup_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at, if_not_exists: true
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to_not receive(:execute).with(/LOCK "public"\."foos3" IN SHARE MODE/)
            expect(ActiveRecord::Base.connection).to_not receive(:execute).with(/CREATE INDEX IF NOT EXISTS "index_foos3_on_updated_at" ON ONLY/)
            expect(ActiveRecord::Base.connection).to_not receive(:execute).with(/CREATE INDEX CONCURRENTLY IF NOT EXISTS/)
            expect(ActiveRecord::Base.connection).to_not receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/)
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }
        end

        it "creates valid index when partially created and if_not_exists is true" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)
          TestHelpers.create_range_partitioned_table(:foos3_sub, migration_klass, with_partman: true)

          setup_migration = Class.new(migration_klass) do
            def up
              unsafe_execute(<<~SQL)
                ALTER TABLE foos3
                ATTACH PARTITION foos3_sub
                FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
              SQL

              unsafe_add_index :foos3, :updated_at, algorithm: :only
              unsafe_add_index :foos3_default, :updated_at

              unsafe_execute(<<~SQL)
                ALTER INDEX index_foos3_on_updated_at
                ATTACH PARTITION index_foos3_default_on_updated_at
              SQL
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at, if_not_exists: true
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX IF NOT EXISTS "index_foos3_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY IF NOT EXISTS/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3_sub" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX IF NOT EXISTS "index_foos3_sub_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY IF NOT EXISTS/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX "public"."index_foos3_sub_on_updated_at"\nATTACH PARTITION/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX "public"."index_foos3_on_updated_at"\nATTACH PARTITION/).exactly(11).times.ordered
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }
        end

        it "creates valid index when table / index name use non-standard characters" do
          TestHelpers.create_range_partitioned_table("foos3'", migration_klass)

          # partman does not allow table names with non-standard characters
          # so we need to create a child partition manually
          setup_migration = Class.new(migration_klass) do
            def up
              unsafe_execute(<<~SQL)
                CREATE TABLE "foos3'_child" PARTITION OF "foos3'"
                FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
              SQL
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index "foos3'", :updated_at, name: "foos3'_bar\"_idx"
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3'" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "foos3'_bar""_idx" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY "index_foos3'_child_on_updated_at" ON/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/).once.ordered
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }

          aggregate_failures do
            ["foos3'", "foos3'_child"].each do |table|
              indexes = ActiveRecord::Base.connection.indexes(table)

              expected_name = table == "foos3'" ? "foos3'_bar\"_idx" : "index_#{table}_on_updated_at"

              expect(indexes.size).to eq(1)
              expect(indexes.first).to have_attributes(
                table: table,
                name: expected_name,
                columns: ["updated_at"],
                using: :btree,
              )
            end
          end
        end

        it "creates valid index when duplicate table exists in different schema" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)
          TestHelpers.create_range_partitioned_table("partman.foos3", migration_klass, with_partman: true)

          setup_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index "partman.foos3", :updated_at
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "partman"\."foos3" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "index_foos3_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/).exactly(10).times.ordered
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }

          aggregate_failures do
            tables_with_indexes = TestHelpers.partitions_for_table(:foos3, schema: "partman").append("foos3")

            expect(tables_with_indexes.size).to eq(11)

            tables_with_indexes.each do |table|
              indexes = ActiveRecord::Base.connection.indexes("partman.#{table}")

              expect(indexes.size).to eq(1)
              expect(indexes.first).to have_attributes(
                table: "partman.#{table}",
                name: "index_#{table}_on_updated_at",
                columns: ["updated_at"],
                using: :btree,
              )
            end
          end
        end

        it "creates valid index when child table exists in different schema" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

          setup_migration = Class.new(migration_klass) do
            def up
              unsafe_execute(<<~SQL)
                CREATE TABLE partman.foos3_child PARTITION OF foos3
                FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
              SQL
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."foos3" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "index_foos3_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/).once.ordered
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }

          aggregate_failures do
            child_indexes = ActiveRecord::Base.connection.indexes("partman.foos3_child")

            expect(child_indexes.size).to eq(1)
            expect(child_indexes.first).to have_attributes(
              table: "partman.foos3_child",
              name: "index_foos3_child_on_updated_at",
              columns: ["updated_at"],
              using: :btree,
            )

            parent_indexes = ActiveRecord::Base.connection.indexes(:foos3)

            expect(parent_indexes.size).to eq(1)
            expect(parent_indexes.first).to have_attributes(
              table: :foos3,
              name: "index_foos3_on_updated_at",
              columns: ["updated_at"],
              using: :btree,
            )
          end
        end

        it "generates index name with hashed identifier when default child index name is too large" do
          TestHelpers.create_range_partitioned_table("x" * 42, migration_klass, with_partman: true)

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index "x" * 42, :updated_at
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

          aggregate_failures do
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/LOCK "public"\."#{"x" * 42}" IN SHARE MODE/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX "index_#{"x" * 42}_on_updated_at" ON ONLY/).once.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/CREATE INDEX CONCURRENTLY "idx_on_updated_at_\w{10}/).exactly(10).times.ordered
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/ALTER INDEX .+\nATTACH PARTITION/).exactly(10).times.ordered
          end

          test_migration.suppress_messages { test_migration.migrate(:up) }
        end

        it "raises error when parent index name is too large" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at, name: "x" * 64
            end
          end

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(
            ArgumentError,
            "Index name '#{"x" * 64}' on table 'foos3' is too long; the limit is 63 characters"
          )
        end

        it "raises error when table is not partitioned" do
          setup_migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos3 do |t|
                t.timestamps null: false
              end
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at
            end
          end

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::InvalidMigrationError, "Table \"foos3\" is not a partitioned table")
        end

        it "raises error and leaves behind invalid index when child index creation missed" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

          ActiveRecord::Base.connection.execute(<<~SQL)
            CREATE TABLE foos3_child1 PARTITION OF foos3
            FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');

            CREATE TABLE foos3_child2 PARTITION OF foos3
            FOR VALUES FROM ('2020-02-01') TO ('2020-03-01');
          SQL

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at
            end
          end

          allow_any_instance_of(PgHaMigrations::Table).to receive(:partitions)
            .and_return([PgHaMigrations::Table.new("foos3_child1", "public")])

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::InvalidMigrationError, "Unexpected state. Parent index \"index_foos3_on_updated_at\" is invalid")

          aggregate_failures do
            %i[foos3 foos3_child1].each do |table|
              indexes = ActiveRecord::Base.connection.indexes(table)

              expect(indexes.size).to eq(1)
              expect(indexes.first).to have_attributes(
                table: table,
                name: "index_#{table}_on_updated_at",
                columns: ["updated_at"],
                using: :btree,
              )
            end

            expect(ActiveRecord::Base.connection.indexes(:foos3_child2)).to be_empty

            expect(ActiveRecord::Base.pluck_from_sql("SELECT indexrelid::regclass::text FROM pg_index WHERE NOT indisvalid")).to contain_exactly(
              "index_foos3_on_updated_at",
            )
          end
        end

        it "raises error and leaves behind invalid index when child sub-partition index creation missed" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass)
          TestHelpers.create_range_partitioned_table(:foos3_sub, migration_klass)

          ActiveRecord::Base.connection.execute(<<~SQL)
            ALTER TABLE foos3
            ATTACH PARTITION foos3_sub
            FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

            CREATE TABLE foos3_sub_child1 PARTITION OF foos3_sub
            FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');

            CREATE TABLE foos3_sub_child2 PARTITION OF foos3_sub
            FOR VALUES FROM ('2020-02-01') TO ('2020-03-01');
          SQL

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at
            end
          end

          allow_any_instance_of(PgHaMigrations::Table).to receive(:partitions).and_wrap_original do |meth|
            if meth.receiver.name == "foos3_sub"
              [PgHaMigrations::Table.new("foos3_sub_child1", "public")]
            else
              meth.call
            end
          end

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::InvalidMigrationError, "Unexpected state. Parent index \"index_foos3_sub_on_updated_at\" is invalid")

          aggregate_failures do
            %i[foos3 foos3_sub foos3_sub_child1].each do |table|
              indexes = ActiveRecord::Base.connection.indexes(table)

              expect(indexes.size).to eq(1)
              expect(indexes.first).to have_attributes(
                table: table,
                name: "index_#{table}_on_updated_at",
                columns: ["updated_at"],
                using: :btree,
              )
            end

            expect(ActiveRecord::Base.connection.indexes(:foos3_sub_child2)).to be_empty

            expect(ActiveRecord::Base.pluck_from_sql("SELECT indexrelid::regclass::text FROM pg_index WHERE NOT indisvalid")).to contain_exactly(
              "index_foos3_on_updated_at",
              "index_foos3_sub_on_updated_at",
            )
          end
        end

        it "raises error when on < Postgres 11" do
          allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(10_00_00)

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index :foos3, :updated_at
            end
          end

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::InvalidMigrationError, "Concurrent partitioned index creation not supported on Postgres databases before version 11")
        end

        it "creates valid index with nulls_not_distinct when multiple child partitions exist" do
          skip "Won't actually be able to create nulls_not_distinct indexes unless Postgres supports it" if ActiveRecord::Base.connection.postgresql_version < 15_00_00

          TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index(
                :foos3,
                :text_column,
                nulls_not_distinct: true
              )
            end
          end

          # Count child partitions to know exactly how many times the method will be called
          child_tables = TestHelpers.partitions_for_table(:foos3)
          # +1 for the parent table itself
          expected_calls = child_tables.size + 1

          # Check that we pass the nulls_not_distinct option to the underlying add_index method
          # exactly the right number of times (once for parent table + once for each child partition)
          expect(ActiveRecord::Base.connection).to receive(:add_index).with(
            anything,
            anything,
            hash_including(nulls_not_distinct: true)
          ).exactly(expected_calls).times.and_call_original

          test_migration.suppress_messages { test_migration.migrate(:up) }

          # Verify the nulls_not_distinct property is actually set on the created indexes
          tables_with_indexes = TestHelpers.partitions_for_table(:foos3).unshift(:foos3)

          tables_with_indexes.each do |table|
            index_name = "index_#{table}_on_text_column"
            index_def = ActiveRecord::Base.connection.indexes(table).find { |idx| idx.name == index_name }

            expect(index_def).to be_present
            expect(index_def.nulls_not_distinct).to be(true),
              "Index '#{index_name}' on table '#{table}' does not have nulls_not_distinct set"
          end
        end

        it "raises error when nulls_not_distinct is provided but PostgreSQL < 15" do
          TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)

          test_migration = Class.new(migration_klass) do
            def up
              safe_add_concurrent_partitioned_index(
                :foos3,
                :text_column,
                nulls_not_distinct: true
              )
            end
          end

          # Temporarily mock the PostgreSQL version to be < 15
          allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(14_00_00)

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(
            PgHaMigrations::InvalidMigrationError,
            "nulls_not_distinct option requires PostgreSQL 15 or higher"
          )
        end
      end

      describe "#safe_remove_concurrent_index" do
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

        it "raises a nice error if options does not have :name key" do
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

          # Ruby 3.4 changed #inspect rendering for hashes
          #
          # https://rubyreferences.github.io/rubychanges/3.4.html#inspect-rendering-have-been-changed
          expected_output = if Gem::Version.new(RUBY_VERSION) < Gem::Version.new("3.4.0")
            /add_check_constraint\(:foos, "bar IS NOT NULL", {:name=>:constraint_foo_bar_is_not_null, :validate=>false}\)/m
          else
            /add_check_constraint\(:foos, "bar IS NOT NULL", {name: :constraint_foo_bar_is_not_null, validate: false}\)/m
          end

          expect do
            test_migration.migrate(:up)
          end.to output(expected_output).to_stdout
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
          alternate_connection_pool = ActiveRecord::ConnectionAdapters::ConnectionPool.new(TestHelpers.pool_config)

          # The #connection method was deprecated in Rails 7.2 in favor of #lease_connection
          alternate_connection = if alternate_connection_pool.respond_to?(:lease_connection)
            alternate_connection_pool.lease_connection
          else
            alternate_connection_pool.connection
          end

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
              waiting_locks = TestHelpers.locks_for_table(:foos, connection: alternate_connection).select { |l| !l.granted }
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

      describe "#safe_create_partitioned_table" do
        it "creates range partition on supported versions" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_partitioned_table :foos3, type: :range, partition_key: :created_at do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, partition_key: :created_at do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :list, partition_key: :created_at do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :list, partition_key: :created_at do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :hash, partition_key: :created_at do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :hash, partition_key: :created_at do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, partition_key: :created_at do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, partition_key: :created_at, primary_key: :pk do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, partition_key: :pk, primary_key: :pk do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, partition_key: ->{ "(created_at::date)" } do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, partition_key: [:created_at, :text_column] do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, infer_primary_key: false, partition_key: [:created_at, :text_column] do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, partition_key: [:created_at, :text_column] do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, id: false, partition_key: :created_at do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, partition_key: :created_at do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, partition_key: :created_at, id: :serial do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :range, partition_key: :created_at do |t|
                t.timestamps null: false
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
              safe_create_partitioned_table :foos3, type: :garbage, partition_key: :created_at do |t|
                t.timestamps null: false
                t.text :text_column
              end
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(ArgumentError, "Expected <type> to be symbol in [:range, :list, :hash] but received :garbage")
        end

        it "raises when partition key is not present" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_partitioned_table :foos3, type: :range, partition_key: nil do |t|
                t.timestamps null: false
                t.text :text_column
              end
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(ArgumentError, "Expected <partition_key> to be present")
        end
      end

      describe "#safe_partman_create_parent" do
        let(:partman_extension) { PgHaMigrations::Extension.new("pg_partman") }
        let(:partition_type) { partman_extension.major_version < 5 ? "native" : "range" }

        describe "when extension not installed" do
          it "raises error" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::MissingExtensionError, "The pg_partman extension is not installed")
          end
        end

        describe "when extension installed" do
          before do
            TestHelpers.install_partman
          end

          it "creates child partitions with defaults" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month"
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            child_tables = TestHelpers.partitions_for_table(:foos3)

            expect(child_tables.size).to eq(10)
            expect(child_tables).to include("foos3_default")

            part_config = TestHelpers.part_config("public.foos3")

            expect(part_config).to have_attributes(
              control: "created_at",
              partition_interval: "P1M",
              partition_type: partition_type,
              premake: 4,
              template_table: "public.template_public_foos3",
              infinite_time_partitions: true,
              inherit_privileges: true,
              retention: nil,
              retention_keep_table: true,
            )
          end
        end

        describe "when extension installed in different schema" do
          before do
            TestHelpers.install_partman(schema: "partman")
          end

          it "creates child partitions with defaults" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month"
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            child_tables = TestHelpers.partitions_for_table(:foos3)

            expect(child_tables.size).to eq(10)
            expect(child_tables).to include("foos3_default")

            part_config = TestHelpers.part_config("public.foos3")

            expect(part_config).to have_attributes(
              control: "created_at",
              partition_interval: "P1M",
              partition_type: partition_type,
              premake: 4,
              template_table: "partman.template_public_foos3",
              infinite_time_partitions: true,
              inherit_privileges: true,
              retention: nil,
              retention_keep_table: true,
            )
          end

          it "creates child partitions with custom options" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_template: true)

            migration = Class.new(migration_klass) do
              class_attribute :current_time, instance_accessor: true

              self.current_time = Time.current

              def up
                safe_partman_create_parent "public.foos3",
                  partition_key: :created_at,
                  interval: "1 week",
                  template_table: "public.foos3_template",
                  premake: 1,
                  start_partition: current_time,
                  infinite_time_partitions: false,
                  inherit_privileges: false,
                  retention: "60 days",
                  retention_keep_table: false
              end
            end

            if ActiveRecord::VERSION::MAJOR < 7
              expect(migration.current_time).to receive(:to_s).with(:db).and_call_original
            else
              expect(migration.current_time).to receive(:to_fs).with(:db).and_call_original
            end

            migration.suppress_messages { migration.migrate(:up) }

            child_tables = TestHelpers.partitions_for_table(:foos3)

            expect(child_tables.size).to eq(3)
            expect(child_tables).to include("foos3_default")

            # Make sure child tables inherit unique index from template table
            child_tables.each do |table|
              unique_index = ActiveRecord::Base.connection.select_value(<<~SQL)
                SELECT 1 FROM pg_indexes
                WHERE tablename = '#{table}' AND indexname = '#{table}_text_column_idx'
              SQL

              expect(unique_index).to eq(1)
            end

            part_config = TestHelpers.part_config("public.foos3")

            expect(part_config).to have_attributes(
              control: "created_at",
              partition_interval: "P7D",
              partition_type: partition_type,
              premake: 1,
              template_table: "public.foos3_template",
              infinite_time_partitions: false,
              inherit_privileges: false,
              retention: "60 days",
              retention_keep_table: false,
            )
          end

          it "uses parent table listed first in the search path when multiple present" do
            TestHelpers.create_range_partitioned_table("public.foos3", migration_klass)
            TestHelpers.create_range_partitioned_table("partman.foos3", migration_klass)

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month"
              end
            end

            old_search_path = ActiveRecord::Base.connection.schema_search_path

            begin
              ActiveRecord::Base.connection.schema_search_path = "public, partman"

              migration.suppress_messages { migration.migrate(:up) }
            ensure
              ActiveRecord::Base.connection.schema_search_path = old_search_path
            end

            expect do
              TestHelpers.part_config("public.foos3")
            end.to_not raise_error
          end

          it "raises error when keyword interval used and compatibility mode true" do
            PgHaMigrations.config.partman_5_compatibility_mode = true

            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "weekly"
              end
            end

            error_message = "Special partition interval values (weekly) are no longer supported. " \
              "Please use a supported interval time value from core PostgreSQL"

            if partman_extension.major_version < 5
              error_message << " or turn partman 5 compatibility mode off"
            end

            error_message << " (https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT)"

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::InvalidMigrationError, error_message)
          end

          it "generates consistent suffixes regardless of partman version when compatibility mode true" do
            PgHaMigrations.config.partman_5_compatibility_mode = true

            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 year"
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            after_part_config = TestHelpers.part_config("public.foos3")

            aggregate_failures do
              expect(after_part_config).to have_attributes(
                partition_interval: "P1Y",
                datetime_string: "YYYYMMDD",
                partition_type: partition_type,
                automatic_maintenance: "on",
              )

              expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
                .to all(match(/^foos3_p\d{8}$/))
            end
          end

          it "raises error when keyword interval used and partman is >= 5 and compatibility mode false" do
            skip "only valid for partman 5+" if partman_extension.major_version < 5

            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "hourly"
              end
            end

            error_message = "Special partition interval values (hourly) are no longer supported. " \
              "Please use a supported interval time value from core PostgreSQL " \
              "(https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT)"

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::InvalidMigrationError, error_message)
          end

          it "allows keyword interval when partman is version 4 and compatibility mode false" do
            skip "only valid for partman 4" unless partman_extension.major_version < 5

            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "quarterly"
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            after_part_config = TestHelpers.part_config("public.foos3")

            aggregate_failures do
              expect(after_part_config).to have_attributes(
                partition_interval: "P3M",
                datetime_string: "YYYY\"q\"Q",
                partition_type: "native",
                automatic_maintenance: "on",
              )

              expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
                .to all(match(/^foos3_p\d{4}q\d{1}$/))
            end
          end

          it "raises error when partition key not present" do
            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: nil, interval: "1 month"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected <partition_key> to be present")
          end

          it "raises error when interval not present" do
            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: nil
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Expected <interval> to be present")
          end

          it "raises error when unsupported optional arg is supplied" do
            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month", foo: "bar"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(ArgumentError, "unknown keyword: :foo")
          end

          it "raises error when on Postgres < 11" do
            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month"
              end
            end

            allow(ActiveRecord::Base.connection).to receive(:postgresql_version).and_return(10_00_00)

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::InvalidMigrationError, "Native partitioning with partman not supported on Postgres databases before version 11")
          end

          it "raises error when parent table does not exist" do
            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::UndefinedTableError, "Table \"foos3\" does not exist in search path")
          end

          it "raises error when parent table does not exist and fully qualified name provided" do
            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent "public.foos3", partition_key: :created_at, interval: "1 month"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::UndefinedTableError, "Table \"public\".\"foos3\" does not exist")
          end

          it "raises error when template table does not exist" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month", template_table: :foos3_template
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::UndefinedTableError, "Table \"foos3_template\" does not exist in search path")
          end

          it "raises error when template table does not exist and fully qualified name provided" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month", template_table: "public.foos3_template"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::UndefinedTableError, "Table \"public\".\"foos3_template\" does not exist")
          end

          it "raises error when parent table is not partitioned" do
            setup_migration = Class.new(migration_klass) do
              def up
                safe_create_table :foos3 do |t|
                  t.timestamps null: false
                  t.text :text_column
                end
              end
            end

            setup_migration.suppress_messages { setup_migration.migrate(:up) }

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month"
              end
            end

            error_message = if partman_extension.major_version < 5
              /you must have created the given parent table as ranged \(not list\) partitioned already/
            else
              /You must have created the given parent table as ranged or list partitioned already/
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(ActiveRecord::StatementInvalid, error_message)
          end

          it "raises error when non-standard table name is used" do
            setup_migration = Class.new(migration_klass) do
              def up
                safe_create_table "foos3'" do |t|
                  t.timestamps null: false
                  t.text :text_column
                end
              end
            end

            setup_migration.suppress_messages { setup_migration.migrate(:up) }

            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent "foos3'", partition_key: :created_at, interval: "1 month"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::InvalidIdentifierError, "Partman requires table names to be lowercase with underscores")
          end

          it "raises error when invalid type used for start partition" do
            migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month", start_partition: "garbage"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::InvalidMigrationError, "Expected <start_partition> to be Date, Time, or DateTime object but received String")
          end
        end
      end

      describe "#safe_partman_update_config" do
        it "raises error when retention is set" do
          migration = Class.new(migration_klass).new

          expect(migration).to_not receive(:unsafe_partman_update_config)

          expect do
            migration.safe_partman_update_config(:foos3, retention: "60 days")
          end.to raise_error(
            PgHaMigrations::UnsafeMigrationError,
            /:retention and\/or :retention_keep_table => false can potentially result in data loss if misconfigured/
          )
        end

        it "raises error when retention_keep_table is set" do
          migration = Class.new(migration_klass).new

          expect(migration).to_not receive(:unsafe_partman_update_config)

          expect do
            migration.safe_partman_update_config(:foos3, retention_keep_table: false)
          end.to raise_error(
            PgHaMigrations::UnsafeMigrationError,
            /:retention and\/or :retention_keep_table => false can potentially result in data loss if misconfigured/
          )
        end

        it "delegates to unsafe_partman_update_config when potentially dangerous args are not set" do
          migration = Class.new(migration_klass).new

          expect(migration).to receive(:unsafe_partman_update_config).with(:foos3, arg1: "foo", arg2: "bar")

          migration.safe_partman_update_config(:foos3, arg1: "foo", arg2: "bar")
        end
      end

      describe "#safe_partman_reapply_privileges" do
        describe "when extension not installed" do
          it "raises error" do
            migration = Class.new(migration_klass) do
              def up
                safe_partman_reapply_privileges :foos3
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::MissingExtensionError, "The pg_partman extension is not installed")
          end
        end

        describe "when extension installed" do
          before do
            TestHelpers.install_partman
          end

          it "applies privileges to new and existing child tables" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            setup_migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "1 month"

                unsafe_execute(<<~SQL)
                  CREATE ROLE foo NOLOGIN;
                  GRANT SELECT ON foos3 TO foo;
                SQL
              end
            end

            setup_migration.suppress_messages { setup_migration.migrate(:up) }

            child_tables = TestHelpers.partitions_for_table(:foos3)

            # post setup validation
            child_tables.each do |table|
              grantees = TestHelpers.grantees_for_table(table)

              # the role was added after the partitions were created
              # and is not automatically propagated
              expect(grantees).to contain_exactly("postgres")
            end

            # this is the main subject of the test
            migration = Class.new(migration_klass) do
              def up
                safe_partman_reapply_privileges :foos3
              end
            end

            migration.suppress_messages { migration.migrate(:up) }

            # post reapply privileges validation
            child_tables.each do |table|
              grantees = TestHelpers.grantees_for_table(table)

              # existing child tables should get privileges from parent after reapplying
              expect(grantees).to contain_exactly("postgres", "foo")
            end

            # secondary setup and validation to ensure partman respects inherit privileges
            TestHelpers.part_config("public.foos3").update!(premake: 10)

            # create additional child partitions
            ActiveRecord::Base.connection.execute("CALL public.run_maintenance_proc()")

            new_child_tables = TestHelpers.partitions_for_table(:foos3)

            expect(new_child_tables.size).to be > child_tables.size

            new_child_tables.each do |table|
              grantees = TestHelpers.grantees_for_table(table)

              # new child tables automatically get privileges from parent
              expect(grantees).to contain_exactly("postgres", "foo")
            end
          end

          it "raises error when table does not exist" do
            migration = Class.new(migration_klass) do
              def up
                safe_partman_reapply_privileges :foos3
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::UndefinedTableError, "Table \"foos3\" does not exist in search path")
          end

          it "raises error when table does not exist and fully qualified name provided" do
            migration = Class.new(migration_klass) do
              def up
                safe_partman_reapply_privileges "public.foos3"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::UndefinedTableError, "Table \"public\".\"foos3\" does not exist")
          end

          it "raises error when table exists but isn't managed by partman" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            migration = Class.new(migration_klass) do
              def up
                safe_partman_reapply_privileges :foos3
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(ActiveRecord::StatementInvalid, /Given table is not managed by this extention: public.foos3/)
          end
        end
      end
    end
  end
end
