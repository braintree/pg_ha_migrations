require "spec_helper"

RSpec.describe PgHaMigrations::UnsafeStatements do
  PgHaMigrations::AllowedVersions::ALLOWED_VERSIONS.each do |migration_klass|
    describe migration_klass do
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

      describe "when configured to disable default migration methods" do
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

      describe "when not configured to disable default migration methods" do
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

      describe "delegated raw methods" do
        it "does not raise when using raw_create_table method" do
          migration = Class.new(migration_klass) do
            def up
              raw_create_table :foos
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end

        it "does not raise when using raw_add_column method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              raw_add_column :foos, :bar, :text
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end

        it "does not raise when using raw_change_table method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              raw_change_table(:foos) { }
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end

        it "does not raise when using raw_drop_table method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              raw_drop_table :foos
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end

        it "does not raise when using raw_rename_table method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              raw_rename_table :foos, :bars
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end

        it "does not raise when using raw_rename_column method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
              raw_rename_column :foos, :bar, :baz
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end

        it "does not raise when using raw_change_column method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
              raw_change_column :foos, :bar, :string
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end

        it "does not raise when using raw_change_column_null method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
              raw_change_column_null :foos, :bar, false
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end

        it "does not raise when using raw_remove_column method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
              raw_remove_column :foos, :bar, :text
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end

        it "does not raise when using raw_add_index method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_add_column :foos, :bar, :text
              raw_add_index :foos, :bar
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end

        it "does not raise when using raw_add_foreign_key method" do
          migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos
              safe_create_table :bars
              safe_add_column :foos, :bar_id, :integer
              raw_add_foreign_key :foos, :bars, :foreign_key => :bar_id
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end

        it "does not raise when using raw_execute method" do
          migration = Class.new(migration_klass) do
            def up
              raw_execute "SELECT current_date"
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to_not raise_error
        end
      end

      describe "delegated unsafe methods" do
        it "renames add_check_constraint to unsafe_add_check_constraint" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table(:foos) { |t| t.integer :bar }
              unsafe_add_check_constraint :foos, "bar > 0", name: :constraint_foo_bar_is_positive
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN ACCESS EXCLUSIVE MODE/, count: 1)

          constraint_name, constraint_validated, constraint_expression = ActiveRecord::Base.tuple_from_sql(<<~SQL)
            SELECT conname, convalidated, pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid = 'foos'::regclass AND contype != 'p'
          SQL

          expect(constraint_name).to eq("constraint_foo_bar_is_positive")
          expect(constraint_validated).to eq(true)
          expect(constraint_expression).to eq("CHECK ((bar > 0))")
        end

        it "renames add_column to unsafe_add_column" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :foos
              unsafe_add_column :foos, :bar, :integer
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN ACCESS EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.columns("foos").map(&:name)).to include("bar")
        end

        it "renames change_column to unsafe_change_column" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table(:foos) { |t| t.string :bar }
              unsafe_change_column :foos, :bar, :text
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN ACCESS EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.columns("foos").detect { |c| c.name == "bar" }.type).to eq(:text)
        end

        it "renames change_column_default to unsafe_change_column_default" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table(:foos) { |t| t.integer :bar }
              unsafe_change_column_default :foos, :bar, 5
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN ACCESS EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.default).to eq("5")
        end

        it "renames drop_table to unsafe_drop_table" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :foos
              unsafe_drop_table :foos
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN ACCESS EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.tables).not_to include("foos")
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

        it "renames remove_check_constraint to unsafe_remove_check_constraint" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table(:foos) { |t| t.integer :bar }
              unsafe_add_check_constraint :foos, "bar > 0", name: "constraint_foo_bar_is_positive"
            end
          end

          migration.suppress_messages { migration.migrate(:up) }

          migration = Class.new(migration_klass) do
            def up
              unsafe_remove_check_constraint :foos, name: :constraint_foo_bar_is_positive
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN ACCESS EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.select_value(<<~SQL)).to eq(false)
            SELECT EXISTS (
              SELECT 1
              FROM pg_constraint
              WHERE conrelid = 'foos'::regclass AND contype != 'p'
            )
          SQL
        end

        it "renames remove_column to unsafe_remove_column" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table(:foos) { |t| t.string :bar }
              unsafe_remove_column :foos, :bar
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN ACCESS EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.columns("foos").map(&:name)).not_to include("bar")
        end

        it "renames rename_column to unsafe_rename_column" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table(:foos) { |t| t.string :bar }
              unsafe_rename_column :foos, :bar, :baz
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN ACCESS EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.columns("foos").map(&:name)).not_to include("bar")
          expect(ActiveRecord::Base.connection.columns("foos").map(&:name)).to include("baz")
        end

        it "renames rename_table to unsafe_rename_table" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :foos
              unsafe_rename_table :foos, :bars
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN ACCESS EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.tables).not_to include("foos")
          expect(ActiveRecord::Base.connection.tables).to include("bars")
        end
      end

      describe "#unsafe_create_table" do
        it "delegates to underlying rails method when :force not provided" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :items do |t|
                t.integer :original_column
              end
            end
          end

          expect(PgHaMigrations.config).to_not receive(:allow_force_create_table)

          migration.suppress_messages { migration.migrate(:up) }

          expect(ActiveRecord::Base.connection.tables).to include("items")
          expect(ActiveRecord::Base.connection.columns("items").map(&:name)).to contain_exactly("id", "original_column")
        end

        it "creates table with :force => true and config.allow_force_create_table = true" do
          allow(PgHaMigrations.config)
            .to receive(:allow_force_create_table)
            .and_return(true)

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
          end.to_not raise_error

          expect(ActiveRecord::Base.connection.tables).to include("items")
          expect(ActiveRecord::Base.connection.columns("items").map(&:name)).to contain_exactly("id", "new_column")
        end

        it "raises error with :force => true and config.allow_force_create_table = false" do
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

          expect(ActiveRecord::Base.connection.tables).to include("items")
          expect(ActiveRecord::Base.connection.columns("items").map(&:name)).to contain_exactly("id", "original_column")
        end
      end

      describe "#unsafe_change_table" do
        it "raises unsafe migration error" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_change_table :foos
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(
            PgHaMigrations::UnsafeMigrationError,
            ":change_table is too generic to even allow an unsafe variant. Use a combination of safe and explicit unsafe migration methods instead"
          )
        end
      end

      describe "#unsafe_add_index" do
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

        it "safely acquires SHARE lock when not creating indexes concurrently" do
          setup_migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :foos do |t|
                t.integer :bar
              end
            end
          end
          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              unsafe_add_index :foos, :bar
            end
          end

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN SHARE MODE/, count: 1)
            .and(make_database_queries(matching: /CREATE INDEX "index_foos_on_bar"/, count: 1))
        end

        it "skips lock acquisition when creating indexes concurrently" do
          setup_migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :foos do |t|
                t.integer :bar
              end
            end
          end
          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          test_migration = Class.new(migration_klass) do
            def up
              unsafe_add_index :foos, :bar, algorithm: :concurrently
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original
          expect(ActiveRecord::Base.connection).to_not receive(:execute).with(/LOCK/)

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to make_database_queries(matching: /CREATE INDEX CONCURRENTLY "index_foos_on_bar"/, count: 1)
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
              unsafe_add_index "x" * 51, [:bar]
            end
          end

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to make_database_queries(matching: /CREATE INDEX "idx_on_bar_d7a594ad66"/, count: 1)

          indexes = ActiveRecord::Base.connection.indexes("x" * 51)
          expect(indexes.size).to eq(1)
          expect(indexes.first).to have_attributes(
            table: "x" * 51,
            name: "idx_on_bar_d7a594ad66",
            columns: ["bar"],
          )
        end

        it "raises error when table does not exist" do
          test_migration = Class.new(migration_klass) do
            def up
              unsafe_add_index :foo, :bar
            end
          end

          expect do
            test_migration.suppress_messages { test_migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::UndefinedTableError, "Table \"foo\" does not exist in search path")
        end
      end

      describe "#unsafe_remove_index" do
        it "safely acquires lock when not removing indexes concurrently" do
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

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN ACCESS EXCLUSIVE MODE/, count: 1)
            .and(make_database_queries(matching: /DROP INDEX\s+"index_foos_on_bar"/, count: 1))

          expect(ActiveRecord::Base.connection.indexes("foos").map(&:columns)).not_to include(["bar"])
        end

        it "skips lock acquisition when removing indexes concurrently" do
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
              unsafe_remove_index :foos, :bar, algorithm: :concurrently
            end
          end

          allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original
          expect(ActiveRecord::Base.connection).to_not receive(:execute).with(/LOCK/)

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /DROP INDEX CONCURRENTLY\s+"index_foos_on_bar"/, count: 1)

          expect(ActiveRecord::Base.connection.indexes("foos").map(&:columns)).not_to include(["bar"])
        end
      end

      describe "#unsafe_add_foreign_key" do
        it "takes out locks on source / target tables and delegates" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table(:foo) { |t| t.bigint :bar_id, null: false }
              unsafe_create_table(:bar)
            end
          end

          migration.suppress_messages { migration.migrate(:up) }

          migration = Class.new(migration_klass) do
            def up
              unsafe_add_foreign_key :foo, :bar
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foo", "public"\."bar" IN SHARE ROW EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.foreign_keys(:foo)).to contain_exactly(
            having_attributes(
              from_table: :foo,
              to_table: "bar",
              options: hash_including(
                column: "bar_id",
                primary_key: "id",
              ),
            )
          )
        end
      end

      describe "#unsafe_remove_foreign_key" do
        it "takes out locks on source / target tables and delegates" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table(:foo) { |t| t.bigint :bar_id, null: false }
              unsafe_create_table(:bar)
              unsafe_add_foreign_key :foo, :bar
            end
          end

          migration.suppress_messages { migration.migrate(:up) }

          expect(ActiveRecord::Base.connection.foreign_keys(:foo)).to contain_exactly(
            having_attributes(
              from_table: :foo,
              to_table: "bar",
              options: hash_including(
                column: "bar_id",
                primary_key: "id",
              ),
            )
          )

          migration = Class.new(migration_klass) do
            def up
              unsafe_remove_foreign_key :foo, :bar
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foo", "public"\."bar" IN ACCESS EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.foreign_keys(:foo)).to be_empty
        end

        it "takes out locks on source / target tables and delegates when :to_table provided as kwarg" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table(:foo) { |t| t.bigint :bar_id, null: false }
              unsafe_create_table(:bar)
              unsafe_add_foreign_key :foo, :bar
            end
          end

          migration.suppress_messages { migration.migrate(:up) }

          expect(ActiveRecord::Base.connection.foreign_keys(:foo)).to contain_exactly(
            having_attributes(
              from_table: :foo,
              to_table: "bar",
              options: hash_including(
                column: "bar_id",
                primary_key: "id",
              ),
            )
          )

          migration = Class.new(migration_klass) do
            def up
              unsafe_remove_foreign_key :foo, to_table: :bar
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foo", "public"\."bar" IN ACCESS EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.foreign_keys(:foo)).to be_empty
        end

        it "raises error if :to_table not provided" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_remove_foreign_key :foo, column: :bar_id
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to raise_error(PgHaMigrations::InvalidMigrationError, "The :to_table positional arg / kwarg is required for lock acquisition")
        end
      end

      describe "#unsafe_rename_enum_value" do
        it "renames a enum value on 10+" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_execute("CREATE TYPE bt_foo_enum AS ENUM ('one', 'two', 'three')")
              unsafe_rename_enum_value :bt_foo_enum, "three", "updated"
            end
          end

          migration.suppress_messages { migration.migrate(:up) }

          expect(TestHelpers.enum_names_and_values).to contain_exactly(
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

      describe "#unsafe_make_column_not_nullable" do
        it "make the column not nullable which will cause the table to be locked" do
          migration = Class.new(migration_klass) do
            def up
              unsafe_create_table :foos
              safe_add_column :foos, :bar, :text
            end
          end

          migration.suppress_messages { migration.migrate(:up) }

          migration = Class.new(migration_klass) do
            def up
              unsafe_make_column_not_nullable :foos, :bar, :estimated_rows => 0
            end
          end

          expect do
            migration.suppress_messages { migration.migrate(:up) }
          end.to make_database_queries(matching: /LOCK "public"\."foos" IN ACCESS EXCLUSIVE MODE/, count: 1)

          expect(ActiveRecord::Base.connection.columns("foos").detect { |column| column.name == "bar" }.null).to eq(false)
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

      describe "#unsafe_partman_update_config" do
        describe "when extension not installed" do
          it "raises error" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_partman_update_config :foos3, inherit_privileges: true
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::InvalidMigrationError, "The pg_partman extension is not installed")
          end
        end

        describe "when extension installed" do
          before do
            ActiveRecord::Base.connection.execute("CREATE EXTENSION pg_partman")

            PgHaMigrations::PartmanConfig.schema = "public"
          end

          it "updates values and reapplies privileges when inherit_privileges changes from true to false" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            setup_migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "monthly"
              end
            end

            setup_migration.suppress_messages { setup_migration.migrate(:up) }

            migration = Class.new(migration_klass) do
              def up
                unsafe_partman_update_config :foos3,
                  inherit_privileges: false,
                  infinite_time_partitions: false,
                  retention: "60 days",
                  retention_keep_table: false,
                  premake: 1
              end
            end

            allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/reapply_privileges/).once

            migration.suppress_messages { migration.migrate(:up) }

            part_config = PgHaMigrations::PartmanConfig.find("public.foos3")

            expect(part_config).to have_attributes(
              inherit_privileges: false,
              infinite_time_partitions: false,
              retention: "60 days",
              retention_keep_table: false,
              premake: 1,
            )
          end

          it "updates values and reapplies privileges when inherit_privileges changes from false to true" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            setup_migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3,
                  partition_key: :created_at,
                  interval: "monthly",
                  inherit_privileges: false,
                  infinite_time_partitions: false
              end
            end

            setup_migration.suppress_messages { setup_migration.migrate(:up) }

            migration = Class.new(migration_klass) do
              def up
                unsafe_partman_update_config :foos3, inherit_privileges: true, infinite_time_partitions: true
              end
            end

            allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original
            expect(ActiveRecord::Base.connection).to receive(:execute).with(/reapply_privileges/).once

            migration.suppress_messages { migration.migrate(:up) }

            part_config = PgHaMigrations::PartmanConfig.find("public.foos3")

            expect(part_config).to have_attributes(
              inherit_privileges: true,
              infinite_time_partitions: true,
            )
          end

          it "updates values and does not reapply privileges when inherit_privileges does not change" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            setup_migration = Class.new(migration_klass) do
              def up
                safe_partman_create_parent :foos3, partition_key: :created_at, interval: "monthly"
              end
            end

            setup_migration.suppress_messages { setup_migration.migrate(:up) }

            migration = Class.new(migration_klass) do
              def up
                unsafe_partman_update_config :foos3, infinite_time_partitions: false
              end
            end

            allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original
            expect(ActiveRecord::Base.connection).to_not receive(:execute).with(/reapply_privileges/)

            migration.suppress_messages { migration.migrate(:up) }

            part_config = PgHaMigrations::PartmanConfig.find("public.foos3")

            expect(part_config).to have_attributes(
              inherit_privileges: true,
              infinite_time_partitions: false,
            )
          end

          it "raises error when table does not exist" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_partman_update_config :foos3, inherit_privileges: true
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(PgHaMigrations::UndefinedTableError, "Table \"foos3\" does not exist in search path")
          end

          it "raises error when table exists but isn't configured with partman" do
            TestHelpers.create_range_partitioned_table(:foos3, migration_klass)

            migration = Class.new(migration_klass) do
              def up
                unsafe_partman_update_config :foos3, inherit_privileges: true
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(ActiveRecord::RecordNotFound, "Couldn't find PgHaMigrations::PartmanConfig with 'parent_table'=public.foos3")
          end

          it "raises error when unsupported arg is supplied" do
            migration = Class.new(migration_klass) do
              def up
                unsafe_partman_update_config :foos3, foo: "bar"
              end
            end

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(ArgumentError, "Unrecognized argument(s): [:foo]")
          end
        end
      end
    end
  end
end
