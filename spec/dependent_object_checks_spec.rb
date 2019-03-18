require "spec_helper"

RSpec.describe PgHaMigrations::UnsafeStatements do
  PgHaMigrations::AllowedVersions::ALLOWED_VERSIONS.each do |migration_klass|
    describe migration_klass do
      describe "#dependent_objects_for_migration_method" do
        it "doesn't return spurious matches for indexes" do
          setup_migration = Class.new(migration_klass) do
            def up
              safe_create_table :foos3 do |t|
                t.timestamps :null => false
                t.text :text_column
              end
              safe_add_concurrent_index :foos3, :text_column
            end
          end

          setup_migration.suppress_messages { setup_migration.migrate(:up) }

          dependent_objects = setup_migration.dependent_objects_for_migration_method(
            :remove_column,
            arguments: [:foos3, :text_column]
          )

          expect(dependent_objects).to eq([
            PgHaMigrations::DependentObjectsChecks::ObjectDependency.new(
              "column",
              "text_column",
              "index",
              "index_foos3_on_text_column"
            )
          ])
        end

        it "raises if allow_dependent_objects contains unknown options" do
          migration = Class.new(migration_klass).new

          expect do
            migration.dependent_objects_for_migration_method(
              :remove_column,
              arguments: [:foos3, :text_column, :allow_dependent_objects => [:indices]]
            )
          end.to raise_error(ArgumentError, /invalid entries in allow_dependent_objects/)
        end
      end

      [true, false].each do |disable_default_migration_methods|
        describe "disable_default_migration_methods = #{disable_default_migration_methods}" do
          method_prefix = disable_default_migration_methods ? "unsafe_" : ""

          before(:each) do
            allow(PgHaMigrations.config).to receive(:disable_default_migration_methods)
              .and_return(disable_default_migration_methods)

            # The default behavior until 2.0 is to disable this feature, but it's a lot easier
            # to write most of these tests if we default to enabled here instead.
            allow(PgHaMigrations.config).to receive(:check_for_dependent_objects).and_return(true)
          end

          describe "##{method_prefix}remove_column" do
            let(:remove_column_method_name) { "#{method_prefix}remove_column".to_sym }

            before(:each) do
              allow_any_instance_of(migration_klass).to receive(:remove_column_method_name)
                .and_return(remove_column_method_name)

              setup_migration = Class.new(migration_klass) do
                def up
                  safe_create_table :foos3 do |t|
                    t.timestamps :null => false
                    t.text :text_column
                  end
                  safe_add_concurrent_index :foos3, :text_column
                end
              end

              setup_migration.suppress_messages { setup_migration.migrate(:up) }
            end

            it "raises when dependent indexes exist" do
              migration = Class.new(migration_klass) do
                def up
                  public_send(remove_column_method_name, :foos3, :text_column)
                end
              end

              expect do
                migration.suppress_messages { migration.migrate(:up) }
              end.to raise_error(PgHaMigrations::UnsafeMigrationError, /index 'index_foos3_on_text_column' depends on column 'text_column'/)

              # We can't drop the index _before_ raising the error :D
              indexes = ActiveRecord::Base.connection.indexes("foos3")
              expect(indexes.size).to eq(1)
              expect(indexes).to all(have_attributes(:table => "foos3", :name => "index_foos3_on_text_column", :columns => ["text_column"]))
            end

            it "ignores dependent indexes when explicitly allowed" do
              migration = Class.new(migration_klass) do
                def up
                  public_send(remove_column_method_name, :foos3, :text_column, :allow_dependent_objects => [:indexes])
                end
              end

              migration.suppress_messages { migration.migrate(:up) }

              expect(ActiveRecord::Base.connection.columns("foos3").map(&:name)).not_to include("text_column")
            end

            it "ignores dependent indexes when dependent object checks are disabled" do
              allow(PgHaMigrations.config).to receive(:check_for_dependent_objects).and_return(false)

              migration = Class.new(migration_klass) do
                def up
                  public_send(remove_column_method_name, :foos3, :text_column)
                end
              end

              migration.suppress_messages { migration.migrate(:up) }

              expect(ActiveRecord::Base.connection.columns("foos3").map(&:name)).not_to include("text_column")
            end
          end
        end
      end
    end
  end
end
