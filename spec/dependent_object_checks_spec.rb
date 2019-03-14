require "spec_helper"

RSpec.describe PgHaMigrations::UnsafeStatements do
  PgHaMigrations::AllowedVersions::ALLOWED_VERSIONS.each do |migration_klass|
    describe migration_klass do
      describe "#dependent_objects_for_migration_method!" do
        it "raises if allow_dependent_objects contains unknown options" do
          # TODO
        end
      end

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
      end

      describe "#unsafe_remove_column" do
        before(:each) do
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
              unsafe_remove_column :foos3, :text_column
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
              unsafe_remove_column :foos3, :text_column, :allow_dependent_objects => [:indexes]
            end
          end

          migration.suppress_messages { migration.migrate(:up) }

          expect(ActiveRecord::Base.connection.columns("foos3").map(&:name)).not_to include("text_column")
        end

        it "ignores dependent indexes when dependent object checks are disabled" do
          # TODO: We need to make this new behavior opt-in
          # at the application level for 1.x since we'll
          # almost certainly existing broken use cases.
        end
      end
    end
  end
end
