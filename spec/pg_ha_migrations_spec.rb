require "spec_helper"

RSpec.describe PgHaMigrations do
  PgHaMigrations::AllowedVersions::ALLOWED_VERSIONS.each do |migration_klass|
    describe "interaction with ActiveRecord::Migration::Compatibility inheritance hierarchy with #{migration_klass.name}" do
      let(:subclass) { Class.new(migration_klass) }

      it "prepends PgHaMigrations modules to the inherited class" do
        expect(subclass.ancestors[0..1]).to eq([
          PgHaMigrations::UnsafeStatements,
          PgHaMigrations::SafeStatements,
        ])
      end

      it "does not include the module more than once" do
        included_modules = subclass.ancestors.select do |ancestor|
          [
            PgHaMigrations::UnsafeStatements,
            PgHaMigrations::SafeStatements,
          ].include?(ancestor)
        end

        expect(included_modules.size).to eq(2)
      end

      describe "method lookup" do
        around(:each) do |example|
          @instance_methods_by_class = [
            PgHaMigrations::UnsafeStatements,
            PgHaMigrations::SafeStatements,
            migration_klass,
          ].each_with_object({}) do |klass, hash|
            hash[klass] = klass.instance_methods
          end

          PgHaMigrations::UnsafeStatements.class_eval do
            delegate_unsafe_method_to_migration_base_class(:pg_ha_migrations_test_method)
            def pg_ha_migrations_test_method
              raise "unexpected execution of unsafe method"
            end
          end

          begin
            example.run
          ensure
            PgHaMigrations::UnsafeStatements.send(:remove_method, :pg_ha_migrations_test_method)
            PgHaMigrations::UnsafeStatements.send(:remove_method, :unsafe_pg_ha_migrations_test_method)
          end

          # Make sure we clean up after ourselves since these tests have to
          # do some dirty poisoning of the classes and modules under test.
          @instance_methods_by_class.each do |klass, methods|
            expect(klass.instance_methods).to match_array(methods)
          end
        end

        it "executes a method from the versioned superclass" do
          # We intentionally avoid using any RSpec mocking here because
          # we need to to be absolutely certain that we've defined this
          # method _directly_ on the class we inherit from.
          begin
            migration_klass.class_eval do
              def pg_ha_migrations_test_method
                "sentinel_value"
              end
            end

            expect(subclass.new.unsafe_pg_ha_migrations_test_method).to eq("sentinel_value")
          ensure
            migration_klass.send(:remove_method, :pg_ha_migrations_test_method)
            expect(migration_klass.instance_methods).not_to include(:pg_ha_migrations_test_method)
          end
        end

        it "executes a magic method on the versioned superclass" do
          # Rails is _very_ smart and implements a lot of migration's
          # functionality via Ruby metaprogramming's method_missing.
          # This is...a major annoyance, but...
          #
          # Because our method now lives outside of the inheritance
          # hierarchy we can go back to using RSpec's method mocking.
          expect(ActiveRecord::Base.connection).to receive(:pg_ha_migrations_test_method).and_return("sentinel_value")
          expect(subclass.new.unsafe_pg_ha_migrations_test_method).to eq("sentinel_value")
        end
      end
    end
  end
end
