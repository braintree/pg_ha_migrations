require "spec_helper"

RSpec.describe PgHaMigrations do
  describe "prepends itself to the compatibility classes" do
    {
      4.2 =>  [
        PgHaMigrations::UnsafeStatements,
        PgHaMigrations::SafeStatements,
        ActiveRecord::Migration::Compatibility::V4_2,
        PgHaMigrations::UnsafeStatements,
        PgHaMigrations::SafeStatements,
        ActiveRecord::Migration::Compatibility::V5_0,
        PgHaMigrations::UnsafeStatements,
        PgHaMigrations::SafeStatements,
        ActiveRecord::Migration::Compatibility::V5_1,
        PgHaMigrations::UnsafeStatements,
        PgHaMigrations::SafeStatements,
        ActiveRecord::Migration::Current,
        ActiveRecord::Migration,
      ],
      5.0 => [
        PgHaMigrations::UnsafeStatements,
        PgHaMigrations::SafeStatements,
        ActiveRecord::Migration::Compatibility::V5_0,
        PgHaMigrations::UnsafeStatements,
        PgHaMigrations::SafeStatements,
        ActiveRecord::Migration::Compatibility::V5_1,
        PgHaMigrations::UnsafeStatements,
        PgHaMigrations::SafeStatements,
        ActiveRecord::Migration::Current,
        ActiveRecord::Migration,
      ],
      5.1 => [
        PgHaMigrations::UnsafeStatements,
        PgHaMigrations::SafeStatements,
        ActiveRecord::Migration::Compatibility::V5_1,
        PgHaMigrations::UnsafeStatements,
        PgHaMigrations::SafeStatements,
        ActiveRecord::Migration::Current,
        ActiveRecord::Migration,
      ],
      5.2 => [
        PgHaMigrations::UnsafeStatements,
        PgHaMigrations::SafeStatements,
        ActiveRecord::Migration::Current,
        ActiveRecord::Migration,
      ]
    }.each do |version, inheritance_chain|
      it "has the correct inheritance chain in #{version}" do
        foo = Class.new(ActiveRecord::Migration[version])
        expect(foo.ancestors[1..-1]).to start_with(inheritance_chain)
      end
    end
  end
end
