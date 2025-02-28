require "bundler/setup"
require "logger"
require "pg_ha_migrations"
require "db-query-matchers"
require "pry"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:all) do
    server_version = ActiveRecord::Base.connection.select_value("SHOW server_version")
    puts "DEBUG: Connecting to Postgres server version #{server_version}"

    DatabaseHelper.drop_objects
  end

  config.after(:each) do
    DatabaseHelper.drop_objects
  end
end

ActiveRecord::Base.configurations = {
  "test" => {
    "adapter" => 'postgresql',
    "host" => ENV["PGHOST"] || 'localhost',
    "port" => ENV["PGPORT"] || 5432,
    "database" => 'pg_ha_migrations_test',
    "encoding" => 'utf8',
    "username" => ENV["PGUSER"] || 'postgres',
    "password" => ENV["PGPASSWORD"] || 'postgres',
  },
}

config =
  if ActiveRecord::VERSION::MAJOR < 7
    ActiveRecord::Base.configurations["test"]
  else
    ActiveRecord::Base.configurations.configs_for(env_name: "test").first
  end

# Avoid having to require Rails when the task references `Rails.env`.
ActiveRecord::Tasks::DatabaseTasks.instance_variable_set('@env', "test")

ActiveRecord::Tasks::DatabaseTasks.drop_current
ActiveRecord::Tasks::DatabaseTasks.create_current
ActiveRecord::Base.establish_connection(config)

module DatabaseHelper
  def self.drop_objects
    ActiveRecord::Base.connection.select_values("SELECT rolname FROM pg_roles WHERE rolname NOT SIMILAR TO '(postgres|pg_%)'").each do |role|
      ActiveRecord::Base.connection.execute(<<~SQL)
        REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM #{role};
        DROP ROLE #{role};
      SQL
    end

    ActiveRecord::Base.connection.execute(<<~SQL)
      DROP EXTENSION IF EXISTS pg_partman CASCADE;
      DROP SCHEMA IF EXISTS partman CASCADE;
    SQL

    ActiveRecord::Base.connection.tables.each do |table|
      ActiveRecord::Base.connection.execute("DROP TABLE IF EXISTS #{ActiveRecord::Base.connection.quote_table_name(table)} CASCADE")
    end

    ActiveRecord::Base.connection.select_values("SELECT typname FROM pg_type WHERE typtype = 'e'").each do |enum|
      ActiveRecord::Base.connection.execute("DROP TYPE #{enum} CASCADE")
    end
  end
end
