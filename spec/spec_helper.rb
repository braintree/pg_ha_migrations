require "bundler/setup"
require "pg_ha_migrations"
require "db-query-matchers"

puts "rspec config"
RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:all) do
    ActiveRecord::Base.connection.tables.each do |table|
      ActiveRecord::Base.connection.execute("DROP TABLE #{table} CASCADE")
    end
    ActiveRecord::Base.connection.select_values("SELECT typname FROM pg_type WHERE typtype = 'e'").each do |enum|
      ActiveRecord::Base.connection.execute("DROP TYPE #{enum} CASCADE")
    end
  end

  config.after(:each) do
    ActiveRecord::Base.connection.tables.each do |table|
      ActiveRecord::Base.connection.execute("DROP TABLE #{table} CASCADE")
    end
    ActiveRecord::Base.connection.select_values("SELECT typname FROM pg_type WHERE typtype = 'e'").each do |enum|
      ActiveRecord::Base.connection.execute("DROP TYPE #{enum} CASCADE")
    end
  end

end

ActiveRecord::Base.configurations = {
  "test" => {
    "adapter" => 'postgresql',
    "host" => 'localhost',
    "port" => 5432,
    "database" => 'pg_ha_migrations_test',
    "encoding" => 'utf8',
    "username" => 'postgres',
    "password" => 'postgres',
  },
}

puts "AR config"
config = ActiveRecord::Base.configurations["test"]

# Avoid having to require Rails when the task references `Rails.env`.
ActiveRecord::Tasks::DatabaseTasks.instance_variable_set('@env', "test")

ActiveRecord::Tasks::DatabaseTasks.drop_current
ActiveRecord::Tasks::DatabaseTasks.create_current
ActiveRecord::Base.establish_connection(config)
