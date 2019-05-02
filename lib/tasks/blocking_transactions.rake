require_relative File.join("..", "pg_ha_migrations")

namespace :pg_ha_migrations do
  desc "Check if blocking database transactions exist"
  task :check_blocking_database_transactions do
    PgHaMigrations::BlockingDatabaseTransactionsReporter.run
  end
end

