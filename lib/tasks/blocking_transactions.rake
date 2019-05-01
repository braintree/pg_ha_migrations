require_relative File.join("..", "pg_ha_migrations")

namespace :pg_ha_migrations do
  desc "Check if blocking database transactions exist"
  task :check_blocking_database_transactions do
    PgHaMigrations::BlockingDatabaseTransactionsReporter.run
  end

  desc "runs out of band migrations"
  task :migrate_out_of_band => :load_db_environment do
    PgHaMigrations::OutOfBandMigrator.run
  end

  task :migrate_oob => :migrate_out_of_band
end

