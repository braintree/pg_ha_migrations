require_relative File.join("..", "pg_ha_migrations")

namespace :pg_ha_migrations do
  desc "Change migration files to be safe"
  task :modify_migrations do
    PgHaMigrations::MigrationModifier.new.modify!
  end
end
