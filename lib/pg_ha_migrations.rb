require "pg_ha_migrations/version"
require "rails"
require "active_record"
require "active_record/migration"
require "relation_to_struct"

module PgHaMigrations
  ActiveRecord::Migration.disable_ddl_transaction = true
  LOCK_TIMEOUT_SECONDS = 5
  LOCK_FAILURE_RETRY_DELAY_MULTLIPLIER = 5

  # Safe versus unsafe in this context specifically means the following:
  # - Safe operations will not block for long periods of time.
  # - Unsafe operations _may_ block for long periods of time.
  UnsafeMigrationError = Class.new(Exception)

  # Invalid migrations are operations which we expect to not function
  # as expected or get the schema into an inconsistent state
  InvalidMigrationError = Class.new(Exception)

  # This gem only supports the PostgreSQL adapter at this time.
  UnsupportedAdapter = Class.new(Exception)
end

require "pg_ha_migrations/blocking_database_transactions"
require "pg_ha_migrations/blocking_database_transactions_reporter"
require "pg_ha_migrations/unsafe_statements"
require "pg_ha_migrations/safe_statements"
require "pg_ha_migrations/allowed_versions"
require "pg_ha_migrations/migration_modifier"
require "pg_ha_migrations/railtie"

PgHaMigrations::AllowedVersions::ALLOWED_VERSIONS.each do |migrations_class|
  migrations_class.prepend(PgHaMigrations::SafeStatements)
  migrations_class.prepend(PgHaMigrations::UnsafeStatements)
end
