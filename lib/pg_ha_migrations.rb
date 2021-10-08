require "pg_ha_migrations/version"
require "rails"
require "active_record"
require "active_record/migration"
require "relation_to_struct"

module PgHaMigrations
  Config = Struct.new(
    :disable_default_migration_methods,
    :check_for_dependent_objects,
    :allow_force_create_table
  )

  def self.config
    @config ||= Config.new(
      true,
      false,
      true
    )
  end

  def self.configure
    yield config
  end

  LOCK_TIMEOUT_SECONDS = 5
  LOCK_FAILURE_RETRY_DELAY_MULTLIPLIER = 5

  # Safe versus unsafe in this context specifically means the following:
  # - Safe operations will not block for long periods of time.
  # - Unsafe operations _may_ block for long periods of time.
  UnsafeMigrationError = Class.new(StandardError)

  # Invalid migrations are operations which we expect to not function
  # as expected or get the schema into an inconsistent state
  InvalidMigrationError = Class.new(StandardError)

  # Unsupported migrations use ActiveRecord::Migration features that
  # we don't support, and therefore will likely have unexpected behavior.
  UnsupportedMigrationError = Class.new(StandardError)

  # This gem only supports the PostgreSQL adapter at this time.
  UnsupportedAdapter = Class.new(StandardError)
end

require "pg_ha_migrations/blocking_database_transactions"
require "pg_ha_migrations/blocking_database_transactions_reporter"
require "pg_ha_migrations/unsafe_statements"
require "pg_ha_migrations/safe_statements"
require "pg_ha_migrations/dependent_objects_checks"
require "pg_ha_migrations/allowed_versions"
require "pg_ha_migrations/railtie"
require "pg_ha_migrations/hacks/disable_ddl_transaction"
require "pg_ha_migrations/hacks/cleanup_unnecessary_output"

module PgHaMigrations::AutoIncluder
  def inherited(klass)
    super(klass) if defined?(super)

    klass.prepend(PgHaMigrations::UnsafeStatements)
    klass.prepend(PgHaMigrations::SafeStatements)
  end
end

ActiveRecord::Migration.singleton_class.prepend(PgHaMigrations::AutoIncluder)
