require "pg_ha_migrations/version"
require "pg_ha_migrations/config"
require "rails"
require "active_record"
require "active_record/migration"
require "relation_to_struct"

module PgHaMigrations
  DEFAULT_CONFIG = {
    disable_ddl_transactions: true,
    disable_default_migration_methods: true,
  }

  def self.config
    @config ||= Config.new(**DEFAULT_CONFIG)
  end

  def self.configure
    yield config
  end

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
require "pg_ha_migrations/railtie"
require "pg_ha_migrations/hacks/disable_ddl_transaction"

module PgHaMigrations::AutoIncluder
  def inherited(klass)
    super(klass) if defined?(super)

    klass.prepend(PgHaMigrations::SafeStatements)
    klass.prepend(PgHaMigrations::UnsafeStatements)
  end
end

ActiveRecord::Migration.singleton_class.prepend(PgHaMigrations::AutoIncluder)
