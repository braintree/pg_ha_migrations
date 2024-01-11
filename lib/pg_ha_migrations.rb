require "pg_ha_migrations/version"
require "rails"
require "active_record"
require "active_record/migration"
require "active_record/connection_adapters/postgresql/utils"
require "active_support/core_ext/numeric/bytes"
require "relation_to_struct"
require "ruby2_keywords"

module PgHaMigrations
  Config = Struct.new(
    :disable_default_migration_methods,
    :check_for_dependent_objects,
    :allow_force_create_table,
    :prefer_single_step_column_addition_with_default,
    :infer_primary_key_on_partitioned_tables,
  )

  def self.config
    @config ||= Config.new(
      true,
      false,
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
  SMALL_TABLE_THRESHOLD_BYTES = 10.megabytes

  PARTITION_TYPES = %i[range list hash]

  PARTMAN_UPDATE_CONFIG_OPTIONS = %i[
    infinite_time_partitions
    inherit_privileges
    premake
    retention
    retention_keep_table
  ]

  # Safe versus unsafe in this context specifically means the following:
  # - Safe operations will not block for long periods of time.
  # - Unsafe operations _may_ block for long periods of time.
  UnsafeMigrationError = Class.new(StandardError)

  # Invalid migrations are operations which we expect to not function
  # as expected or get the schema into an inconsistent state
  InvalidMigrationError = Class.new(StandardError)

  # Operations violating a best practice, but not actually unsafe will
  # raise this error. For example, adding a column without a default and
  # then setting its default in a second action in a single migration
  # isn't our documented best practice and will raise this error.
  BestPracticeError = Class.new(StandardError)

  # Unsupported migrations use ActiveRecord::Migration features that
  # we don't support, and therefore will likely have unexpected behavior.
  UnsupportedMigrationError = Class.new(StandardError)

  # This gem only supports the PostgreSQL adapter at this time.
  UnsupportedAdapter = Class.new(StandardError)

  # Some methods need to inspect the attributes of a table. In such cases,
  # this error will be raised if the table does not exist
  UndefinedTableError = Class.new(StandardError)
end

require "pg_ha_migrations/relation"
require "pg_ha_migrations/blocking_database_transactions"
require "pg_ha_migrations/blocking_database_transactions_reporter"
require "pg_ha_migrations/partman_config"
require "pg_ha_migrations/lock_mode"
require "pg_ha_migrations/unsafe_statements"
require "pg_ha_migrations/safe_statements"
require "pg_ha_migrations/dependent_objects_checks"
require "pg_ha_migrations/allowed_versions"
require "pg_ha_migrations/railtie"
require "pg_ha_migrations/hacks/disable_ddl_transaction"
require "pg_ha_migrations/hacks/cleanup_unnecessary_output"
require "pg_ha_migrations/hacks/add_index_on_only"

module PgHaMigrations::AutoIncluder
  def inherited(klass)
    super(klass) if defined?(super)

    klass.prepend(PgHaMigrations::UnsafeStatements)
    klass.prepend(PgHaMigrations::SafeStatements)
  end
end

ActiveRecord::Migration.singleton_class.prepend(PgHaMigrations::AutoIncluder)
