module PgHaMigrations::SafeStatements
  def safe_create_table(table, options={}, &block)
    if options[:force]
      raise PgHaMigrations::UnsafeMigrationError.new(":force is NOT SAFE! Explicitly call unsafe_drop_table first if you want to recreate an existing table")
    end

    unsafe_create_table(table, options, &block)
  end

  def safe_create_enum_type(name, values=nil)
    case values
    when nil
      raise ArgumentError, "safe_create_enum_type expects a set of values; if you want an enum with no values please pass an empty array"
    when []
      unsafe_execute("CREATE TYPE #{PG::Connection.quote_ident(name.to_s)} AS ENUM ()")
    else
      escaped_values = values.map do |value|
        "'#{PG::Connection.escape_string(value.to_s)}'"
      end
      unsafe_execute("CREATE TYPE #{PG::Connection.quote_ident(name.to_s)} AS ENUM (#{escaped_values.join(',')})")
    end
  end

  def safe_add_enum_value(name, value)
    unsafe_execute("ALTER TYPE #{PG::Connection.quote_ident(name.to_s)} ADD VALUE '#{PG::Connection.escape_string(value)}'")
  end

  def safe_add_column(table, column, type, options = {})
    if options.has_key?(:default) && options[:default].include?("(")
      raise PgHaMigrations::UnsafeMigrationError.new(":default is NOT SAFE! Use safe_change_column_default afterwards then backfill the data to prevent locking the table")
    end

    unless ActiveRecord::Base.connection.postgresql_version >= 11_00_00
      if options.has_key? :default
        raise PgHaMigrations::UnsafeMigrationError.new(":default is NOT SAFE! Use safe_change_column_default afterwards then backfill the data to prevent locking the table")
      end
      if options[:null] == false
        raise PgHaMigrations::UnsafeMigrationError.new(":null => false is NOT SAFE if the table has data! If you _really_ want to do this, use unsafe_make_column_not_nullable")
      end
    end

    unsafe_add_column(table, column, type, options)
  end

  def unsafe_add_column(table, column, type, options = {})
    safely_acquire_lock_for_table(table) do
      super(table, column, type, options)
    end
  end

  def safe_change_column_default(table_name, column_name, default_value)
    column = connection.send(:column_for, table_name, column_name)

    if default_value.present? &&
       !default_value.is_a?(Proc) &&
       quote_default_expression(default_value, column) == "NULL"
      raise PgHaMigrations::InvalidMigrationError, "Requested new default value of <#{default_value}>, but that casts to NULL for the type <#{column.type}>. Did you mean to you mean to use a Proc instead?"
    end

    safely_acquire_lock_for_table(table_name) do
      unsafe_change_column_default(table_name, column_name, default_value)
    end
  end

  def safe_make_column_nullable(table, column)
    safely_acquire_lock_for_table(table) do
      unsafe_execute "ALTER TABLE #{table} ALTER COLUMN #{column} DROP NOT NULL"
    end
  end

  def unsafe_make_column_not_nullable(table, column, options={}) # options arg is only present for backwards compatiblity
    safely_acquire_lock_for_table(table) do
      unsafe_execute "ALTER TABLE #{table} ALTER COLUMN #{column} SET NOT NULL"
    end
  end

  def safe_add_concurrent_index(table, columns, options={})
    unsafe_add_index(table, columns, options.merge(:algorithm => :concurrently))
  end

  def safe_remove_concurrent_index(table, options={})
    unless options.is_a?(Hash) && options.key?(:name)
      raise ArgumentError, "Expected safe_remove_concurrent_index to be called with arguments (table_name, :name => ...)"
    end
    unless ActiveRecord::Base.connection.postgresql_version >= 90600
      raise PgHaMigrations::InvalidMigrationError, "Removing an index concurrently is not supported on Postgres 9.1 databases"
    end
    index_size = select_value("SELECT pg_size_pretty(pg_relation_size('#{options[:name]}'))")
    say "Preparing to drop index #{options[:name]} which is #{index_size} on disk..."
    unsafe_remove_index(table, options.merge(:algorithm => :concurrently))
  end

  def safe_set_maintenance_work_mem_gb(gigabytes)
    unsafe_execute("SET maintenance_work_mem = '#{PG::Connection.escape_string(gigabytes.to_s)} GB'")
  end

  def _per_migration_caller
    @_per_migration_caller ||= Kernel.caller
  end

  def _check_postgres_adapter!
    expected_adapter = "PostgreSQL"
    actual_adapter = ActiveRecord::Base.connection.adapter_name
    raise PgHaMigrations::UnsupportedAdapter, "This gem only works with the #{expected_adapter} adapter, found #{actual_adapter} instead" unless actual_adapter == expected_adapter
  end

  def exec_migration(conn, direction)
    _check_postgres_adapter!
    super(conn, direction)
  end

  def safely_acquire_lock_for_table(table, &block)
    _check_postgres_adapter!
    table = table.to_s
    quoted_table_name = connection.quote_table_name(table)

    successfully_acquired_lock = false

    until successfully_acquired_lock
      while (
        blocking_transactions = PgHaMigrations::BlockingDatabaseTransactions.find_blocking_transactions("#{PgHaMigrations::LOCK_TIMEOUT_SECONDS} seconds")
        blocking_transactions.any? { |query| query.tables_with_locks.include?(table) }
      )
        say "Waiting on blocking transactions:"
        blocking_transactions.each do |blocking_transaction|
          say blocking_transaction.description
        end
        sleep(PgHaMigrations::LOCK_TIMEOUT_SECONDS)
      end

      connection.transaction do
        adjust_timeout_method = connection.postgresql_version >= 90300 ? :adjust_lock_timeout : :adjust_statement_timeout
        begin
          method(adjust_timeout_method).call(PgHaMigrations::LOCK_TIMEOUT_SECONDS) do
            connection.execute("LOCK #{quoted_table_name};")
          end
          successfully_acquired_lock = true
        rescue ActiveRecord::StatementInvalid => e
          if e.message =~ /PG::LockNotAvailable.+ lock timeout/ || e.message =~ /PG::QueryCanceled.+ statement timeout/
            sleep_seconds = PgHaMigrations::LOCK_FAILURE_RETRY_DELAY_MULTLIPLIER * PgHaMigrations::LOCK_TIMEOUT_SECONDS
            say "Timed out trying to acquire an exclusive lock on the #{quoted_table_name} table."
            say "Sleeping for #{sleep_seconds}s to allow potentially queued up queries to finish before continuing."
            sleep(sleep_seconds)

            raise ActiveRecord::Rollback
          else
            raise e
          end
        end

        if successfully_acquired_lock
          block.call
        end
      end
    end
  end

  def adjust_lock_timeout(timeout_seconds = PgHaMigrations::LOCK_TIMEOUT_SECONDS, &block)
    _check_postgres_adapter!
    original_timeout = ActiveRecord::Base.value_from_sql("SHOW lock_timeout").sub(/s\Z/, '').to_i * 1000
    begin
      connection.execute("SET lock_timeout = #{PG::Connection.escape_string((timeout_seconds * 1000).to_s)};")
      block.call
    ensure
      begin
        connection.execute("SET lock_timeout = #{original_timeout};")
      rescue ActiveRecord::StatementInvalid => e
        if e.message =~ /PG::InFailedSqlTransaction/
          # If we're in a failed transaction the `SET lock_timeout` will be rolled back,
          # so we don't need to worry about cleaning up, and we can't execute SQL anyway.
        else
          raise e
        end
      end
    end
  end

  def adjust_statement_timeout(timeout_seconds, &block)
    _check_postgres_adapter!
    original_timeout = ActiveRecord::Base.value_from_sql("SHOW statement_timeout").sub(/s\Z/, '').to_i * 1000
    begin
      connection.execute("SET statement_timeout = #{PG::Connection.escape_string((timeout_seconds * 1000).to_s)};")
      block.call
    ensure
      begin
        connection.execute("SET statement_timeout = #{original_timeout};")
      rescue ActiveRecord::StatementInvalid => e
        if e.message =~ /PG::InFailedSqlTransaction/
          # If we're in a failed transaction the `SET lock_timeout` will be rolled back,
          # so we don't need to worry about cleaning up, and we can't execute SQL anyway.
        else
          raise e
        end
      end
    end
  end
end
