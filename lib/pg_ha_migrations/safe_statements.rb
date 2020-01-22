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
    if options.has_key? :default
      raise PgHaMigrations::UnsafeMigrationError.new(":default is NOT SAFE! Use safe_change_column_default afterwards then backfill the data to prevent locking the table")
    end
    if options[:null] == false
      raise PgHaMigrations::UnsafeMigrationError.new(":null => false is NOT SAFE if the table has data! If you _really_ want to do this, use unsafe_make_column_not_nullable")
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

    # In 5.2 we have an edge whereby passing in a string literal with an expression
    # results in confusing behavior because instead of being executed in the database
    # that expression is turned into a Ruby nil before being sent to the database layer;
    # this seems to be an expected side effect of a change that was targeted at a use
    # case unrelated to migrations: https://github.com/rails/rails/commit/7b2dfdeab6e4ef096e4dc1fe313056f08ccf7dc5
    #
    # On the other hand, the behavior in 5.1 is also confusing because it quotes the
    # expression (instead of maintaining the string as-is), which results in Postgres
    # evaluating the expression once when executing the DDL and setting the default to
    # the constant result of that evaluation rather than setting the default to the
    # expression itself.
    #
    # Therefore we want to disallow passing in an expression directly as a string and
    # require the use of a Proc instead with specific quoting rules to determine exact
    # behavior. It's fairly difficult (without relying on something like the PgQuery gem
    # which requires native extensions built with the Postgres dev packages installed)
    # to determine if a string literal represent an expression or just a constant. So
    # instead of trying to parse the expression, we employ a set of heuristics:
    # - If the column is text-like or binary, then we can allow anything in the default
    #   value since a Ruby string there will always coerce directly to the equivalent
    #   text/binary value rather than being interpreted as a DDL-time expression.
    # - Otherwise, disallow any Ruby string values and instead require the Ruby object
    #   type that maps to the column type.
    #
    # These heuristics eliminate (virtually?) all ambiguity. In theory there's a
    # possiblity that some custom object could be coerced-Ruby side into a SQL string
    # that does something weird here, but that seems an odd enough case that we can
    # safely ignore it.
    if default_value.present? &&
       !default_value.is_a?(Proc) &&
       (
         connection.quote_default_expression(default_value, column) == "NULL" ||
         (
           ![:string, :text, :binary].include?(column.sql_type_metadata.type) &&
           default_value.is_a?(String)
         )
       )
      raise PgHaMigrations::InvalidMigrationError, <<~ERROR
        Setting a default value to an expression using a string literal is ambiguous.

        If you want the default to be:
        * ...a constant scalar value, use the matching Ruby object type instead of a string if possible (e.g., `DateTime.new(...)`).
        * ...an expression evaluated at runtime for each row, then pass a Proc that returns the expression string (e.g., `-> { "NOW()" }`).
        * ...an expression evaluated at migration time, then pass a Proc that returns a quoted expression string (e.g., `-> { "'NOW()'" }`).
      ERROR
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
    unless ActiveRecord::Base.connection.postgresql_version >= 9_06_00
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

  def migrate(direction)
    if respond_to?(:change)
      raise PgHaMigrations::UnsupportedMigrationError, "Tracking changes for automated rollback is not supported; use explicit #up instead."
    end

    super(direction)
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
        adjust_timeout_method = connection.postgresql_version >= 9_03_00 ? :adjust_lock_timeout : :adjust_statement_timeout
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
