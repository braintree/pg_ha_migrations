module PgHaMigrations::SafeStatements
  PARTITION_TYPES = %i[range list hash]

  PARTMAN_CREATE_PARENT_OPTIONS = %i[
    premake
    automatic_maintenance
    start_partition
    epoch
    template_table
    jobmon
  ]

  PARTMAN_UPDATE_CONFIG_OPTIONS = %i[
    infinite_time_partitions
    inherit_privileges
  ]

  def safe_added_columns_without_default_value
    @safe_added_columns_without_default_value ||= []
  end

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

  def unsafe_rename_enum_value(name, old_value, new_value)
    if ActiveRecord::Base.connection.postgresql_version < 10_00_00
      raise PgHaMigrations::InvalidMigrationError, "Renaming an enum value is not supported on Postgres databases before version 10"
    end

    unsafe_execute("ALTER TYPE #{PG::Connection.quote_ident(name.to_s)} RENAME VALUE '#{PG::Connection.escape_string(old_value)}' TO '#{PG::Connection.escape_string(new_value)}'")
  end

  def safe_add_column(table, column, type, options = {})
    # Note: we don't believe we need to consider the odd case where
    # `:default => nil` or `:default => -> { null }` (or similar) is
    # passed because:
    # - It's OK to exclude that case with an "unnecessary" `raise`
    #   below as it doesn't make semantic sense anyway.
    # - If `:null => false` is also passed we are assuming Postgres's
    #   seq scan of the table (to verify the NOT NULL constraint) will
    #   short-circuit (though we have not confirmed that).
    if options.has_key?(:default)
      if ActiveRecord::Base.connection.postgresql_version < 11_00_00
        raise PgHaMigrations::UnsafeMigrationError.new(":default is NOT SAFE! Use safe_change_column_default afterwards then backfill the data to prevent locking the table")
      elsif options[:default].is_a?(Proc) || (options[:default].is_a?(String) && !([:string, :text, :binary].include?(type.to_sym) || _type_is_enum(type)))
        raise PgHaMigrations::UnsafeMigrationError.new(":default is not safe if the default value is volatile. Use safe_change_column_default afterwards then backfill the data to prevent locking the table")
      end
    elsif options[:null] == false
      raise PgHaMigrations::UnsafeMigrationError.new(":null => false is NOT SAFE if the table has data! If you _really_ want to do this, use unsafe_make_column_not_nullable")
    end

    unless options.has_key?(:default)
      self.safe_added_columns_without_default_value << [table.to_s, column.to_s]
    end

    unsafe_add_column(table, column, type, options)
  end

  def unsafe_add_column(table, column, type, options = {})
    safely_acquire_lock_for_table(table) do
      super(table, column, type, **options)
    end
  end

  def safe_change_column_default(table_name, column_name, default_value)
    if PgHaMigrations.config.prefer_single_step_column_addition_with_default &&
        ActiveRecord::Base.connection.postgresql_version >= 11_00_00 &&
        self.safe_added_columns_without_default_value.include?([table_name.to_s, column_name.to_s])
      raise PgHaMigrations::BestPracticeError, "On Postgres 11+ it's safe to set a constant default value when adding a new column; please set the default value as part of the column addition"
    end

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
    # - Custom enum types are a special case: they also are treated like strings by
    #   Rails, so we want to allow those as-is.
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
           ![:string, :text, :binary, :enum].include?(column.sql_type_metadata.type) &&
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
    unsafe_add_index(table, columns, **options.merge(:algorithm => :concurrently))
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
    unsafe_remove_index(table, **options.merge(:algorithm => :concurrently))
  end

  def safe_set_maintenance_work_mem_gb(gigabytes)
    unsafe_execute("SET maintenance_work_mem = '#{PG::Connection.escape_string(gigabytes.to_s)} GB'")
  end

  def safe_add_unvalidated_check_constraint(table, expression, name:)
    unsafe_add_check_constraint(table, expression, name: name, validate: false)
  end

  def unsafe_add_check_constraint(table, expression, name:, validate: true)
    raise ArgumentError, "Expected <name> to be present" unless name.present?

    quoted_table_name = connection.quote_table_name(table)
    quoted_constraint_name = connection.quote_table_name(name)
    sql = "ALTER TABLE #{quoted_table_name} ADD CONSTRAINT #{quoted_constraint_name} CHECK (#{expression}) #{validate ? "" : "NOT VALID"}"

    safely_acquire_lock_for_table(table) do
      say_with_time "add_check_constraint(#{table.inspect}, #{expression.inspect}, name: #{name.inspect}, validate: #{validate.inspect})" do
        connection.execute(sql)
      end
    end
  end

  def safe_validate_check_constraint(table, name:)
    raise ArgumentError, "Expected <name> to be present" unless name.present?

    quoted_table_name = connection.quote_table_name(table)
    quoted_constraint_name = connection.quote_table_name(name)
    sql = "ALTER TABLE #{quoted_table_name} VALIDATE CONSTRAINT #{quoted_constraint_name}"

    say_with_time "validate_check_constraint(#{table.inspect}, name: #{name.inspect})" do
      connection.execute(sql)
    end
  end

  def safe_rename_constraint(table, from:, to:)
    raise ArgumentError, "Expected <from> to be present" unless from.present?
    raise ArgumentError, "Expected <to> to be present" unless to.present?

    quoted_table_name = connection.quote_table_name(table)
    quoted_constraint_from_name = connection.quote_table_name(from)
    quoted_constraint_to_name = connection.quote_table_name(to)
    sql = "ALTER TABLE #{quoted_table_name} RENAME CONSTRAINT #{quoted_constraint_from_name} TO #{quoted_constraint_to_name}"

    safely_acquire_lock_for_table(table) do
      say_with_time "rename_constraint(#{table.inspect}, from: #{from.inspect}, to: #{to.inspect})" do
        connection.execute(sql)
      end
    end
  end

  def unsafe_remove_constraint(table, name:)
    raise ArgumentError, "Expected <name> to be present" unless name.present?

    quoted_table_name = connection.quote_table_name(table)
    quoted_constraint_name = connection.quote_table_name(name)
    sql = "ALTER TABLE #{quoted_table_name} DROP CONSTRAINT #{quoted_constraint_name}"

    safely_acquire_lock_for_table(table) do
      say_with_time "remove_constraint(#{table.inspect}, name: #{name.inspect})" do
        connection.execute(sql)
      end
    end
  end

  def safe_create_partitioned_table(table, key:, type:, infer_primary_key: nil, **options, &block)
    raise ArgumentError, "Expected <key> to be present" unless key.present?

    unless PARTITION_TYPES.include?(type)
      raise ArgumentError, "Expected <type> to be symbol in #{PARTITION_TYPES}"
    end

    if ActiveRecord::Base.connection.postgresql_version < 10_00_00
      raise PgHaMigrations::InvalidMigrationError, "Native partitioning not supported on Postgres databases before version 10"
    end

    if type == :hash && ActiveRecord::Base.connection.postgresql_version < 11_00_00
      raise PgHaMigrations::InvalidMigrationError, "Hash partitioning not supported on Postgres databases before version 11"
    end

    if infer_primary_key.nil?
      infer_primary_key = PgHaMigrations.config.infer_primary_key_on_partitioned_tables
    end

    # Newer versions of Rails will set the primary key column to the type :primary_key.
    # This performs some extra logic that we can't easily undo which causes problems when
    # trying to inject the partition key into the PK. Now, it would be nice to lookup the
    # default primary key type instead of simply using :bigserial, but it doesn't appear
    # that we have access to the Rails configuration from within our migrations.
    if options[:id].nil? || options[:id] == :primary_key
      options[:id] = :bigserial
    end

    quoted_partition_key = if key.is_a?(Proc)
      # Lambda syntax, like in other migration methods, implies an expression that
      # cannot be easily sanitized.
      #
      # e.g ->{ "(created_at::date)" }
      key.call.to_s
    else
      # Otherwise, assume key is a column name or array of column names
      Array.wrap(key).map { |col| connection.quote_column_name(col) }.join(",")
    end

    options[:options] = "PARTITION BY #{type.upcase} (#{quoted_partition_key})"

    safe_create_table(table, options) do |td|
      block.call(td) if block

      next unless options[:id]

      pk_columns = td.columns.each_with_object([]) do |col, arr|
        next unless col.options[:primary_key]

        col.options[:primary_key] = false

        arr << col.name
      end

      if infer_primary_key && !key.is_a?(Proc) && ActiveRecord::Base.connection.postgresql_version >= 11_00_00
        td.primary_keys(pk_columns.concat(Array.wrap(key)).map(&:to_s).uniq)
      end
    end
  end

  def safe_partman_create_parent(table, key:, interval:, **options)
    raise ArgumentError, "Expected <key> to be present" unless key.present?
    raise ArgumentError, "Expected <interval> to be present" unless interval.present?

    invalid_options = options.keys - PARTMAN_CREATE_PARENT_OPTIONS

    raise ArgumentError, "Unrecognized optional argument(s): #{invalid_options}" unless invalid_options.empty?

    if ActiveRecord::Base.connection.postgresql_version < 10_00_00
      raise PgHaMigrations::InvalidMigrationError, "Native partitioning not supported on Postgres databases before version 10"
    end

    options[:template_table] = _fully_qualified_name(options[:template_table]) if options[:template_table].present?

    options = options.merge(
      parent_table: _fully_qualified_name(table),
      control: key,
      type: "native",
      interval: interval,
    ).compact

    create_parent_sql = options.map { |k, v| "p_#{k} := '#{v}'" }.join(", ")

    connection.execute("SELECT #{_quoted_partman_schema}.create_parent(#{create_parent_sql})")
  end

  def safe_partman_update_config(table, **options)
    invalid_options = options.keys - PARTMAN_UPDATE_CONFIG_OPTIONS

    raise ArgumentError, "Unrecognized argument(s): #{invalid_options}" unless invalid_options.empty?

    PgHaMigrations::PartmanConfig.schema = _quoted_partman_schema

    PgHaMigrations::PartmanConfig
      .find(_fully_qualified_name(table))
      .update!(**options)
  end

  def safe_partman_reapply_privileges(table)
    connection.execute("SELECT #{_quoted_partman_schema}.reapply_privileges('#{_fully_qualified_name(table)}')")
  end

  def _quoted_partman_schema
    schema = connection.select_value(<<~SQL)
      SELECT nspname
      FROM pg_namespace JOIN pg_extension
        ON pg_namespace.oid = pg_extension.extnamespace
      WHERE pg_extension.extname = 'pg_partman'
    SQL

    raise PgHaMigrations::InvalidMigrationError, "The pg_partman extension is not installed" unless schema.present?

    connection.quote_schema_name(schema)
  end

  def _fully_qualified_name(table)
    return table if table.to_s.include?(".")

    schema = connection.select_value(<<~SQL)
      SELECT schemaname
      FROM pg_tables
      WHERE tablename = '#{table}' AND schemaname = ANY (current_schemas(false))
      ORDER BY array_position(current_schemas(false), schemaname)
    SQL

    raise PgHaMigrations::InvalidMigrationError, "Could not find #{table} in search path" unless schema.present?

    "#{schema}.#{table}"
  end

  def _per_migration_caller
    @_per_migration_caller ||= Kernel.caller
  end

  def _check_postgres_adapter!
    expected_adapter = "PostgreSQL"
    actual_adapter = ActiveRecord::Base.connection.adapter_name
    raise PgHaMigrations::UnsupportedAdapter, "This gem only works with the #{expected_adapter} adapter, found #{actual_adapter} instead" unless actual_adapter == expected_adapter
  end

  def _type_is_enum(type)
    ActiveRecord::Base.connection.select_values("SELECT typname FROM pg_type JOIN pg_enum ON pg_type.oid = pg_enum.enumtypid").include?(type.to_s)
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
