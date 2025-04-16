module PgHaMigrations::SafeStatements
  def safe_added_columns_without_default_value
    @safe_added_columns_without_default_value ||= []
  end

  def safe_create_table(table, **options, &block)
    if options[:force]
      raise PgHaMigrations::UnsafeMigrationError.new(":force is NOT SAFE! Explicitly call unsafe_drop_table first if you want to recreate an existing table")
    end

    unsafe_create_table(table, **options, &block)
  end

  def safe_create_enum_type(name, values=nil)
    case values
    when nil
      raise ArgumentError, "safe_create_enum_type expects a set of values; if you want an enum with no values please pass an empty array"
    when []
      raw_execute("CREATE TYPE #{PG::Connection.quote_ident(name.to_s)} AS ENUM ()")
    else
      escaped_values = values.map do |value|
        "'#{PG::Connection.escape_string(value.to_s)}'"
      end
      raw_execute("CREATE TYPE #{PG::Connection.quote_ident(name.to_s)} AS ENUM (#{escaped_values.join(',')})")
    end
  end

  def safe_add_enum_value(name, value)
    raw_execute("ALTER TYPE #{PG::Connection.quote_ident(name.to_s)} ADD VALUE '#{PG::Connection.escape_string(value)}'")
  end

  def safe_add_column(table, column, type, **options)
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
      raise PgHaMigrations::UnsafeMigrationError.new(":null => false is NOT SAFE if the table has data! If you want to do this, use safe_make_column_not_nullable")
    end

    unless options.has_key?(:default)
      self.safe_added_columns_without_default_value << [table.to_s, column.to_s]
    end

    unsafe_add_column(table, column, type, **options)
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
    quoted_table_name = connection.quote_table_name(table)
    quoted_column_name = connection.quote_column_name(column)

    safely_acquire_lock_for_table(table) do
      raw_execute "ALTER TABLE #{quoted_table_name} ALTER COLUMN #{quoted_column_name} DROP NOT NULL"
    end
  end

  # Postgres 12+ can use a valid CHECK constraint to validate that no values of a column are null, avoiding
  # a full table scan while holding an exclusive lock on the table when altering a column to NOT NULL
  #
  # Source:
  # https://dba.stackexchange.com/questions/267947/how-can-i-set-a-column-to-not-null-without-locking-the-table-during-a-table-scan/268128#268128
  # (https://archive.is/X55up)
  def safe_make_column_not_nullable(table, column)
    if ActiveRecord::Base.connection.postgresql_version < 12_00_00
      raise PgHaMigrations::InvalidMigrationError, "Cannot safely make a column non-nullable before Postgres 12"
    end

    validated_table = PgHaMigrations::Table.from_table_name(table)
    tmp_constraint_name = "tmp_not_null_constraint_#{OpenSSL::Digest::SHA256.hexdigest(column.to_s).first(7)}"

    if validated_table.check_constraints.any? { |c| c.name == tmp_constraint_name }
      raise PgHaMigrations::InvalidMigrationError, "A constraint #{tmp_constraint_name.inspect} already exists. " \
        "This implies that a previous invocation of this method failed and left behind a temporary constraint. " \
        "Please drop the constraint before attempting to run this method again."
    end

    safe_add_unvalidated_check_constraint(table, "#{connection.quote_column_name(column)} IS NOT NULL", name: tmp_constraint_name)
    safe_validate_check_constraint(table, name: tmp_constraint_name)

    # "Ordinarily this is checked during the ALTER TABLE by scanning the entire table; however, if a
    # valid CHECK constraint is found which proves no NULL can exist, then the table scan is
    # skipped."
    # See: https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DESC-SET-DROP-NOT-NULL
    unsafe_make_column_not_nullable(table, column)
    unsafe_remove_constraint(table, name: tmp_constraint_name)
  end

  def safe_make_column_not_nullable_from_check_constraint(table, column, constraint_name:)
    unless ActiveRecord::Base.connection.postgresql_version >= 12_00_00
      raise PgHaMigrations::InvalidMigrationError, "Cannot safely make a column non-nullable before Postgres 12"
    end

    unless constraint_name
      raise ArgumentError, "Expected <constraint_name> to be present"
    end
    constraint_name = constraint_name.to_s

    quoted_table_name = connection.quote_table_name(table)
    quoted_column_name = connection.quote_column_name(column)

    validated_table = PgHaMigrations::Table.from_table_name(table)
    constraint = validated_table.check_constraints.find do |c|
      c.name == constraint_name
    end

    unless constraint
      raise PgHaMigrations::InvalidMigrationError, "The provided constraint does not exist"
    end

    unless constraint.validated
      raise PgHaMigrations::InvalidMigrationError, "The provided constraint is not validated"
    end

    # The constraint has to actually prove that no null values exist, so the
    # constraint condition can't simply include the `IS NOT NULL` check. We
    # don't try to handle all possible cases here. For example,
    # `a IS NOT NULL AND b IS NOT NULL` would prove what we need, but it would
    # be complicated to check. We must ensure, however, that we're not too
    # loose. For example, `a IS NOT NULL OR b IS NOT NULL` would not prove that
    # `a IS NOT NULL`.
    unless constraint.definition =~ /\ACHECK \(*(#{Regexp.escape(column.to_s)}|#{Regexp.escape(quoted_column_name)}) IS NOT NULL\)*\Z/i
      raise PgHaMigrations::InvalidMigrationError, "The provided constraint does not enforce non-null values for the column"
    end

    # "Ordinarily this is checked during the ALTER TABLE by scanning the entire table; however, if a
    # valid CHECK constraint is found which proves no NULL can exist, then the table scan is
    # skipped."
    # See: https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DESC-SET-DROP-NOT-NULL
    unsafe_make_column_not_nullable(table, column)
  end

  def safe_add_index_on_empty_table(table, columns, **options)
    if options[:algorithm] == :concurrently
      raise ArgumentError, "Cannot call safe_add_index_on_empty_table with :algorithm => :concurrently"
    end

    # Check if nulls_not_distinct was provided but PostgreSQL version doesn't support it
    if options[:nulls_not_distinct] && ActiveRecord::Base.connection.postgresql_version < 15_00_00
      raise PgHaMigrations::InvalidMigrationError, "nulls_not_distinct option requires PostgreSQL 15 or higher"
    end

    # Avoids taking out an unnecessary SHARE lock if the table does have data
    ensure_small_table!(table, empty: true)

    safely_acquire_lock_for_table(table, mode: :share) do
      # Ensure data wasn't written in the split second after the first check
      ensure_small_table!(table, empty: true)

      unsafe_add_index(table, columns, **options)
    end
  end

  def safe_add_concurrent_index(table, columns, **options)
    # Check if nulls_not_distinct was provided but PostgreSQL version doesn't support it
    if options[:nulls_not_distinct] && ActiveRecord::Base.connection.postgresql_version < 15_00_00
      raise PgHaMigrations::InvalidMigrationError, "nulls_not_distinct option requires PostgreSQL 15 or higher"
    end

    unsafe_add_index(table, columns, **options.merge(:algorithm => :concurrently))
  end

  def safe_remove_concurrent_index(table, **options)
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

  def safe_add_concurrent_partitioned_index(
    table,
    columns,
    name: nil,
    if_not_exists: nil,
    using: nil,
    unique: nil,
    where: nil,
    comment: nil,
    nulls_not_distinct: nil
  )
    # Check if nulls_not_distinct was provided but PostgreSQL version doesn't support it
    if !nulls_not_distinct.nil? && ActiveRecord::Base.connection.postgresql_version < 15_00_00
      raise PgHaMigrations::InvalidMigrationError, "nulls_not_distinct option requires PostgreSQL 15 or higher"
    end

    if ActiveRecord::Base.connection.postgresql_version < 11_00_00
      raise PgHaMigrations::InvalidMigrationError, "Concurrent partitioned index creation not supported on Postgres databases before version 11"
    end

    parent_table = PgHaMigrations::Table.from_table_name(table)

    raise PgHaMigrations::InvalidMigrationError, "Table #{parent_table.inspect} is not a partitioned table" unless parent_table.natively_partitioned?

    parent_index = if name.present?
      PgHaMigrations::Index.new(name, parent_table)
    else
      PgHaMigrations::Index.from_table_and_columns(parent_table, columns)
    end

    # Short-circuit when if_not_exists: true and index already valid
    return if if_not_exists && parent_index.valid?

    child_indexes = parent_table.partitions.map do |child_table|
      PgHaMigrations::Index.from_table_and_columns(child_table, columns)
    end

    # CREATE INDEX ON ONLY parent_table
    unsafe_add_index(
      parent_table.fully_qualified_name,
      columns,
      name: parent_index.name,
      if_not_exists: if_not_exists,
      using: using,
      unique: unique,
      nulls_not_distinct: nulls_not_distinct,
      where: where,
      comment: comment,
      algorithm: :only, # see lib/pg_ha_migrations/hacks/add_index_on_only.rb
    )

    child_indexes.each do |child_index|
      add_index_method = if child_index.table.natively_partitioned?
        :safe_add_concurrent_partitioned_index
      else
        :safe_add_concurrent_index
      end

      send(
        add_index_method,
        child_index.table.fully_qualified_name,
        columns,
        name: child_index.name,
        if_not_exists: if_not_exists,
        using: using,
        unique: unique,
        nulls_not_distinct: nulls_not_distinct,
        where: where,
      )
    end

    # Avoid taking out an unnecessary lock if there are no child tables to attach
    if child_indexes.present?
      safely_acquire_lock_for_table(parent_table.fully_qualified_name) do
        child_indexes.each do |child_index|
          say_with_time "Attaching index #{child_index.inspect} to #{parent_index.inspect}" do
            connection.execute(<<~SQL)
              ALTER INDEX #{parent_index.fully_qualified_name}
              ATTACH PARTITION #{child_index.fully_qualified_name}
            SQL
          end
        end
      end
    end

    raise PgHaMigrations::InvalidMigrationError, "Unexpected state. Parent index #{parent_index.inspect} is invalid" unless parent_index.valid?
  end

  def safe_set_maintenance_work_mem_gb(gigabytes)
    raw_execute("SET maintenance_work_mem = '#{PG::Connection.escape_string(gigabytes.to_s)} GB'")
  end

  def safe_add_unvalidated_check_constraint(table, expression, name:)
    unsafe_add_check_constraint(table, expression, name: name, validate: false)
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

  def safe_create_partitioned_table(table, partition_key:, type:, infer_primary_key: nil, **options, &block)
    raise ArgumentError, "Expected <partition_key> to be present" unless partition_key.present?

    unless PgHaMigrations::PARTITION_TYPES.include?(type)
      raise ArgumentError, "Expected <type> to be symbol in #{PgHaMigrations::PARTITION_TYPES} but received #{type.inspect}"
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

    quoted_partition_key = if partition_key.is_a?(Proc)
      # Lambda syntax, like in other migration methods, implies an expression that
      # cannot be easily sanitized.
      #
      # e.g ->{ "(created_at::date)" }
      partition_key.call.to_s
    else
      # Otherwise, assume key is a column name or array of column names
      Array.wrap(partition_key).map { |col| connection.quote_column_name(col) }.join(",")
    end

    options[:options] = "PARTITION BY #{type.upcase} (#{quoted_partition_key})"

    safe_create_table(table, **options) do |td|
      block.call(td) if block

      next unless options[:id]

      pk_columns = td.columns.each_with_object([]) do |col, arr|
        next unless col.options[:primary_key]

        col.options[:primary_key] = false

        arr << col.name
      end

      if infer_primary_key && !partition_key.is_a?(Proc) && ActiveRecord::Base.connection.postgresql_version >= 11_00_00
        td.primary_keys(pk_columns.concat(Array.wrap(partition_key)).map(&:to_s).uniq)
      end
    end
  end

  def safe_partman_create_parent(
    table,
    partition_key:,
    interval:,
    infinite_time_partitions: true,
    inherit_privileges: true,
    premake: nil,
    start_partition: nil,
    template_table: nil,
    retention: nil,
    retention_keep_table: nil
  )
    raise ArgumentError, "Expected <partition_key> to be present" unless partition_key.present?
    raise ArgumentError, "Expected <interval> to be present" unless interval.present?

    if ActiveRecord::Base.connection.postgresql_version < 11_00_00
      raise PgHaMigrations::InvalidMigrationError, "Native partitioning with partman not supported on Postgres databases before version 11"
    end

    formatted_start_partition = nil

    if start_partition.present?
      if !start_partition.is_a?(Date) && !start_partition.is_a?(Time) && !start_partition.is_a?(DateTime)
        raise PgHaMigrations::InvalidMigrationError, "Expected <start_partition> to be Date, Time, or DateTime object but received #{start_partition.class}"
      end

      formatted_start_partition = if start_partition.respond_to?(:to_fs)
        start_partition.to_fs(:db)
      else
        start_partition.to_s(:db)
      end
    end

    create_parent_options = {
      parent_table: _fully_qualified_table_name_for_partman(table),
      template_table: template_table ? _fully_qualified_table_name_for_partman(template_table) : nil,
      control: partition_key,
      type: "native",
      interval: interval,
      premake: premake,
      start_partition: formatted_start_partition,
    }.compact

    create_parent_sql = create_parent_options.map { |k, v| "p_#{k} := #{connection.quote(v)}" }.join(", ")

    log_message = "partman_create_parent(#{table.inspect}, " \
      "partition_key: #{partition_key.inspect}, " \
      "interval: #{interval.inspect}, " \
      "premake: #{premake.inspect}, " \
      "start_partition: #{start_partition.inspect}, " \
      "template_table: #{template_table.inspect})"

    say_with_time(log_message) do
      connection.execute("SELECT #{_quoted_partman_schema}.create_parent(#{create_parent_sql})")
    end

    update_config_options = {
      infinite_time_partitions: infinite_time_partitions,
      inherit_privileges: inherit_privileges,
      retention: retention,
      retention_keep_table: retention_keep_table,
    }.compact

    unsafe_partman_update_config(table, **update_config_options)
  end

  def safe_partman_update_config(table, **options)
    if options[:retention].present? || options[:retention_keep_table] == false
      raise PgHaMigrations::UnsafeMigrationError.new(":retention and/or :retention_keep_table => false can potentially result in data loss if misconfigured. Please use unsafe_partman_update_config if you want to set these options")
    end

    unsafe_partman_update_config(table, **options)
  end

  def safe_partman_reapply_privileges(table)
    say_with_time "partman_reapply_privileges(#{table.inspect})" do
      connection.execute("SELECT #{_quoted_partman_schema}.reapply_privileges('#{_fully_qualified_table_name_for_partman(table)}')")
    end
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

  def _fully_qualified_table_name_for_partman(table)
    table = PgHaMigrations::Table.from_table_name(table)

    [table.schema, table.name].each do |identifier|
      if identifier.to_s !~ /^[a-z_][a-z_\d]*$/
        raise PgHaMigrations::InvalidMigrationError, "Partman requires schema / table names to be lowercase with underscores"
      end
    end.join(".")
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

  def safely_acquire_lock_for_table(*tables, mode: :access_exclusive, &block)
    _check_postgres_adapter!

    target_tables = PgHaMigrations::TableCollection.from_table_names(tables, mode)

    if @parent_lock_tables
      if !target_tables.subset?(@parent_lock_tables)
        raise PgHaMigrations::InvalidMigrationError,
          "Nested lock detected! Cannot acquire lock on #{target_tables.to_sql} " \
          "while #{@parent_lock_tables.to_sql} is locked."
      end

      if @parent_lock_tables.mode < target_tables.mode
        raise PgHaMigrations::InvalidMigrationError,
          "Lock escalation detected! Cannot change lock level from :#{@parent_lock_tables.mode} " \
          "to :#{target_tables.mode} for #{target_tables.to_sql}."
      end

      # If in a nested context and all of the above checks have passed,
      # we have already acquired the lock (or a lock at a higher level),
      # and can simply execute the block and short-circuit.
      block.call

      return
    end

    successfully_acquired_lock = false

    until successfully_acquired_lock
      loop do
        blocking_transactions = PgHaMigrations::BlockingDatabaseTransactions.find_blocking_transactions("#{PgHaMigrations::LOCK_TIMEOUT_SECONDS} seconds")

        # Locking a partitioned table will also lock child tables (including sub-partitions),
        # so we need to check for blocking queries on those tables as well
        target_tables_with_partitions = target_tables.with_partitions

        break unless blocking_transactions.any? do |query|
          query.tables_with_locks.any? do |locked_table|
            target_tables_with_partitions.any? do |target_table|
              target_table.conflicts_with?(locked_table)
            end
          end
        end

        say "Waiting on blocking transactions:"
        blocking_transactions.each do |blocking_transaction|
          say blocking_transaction.description
        end
        sleep(PgHaMigrations::LOCK_TIMEOUT_SECONDS)
      end

      connection.transaction do
        begin
          # A lock timeout would apply to each individual table in the query,
          # so we made a conscious decision to use a statement timeout here
          # to keep behavior consistent in a multi-table lock scenario.
          adjust_statement_timeout(PgHaMigrations::LOCK_TIMEOUT_SECONDS) do
            connection.execute("LOCK #{target_tables.to_sql} IN #{target_tables.mode.to_sql} MODE;")
          end
          successfully_acquired_lock = true
        rescue ActiveRecord::StatementInvalid => e
          # It is still possible to hit a lock timeout if the session has
          # that value set to something less than LOCK_TIMEOUT_SECONDS.
          # We should retry when either of these exceptions are raised.
          if e.message =~ /PG::LockNotAvailable.+ lock timeout/ || e.message =~ /PG::QueryCanceled.+ statement timeout/
            sleep_seconds = PgHaMigrations::LOCK_FAILURE_RETRY_DELAY_MULTLIPLIER * PgHaMigrations::LOCK_TIMEOUT_SECONDS
            say "Timed out trying to acquire #{target_tables.mode.to_sql} lock on #{target_tables.to_sql}."
            say "Sleeping for #{sleep_seconds}s to allow potentially queued up queries to finish before continuing."
            sleep(sleep_seconds)

            raise ActiveRecord::Rollback
          else
            raise e
          end
        end

        if successfully_acquired_lock
          @parent_lock_tables = target_tables

          begin
            block.call
          ensure
            @parent_lock_tables = nil
          end
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

  def ensure_small_table!(table, empty: false, threshold: PgHaMigrations::SMALL_TABLE_THRESHOLD_BYTES)
    table = PgHaMigrations::Table.from_table_name(table)

    if empty && table.has_rows?
      raise PgHaMigrations::InvalidMigrationError, "Table #{table.inspect} has rows"
    end

    if table.total_bytes > threshold
      raise PgHaMigrations::InvalidMigrationError, "Table #{table.inspect} is larger than #{threshold} bytes"
    end
  end
end
