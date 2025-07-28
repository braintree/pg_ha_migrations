module TestHelpers
  TableLock = Struct.new(:table, :lock_type, :granted, :pid)
  def self.locks_for_table(table, connection:)
    identifiers = table.to_s.split(".")

    identifiers.prepend("public") if identifiers.size == 1

    schema, table = identifiers

    connection.structs_from_sql(TableLock, <<-SQL)
      SELECT pg_class.relname AS table, pg_locks.mode AS lock_type, granted, pid
      FROM pg_locks
      JOIN pg_class ON pg_locks.relation = pg_class.oid
      JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
      WHERE pid IS DISTINCT FROM pg_backend_pid()
        AND pg_class.relkind IN ('r', 'p') -- 'r' is a standard table; 'p' is a partition parent
        AND pg_class.relname = '#{table}'
        AND pg_namespace.nspname = '#{schema}'
    SQL
  end

  def self.partitions_for_table(table, schema: "public")
    ActiveRecord::Base.connection.select_values(<<~SQL)
      SELECT child.relname
      FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
        JOIN pg_namespace    ON pg_namespace.oid      = child.relnamespace
      WHERE parent.relname = '#{table}'
        AND pg_namespace.nspname = '#{schema}'
    SQL
  end

  def self.grantees_for_table(table)
    ActiveRecord::Base.connection.select_values(<<~SQL)
      SELECT DISTINCT(grantee)
      FROM information_schema.role_table_grants
      WHERE table_name = '#{table}'
    SQL
  end

  def self.create_range_partitioned_table(table, migration_klass, with_template: false, with_partman: false)
    migration = Class.new(migration_klass) do
      class_attribute :table, :with_template, :with_partman, instance_accessor: true

      self.table = table
      self.with_template = with_template
      self.with_partman = with_partman

      def up
        safe_create_partitioned_table table, type: :range, partition_key: :created_at do |t|
          t.timestamps null: false
          t.text :text_column
        end

        if with_template
          safe_create_table "#{table}_template", id: false do |t|
            t.text :text_column, index: {unique: true}
          end
        end

        if with_partman
          template_table = with_template ? "#{table}_template" : nil

          safe_partman_create_parent(
            table,
            partition_key: :created_at,
            interval: TestHelpers.partition_interval("weekly"),
            template_table: template_table
          )
        end
      end
    end

    migration.suppress_messages { migration.migrate(:up) }
  end

  def self.install_partman(schema: "public")
    ActiveRecord::Base.connection.execute(<<~SQL)
      CREATE SCHEMA IF NOT EXISTS #{schema};
      CREATE EXTENSION pg_partman SCHEMA #{schema};
    SQL
  end

  def self.partition_interval(interval)
    partman_extension = PgHaMigrations::Extension.new("pg_partman")

    return interval unless partman_extension.installed?
    return interval if partman_extension.major_version < 5

    case interval
    when "weekly"
      "1 week"
    when "monthly"
      "1 month"
    else
      interval
    end
  end

  def self.part_config(parent_table)
    PgHaMigrations::PartmanConfig.find(
      parent_table,
      partman_extension: PgHaMigrations::Extension.new("pg_partman")
    )
  end

  def self.enum_names_and_values
    ActiveRecord::Base.connection.execute(<<~SQL).to_a
      SELECT pg_type.typname AS name,
             pg_enum.enumlabel AS value
      FROM pg_type
      JOIN pg_enum ON pg_enum.enumtypid = pg_type.oid
    SQL
  end

  def self.pool_config
    ActiveRecord::ConnectionAdapters::PoolConfig.new(
      ActiveRecord::Base,
      ActiveRecord::Base.connection_pool.db_config,
      ActiveRecord::Base.current_role,
      ActiveRecord::Base.current_shard
    )
  end
end
