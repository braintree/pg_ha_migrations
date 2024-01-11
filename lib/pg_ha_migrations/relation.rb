module PgHaMigrations
  Relation = Struct.new(:name, :schema, :mode) do
    def self.connection
      ActiveRecord::Base.connection
    end

    delegate :inspect, to: :name
    delegate :connection, to: :class

    def initialize(name, schema, mode=nil)
      super(name, schema)

      self.mode = LockMode.new(mode) if mode.present?
    end

    def conflicts_with?(other)
      self == other && (
        mode.nil? || other.mode.nil? || mode.conflicts_with?(other.mode)
      )
    end

    def fully_qualified_name
      @fully_qualified_name ||= [
        PG::Connection.quote_ident(schema),
        PG::Connection.quote_ident(name),
      ].join(".")
    end

    def present?
      name.present? && schema.present?
    end

    def ==(other)
      other.is_a?(Relation) && name == other.name && schema == other.schema
    end
  end

  class Table < Relation
    def self.from_table_name(table, mode=nil)
      pg_name = ActiveRecord::ConnectionAdapters::PostgreSQL::Utils.extract_schema_qualified_name(table.to_s)

      schema_conditional = if pg_name.schema
        "#{connection.quote(pg_name.schema)}"
      else
        "ANY (current_schemas(false))"
      end

      schema = connection.select_value(<<~SQL)
        SELECT schemaname
        FROM pg_tables
        WHERE tablename = #{connection.quote(pg_name.identifier)} AND schemaname = #{schema_conditional}
        ORDER BY array_position(current_schemas(false), schemaname)
        LIMIT 1
      SQL

      raise UndefinedTableError, "Table #{pg_name.quoted} does not exist#{" in search path" unless pg_name.schema}" unless schema.present?

      new(pg_name.identifier, schema, mode)
    end

    def natively_partitioned?
      return @natively_partitioned if defined?(@natively_partitioned)

      @natively_partitioned = !!connection.select_value(<<~SQL)
        SELECT true
        FROM pg_partitioned_table, pg_class, pg_namespace
        WHERE pg_class.oid = pg_partitioned_table.partrelid
          AND pg_class.relnamespace = pg_namespace.oid
          AND pg_class.relname = #{connection.quote(name)}
          AND pg_namespace.nspname = #{connection.quote(schema)}
      SQL
    end

    def partitions(include_sub_partitions: false, include_self: false)
      tables = connection.structs_from_sql(self.class, <<~SQL)
        SELECT child.relname AS name, child_ns.nspname AS schema, NULLIF('#{mode}', '') AS mode
        FROM pg_inherits
          JOIN pg_class parent        ON pg_inherits.inhparent = parent.oid
          JOIN pg_class child         ON pg_inherits.inhrelid  = child.oid
          JOIN pg_namespace parent_ns ON parent.relnamespace = parent_ns.oid
          JOIN pg_namespace child_ns  ON child.relnamespace = child_ns.oid
        WHERE parent.relname = #{connection.quote(name)}
          AND parent_ns.nspname = #{connection.quote(schema)}
        ORDER BY child.oid -- Ensure consistent ordering for tests
      SQL

      if include_sub_partitions
        sub_partitions = tables.each_with_object([]) do |table, arr|
          arr.concat(table.partitions(include_sub_partitions: true))
        end

        tables.concat(sub_partitions)
      end

      tables.prepend(self) if include_self

      tables
    end

    def has_rows?
      connection.select_value("SELECT EXISTS (SELECT 1 FROM #{fully_qualified_name} LIMIT 1)")
    end

    def total_bytes
      connection.select_value(<<~SQL)
        SELECT pg_total_relation_size(pg_class.oid)
        FROM pg_class, pg_namespace
        WHERE pg_class.relname = #{connection.quote(name)}
          AND pg_namespace.nspname = #{connection.quote(schema)}
      SQL
    end
  end

  class Index < Relation
    MAX_NAME_SIZE = 63 # bytes

    def self.from_table_and_columns(table, columns)
      name = connection.index_name(table.name, columns)

      # modified from https://github.com/rails/rails/pull/47753
      if name.bytesize > MAX_NAME_SIZE
        hashed_identifier = "_#{OpenSSL::Digest::SHA256.hexdigest(name).first(10)}"
        description = name.sub("index_#{table.name}_on", "idx_on")

        short_limit = MAX_NAME_SIZE - hashed_identifier.bytesize
        short_description = description.mb_chars.limit(short_limit).to_s

        name = "#{short_description}#{hashed_identifier}"
      end

      new(name, table)
    end

    attr_accessor :table

    def initialize(name, table)
      super(name, table.schema)

      self.table = table

      connection.send(:validate_index_length!, table.name, name)
    end

    def valid?
      !!connection.select_value(<<~SQL)
        SELECT pg_index.indisvalid
        FROM pg_index, pg_class, pg_namespace
        WHERE pg_class.oid = pg_index.indexrelid
          AND pg_class.relnamespace = pg_namespace.oid
          AND pg_namespace.nspname = #{connection.quote(schema)}
          AND pg_class.relname = #{connection.quote(name)}
      SQL
    end
  end
end
