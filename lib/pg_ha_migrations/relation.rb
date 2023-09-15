module PgHaMigrations
  Relation = Struct.new(:name, :schema) do
    def self.connection
      ActiveRecord::Base.connection
    end

    delegate :inspect, to: :name
    delegate :connection, to: :class

    def fully_qualified_name
      @fully_qualified_name ||= [
        PG::Connection.quote_ident(schema),
        PG::Connection.quote_ident(name),
      ].join(".")
    end

    def present?
      name.present? && schema.present?
    end
  end

  class Table < Relation
    def self.from_table_name(table)
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

      new(pg_name.identifier, schema)
    end

    def natively_partitioned?
      !!connection.select_value(<<~SQL)
        SELECT true
        FROM pg_partitioned_table, pg_class, pg_namespace
        WHERE pg_class.oid = pg_partitioned_table.partrelid
          AND pg_class.relnamespace = pg_namespace.oid
          AND pg_class.relname = #{connection.quote(name)}
          AND pg_namespace.nspname = #{connection.quote(schema)}
      SQL
    end

    def partitions(include_sub_partitions: false)
      tables = connection.structs_from_sql(self.class, <<~SQL)
        SELECT child.relname AS name, child_ns.nspname AS schema
        FROM pg_inherits
          JOIN pg_class parent        ON pg_inherits.inhparent = parent.oid
          JOIN pg_class child         ON pg_inherits.inhrelid  = child.oid
          JOIN pg_namespace parent_ns ON parent.relnamespace = parent_ns.oid
          JOIN pg_namespace child_ns  ON child.relnamespace = child_ns.oid
        WHERE parent.relname = #{connection.quote(name)}
          AND parent_ns.nspname = #{connection.quote(schema)}
      SQL

      if include_sub_partitions
        sub_partitions = tables.each_with_object([]) do |table, arr|
          arr.concat(table.partitions(include_sub_partitions: true))
        end

        tables.concat(sub_partitions)
      end

      tables
    end
  end

  class Index < Relation
    # TODO: implement shortening to ensure < 63 bytes
    def self.from_table_and_columns(table, columns)
      new(connection.index_name(table.name, columns), table)
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
