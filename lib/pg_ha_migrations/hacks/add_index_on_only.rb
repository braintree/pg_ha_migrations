require "active_record/connection_adapters/postgresql_adapter"
require "active_record/connection_adapters/postgresql/schema_creation"

module PgHaMigrations
  module ActiveRecordHacks
    module IndexAlgorithms
      def index_algorithms
        super.merge(only: "ONLY")
      end
    end

    module CreateIndexDefinition
      def visit_CreateIndexDefinition(o)
        if o.algorithm == "ONLY"
          o.algorithm = nil

          quoted_index = quote_column_name(o.index.name)
          quoted_table = quote_table_name(o.index.table)

          super.sub("#{quoted_index} ON #{quoted_table}", "#{quoted_index} ON ONLY #{quoted_table}")
        else
          super
        end
      end
    end
  end
end

ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.prepend(PgHaMigrations::ActiveRecordHacks::IndexAlgorithms)
ActiveRecord::ConnectionAdapters::PostgreSQL::SchemaCreation.prepend(PgHaMigrations::ActiveRecordHacks::CreateIndexDefinition)
