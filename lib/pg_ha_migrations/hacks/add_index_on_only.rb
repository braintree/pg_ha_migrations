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

          original_query = super

          if original_query.gsub(" ON ").count > 1
            raise PgHaMigrations::InvalidMigrationError, "Found multiple occurrences of \"ON\" in query string for index #{o.index.name.inspect}; cannot safely replace with \"ON ONLY\""
          end

          super.sub(" ON ", " ON ONLY ")
        else
          super
        end
      end
    end
  end
end

ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.prepend(PgHaMigrations::ActiveRecordHacks::IndexAlgorithms)
ActiveRecord::ConnectionAdapters::PostgreSQL::SchemaCreation.prepend(PgHaMigrations::ActiveRecordHacks::CreateIndexDefinition)
