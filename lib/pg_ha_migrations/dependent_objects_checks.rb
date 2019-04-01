module PgHaMigrations::DependentObjectsChecks
  ALLOWED_TYPES_OPTIONS = [:indexes].freeze

  ObjectDependency = Struct.new(:owner_type, :owner_name, :dependent_type, :dependent_name) do
    def error_text
      "#{dependent_type} '#{dependent_name}' depends on #{owner_type} '#{owner_name}'"
    end
  end

  def dependent_objects_for_migration_method(method_name, arguments:)
    allowed_types = arguments.last.is_a?(Hash) ? arguments.last[:allow_dependent_objects] || [] : []

    if (invalid_allowed_types = allowed_types - ALLOWED_TYPES_OPTIONS).present?
      raise ArgumentError, "Received invalid entries in allow_dependent_objects: #{invalid_allowed_types.inspect}"
    end

    dependent_objects = []

    case method_name
    when :remove_column
      table_name = arguments[0]
      column_name = arguments[1]

      unless allowed_types.include?(:indexes)
        # https://www.postgresql.org/docs/current/catalog-pg-depend.html
        # deptype:
        # - 'a' (DEPENDENCY_AUTO): the dependent object should be automatically
        #   dropped if the referenced object is dropped
        indexes = ActiveRecord::Base.structs_from_sql(ObjectDependency, <<~SQL)
          SELECT
            'column' AS owner_type,
            ref_attr.attname AS owner_name,
            'index' AS dependent_type,
            dep_class.relname AS dependent_name
          FROM pg_catalog.pg_depend
          JOIN pg_catalog.pg_class ref_class ON ref_class.oid = pg_depend.refobjid
          JOIN pg_catalog.pg_attribute ref_attr ON ref_attr.attrelid = ref_class.oid
            AND ref_attr.attnum = pg_depend.refobjsubid
          JOIN pg_catalog.pg_class dep_class ON dep_class.oid = pg_depend.objid
          WHERE pg_depend.deptype = 'a'
            AND pg_depend.refclassid = 'pg_class'::regclass
            -- This doesn't currently handle table names that are duplicative across schemas.
            AND ref_class.relname = '#{PG::Connection.escape_string(table_name.to_s)}'
            AND ref_attr.attname = '#{PG::Connection.escape_string(column_name.to_s)}'
            AND pg_depend.classid = 'pg_class'::regclass
        SQL
        dependent_objects.concat(indexes)
      end
    end
  end

  def disallow_migration_method_if_dependent_objects!(method_name, arguments:)
    dependent_objects = dependent_objects_for_migration_method(method_name, arguments: arguments)

    if dependent_objects.present?
      raise PgHaMigrations::UnsafeMigrationError, dependent_objects.map(&:error_text).join("; ")
    end
  end
end

ActiveRecord::Migration.prepend(PgHaMigrations::DependentObjectsChecks)
