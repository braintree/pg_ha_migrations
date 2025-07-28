module PgHaMigrations
  class Extension
    attr_reader :name, :schema, :version

    def initialize(name)
      @name = name

      @schema, @version = ActiveRecord::Base.connection.select_rows(<<~SQL).first
        SELECT nspname, extversion
        FROM pg_namespace JOIN pg_extension
          ON pg_namespace.oid = pg_extension.extnamespace
        WHERE pg_extension.extname = #{ActiveRecord::Base.connection.quote(name)}
        LIMIT 1
      SQL
    end

    def quoted_schema
      return unless schema

      PG::Connection.quote_ident(schema)
    end

    def major_version
      return unless version

      Gem::Version.new(version)
        .segments
        .first
    end

    def installed?
      !!schema && !!version
    end
  end
end
