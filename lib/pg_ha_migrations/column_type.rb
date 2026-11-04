module PgHaMigrations
  # A column type as Postgres sees it: the base type name (pg_type.typname)
  # plus the type modifier (atttypmod), e.g. character varying(20) is
  # ("varchar", 24) and an unconstrained numeric is ("numeric", -1).
  #
  # Used by safe_change_column to decide whether a type change can be
  # applied without rewriting the table.
  ColumnType = Struct.new(:name, :typmod, :formatted) do
    def self.connection
      ActiveRecord::Base.connection
    end

    delegate :connection, to: :class

    # Postgres stores varchar / numeric type modifiers with a 4 byte offset.
    VARHDRSZ = 4

    # timestamp, time and interval precision defaults to (and maxes out at) 6.
    MAX_DATETIME_PRECISION = 6

    DATETIME_TYPES = %w[timestamp timestamptz time timetz].freeze

    INTERVAL_FULL_RANGE = 0x7FFF
    INTERVAL_FULL_PRECISION = 0xFFFF

    # Time zones that Postgres treats as a fixed zero UTC offset. Converting
    # between timestamp and timestamptz only skips the table rewrite when the
    # session time zone is one of these. This list is intentionally
    # conservative: an unlisted zone results in an error, not a rewrite.
    UTC_TIME_ZONES = %w[
      utc etc/utc uct etc/uct gmt etc/gmt universal etc/universal zulu etc/zulu
    ].freeze

    def self.from_column(table, column)
      row = connection.select_one(<<~SQL)
        SELECT
          pg_type.typname AS name,
          pg_attribute.atttypmod AS typmod,
          format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS formatted
        FROM pg_attribute
        JOIN pg_type ON pg_type.oid = pg_attribute.atttypid
        WHERE pg_attribute.attrelid = #{connection.quote(table.fully_qualified_name)}::regclass
          AND pg_attribute.attname = #{connection.quote(column.to_s)}
          AND pg_attribute.attnum > 0
          AND NOT pg_attribute.attisdropped
      SQL

      unless row
        raise PgHaMigrations::InvalidMigrationError, "Column #{column.to_s.inspect} does not exist on table #{table.inspect}"
      end

      new(row["name"], row["typmod"].to_i, row["formatted"])
    end

    # Let Postgres itself parse the type so we compare exactly what
    # ALTER COLUMN ... TYPE <sql_type> would produce.
    def self.from_sql_type(sql_type)
      result = connection.execute("SELECT CAST(NULL AS #{sql_type})")
      type_oid = result.ftype(0)
      typmod = result.fmod(0)
      result.clear

      row = connection.select_one(<<~SQL)
        SELECT typname AS name, format_type(oid, #{typmod.to_i}) AS formatted
        FROM pg_type
        WHERE oid = #{type_oid.to_i}
      SQL

      new(row["name"], typmod, row["formatted"])
    end

    def self.utc_session_time_zone?
      UTC_TIME_ZONES.include?(connection.select_value("SHOW TimeZone").to_s.downcase)
    end

    def to_s
      formatted
    end

    # True when switching between timestamp and timestamptz. Postgres can
    # avoid the table rewrite in that case, but it still rebuilds indexes on
    # the column because the operator class changes.
    def time_zone_change?(new_type)
      [name, new_type.name].sort == %w[timestamp timestamptz]
    end

    # Whether ALTER COLUMN ... TYPE from self to new_type is a binary
    # coercible change that Postgres applies without rewriting the table.
    #
    # The rules mirror the planner support functions Postgres uses to turn
    # these casts into no-ops (varchar_support, numeric_support,
    # varbit_support, timestamp_support / time_support, interval_support)
    # plus the binary coercible casts between distinct types. Anything not
    # listed here is treated as requiring a rewrite.
    def rewrite_free_change_to?(new_type, utc_session_time_zone: false)
      return true if name == new_type.name && typmod == new_type.typmod

      case [name, new_type.name]
      when %w[varchar varchar], %w[varbit varbit]
        new_type.typmod < 0 || (typmod >= 0 && new_type.typmod >= typmod)
      when %w[varchar text], %w[cidr inet]
        true
      when %w[text varchar]
        new_type.typmod < 0
      when %w[numeric numeric]
        return true if new_type.typmod < 0
        return false if typmod < 0

        old_precision, old_scale = numeric_precision_and_scale(typmod)
        new_precision, new_scale = numeric_precision_and_scale(new_type.typmod)

        new_scale == old_scale && new_precision >= old_precision
      when %w[timestamp timestamptz], %w[timestamptz timestamp]
        return false unless utc_session_time_zone
        return false if connection.postgresql_version < 12_00_00

        datetime_precision(new_type.typmod) >= datetime_precision(typmod)
      when %w[interval interval]
        full_range_interval?(typmod) &&
          full_range_interval?(new_type.typmod) &&
          interval_precision(new_type.typmod) >= interval_precision(typmod)
      else
        DATETIME_TYPES.include?(name) &&
          name == new_type.name &&
          datetime_precision(new_type.typmod) >= datetime_precision(typmod)
      end
    end

    private

    def numeric_precision_and_scale(mod)
      mod -= VARHDRSZ
      # Scale is stored as an 11 bit signed integer (negative scale is allowed on PG 15+)
      [(mod >> 16) & 0xFFFF, ((mod & 0x7FF) ^ 1024) - 1024]
    end

    def datetime_precision(mod)
      mod < 0 ? MAX_DATETIME_PRECISION : [mod, MAX_DATETIME_PRECISION].min
    end

    def full_range_interval?(mod)
      mod < 0 || ((mod >> 16) & INTERVAL_FULL_RANGE) == INTERVAL_FULL_RANGE
    end

    def interval_precision(mod)
      return MAX_DATETIME_PRECISION if mod < 0

      precision = mod & INTERVAL_FULL_PRECISION
      precision == INTERVAL_FULL_PRECISION ? MAX_DATETIME_PRECISION : [precision, MAX_DATETIME_PRECISION].min
    end
  end
end
