module PgHaMigrations
  class BlockingDatabaseTransactions
    LongRunningTransaction = Struct.new(:database, :current_query, :state, :transaction_age, :tables_with_locks) do
      def description
        locked_tables = tables_with_locks.compact
        [
          database,
          locked_tables.size > 0 ? "tables (#{locked_tables.join(', ')})" : nil,
          "#{idle? ? "currently idle " : ""}transaction open for #{transaction_age}",
          "#{idle? ? "last " : ""}query: #{current_query}"
        ].compact.join(" | ")
      end

      def concurrent_index_creation?
        !!current_query.match(/create\s+index\s+concurrently/i)
      end

      def idle?
        state == "idle in transaction"
      end
    end

    def self.autovacuum_regex
      "^autovacuum: (?!.*to prevent wraparound)"
    end

    def self.find_blocking_transactions(minimum_transaction_age = "0 seconds")
      pid_column, query_column = if ActiveRecord::Base.connection.select_value("SHOW server_version") =~ /9\.1/
        ["procpid", "current_query"]
      else
        ["pid", "query"]
      end

      raw_query = <<-SQL
        SELECT
          psa.datname as database, -- Will only ever be one database
          psa.#{query_column} as current_query,
          psa.state,
          clock_timestamp() - psa.xact_start AS transaction_age,
          array_agg(distinct c.relname) AS tables_with_locks
        FROM pg_stat_activity psa -- Cluster wide
          LEFT JOIN pg_locks l ON (psa.#{pid_column} = l.pid)  -- Cluster wide
          LEFT JOIN pg_class c ON ( -- Database wide
            l.locktype = 'relation'
            AND l.relation = c.oid
            -- Be explicit about this being for a single database -- it's already implicit in
            -- the relations used, and if we don't restrict this we could get incorrect results
            -- with oid collisions from pg_namespace and pg_class.
            AND l.database = (SELECT d.oid FROM pg_database d WHERE d.datname = current_database())
          )
          LEFT JOIN pg_namespace ns ON (c.relnamespace = ns.oid) -- Database wide
        WHERE psa.#{pid_column} != pg_backend_pid()
          AND (
            l.locktype != 'relation'
            OR (
               ns.nspname != 'pg_catalog'
               AND c.relkind = 'r'
            )
          )
          AND psa.xact_start < clock_timestamp() - ?::interval
          AND psa.#{query_column} !~ ?
        GROUP BY psa.datname, psa.#{query_column}, psa.state, psa.xact_start
      SQL

      query = ActiveRecord::Base.send(:sanitize_sql_for_conditions, [raw_query, minimum_transaction_age, autovacuum_regex])

      ActiveRecord::Base.structs_from_sql(LongRunningTransaction, query)
    end
  end
end
