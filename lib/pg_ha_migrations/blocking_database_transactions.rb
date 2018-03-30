module PgHaMigrations
  class BlockingDatabaseTransactions
    LongRunningTransaction = Struct.new(:database, :current_query, :transaction_age, :tables_with_locks) do
      def description
        "#{database} | tables (#{tables_with_locks.join(', ')}) have been locked for #{transaction_age} | query: #{current_query}"
      end

      def concurrent_index_creation?
        !!current_query.match(/create\s+index\s+concurrently/i)
      end
    end

    def self.autovacuum_regex
      "^autovacuum: (?!.*to prevent wraparound)"
    end

    def self.find_blocking_transactions(minimum_transaction_age = "0 seconds")
      pid_column, state_column = if ActiveRecord::Base.connection.select_value("SHOW server_version") =~ /9\.1/
        ["procpid", "current_query"]
      else
        ["pid", "query"]
      end

      raw_query = <<-SQL
        SELECT
          psa.datname as database, -- Will only ever be one database
          psa.#{state_column} as current_query,
          clock_timestamp() - psa.xact_start AS transaction_age,
          array_agg(distinct c.relname) AS tables_with_locks
        FROM pg_stat_activity psa -- Cluster wide
          JOIN pg_locks l ON (psa.#{pid_column} = l.pid)  -- Cluster wide
          JOIN pg_class c ON (l.relation = c.oid) -- Database wide
          JOIN pg_namespace ns ON (c.relnamespace = ns.oid) -- Database wide
        WHERE psa.#{pid_column} != pg_backend_pid()
          AND ns.nspname != 'pg_catalog'
          AND c.relkind = 'r'
          AND psa.xact_start < clock_timestamp() - ?::interval
          AND psa.#{state_column} !~ ?
          AND (
            -- Be explicit about this being for a single database -- it's already implicit in
            -- the relations used, and if we don't restrict this we could get incorrect results
            -- with oid collisions from pg_namespace and pg_class.
            l.database = 0
            OR l.database = (SELECT d.oid FROM pg_database d WHERE d.datname = current_database())
          )
        GROUP BY psa.datname, psa.#{state_column}, psa.xact_start
      SQL

      query = ActiveRecord::Base.send(:sanitize_sql_for_conditions, [raw_query, minimum_transaction_age, autovacuum_regex])

      ActiveRecord::Base.structs_from_sql(LongRunningTransaction, query)
    end
  end
end
