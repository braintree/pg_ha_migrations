module PgHaMigrations
  module OutOfBandMigrator

    def self.instructions
      """
        =================================== Instructions ===================================
        print blocking_database_transactions - Print blocking database transactions
        print migrations_state               - Print all non-deployed migrations
        print instructions                   - Print this message
        #{migrate_command}
        exit                                 - Exit the Out of Band Tactical Command Center
        ====================================================================================
      """
    end

    def self.migrate_command
      "migrate <version>                 - Run a migration, e.g. `migrate 24603`"
    end
  end
end
