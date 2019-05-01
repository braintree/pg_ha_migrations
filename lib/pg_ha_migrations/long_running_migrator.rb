module PgHaMigrations
  class LongRunningMigrator < ActiveRecord::Migrator
    def ddl_transaction(_migration = nil, &block)
      ActiveRecord::Base.connection.execute "set statement_timeout = 0"
      block.call
    end
  end
end
