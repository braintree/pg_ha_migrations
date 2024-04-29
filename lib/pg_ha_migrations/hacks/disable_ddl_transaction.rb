require "active_record/migration"

module PgHaMigrations
  module ActiveRecordHacks
    module DisableDdlTransaction
      def disable_ddl_transaction
        # Default to disabled unless someone has set it elsewhere
        defined?(@disable_ddl_transaction) ? @disable_ddl_transaction : PgHaMigrations.config.disable_ddl_transactions_by_default
      end
    end
  end
end

ActiveRecord::Migration.singleton_class.prepend(PgHaMigrations::ActiveRecordHacks::DisableDdlTransaction)
