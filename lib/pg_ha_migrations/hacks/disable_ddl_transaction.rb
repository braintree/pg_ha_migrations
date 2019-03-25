require "active_record/migration"

module PgHaMigrations
  module ActiveRecordHacks
    module DisableDdlTransaction
      def disable_ddl_transaction
        # Use configured default unless someone has set it elsewhere
        if defined?(@disable_ddl_transaction)
          @disable_ddl_transaction
        else
          PgHaMigrations.config.disable_ddl_transactions
        end
      end
    end
  end
end

ActiveRecord::Migration.singleton_class.prepend(PgHaMigrations::ActiveRecordHacks::DisableDdlTransaction)

