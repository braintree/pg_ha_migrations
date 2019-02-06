require "active_record/migration"

module ActiveRecordHack
  module DisableDdlTransaction
    def disable_ddl_transaction
      # Default to disabled unless someone has set it elsewhere
      defined?(@disable_ddl_transaction) ? @disable_ddl_transaction : true
    end
  end
end

ActiveRecord::Migration.singleton_class.prepend(ActiveRecordHack::DisableDdlTransaction)

