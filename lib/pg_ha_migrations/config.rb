module PgHaMigrations
  class Config
    attr_accessor :disable_default_migration_methods,
                  :disable_ddl_transactions

    def initialize(disable_default_migration_methods:,
                   disable_ddl_transactions:)
      @disable_default_migration_methods = disable_default_migration_methods
      @disable_ddl_transactions = disable_ddl_transactions
    end
  end
end
