module PgHaMigrations
  class Config
    attr_accessor :disable_default_migration_methods

    def initialize(disable_default_migration_methods:)
      @disable_default_migration_methods = disable_default_migration_methods
    end
  end
end
