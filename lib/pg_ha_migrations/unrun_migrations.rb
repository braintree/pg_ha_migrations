module PgHaMigrations
  module UnrunMigrations
    def self.unrun_migrations(suffix)
      _expected_migrations(suffix) - _actual_migrations(suffix)
    end

    def self._expected_migrations(suffix)
      expected_migrations = []
      _migration_files(suffix).each do |migration_file|
        version = _migration_version(migration_file)

        expected_migrations << {
          :version => version,
        }
      end
      expected_migrations
    end

    def self._actual_migrations(suffix)
      actual_migrations = []
      ActiveRecord::Base.connection.migration_context.get_all_versions.each do |version|
        actual_migrations << {
          :version => version.to_s
        }
      end
      actual_migrations
    end

    def self._migration_version(migration_file)
      migration_file.split("/").last.split("_").first
    end

    def self._migration_files(target, suffix)
    end
  end
end
