module PgHaMigrations
  module UnrunMigrations
    def self.report(migration_files_path)
      migrations = unrun_migrations(migration_files_path)
      "Unrun migrations:\n" +
        migrations.map { |migration| migration[:version] }.join("\n")
    end

    def self.unrun_migrations(migration_files_path)
      _expected_migrations(migration_files_path) - _actual_migrations(migration_files_path)
    end

    def self._expected_migrations(migration_files_path)
      expected_migrations = []
      _migration_files(migration_files_path).each do |migration_file|
        version = _migration_version(migration_file)

        expected_migrations << {
          :version => version,
        }
      end
      expected_migrations
    end

    def self._actual_migrations(migration_files_path)
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

    def self._migration_files(migration_files_path)
      Dir.glob("#{migration_files_path}/*")
    end
  end
end
