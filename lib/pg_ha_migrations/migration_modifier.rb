module PgHaMigrations
  class MigrationModifier
    attr_reader :directory

    def initialize(directory="db/migrate")
      @directory = Dir.new(directory)
    end

    def modify!
      directory.each do |file_name|
        file_path = File.join(directory.path, file_name)
        next unless File.file?(file_path)
        file_contents = File.read(file_path)
        File.open(file_path, 'w') do |file|
          file_contents.sub!(/ create_table /, " safe_create_table ")
          file_contents.sub!(/ add_column /, " safe_add_column ")
          file_contents.sub!(/ add_index /, " safe_add_concurrent_index ")
          file_contents.sub!(/,? algorithm: :concurrently/, "")
          file.write(file_contents)
        end
      end
      # Readme warning
    end
  end
end
