module PgHaMigrations
  class MigrationModifier
    attr_reader :directory

    def initialize(directory='db/migrate')
      @directory = Dir.new(directory)
    end

    def modify!
      directory.each do |file_name|
        file_path = File.join(directory.path, file_name)
        next unless File.file?(file_path)
        file_contents = File.read(file_path)
        File.open(file_path, 'w') do |file|
          file_contents.gsub!(/ create_table /, ' safe_create_table ')
          file_contents.gsub!(/ add_column /, ' safe_add_column ')
          file_contents.gsub!(/ (add|remove)_index /, ' safe_\1_concurrent_index ')
          file_contents.gsub!(/,? algorithm: :concurrently/, '')
          file_contents.gsub!(/ ((change|drop|rename)_table) /, ' unsafe_\1 ')
          file_contents.gsub!(/ ((change|remove|rename)_column) /, ' unsafe_\1 ')
          file_contents.gsub!(/ (add_foreign_key) /, ' unsafe_\1 ')
          file_contents.gsub!(/ change_column_null (.*), false$/, ' unsafe_make_column_not_nullable \1')
          file_contents.gsub!(/ change_column_null (.*), true$/, ' safe_make_column_nullable \1')
          file_contents.gsub!(/ change_column_default /, ' safe_change_column_default ')
          file_contents.gsub!(/ execute( |\()"/, ' unsafe_execute\1"')
          file.write(file_contents)
        end
      end
      # Readme warning
    end
  end
end
