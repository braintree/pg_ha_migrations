module PgHaMigrations::UnsafeStatements
  def self.delegate_unsafe_method_to_connection(method_name)
    define_method("unsafe_#{method_name}") do |*args, &block|
      arg_list = args.map { |arg| arg.inspect }.join(', ')
      # say_with_time args taken from https://github.com/rails/rails/blob/4-2-stable/activerecord/lib/active_record/migration.rb#L654
      say_with_time "#{method_name}(#{arg_list})" do
        self.class.superclass.send(method_name, *args, &block)
      end
    end
  end

  delegate_unsafe_method_to_connection :create_table
  def create_table(name, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":create_table is NOT SAFE! Use safe_create_table instead")
  end

  delegate_unsafe_method_to_connection :add_column
  def add_column(table, column, type, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":add_column is NOT SAFE! Use safe_add_column instead")
  end

  delegate_unsafe_method_to_connection :change_table
  def change_table(name, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":change_table is NOT SAFE! Use a combination of safe and explicit unsafe migration methods instead")
  end

  delegate_unsafe_method_to_connection :drop_table
  def drop_table(name)
    raise PgHaMigrations::UnsafeMigrationError.new(":drop_table is NOT SAFE! Explicitly call :unsafe_drop_table to proceed")
  end

  delegate_unsafe_method_to_connection :rename_table
  def rename_table(old_name, new_name)
    raise PgHaMigrations::UnsafeMigrationError.new(":rename_table is NOT SAFE! Explicitly call :unsafe_rename_table to proceed")
  end

  delegate_unsafe_method_to_connection :rename_column
  def rename_column(table_name, column_name, new_column_name)
    raise PgHaMigrations::UnsafeMigrationError.new(":rename_column is NOT SAFE! Explicitly call :unsafe_rename_column to proceed")
  end

  delegate_unsafe_method_to_connection :change_column
  def change_column(table_name, column_name, type, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":change_column is NOT SAFE! Use a combination of safe and explicit unsafe migration methods instead")
  end

  def change_column_null(table_name, column_name, null, default = nil)
    raise PgHaMigrations::UnsafeMigrationError.new(<<-EOS.strip_heredoc)
    :change_column_null is NOT (guaranteed to be) SAFE! Either use :safe_make_column_nullable or explicitly call :unsafe_make_column_not_nullable to proceed
    EOS
  end

  delegate_unsafe_method_to_connection :remove_column
  def remove_column(table_name, column_name, type, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":remove_column is NOT SAFE! Explicitly call :unsafe_remove_column to proceed")
  end

  delegate_unsafe_method_to_connection :add_index
  def add_index(table_name, column_names, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":add_index is NOT SAFE! Use safe_add_concurrent_index instead")
  end

  delegate_unsafe_method_to_connection :execute
  def execute(sql, name = nil)
    raise PgHaMigrations::UnsafeMigrationError.new(":execute is NOT SAFE! Explicitly call :unsafe_execute to proceed")
  end

  delegate_unsafe_method_to_connection :remove_index
  def remove_index(table_name, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":remove_index is NOT SAFE! Use safe_remove_concurrent_index instead for Postgres 9.6 databases; Explicitly call :unsafe_remove_index to proceed on Postgres 9.1")
  end

  delegate_unsafe_method_to_connection :add_foreign_key
  def add_foreign_key(from_table, to_table, options)
    raise PgHaMigrations::UnsafeMigrationError.new(":add_foreign_key is NOT SAFE! Explicitly call :unsafe_add_foreign_key only if you have guidance from a migration reviewer in #service-app-db.")
  end
end
