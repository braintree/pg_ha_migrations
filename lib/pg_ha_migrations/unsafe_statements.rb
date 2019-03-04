module PgHaMigrations::UnsafeStatements
  def self.delegate_unsafe_method_to_migration_base_class(method_name)
    define_method("unsafe_#{method_name}") do |*args, &block|
      # Dispatching here is a bit complicated: we need to execute the method
      # belonging to the first member of the inheritance chain (besides
      # UnsafeStatements). If don't find the method in the inheritance chain,
      # we need to rely on the ActiveRecord::Migration#method_missing
      # implementation since much of ActiveRecord::Migration's functionality
      # is not implemented in real methods but rather by proxying.
      #
      # For example, ActiveRecord::Migration doesn't define #create_table.
      # Instead ActiveRecord::Migration#method_missing proxies the method
      # to the connection. However some migration compatibility version
      # subclasses _do_ explicitly define #create_table, so we can't rely
      # on only one way of finding the proper dispatch target.

      # Exclude our `raise` guard implementations.
      ancestors_without_unsafe_statements = self.class.ancestors - [PgHaMigrations::UnsafeStatements]

      delegate_method = self.method(method_name)
      candidate_method = delegate_method

      # Find the first usable method in the ancestor chain
      # or stop looking if there are no more possible
      # implementations.
      until candidate_method.nil? || ancestors_without_unsafe_statements.include?(candidate_method.owner)
        candidate_method = candidate_method.super_method
      end

      if candidate_method
        delegate_method = candidate_method
      end

      # If we failed to find a concrete implementation from the
      # inheritance chain, use ActiveRecord::Migrations# method_missing
      # otherwise use the method from the inheritance chain.
      if delegate_method.owner == PgHaMigrations::UnsafeStatements
        method_missing(method_name, *args, &block)
      else
        delegate_method.call(*args, &block)
      end
    end
  end

  delegate_unsafe_method_to_migration_base_class :create_table
  def create_table(name, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":create_table is NOT SAFE! Use safe_create_table instead")
  end

  delegate_unsafe_method_to_migration_base_class :add_column
  def add_column(table, column, type, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":add_column is NOT SAFE! Use safe_add_column instead")
  end

  delegate_unsafe_method_to_migration_base_class :change_table
  def change_table(name, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":change_table is NOT SAFE! Use a combination of safe and explicit unsafe migration methods instead")
  end

  delegate_unsafe_method_to_migration_base_class :drop_table
  def drop_table(name)
    raise PgHaMigrations::UnsafeMigrationError.new(":drop_table is NOT SAFE! Explicitly call :unsafe_drop_table to proceed")
  end

  delegate_unsafe_method_to_migration_base_class :rename_table
  def rename_table(old_name, new_name)
    raise PgHaMigrations::UnsafeMigrationError.new(":rename_table is NOT SAFE! Explicitly call :unsafe_rename_table to proceed")
  end

  delegate_unsafe_method_to_migration_base_class :rename_column
  def rename_column(table_name, column_name, new_column_name)
    raise PgHaMigrations::UnsafeMigrationError.new(":rename_column is NOT SAFE! Explicitly call :unsafe_rename_column to proceed")
  end

  delegate_unsafe_method_to_migration_base_class :change_column
  def change_column(table_name, column_name, type, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":change_column is NOT SAFE! Use a combination of safe and explicit unsafe migration methods instead")
  end

  def change_column_null(table_name, column_name, null, default = nil)
    raise PgHaMigrations::UnsafeMigrationError.new(<<-EOS.strip_heredoc)
    :change_column_null is NOT (guaranteed to be) SAFE! Either use :safe_make_column_nullable or explicitly call :unsafe_make_column_not_nullable to proceed
    EOS
  end

  delegate_unsafe_method_to_migration_base_class :remove_column
  def remove_column(table_name, column_name, type, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":remove_column is NOT SAFE! Explicitly call :unsafe_remove_column to proceed")
  end

  delegate_unsafe_method_to_migration_base_class :add_index
  def add_index(table_name, column_names, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":add_index is NOT SAFE! Use safe_add_concurrent_index instead")
  end

  delegate_unsafe_method_to_migration_base_class :execute
  def execute(sql, name = nil)
    if caller[0] =~ /lib\/active_record\/migration\/compatibility.rb/
      super
    else
      raise PgHaMigrations::UnsafeMigrationError.new(":execute is NOT SAFE! Explicitly call :unsafe_execute to proceed")
    end
  end

  delegate_unsafe_method_to_migration_base_class :remove_index
  def remove_index(table_name, options={})
    raise PgHaMigrations::UnsafeMigrationError.new(":remove_index is NOT SAFE! Use safe_remove_concurrent_index instead for Postgres 9.6 databases; Explicitly call :unsafe_remove_index to proceed on Postgres 9.1")
  end

  delegate_unsafe_method_to_migration_base_class :add_foreign_key
  def add_foreign_key(from_table, to_table, options)
    raise PgHaMigrations::UnsafeMigrationError.new(":add_foreign_key is NOT SAFE! Explicitly call :unsafe_add_foreign_key only if you have guidance from a migration reviewer in #service-app-db.")
  end
end
