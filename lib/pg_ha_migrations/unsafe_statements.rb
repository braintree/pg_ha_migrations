module PgHaMigrations::UnsafeStatements
  def self.disable_or_delegate_default_method(method_name, error_message, allow_reentry_from_compatibility_module: false)
    define_method(method_name) do |*args, &block|
      if PgHaMigrations.config.check_for_dependent_objects
        disallow_migration_method_if_dependent_objects!(method_name, arguments: args)
      end

      if PgHaMigrations.config.disable_default_migration_methods
        # Most migration methods are only ever called by a migration and
        # therefore aren't re-entrant or callable from another migration
        # method, but `execute` is called directly by at least one of the
        # implementations in `ActiveRecord::Migration::Compatibility` so
        # we have to explicitly handle that case by allowing execution of
        # the original implementation by its original name.
        unless  allow_reentry_from_compatibility_module && caller[0] =~ /lib\/active_record\/migration\/compatibility.rb/
          raise PgHaMigrations::UnsafeMigrationError, error_message
        end
      end

      execute_ancestor_statement(method_name, *args, &block)
    end
  end

  def self.delegate_unsafe_method_to_migration_base_class(method_name)
    define_method("unsafe_#{method_name}") do |*args, &block|
      if PgHaMigrations.config.check_for_dependent_objects
        disallow_migration_method_if_dependent_objects!(method_name, arguments: args)
      end

      execute_ancestor_statement(method_name, *args, &block)
    end
  end

  delegate_unsafe_method_to_migration_base_class :add_column
  delegate_unsafe_method_to_migration_base_class :change_table
  delegate_unsafe_method_to_migration_base_class :drop_table
  delegate_unsafe_method_to_migration_base_class :rename_table
  delegate_unsafe_method_to_migration_base_class :rename_column
  delegate_unsafe_method_to_migration_base_class :change_column
  delegate_unsafe_method_to_migration_base_class :remove_column
  delegate_unsafe_method_to_migration_base_class :add_index
  delegate_unsafe_method_to_migration_base_class :execute
  delegate_unsafe_method_to_migration_base_class :remove_index
  delegate_unsafe_method_to_migration_base_class :add_foreign_key

  disable_or_delegate_default_method :create_table, ":create_table is NOT SAFE! Use safe_create_table instead"
  disable_or_delegate_default_method :add_column, ":add_column is NOT SAFE! Use safe_add_column instead"
  disable_or_delegate_default_method :change_table, ":change_table is NOT SAFE! Use a combination of safe and explicit unsafe migration methods instead"
  disable_or_delegate_default_method :drop_table, ":drop_table is NOT SAFE! Explicitly call :unsafe_drop_table to proceed"
  disable_or_delegate_default_method :rename_table, ":rename_table is NOT SAFE! Explicitly call :unsafe_rename_table to proceed"
  disable_or_delegate_default_method :rename_column, ":rename_column is NOT SAFE! Explicitly call :unsafe_rename_column to proceed"
  disable_or_delegate_default_method :change_column, ":change_column is NOT SAFE! Use a combination of safe and explicit unsafe migration methods instead"
  disable_or_delegate_default_method :change_column_null, ":change_column_null is NOT (guaranteed to be) SAFE! Either use :safe_make_column_nullable or explicitly call :unsafe_make_column_not_nullable to proceed"
  disable_or_delegate_default_method :remove_column, ":remove_column is NOT SAFE! Explicitly call :unsafe_remove_column to proceed"
  disable_or_delegate_default_method :add_index, ":add_index is NOT SAFE! Use safe_add_concurrent_index instead"
  disable_or_delegate_default_method :execute, ":execute is NOT SAFE! Explicitly call :unsafe_execute to proceed", allow_reentry_from_compatibility_module: true
  disable_or_delegate_default_method :remove_index, ":remove_index is NOT SAFE! Use safe_remove_concurrent_index instead for Postgres 9.6 databases; Explicitly call :unsafe_remove_index to proceed on Postgres 9.1"
  disable_or_delegate_default_method :add_foreign_key, ":add_foreign_key is NOT SAFE! Explicitly call :unsafe_add_foreign_key only if you have guidance from a migration reviewer in #service-app-db."

  def unsafe_create_table(table, options={}, &block)
    if options[:force] && !PgHaMigrations.config.allow_force_create_table
      raise PgHaMigrations::UnsafeMigrationError.new(":force is NOT SAFE! Explicitly call unsafe_drop_table first if you want to recreate an existing table")
    end

    execute_ancestor_statement(:create_table, table, options, &block)
  end

  def execute_ancestor_statement(method_name, *args, &block)
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
