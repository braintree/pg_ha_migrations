# This is an internal class that is not meant to be used directly
class PgHaMigrations::PartmanConfig < ActiveRecord::Base
  self.primary_key = :parent_table

  def self.find(parent_table, partman_extension:)
    unless partman_extension.installed?
      raise PgHaMigrations::MissingExtensionError, "The pg_partman extension is not installed"
    end

    self.table_name = "#{partman_extension.quoted_schema}.part_config"

    super(parent_table)
  end
end
