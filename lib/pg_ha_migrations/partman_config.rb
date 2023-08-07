# This is an internal class that is not meant to be used directly
class PgHaMigrations::PartmanConfig < ActiveRecord::Base
  self.primary_key = :parent_table

  # This method is called by unsafe_partman_update_config to set the fully
  # qualified table name, as partman is often installed in a schema that
  # is not included the application's search path
  def self.schema=(schema)
    self.table_name = "#{schema}.part_config"
  end
end
