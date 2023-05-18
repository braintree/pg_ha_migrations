class PgHaMigrations::PartmanConfig < ActiveRecord::Base
  self.primary_key = :parent_table

  def self.schema=(schema)
    self.table_name = "#{schema}.part_config"
  end
end
