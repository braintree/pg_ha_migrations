class Test < ActiveRecord::Migration[4.2]
  def up
    rename_column :foo, :column_a, :column_b
  end
end
