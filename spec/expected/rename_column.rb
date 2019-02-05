class Test < ActiveRecord::Migration[4.2]
  def up
    unsafe_rename_column :foo, :column_a, :column_b
  end
end
