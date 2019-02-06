class Test < ActiveRecord::Migration[4.2]
  def up
    safe_add_column :foo, :column_a, :string
    unsafe_add_column :foo, :column_a, :string, null: false
  end
end
