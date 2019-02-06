class Test < ActiveRecord::Migration[4.2]
  def up
    add_column :foo, :column_a, :string
    add_column :foo, :column_a, :string, null: false
  end
end
