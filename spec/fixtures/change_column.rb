class Test < ActiveRecord::Migration[4.2]
  def up
    change_column :foo, :column_a, :string, null: false
  end
end
