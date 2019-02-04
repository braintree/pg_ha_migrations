class Test < ActiveRecord::Migration[4.2]
  def up
    add_column :foo, :column_a, :string
  end
end
