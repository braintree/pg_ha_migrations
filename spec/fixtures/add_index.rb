class Test < ActiveRecord::Migration[4.2]
  def up
    add_index :foo, :column_a, algorithm: :concurrently
  end
end
