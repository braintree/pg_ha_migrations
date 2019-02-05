class Test < ActiveRecord::Migration[4.2]
  def up
    safe_add_concurrent_index :foo, :column_a
  end
end
