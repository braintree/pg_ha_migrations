class Test < ActiveRecord::Migration[4.2]
  def up
    unsafe_remove_column :foo, :bar
  end
end
