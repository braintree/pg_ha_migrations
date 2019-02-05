class Test < ActiveRecord::Migration[4.2]
  def up
    unsafe_rename_table :foo, :bar
  end
end
