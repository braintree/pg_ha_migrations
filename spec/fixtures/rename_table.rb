class Test < ActiveRecord::Migration[4.2]
  def up
    rename_table :foo, :bar
  end
end
