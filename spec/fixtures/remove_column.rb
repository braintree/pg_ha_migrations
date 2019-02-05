class Test < ActiveRecord::Migration[4.2]
  def up
    remove_column :foo, :bar
  end
end
