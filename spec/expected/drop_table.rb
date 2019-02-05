class Test < ActiveRecord::Migration[4.2]
  def up
    unsafe_drop_table :foo
  end
end
