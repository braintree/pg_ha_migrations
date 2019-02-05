class Test < ActiveRecord::Migration[4.2]
  def up
    drop_table :foo
  end
end
