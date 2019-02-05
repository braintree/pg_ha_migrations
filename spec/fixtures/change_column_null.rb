class Test < ActiveRecord::Migration[4.2]
  def up
    change_column_null :foo, :column_a, false
    change_column_null :bar, :column_b, true
  end
end
