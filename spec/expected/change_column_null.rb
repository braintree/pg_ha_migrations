class Test < ActiveRecord::Migration[4.2]
  def up
    unsafe_make_column_not_nullable :foo, :column_a
    safe_make_column_nullable :bar, :column_b
  end
end
