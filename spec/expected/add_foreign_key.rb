class Test < ActiveRecord::Migration[4.2]
  def up
    unsafe_add_foreign_key :foo, :bar, column: :column_a
  end
end
