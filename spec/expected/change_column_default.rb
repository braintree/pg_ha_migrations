class Test < ActiveRecord::Migration[4.2]
  def up
    safe_change_column_default :foo, :column_a, false
  end
end
