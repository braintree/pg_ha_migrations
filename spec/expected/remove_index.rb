class Test < ActiveRecord::Migration[4.2]
  def up
    safe_remove_concurrent_index name: :foo
  end
end
