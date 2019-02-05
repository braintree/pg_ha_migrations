class Test < ActiveRecord::Migration[4.2]
  def up
    remove_index name: :foo
  end
end
