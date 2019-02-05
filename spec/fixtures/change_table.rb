class Test < ActiveRecord::Migration[4.2]
  def up
    change_table :foo do |t|
      t.timestamps null: true
    end
  end
end
