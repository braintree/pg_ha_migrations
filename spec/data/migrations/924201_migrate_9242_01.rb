class Migrate924201 < ActiveRecord::Migration::Current
  def up
    safe_create_table :bar1 do |t|
      t.timestamps :null => false
      t.text :text_column
    end
    safe_create_table :bar2 do |t|
      t.timestamps :null => false
      t.text :text_column
    end
  end
end
