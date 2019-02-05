class Test < ActiveRecord::Migration[4.2]
  def up
    safe_create_table :foo, id: :bigserial do |t|
      t.text :bar, null: false
    end
  end
end
