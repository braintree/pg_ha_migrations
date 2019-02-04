class Test < ActiveRecord::Migration[4.2]
  def up
    create_table :foo, id: :bigserial do |t|
      t.text :bar, null: false
    end
  end
end
