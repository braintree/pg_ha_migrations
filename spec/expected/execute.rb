class Test < ActiveRecord::Migration[4.2]
  def up
    unsafe_execute "GRANT SELECT ON foo TO bar_ro"
    unsafe_execute("ALTER TABLE baz ALTER COLUMN created_at SET DEFAULT now()")
  end
end
