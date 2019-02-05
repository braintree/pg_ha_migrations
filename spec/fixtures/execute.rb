class Test < ActiveRecord::Migration[4.2]
  def up
    execute "GRANT SELECT ON foo TO bar_ro"
    execute("ALTER TABLE baz ALTER COLUMN created_at SET DEFAULT now()")
  end
end
