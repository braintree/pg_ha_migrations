class Test < ActiveRecord::Migration[4.2]
  def up
    unsafe_execute 'GRANT SELECT ON foo TO bar_ro'
    unsafe_execute("ALTER TABLE baz ALTER COLUMN created_at SET DEFAULT now()")
    unsafe_execute(<<-SQL)
    GRANT SELECT ON this_is_a_very_long_table_name_too_long_to_not_use_a_docstring TO execute
    SQL
  end
end
