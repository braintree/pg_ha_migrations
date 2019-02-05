require "spec_helper"

RSpec.describe PgHaMigrations do
  it "disables ddl transactions" do
    expect(ActiveRecord::Migration.disable_ddl_transaction).to be_truthy
  end
end
