require "spec_helper"

RSpec.describe PgHaMigrations do
  it "has a version number" do
    expect(PgHaMigrations::VERSION).not_to be nil
  end
end
