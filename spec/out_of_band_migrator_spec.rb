require "spec_helper"

RSpec.describe PgHaMigrations::OutOfBandMigrator do
  describe "self.instructions" do
    it "has instructions" do
      instructions = PgHaMigrations::OutOfBandMigrator.instructions
      expect(instructions).to match(/migrations_state/)
      expect(instructions).to match(/blocking_database_transactions/)
      expect(instructions).to match(/migrate/)
      expect(instructions).to match(/exit/)
    end
  end
end
