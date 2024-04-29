require "spec_helper"

RSpec.describe PgHaMigrations::ActiveRecordHacks::DisableDdlTransaction do
  describe 'default configuration' do
    it 'sets disable_ddl_transaction to true' do
      expect(ActiveRecord::Migration.disable_ddl_transaction).to eq(true)
    end
  end

  describe 'disable_ddl_transactions_by_default set to false' do
    before do
      PgHaMigrations.configure do |config|
        config.disable_ddl_transactions_by_default = false
      end
    end

    after do
      PgHaMigrations.configure do |config|
        config.disable_ddl_transactions_by_default = true
      end
    end

    it 'sets disable_ddl_transactions_by_default to false' do
      expect(ActiveRecord::Migration.disable_ddl_transaction).to eq(false)
    end
  end
end
