require "spec_helper"

RSpec.describe PgHaMigrations::ActiveRecordHacks::DisableDdlTransaction do
  describe 'default configuration' do
    it 'sets disable_ddl_transaction to true' do
      expect(ActiveRecord::Migration.disable_ddl_transaction).to eq(true)
    end
  end

  describe 'always_disable_ddl_transactions set to false' do
    before do
      PgHaMigrations.configure do |config|
        config.always_disable_ddl_transactions = false
      end
    end

    after do
      PgHaMigrations.configure do |config|
        config.always_disable_ddl_transactions = true
      end
    end

    it 'sets always_disable_ddl_transactions to false' do
      expect(ActiveRecord::Migration.disable_ddl_transaction).to eq(false)
    end
  end
end
