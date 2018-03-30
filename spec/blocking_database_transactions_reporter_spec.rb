require "spec_helper"

RSpec.describe PgHaMigrations::BlockingDatabaseTransactionsReporter do
  describe "self.run" do
    before(:each) do
      @output = ""
      allow(PgHaMigrations::BlockingDatabaseTransactionsReporter).to receive(:_puts) do |msg|
        @output << msg
      end
    end

    it "Does not puts when no blocking transactions exist" do
      allow(PgHaMigrations::BlockingDatabaseTransactionsReporter).to receive(:get_blocking_transactions).and_return(
        {
          "Primary database" => [],
        }
      )
      expect(PgHaMigrations::BlockingDatabaseTransactionsReporter).to_not receive(:_puts)

      PgHaMigrations::BlockingDatabaseTransactionsReporter.run
    end

    it "prints the blocking transactions" do
      allow(PgHaMigrations::BlockingDatabaseTransactionsReporter).to receive(:get_blocking_transactions).and_return(
        {
          "Primary database" => [
            double(:description => "transaction1", :concurrent_index_creation? => false),
            double(:description => "transaction2", :concurrent_index_creation? => true),
          ],
        }
      )

      expect(PgHaMigrations::BlockingDatabaseTransactionsReporter).to receive(:_puts) do |message|
        expect(message).to match(/Potentially blocking transactions/)
        expect(message).to match(/Primary database:\s+transaction1\s+transaction2/m)
        expect(message).to match(/^\s+Warning: concurrent indexes/)
      end

      PgHaMigrations::BlockingDatabaseTransactionsReporter.run
    end

    it "returns a description of query and tables if something is running" do
      stub_const("PgHaMigrations::BlockingDatabaseTransactionsReporter::CHECK_DURATION", "1 second")

      thread_errors = Queue.new
      expect(ActiveRecord::Base.connection.tables).not_to include("foos1")
      expect(ActiveRecord::Base.connection.tables).not_to include("foos2")

      migration = Class.new(ActiveRecord::Migration::Current) do
        def up
          safe_create_table :foos1 do |t|
            t.timestamps :null => false
            t.text :text_column
          end
          safe_create_table :foos2 do |t|
            t.timestamps :null => false
            t.text :text_column
          end
        end
      end
      migration.suppress_messages { migration.migrate(:up) }

      mutex = Mutex.new
      thread = Thread.new do
        begin
          mutex.lock
          ActiveRecord::Base.connection.execute(<<-SQL)
            select pg_sleep(4)
            from (values (1)) t(n)
            left outer join foos1 on true
            left outer join foos2 on true
          SQL
        rescue => e
          thread_errors << e
        end
      end

      Thread.pass while !mutex.locked? && thread_errors.empty?
      sleep(2)

      begin
        expect(PgHaMigrations::BlockingDatabaseTransactionsReporter).to receive(:_puts) do |message|
          expect(message).to match(/Potentially blocking transactions/)
          database = "pg_ha_migrations_test"
          expect(message).to match(/Primary database:\n\s+#{database} | tables \(foos1, foos2\).*pg_sleep/m)
        end

        PgHaMigrations::BlockingDatabaseTransactionsReporter.run
      ensure
        thread.join
      end

      unless thread_errors.empty?
        raise thread_errors.pop
      end
    end
  end
end

