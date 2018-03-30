require 'stringio'

module PgHaMigrations
  class BlockingDatabaseTransactionsReporter
    CHECK_DURATION = "30 seconds"

    def self.run
      blocking_transactions = get_blocking_transactions
      has_transactions = blocking_transactions.values.flatten.present?
      _puts(report(blocking_transactions)) if has_transactions
    end

    def self.report(transactions)
      report = StringIO.new
      report << "Potentially blocking transactions:\n"
      transactions.each do |db_description, blocking_transactions|
        report << "#{db_description}:\n"
        if blocking_transactions.empty?
          report << "\t(no long running transactions)\n\n"
        else
          blocking_transactions.each do |transaction|
            report << "\t#{transaction.description}\n\n"
          end
        end

        if blocking_transactions.any?(&:concurrent_index_creation?)
          report << <<-eos.strip_heredoc.lines.map { |line| "\t#{line}" }.join
            Warning: concurrent indexes are currently being built. If you have any other
                     migrations in this deploy that will attempt to create additional
                     concurrent indexes on the same physical database (even if the table
                     being indexes is on another dimension) those migrations will not be
                     able to complete until the in-progress index creations finish.

                     For more information, see #service-db in Slack.\n
          eos
          report << "\n" # Blank line intentional
        end
      end
      report.string
    end

    def self.get_blocking_transactions
      {
        "Primary database" => PgHaMigrations::BlockingDatabaseTransactions.find_blocking_transactions(CHECK_DURATION)
      }
    end

    def self._puts(msg)
      puts msg
    end
  end
end
