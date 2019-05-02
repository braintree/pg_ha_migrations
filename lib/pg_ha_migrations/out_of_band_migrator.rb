module PgHaMigrations
  class OutOfBandMigrator
    def initialize(migration_files_path, stdout=STDOUT)
      @migration_files_path = migration_files_path
      @stdout = stdout
    end

    def run
      _puts(migrations_state)
      _puts(blocking_database_transactions)
      _puts(instructions)

      while command = Readline.readline('> ', true)
        parsed_command = parse_command(command)
        if should_exit?(parsed_command[0])
          _puts("exiting...")
          break
        end

        adjusted_command = parsed_command.reject{ |command| command =~ /version/i }
        execute_command(adjusted_command)
      end
    end

    def migrations_state
      unrun_migrations = PgHaMigrations::UnrunMigrations.unrun_migrations(@migration_files_path)
      if unrun_migrations.any?
        unrun_migrations_report = PgHaMigrations::UnrunMigrations.report(@migration_files_path)
      else
        unrun_migrations_report = "No unrun out-of-band migrations."
      end
      """
        ========================= Out Of Band Migrations To Be Run =========================
        #{unrun_migrations_report.lines.map { |line| "\t#{line}" }.join(" ")}
        ====================================================================================
      """
    end

    def instructions
      """
        =================================== Instructions ===================================
        print blocking_database_transactions - Print blocking database transactions
        print migrations_state               - Print all non-deployed migrations
        print instructions                   - Print this message
        #{migrate_command}
        exit                                 - Exit the Out of Band Tactical Command Center
        ====================================================================================
      """
    end

    def blocking_database_transactions
      blocking_transactions = PgHaMigrations::BlockingDatabaseTransactionsReporter.get_blocking_transactions
      report = PgHaMigrations::BlockingDatabaseTransactionsReporter.report(blocking_transactions)
      """
        ========================== Blocking Database Transactions ==========================
        #{report.lines.map{ |line| "\t#{line}" }.join(" ")}
        ====================================================================================
      """
    end

    def parse_command(command_string)
      if command_string.blank?
        ["exit"]
      else
        command_string.split
      end
    end

    def migrate_command
      "migrate <version>                 - Run a migration, e.g. `migrate 24603`"
    end

    def should_exit?(command)
      command == "exit"
    end

    def execute_command(command)
      cmd = command.shift
      args = command
      case cmd
      when "print"
        execute_print(args)
      when "migrate"
        execute_migrate(args)
      else
        _puts argument_error
      end
    end

    def execute_migrate(args)
      target_version = args.first.to_i
      migration_to_run = context.migrations.find {|m| m.version == target_version }
      unless migration_to_run
        _puts "Migration #{target_version} does not exist in #{@migration_files_path}."
        return
      end
      PgHaMigrations::LongRunningMigrator.new(:up, [migration_to_run]).migrate
    end

    def context
      @migration_context ||= ActiveRecord::MigrationContext.new(@migration_files_path)
    end

    def execute_print(args)
      print_command = args.shift
      case print_command
      when "migrations_state"
        _puts migrations_state
      when "blocking_database_transactions"
        _puts blocking_database_transactions
      when "instructions"
        _puts instructions
      else
        _puts argument_error
      end
    end

    def argument_error
      "Unknown command.\n\n" + instructions
    end

    def _prompt
      @stdout.print "> "
    end

    def _puts(msg)
      @stdout.puts msg
    end
  end
end
