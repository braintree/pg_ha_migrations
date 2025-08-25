require "spec_helper"

# Note: This test file is split out for speed purposes; partman renaming tests
# are slow, and we aren't often modifying those methods, so in local
# development, we can run this test separately from the rest of the safe
# statements tests.
RSpec.describe PgHaMigrations::SafeStatements, "partman renaming" do
  let(:migration_klass) { ActiveRecord::Migration::Current }

  describe "#safe_partman_standardize_partition_naming" do
    describe "when extension not installed" do
      it "raises error" do
        migration = Class.new(migration_klass) do
          def up
            safe_partman_standardize_partition_naming :foos3
          end
        end

        expect do
          migration.suppress_messages { migration.migrate(:up) }
        end.to raise_error(PgHaMigrations::MissingExtensionError, "The pg_partman extension is not installed")
      end
    end

    describe "when extension installed" do
      let(:partman_extension) { PgHaMigrations::Extension.new("pg_partman") }

      before do
        TestHelpers.install_partman

        skip "Tests only relevant for partman 4" unless partman_extension.major_version == 4
      end

      # The data below is to drive testing of renaming partitions of different intervals. The
      # tests are the same for every interval. This enables us to reuse several tests many times.
      {
        "2 years" => {
          interval_code: "P2Y",
          source_datetime_string: "YYYY",
          source_name_pattern: /^foos3_p\d{4}$/,
          target_datetime_string: "YYYYMMDD",
          target_table_name_pattern: /^foos3_p\d{4}0101$/,
        },
        "yearly" => {
          interval_code: "P1Y",
          source_datetime_string: "YYYY",
          source_name_pattern: /^foos3_p\d{4}$/,
          target_datetime_string: "YYYYMMDD",
          target_table_name_pattern: /^foos3_p\d{4}0101$/,
        },
        "6 months" => {
          interval_code: "P6M",
          source_datetime_string: "YYYY_MM",
          source_name_pattern: /^foos3_p\d{4}_\d{2}$/,
          target_datetime_string: "YYYYMMDD",
          target_table_name_pattern: /^foos3_p\d{6}01$/,
        },
        "quarterly" => {
          interval_code: "P3M",
          source_datetime_string: "YYYY\"q\"Q",
          source_name_pattern: /^foos3_p\d{4}q\d{1}$/,
          target_datetime_string: "YYYYMMDD",
          target_table_name_pattern: /^foos3_p\d{4}(01|04|07|10)01$/,
          example_bad_table_name: "foos3_p1999q5",
        },
        "monthly" => {
          interval_code: "P1M",
          source_datetime_string: "YYYY_MM",
          source_name_pattern: /^foos3_p\d{4}_\d{2}$/,
          target_datetime_string: "YYYYMMDD",
          target_table_name_pattern: /^foos3_p\d{6}01$/,
        },
        "15 days" => {
          interval_code: "P15D",
          source_datetime_string: "YYYY_MM_DD",
          source_name_pattern: /^foos3_p\d{4}_\d{2}_\d{2}$/,
          target_datetime_string: "YYYYMMDD",
          target_table_name_pattern: /^foos3_p\d{8}$/,
        },
        "weekly" => {
          interval_code: "P7D",
          source_datetime_string: "IYYY\"w\"IW",
          source_name_pattern: /^foos3_p\d{4}w\d{2}$/,
          target_datetime_string: "YYYYMMDD",
          target_table_name_pattern: /^foos3_p\d{8}$/,
        },
        "daily" => {
          interval_code: "P1D",
          source_datetime_string: "YYYY_MM_DD",
          source_name_pattern: /^foos3_p\d{4}_\d{2}_\d{2}$/,
          target_datetime_string: "YYYYMMDD",
          target_table_name_pattern: /^foos3_p\d{8}$/,
        },
        "12 hours" =>  {
          interval_code: "PT12H",
          source_datetime_string: "YYYY_MM_DD_HH24MI",
          source_name_pattern: /^foos3_p\d{4}_\d{2}_\d{2}_\d{4}$/,
          target_datetime_string: "YYYYMMDD_HH24MISS",
          target_table_name_pattern: /^foos3_p\d{8}_\d{6}$/,
        },
        "hourly" =>  {
          interval_code: "PT1H",
          source_datetime_string: "YYYY_MM_DD_HH24MI",
          source_name_pattern: /^foos3_p\d{4}_\d{2}_\d{2}_\d{4}$/,
          target_datetime_string: "YYYYMMDD_HH24MISS",
          target_table_name_pattern: /^foos3_p\d{8}_\d{6}$/,
        },
        "half-hour" => {
          interval_code: "PT30M",
          source_datetime_string: "YYYY_MM_DD_HH24MI",
          source_name_pattern: /^foos3_p\d{4}_\d{2}_\d{2}_\d{4}$/,
          target_datetime_string: "YYYYMMDD_HH24MISS",
          target_table_name_pattern: /^foos3_p\d{8}_\d{6}$/,
        },
        "quarter-hour" => {
          interval_code: "PT15M",
          source_datetime_string: "YYYY_MM_DD_HH24MI",
          source_name_pattern: /^foos3_p\d{4}_\d{2}_\d{2}_\d{4}$/,
          target_datetime_string: "YYYYMMDD_HH24MISS",
          target_table_name_pattern: /^foos3_p\d{8}_\d{6}$/,
        },
        "1 minute" => {
          interval_code: "PT1M",
          source_datetime_string: "YYYY_MM_DD_HH24MI",
          source_name_pattern: /^foos3_p\d{4}_\d{2}_\d{2}_\d{4}$/,
          target_datetime_string: "YYYYMMDD_HH24MISS",
          target_table_name_pattern: /^foos3_p\d{8}_\d{6}$/,
        },
        "30 seconds" => {
          interval_code: "PT30S",
          source_datetime_string: "YYYY_MM_DD_HH24MISS",
          source_name_pattern: /^foos3_p\d{4}_\d{2}_\d{2}_\d{6}$/,
          target_datetime_string: "YYYYMMDD_HH24MISS",
          target_table_name_pattern: /^foos3_p\d{8}_\d{6}$/,
        },
        "1 second" => {
          interval_code: "PT1S",
          source_datetime_string: "YYYY_MM_DD_HH24MISS",
          source_name_pattern: /^foos3_p\d{4}_\d{2}_\d{2}_\d{6}$/,
          target_datetime_string: "YYYYMMDD_HH24MISS",
          target_table_name_pattern: /^foos3_p\d{8}_\d{6}$/,
        },
      }.each do |interval, expectations|
        context "with #{interval.inspect} interval" do
          it "renames tables and maintenance continues to function" do
            TestHelpers.create_range_partitioned_table(
              :foos3,
              migration_klass,
              with_partman: true,
              interval: interval
            )

            migration = Class.new(migration_klass) do
              def up
                safe_partman_standardize_partition_naming :foos3
              end
            end

            before_part_config = TestHelpers.part_config("public.foos3")

            expect(before_part_config).to have_attributes(
              partition_interval: expectations[:interval_code],
              datetime_string: expectations[:source_datetime_string],
              partition_type: "native",
              automatic_maintenance: "on",
            )

            expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
              .to all(match(expectations[:source_name_pattern]))

            allow(ActiveRecord::Base.connection).to receive(:exec_update).and_call_original
            allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

            expect(ActiveRecord::Base.connection).to receive(:exec_update).with(
              /UPDATE "public"."part_config" SET "automatic_maintenance"/,
              anything,
              [having_attributes(value: "off"), having_attributes(value: "public.foos3")],
            ).once.ordered

            expect(ActiveRecord::Base.connection).to receive(:execute)
              .with(/LOCK "public"\."foos3", "public"\."part_config" IN ACCESS EXCLUSIVE MODE/)
              .once.ordered

            expect do
              migration.migrate(:up)
            end.to output(
              /partman_standardize_partition_naming\(:foos3, statement_timeout: 1\) - Renaming \d+ partition\(s\)/
            ).to_stdout

            after_part_config = TestHelpers.part_config("public.foos3")

            expect(after_part_config).to have_attributes(
              partition_interval: expectations[:interval_code],
              datetime_string: expectations[:target_datetime_string],
              partition_type: "native",
              automatic_maintenance: "on",
            )

            child_tables = TestHelpers.partitions_for_table(:foos3, exclude_default: true)

            expect(child_tables).to all(match(expectations[:target_table_name_pattern]))

            TestHelpers.part_config("public.foos3").update!(premake: 10)

            # create additional child partitions
            ActiveRecord::Base.connection.execute("CALL public.run_maintenance_proc()")

            new_child_tables = TestHelpers.partitions_for_table(:foos3, exclude_default: true)

            expect(new_child_tables.size).to be > child_tables.size

            expect(new_child_tables).to all(match(expectations[:target_table_name_pattern]))
          end

          it "raises error when partition name in unexpected format" do
            TestHelpers.create_range_partitioned_table(
              :foos3,
              migration_klass,
              with_partman: true,
              interval: interval
            )

            migration = Class.new(migration_klass) do
              def up
                safe_partman_standardize_partition_naming :foos3
              end
            end

            before_part_config = TestHelpers.part_config("public.foos3")

            expect(before_part_config).to have_attributes(
              partition_interval: expectations[:interval_code],
              datetime_string: expectations[:source_datetime_string],
              partition_type: "native",
              automatic_maintenance: "on",
            )

            before_partition_names = TestHelpers.partitions_for_table(:foos3, exclude_default: true)

            expect(before_partition_names).to all(match(expectations[:source_name_pattern]))

            bad_table_name = expectations.fetch(:example_bad_table_name, "bar_pabcdefg")

            ActiveRecord::Base.connection.execute(<<~SQL)
              ALTER TABLE #{before_partition_names[2]} RENAME TO #{bad_table_name}
            SQL

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(
              PgHaMigrations::InvalidIdentifierError,
              /Expected "#{bad_table_name}" to match \/.+\//
            )

            after_part_config = TestHelpers.part_config("public.foos3")

            expect(after_part_config).to have_attributes(
              partition_interval: expectations[:interval_code],
              datetime_string: expectations[:source_datetime_string],
              partition_type: "native",
              automatic_maintenance: "on",
            )

            after_partition_names = TestHelpers.partitions_for_table(:foos3, exclude_default: true)
            expect(after_partition_names).to include(bad_table_name)

            unchanged_partitions = after_partition_names - [bad_table_name]
            expect(unchanged_partitions).to all(match(expectations[:source_name_pattern]))
          end

          it "raises error when partition datetime string is unexpected" do
            TestHelpers.create_range_partitioned_table(
              :foos3,
              migration_klass,
              with_partman: true,
              interval: interval
            )

            migration = Class.new(migration_klass) do
              def up
                safe_partman_standardize_partition_naming :foos3
              end
            end

            TestHelpers.part_config("public.foos3").update!(datetime_string: "bad_date")

            before_part_config = TestHelpers.part_config("public.foos3")

            expect(before_part_config).to have_attributes(
              partition_interval: expectations[:interval_code],
              datetime_string: "bad_date",
              partition_type: "native",
              automatic_maintenance: "on",
            )

            expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
              .to all(match(expectations[:source_name_pattern]))

            expect do
              migration.suppress_messages { migration.migrate(:up) }
            end.to raise_error(
              PgHaMigrations::InvalidPartConfigError,
              "Expected datetime_string to be #{expectations[:source_datetime_string].inspect} " \
              "but received \"bad_date\""
            )

            after_part_config = TestHelpers.part_config("public.foos3")

            expect(after_part_config).to have_attributes(
              partition_interval: expectations[:interval_code],
              datetime_string: "bad_date",
              partition_type: "native",
              automatic_maintenance: "on",
            )

            expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
              .to all(match(expectations[:source_name_pattern]))
          end
        end
      end

      it "raises error when partition type is unexpected" do
        TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)

        migration = Class.new(migration_klass) do
          def up
            safe_partman_standardize_partition_naming :foos3
          end
        end

        TestHelpers.part_config("public.foos3").update!(partition_type: "partman")

        before_part_config = TestHelpers.part_config("public.foos3")

        expect(before_part_config).to have_attributes(
          partition_interval: "P7D",
          datetime_string: "IYYY\"w\"IW",
          partition_type: "partman",
          automatic_maintenance: "on",
        )

        expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
          .to all(match(/^foos3_p\d{4}w\d{2}$/))

        expect do
          migration.suppress_messages { migration.migrate(:up) }
        end.to raise_error(
          PgHaMigrations::InvalidPartConfigError,
          "Expected partition_type to be in [\"native\", \"range\"] but received \"partman\""
        )

        after_part_config = TestHelpers.part_config("public.foos3")

        expect(after_part_config).to have_attributes(
          partition_interval: "P7D",
          datetime_string: "IYYY\"w\"IW",
          partition_type: "partman",
          automatic_maintenance: "on",
        )

        expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
          .to all(match(/^foos3_p\d{4}w\d{2}$/))
      end

      it "raises error when complex partition interval provided" do
        TestHelpers.create_range_partitioned_table(
          :foos3,
          migration_klass,
          with_partman: true,
          interval: "1 year, 6 months",
        )

        migration = Class.new(migration_klass) do
          def up
            safe_partman_standardize_partition_naming :foos3
          end
        end

        before_part_config = TestHelpers.part_config("public.foos3")

        expect(before_part_config).to have_attributes(
          partition_interval: "P1Y6M",
          datetime_string: "YYYY",
          partition_type: "native",
          automatic_maintenance: "on",
        )

        expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
          .to all(match(/^foos3_p\d{4}$/))

        expect do
          migration.suppress_messages { migration.migrate(:up) }
        end.to raise_error(
          PgHaMigrations::InvalidPartConfigError,
          "Partition renaming for complex partition_interval \"P1Y6M\" not supported"
        )

        after_part_config = TestHelpers.part_config("public.foos3")

        expect(after_part_config).to have_attributes(
          partition_interval: "P1Y6M",
          datetime_string: "YYYY",
          partition_type: "native",
          automatic_maintenance: "on",
        )

        expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
          .to all(match(/^foos3_p\d{4}$/))
      end

      it "raises error when partition interval less than 1 second" do
        TestHelpers.create_range_partitioned_table(:foos3, migration_klass, with_partman: true)

        migration = Class.new(migration_klass) do
          def up
            safe_partman_standardize_partition_naming :foos3
          end
        end

        TestHelpers.part_config("public.foos3").update!(partition_interval: "PT0.5S")

        before_part_config = TestHelpers.part_config("public.foos3")

        expect(before_part_config).to have_attributes(
          partition_interval: "PT0.5S",
          datetime_string: "IYYY\"w\"IW",
          partition_type: "native",
          automatic_maintenance: "on",
        )

        expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
          .to all(match(/^foos3_p\d{4}w\d{2}$/))

        expect do
          migration.suppress_messages { migration.migrate(:up) }
        end.to raise_error(
          PgHaMigrations::InvalidPartConfigError,
          "Expected partition_interval to be greater than 1 second but received \"PT0.5S\""
        )

        after_part_config = TestHelpers.part_config("public.foos3")

        expect(after_part_config).to have_attributes(
          partition_interval: "PT0.5S",
          datetime_string: "IYYY\"w\"IW",
          partition_type: "native",
          automatic_maintenance: "on",
        )

        expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
          .to all(match(/^foos3_p\d{4}w\d{2}$/))
      end

      it "raises error and rolls back transaction when statement timeout exceeded" do
        TestHelpers.create_range_partitioned_table(
          :foos3,
          migration_klass,
          with_partman: true,
          interval: "1 year",
        )

        migration = Class.new(migration_klass) do
          def up
            safe_partman_standardize_partition_naming :foos3, statement_timeout: 0.1
          end
        end

        before_part_config = TestHelpers.part_config("public.foos3")

        TestHelpers.part_config("public.foos3").update!(premake: 1_000)

        ActiveRecord::Base.connection.execute("CALL public.run_maintenance_proc()")

        expect(before_part_config).to have_attributes(
          partition_interval: "P1Y",
          datetime_string: "YYYY",
          partition_type: "native",
          automatic_maintenance: "on",
        )

        expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
          .to all(match(/^foos3_p\d{4}$/))

        expect do
          migration.suppress_messages { migration.migrate(:up) }
        end.to raise_error(ActiveRecord::StatementInvalid, /statement timeout/)

        after_part_config = TestHelpers.part_config("public.foos3")

        expect(after_part_config).to have_attributes(
          partition_interval: "P1Y",
          datetime_string: "YYYY",
          partition_type: "native",
          automatic_maintenance: "on",
        )

        expect(TestHelpers.partitions_for_table(:foos3, exclude_default: true))
          .to all(match(/^foos3_p\d{4}$/))
      end
    end
  end
end
