require "spec_helper"

# Note: This test file is split out for speed purposes; lock acquisition tests
# are slow, and we aren't often modifying those methods, so in local
# development, we can run this test separately from the rest of the safe
# statements tests.
RSpec.describe PgHaMigrations::SafeStatements, "lock acquisition" do
  let(:migration_klass) { ActiveRecord::Migration::Current }

  ["bogus_table", :bogus_table, "public.bogus_table"].each do |table_name|
    describe "#safely_acquire_lock_for_table #{table_name} of type #{table_name.class.name}" do
      let(:alternate_connection_pool) do
        ActiveRecord::ConnectionAdapters::ConnectionPool.new(TestHelpers.pool_config)
      end
      let(:alternate_connection) do
        # The #connection method was deprecated in Rails 7.2 in favor of #lease_connection
        if alternate_connection_pool.respond_to?(:lease_connection)
          alternate_connection_pool.lease_connection
        else
          alternate_connection_pool.connection
        end
      end
      let(:alternate_connection_2) do
        # The #connection method was deprecated in Rails 7.2 in favor of #lease_connection
        if alternate_connection_pool.respond_to?(:lease_connection)
          alternate_connection_pool.lease_connection
        else
          alternate_connection_pool.connection
        end
      end
      let(:migration) { Class.new(migration_klass).new }

      before(:each) do
        ActiveRecord::Base.connection.execute(<<~SQL)
          CREATE TABLE #{table_name}(pk SERIAL, i INTEGER);
          CREATE TABLE #{table_name}_2(pk SERIAL, i INTEGER);
          CREATE SCHEMA partman;
          CREATE EXTENSION pg_partman SCHEMA partman;
        SQL
      end

      after(:each) do
        alternate_connection_pool.disconnect!
      end

      it "executes the block" do
        expect do |block|
          migration.safely_acquire_lock_for_table(table_name, &block)
        end.to yield_control
      end

      it "acquires an exclusive lock on the table by default" do
        migration.safely_acquire_lock_for_table(table_name) do
          expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table",
              lock_type: "AccessExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            )
          )
        end
      end

      it "acquires exclusive locks by default when multiple tables provided" do
        migration.safely_acquire_lock_for_table(table_name, "bogus_table_2") do
          expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table",
              lock_type: "AccessExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            )
          )

          expect(TestHelpers.locks_for_table("bogus_table_2", connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table_2",
              lock_type: "AccessExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            )
          )
        end
      end

      it "acquires a lock in a different mode when provided" do
        migration.safely_acquire_lock_for_table(table_name, mode: :share) do
          expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table",
              lock_type: "ShareLock",
              granted: true,
              pid: kind_of(Integer),
            )
          )
        end
      end

      it "acquires locks in a different mode when multiple tables and mode provided" do
        migration.safely_acquire_lock_for_table(table_name, "bogus_table_2", mode: :share_row_exclusive) do
          expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table",
              lock_type: "ShareRowExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            )
          )

          expect(TestHelpers.locks_for_table("bogus_table_2", connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table_2",
              lock_type: "ShareRowExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            )
          )
        end
      end

      it "raises error when invalid lock mode provided" do
        expect do
          migration.safely_acquire_lock_for_table(table_name, mode: :garbage) {}
        end.to raise_error(
          ArgumentError,
          "Unrecognized lock mode :garbage. Valid modes: [:access_share, :row_share, :row_exclusive, :share_update_exclusive, :share, :share_row_exclusive, :exclusive, :access_exclusive]"
        )
      end

      it "releases the lock even after an exception" do
        begin
          migration.safely_acquire_lock_for_table(table_name) do
            raise "bogus error"
          end
        rescue
          # Throw away error.
        end
        expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty
      end

      it "releases the lock even after a swallowed postgres exception" do
        migration.safely_acquire_lock_for_table(table_name) do
          expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table",
              lock_type: "AccessExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            ),
          )

          begin
            migration.connection.execute("SELECT * FROM garbage")
          rescue
          end

          expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty

          expect do
            migration.connection.execute("SELECT * FROM bogus_table")
          end.to raise_error(ActiveRecord::StatementInvalid, /PG::InFailedSqlTransaction/)
        end

        expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty
      end

      it "waits to acquire a lock if the table is already blocked" do
        block_call_count = 0
        expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions).exactly(3).times do |*args|
          # Verify that the method under test hasn't taken out a lock.
          expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty

          block_call_count += 1
          if block_call_count < 3
            [PgHaMigrations::BlockingDatabaseTransactions::LongRunningTransaction.new("", "", 5, "active", [["bogus_table", "public", "AccessExclusiveLock"]])]
          else
            []
          end
        end

        migration.suppress_messages do
          migration.safely_acquire_lock_for_table(table_name) do
            expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).not_to be_empty
          end
        end
      end

      it "times out the lock query after LOCK_TIMEOUT_SECONDS when multiple tables provided" do
        stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)
        stub_const("PgHaMigrations::LOCK_FAILURE_RETRY_DELAY_MULTLIPLIER", 0)
        allow(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions).and_return([])
        allow(ActiveRecord::Base.connection).to receive(:execute).and_call_original

        expect(ActiveRecord::Base.connection).to receive(:execute)
          .with("LOCK \"public\".\"bogus_table\", \"public\".\"bogus_table_2\" IN ACCESS EXCLUSIVE MODE;")
          .at_least(2)
          .times

        begin
          query_thread = Thread.new do
            alternate_connection.execute("BEGIN; LOCK bogus_table_2;")
            sleep 3
            alternate_connection.execute("ROLLBACK")
          end

          sleep 0.5

          migration.suppress_messages do
            migration.safely_acquire_lock_for_table(table_name, "bogus_table_2") do
              aggregate_failures do
                expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection_2)).not_to be_empty
                expect(TestHelpers.locks_for_table("bogus_table_2", connection: alternate_connection_2)).not_to be_empty
              end
            end
          end
        ensure
          query_thread.join
        end
      end

      it "does not wait to acquire a lock if the table has an existing but non-conflicting lock" do
        stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)

        begin
          thread = Thread.new do
            ActiveRecord::Base.connection.execute(<<~SQL)
              LOCK bogus_table IN EXCLUSIVE MODE;
              SELECT pg_sleep(2);
            SQL
          end

          sleep 1.1

          expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions)
            .once
            .and_call_original

          migration.suppress_messages do
            migration.safely_acquire_lock_for_table(table_name, mode: :access_share) do
              locks_for_table = TestHelpers.locks_for_table(table_name, connection: alternate_connection)

              aggregate_failures do
                expect(locks_for_table).to contain_exactly(
                  having_attributes(
                    table: "bogus_table",
                    lock_type: "ExclusiveLock",
                    granted: true,
                    pid: kind_of(Integer),
                  ),
                  having_attributes(
                    table: "bogus_table",
                    lock_type: "AccessShareLock",
                    granted: true,
                    pid: kind_of(Integer),
                  ),
                )

                expect(locks_for_table.first.pid).to_not eq(locks_for_table.last.pid)
              end
            end
          end
        ensure
          thread.join
        end
      end

      it "waits to acquire a lock if the table has an existing and conflicting lock" do
        stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)

        begin
          thread = Thread.new do
            ActiveRecord::Base.connection.execute(<<~SQL)
              LOCK bogus_table IN SHARE UPDATE EXCLUSIVE MODE;
              SELECT pg_sleep(3);
            SQL
          end

          sleep 1.1

          expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions)
            .at_least(2)
            .times
            .and_call_original

          migration.suppress_messages do
            migration.safely_acquire_lock_for_table(table_name, mode: :share_row_exclusive) do
              expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
                having_attributes(
                  table: "bogus_table",
                  lock_type: "ShareRowExclusiveLock",
                  granted: true,
                  pid: kind_of(Integer),
                )
              )
            end
          end
        ensure
          thread.join
        end
      end

      it "does not wait to acquire a lock if a table with the same name but in different schema is blocked" do
        stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)

        ActiveRecord::Base.connection.execute("CREATE TABLE partman.bogus_table(pk SERIAL, i INTEGER)")

        begin
          thread = Thread.new do
            ActiveRecord::Base.connection.execute(<<~SQL)
              LOCK partman.bogus_table;
              SELECT pg_sleep(2);
            SQL
          end

          sleep 1.1

          expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions)
            .once
            .and_call_original

          migration.suppress_messages do
            migration.safely_acquire_lock_for_table(table_name) do
              locks_for_table = TestHelpers.locks_for_table(table_name, connection: alternate_connection)
              locks_for_other_table = TestHelpers.locks_for_table("partman.bogus_table", connection: alternate_connection)

              aggregate_failures do
                expect(locks_for_table).to contain_exactly(
                  having_attributes(
                    table: "bogus_table",
                    lock_type: "AccessExclusiveLock",
                    granted: true,
                    pid: kind_of(Integer),
                  )
                )

                expect(locks_for_other_table).to contain_exactly(
                  having_attributes(
                    table: "bogus_table",
                    lock_type: "AccessExclusiveLock",
                    granted: true,
                    pid: kind_of(Integer),
                  )
                )

                expect(locks_for_table.first.pid).to_not eq(locks_for_other_table.first.pid)
              end
            end
          end
        ensure
          thread.join
        end
      end

      it "waits to acquire a lock if the table is partitioned and child table is blocked" do
        stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)

        ActiveRecord::Base.connection.drop_table(table_name)
        TestHelpers.create_range_partitioned_table(table_name, migration_klass, with_partman: true)

        begin
          thread = Thread.new do
            ActiveRecord::Base.connection.execute(<<~SQL)
              LOCK bogus_table_default;
              SELECT pg_sleep(3);
            SQL
          end

          sleep 1.1

          expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions)
            .at_least(2)
            .times
            .and_call_original

          migration.suppress_messages do
            migration.safely_acquire_lock_for_table(table_name) do
              locks_for_parent = TestHelpers.locks_for_table(table_name, connection: alternate_connection)
              locks_for_child = TestHelpers.locks_for_table("bogus_table_default", connection: alternate_connection)

              aggregate_failures do
                expect(locks_for_parent).to contain_exactly(
                  having_attributes(
                    table: "bogus_table",
                    lock_type: "AccessExclusiveLock",
                    granted: true,
                    pid: kind_of(Integer),
                  )
                )

                expect(locks_for_child).to contain_exactly(
                  having_attributes(
                    table: "bogus_table_default",
                    lock_type: "AccessExclusiveLock",
                    granted: true,
                    pid: kind_of(Integer),
                  )
                )

                expect(locks_for_parent.first.pid).to eq(locks_for_child.first.pid)
              end
            end
          end
        ensure
          thread.join
        end
      end

      it "waits to acquire a lock if the table is partitioned and child sub-partition is blocked" do
        stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)

        ActiveRecord::Base.connection.drop_table(table_name)
        TestHelpers.create_range_partitioned_table(table_name, migration_klass)
        TestHelpers.create_range_partitioned_table("#{table_name}_sub", migration_klass, with_partman: true)
        ActiveRecord::Base.connection.execute(<<~SQL)
          ALTER TABLE bogus_table
          ATTACH PARTITION bogus_table_sub
          FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
        SQL

        begin
          thread = Thread.new do
            ActiveRecord::Base.connection.execute(<<~SQL)
              LOCK bogus_table_sub_default;
              SELECT pg_sleep(3);
            SQL
          end

          sleep 1.1

          expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions)
            .at_least(2)
            .times
            .and_call_original

          migration.suppress_messages do
            migration.safely_acquire_lock_for_table(table_name) do
              locks_for_parent = TestHelpers.locks_for_table(table_name, connection: alternate_connection)
              locks_for_sub = TestHelpers.locks_for_table("bogus_table_sub_default", connection: alternate_connection)

              aggregate_failures do
                expect(locks_for_parent).to contain_exactly(
                  having_attributes(
                    table: "bogus_table",
                    lock_type: "AccessExclusiveLock",
                    granted: true,
                    pid: kind_of(Integer),
                  )
                )

                expect(locks_for_sub).to contain_exactly(
                  having_attributes(
                    table: "bogus_table_sub_default",
                    lock_type: "AccessExclusiveLock",
                    granted: true,
                    pid: kind_of(Integer),
                  )
                )

                expect(locks_for_parent.first.pid).to eq(locks_for_sub.first.pid)
              end
            end
          end
        ensure
          thread.join
        end
      end

      it "waits to acquire a lock if the table is non-natively partitioned and child table is blocked" do
        stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)

        ActiveRecord::Base.connection.execute(<<~SQL)
          CREATE TABLE bogus_table_child(pk SERIAL, i INTEGER) INHERITS (#{table_name})
        SQL

        begin
          thread = Thread.new do
            ActiveRecord::Base.connection.execute(<<~SQL)
              LOCK bogus_table_child;
              SELECT pg_sleep(3);
            SQL
          end

          sleep 1.1

          expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions)
            .at_least(2)
            .times
            .and_call_original

          migration.suppress_messages do
            migration.safely_acquire_lock_for_table(table_name) do
              locks_for_parent = TestHelpers.locks_for_table(table_name, connection: alternate_connection)
              locks_for_child = TestHelpers.locks_for_table("bogus_table_child", connection: alternate_connection)

              aggregate_failures do
                expect(locks_for_parent).to contain_exactly(
                  having_attributes(
                    table: "bogus_table",
                    lock_type: "AccessExclusiveLock",
                    granted: true,
                    pid: kind_of(Integer),
                  )
                )

                expect(locks_for_child).to contain_exactly(
                  having_attributes(
                    table: "bogus_table_child",
                    lock_type: "AccessExclusiveLock",
                    granted: true,
                    pid: kind_of(Integer),
                  )
                )

                expect(locks_for_parent.first.pid).to eq(locks_for_child.first.pid)
              end
            end
          end
        ensure
          thread.join
        end
      end

      it "fails lock acquisition quickly if Postgres doesn't grant an exclusive lock but then retries" do
        stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)

        expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions).exactly(2).times.and_return([])

        alternate_connection.execute("BEGIN; LOCK #{table_name};")

        lock_call_count = 0
        time_before_lock_calls = Time.now

        allow(ActiveRecord::Base.connection).to receive(:execute).at_least(:once).and_call_original
        expect(ActiveRecord::Base.connection).to receive(:execute).with("LOCK \"public\".\"bogus_table\" IN ACCESS EXCLUSIVE MODE;").exactly(2).times.and_wrap_original do |m, *args|
          lock_call_count += 1

          if lock_call_count == 2
            # Get rid of the lock we were holding.
            alternate_connection.execute("ROLLBACK;")
          end

          return_value = nil
          exception = nil
          begin
            return_value = m.call(*args)
          rescue => e
            exception = e
          end

          if lock_call_count == 1
            # First lock attempt should fail fast.
            expect(Time.now - time_before_lock_calls).to be >= 1.seconds
            expect(Time.now - time_before_lock_calls).to be < 5.seconds
            expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty

            expect(migration).to receive(:sleep).with(1 * PgHaMigrations::LOCK_FAILURE_RETRY_DELAY_MULTLIPLIER) # Stubbed seconds times multiplier
          else
            # Second lock attempt should succeed.
            expect(exception).not_to be_present
            expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).not_to be_empty
          end

          if exception
            raise exception
          else
            return_value
          end
        end

        expect do
          migration.safely_acquire_lock_for_table(table_name) { }
        end.to output(/Timed out trying to acquire ACCESS EXCLUSIVE lock.+"public"\."bogus_table"/m).to_stdout
      end

      it "doesn't kill a long running query inside of the lock" do
        stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)

        migration.safely_acquire_lock_for_table(table_name) do
          time_before_select_call = Time.now
          expect do
            ActiveRecord::Base.connection.execute("SELECT pg_sleep(3)")
          end.not_to raise_error
          time_after_select_call = Time.now

          expect(time_after_select_call - time_before_select_call).to be >= 3.seconds
        end
      end

      it "prints out helpful information when waiting for a lock" do
        blocking_queries_calls = 0
        expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions).exactly(2).times do |*args|
          blocking_queries_calls += 1
          if blocking_queries_calls == 1
            [PgHaMigrations::BlockingDatabaseTransactions::LongRunningTransaction.new("", "some_sql_query", "active", 5, [["bogus_table", "public", "AccessExclusiveLock"]])]
          else
            []
          end
        end

        expect do
          migration = Class.new(migration_klass) do
            class_attribute :table_name, instance_accessor: true

            self.table_name = table_name

            def up
              safely_acquire_lock_for_table(table_name) { }
            end
          end

          migration.migrate(:up)
        end.to output(/blocking transactions.+tables.+bogus_table.+some_sql_query/m).to_stdout
      end

      it "allows re-entrancy" do
        migration.safely_acquire_lock_for_table(table_name) do
          migration.safely_acquire_lock_for_table(table_name) do
            expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
              having_attributes(
                table: "bogus_table",
                lock_type: "AccessExclusiveLock",
                granted: true,
                pid: kind_of(Integer),
              ),
            )
          end

          expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table",
              lock_type: "AccessExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            ),
          )
        end

        expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty
      end

      it "allows re-entrancy when multiple tables provided" do
        migration.safely_acquire_lock_for_table(table_name, "bogus_table_2") do
          # The ordering of the args is intentional here to ensure
          # the array sorting and equality logic works as intended
          migration.safely_acquire_lock_for_table("bogus_table_2", table_name) do
            expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
              having_attributes(
                table: "bogus_table",
                lock_type: "AccessExclusiveLock",
                granted: true,
                pid: kind_of(Integer),
              ),
            )

            expect(TestHelpers.locks_for_table("bogus_table_2", connection: alternate_connection)).to contain_exactly(
              having_attributes(
                table: "bogus_table_2",
                lock_type: "AccessExclusiveLock",
                granted: true,
                pid: kind_of(Integer),
              ),
            )
          end

          expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table",
              lock_type: "AccessExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            ),
          )

          expect(TestHelpers.locks_for_table("bogus_table_2", connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table_2",
              lock_type: "AccessExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            ),
          )
        end

        expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty
        expect(TestHelpers.locks_for_table("bogus_table_2", connection: alternate_connection)).to be_empty
      end

      it "allows re-entrancy when multiple tables provided and nested lock targets a subset of tables" do
        migration.safely_acquire_lock_for_table(table_name, "bogus_table_2") do
          migration.safely_acquire_lock_for_table(table_name) do
            expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
              having_attributes(
                table: "bogus_table",
                lock_type: "AccessExclusiveLock",
                granted: true,
                pid: kind_of(Integer),
              ),
            )

            expect(TestHelpers.locks_for_table("bogus_table_2", connection: alternate_connection)).to contain_exactly(
              having_attributes(
                table: "bogus_table_2",
                lock_type: "AccessExclusiveLock",
                granted: true,
                pid: kind_of(Integer),
              ),
            )
          end

          expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table",
              lock_type: "AccessExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            ),
          )

          expect(TestHelpers.locks_for_table("bogus_table_2", connection: alternate_connection)).to contain_exactly(
            having_attributes(
              table: "bogus_table_2",
              lock_type: "AccessExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            ),
          )
        end

        expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty
        expect(TestHelpers.locks_for_table("bogus_table_2", connection: alternate_connection)).to be_empty
      end

      it "does not allow re-entrancy when multiple tables provided and nested lock targets a superset of tables" do
        expect do
          migration.safely_acquire_lock_for_table(table_name) do
            expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to contain_exactly(
              having_attributes(
                table: "bogus_table",
                lock_type: "AccessExclusiveLock",
                granted: true,
                pid: kind_of(Integer),
              ),
            )

            migration.safely_acquire_lock_for_table(table_name, "bogus_table_2") {}
          end
        end.to raise_error(
          PgHaMigrations::InvalidMigrationError,
          "Nested lock detected! Cannot acquire lock on \"public\".\"bogus_table\", \"public\".\"bogus_table_2\" while \"public\".\"bogus_table\" is locked."
        )

        expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty
        expect(TestHelpers.locks_for_table("bogus_table_2", connection: alternate_connection)).to be_empty
      end

      it "allows re-entrancy when inner lock is a lower level" do
        migration.safely_acquire_lock_for_table(table_name) do
          migration.safely_acquire_lock_for_table(table_name, mode: :exclusive) do
            locks_for_table = TestHelpers.locks_for_table(table_name, connection: alternate_connection)

            # We skip the actual ExclusiveLock acquisition in Postgres
            # since the parent lock is a higher level
            expect(locks_for_table).to contain_exactly(
              having_attributes(
                table: "bogus_table",
                lock_type: "AccessExclusiveLock",
                granted: true,
                pid: kind_of(Integer),
              ),
            )
          end

          locks_for_table = TestHelpers.locks_for_table(table_name, connection: alternate_connection)

          expect(locks_for_table).to contain_exactly(
            having_attributes(
              table: "bogus_table",
              lock_type: "AccessExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            ),
          )
        end

        expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty
      end

      it "allows re-entrancy when escalating inner lock but not the parent lock" do
        migration.safely_acquire_lock_for_table(table_name) do
          migration.safely_acquire_lock_for_table(table_name, mode: :share) do
            migration.safely_acquire_lock_for_table(table_name, mode: :exclusive) do
              locks_for_table = TestHelpers.locks_for_table(table_name, connection: alternate_connection)

              # We skip the actual ShareLock / ExclusiveLock acquisition
              # in Postgres since the parent lock is a higher level
              expect(locks_for_table).to contain_exactly(
                having_attributes(
                  table: "bogus_table",
                  lock_type: "AccessExclusiveLock",
                  granted: true,
                  pid: kind_of(Integer),
                ),
              )
            end
          end

          locks_for_table = TestHelpers.locks_for_table(table_name, connection: alternate_connection)

          expect(locks_for_table).to contain_exactly(
            having_attributes(
              table: "bogus_table",
              lock_type: "AccessExclusiveLock",
              granted: true,
              pid: kind_of(Integer),
            ),
          )
        end

        expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty
      end

      it "does not allow re-entrancy when lock escalation detected" do
        expect do
          migration.safely_acquire_lock_for_table(table_name, mode: :share) do
            expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).not_to be_empty

            # attempting a nested lock twice to ensure the
            # thread variable doesn't incorrectly get reset
            expect do
              migration.safely_acquire_lock_for_table(table_name, mode: :exclusive) {}
            end.to raise_error(
              PgHaMigrations::InvalidMigrationError,
              "Lock escalation detected! Cannot change lock level from :share to :exclusive for \"public\".\"bogus_table\"."
            )

            # the exception above was caught and therefore the parent lock shouldn't be released yet
            expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to_not be_empty

            migration.safely_acquire_lock_for_table(table_name, mode: :exclusive) {}
          end
        end.to raise_error(
          PgHaMigrations::InvalidMigrationError,
          "Lock escalation detected! Cannot change lock level from :share to :exclusive for \"public\".\"bogus_table\"."
        )

        expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty
      end

      it "skips blocking query check for nested lock acquisition" do
        stub_const("PgHaMigrations::LOCK_TIMEOUT_SECONDS", 1)

        query_thread = nil

        expect(PgHaMigrations::BlockingDatabaseTransactions).to receive(:find_blocking_transactions)
          .once
          .and_call_original

        begin
          migration.safely_acquire_lock_for_table(table_name) do
            query_thread = Thread.new { alternate_connection.execute("SELECT * FROM bogus_table") }

            sleep 2

            migration.safely_acquire_lock_for_table(table_name) do
              expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection_2)).to contain_exactly(
                having_attributes(
                  table: "bogus_table",
                  lock_type: "AccessExclusiveLock",
                  granted: true,
                  pid: kind_of(Integer),
                ),
                having_attributes(
                  table: "bogus_table",
                  lock_type: "AccessShareLock",
                  granted: false,
                  pid: kind_of(Integer),
                ),
              )
            end
          end
        ensure
          query_thread.join if query_thread
        end

        expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty
      end

      it "raises error when attempting nested lock on different table" do
        ActiveRecord::Base.connection.execute("CREATE TABLE foo(pk SERIAL, i INTEGER)")

        expect do
          migration.safely_acquire_lock_for_table(table_name) do
            expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).not_to be_empty

            # attempting a nested lock twice to ensure the
            # thread variable doesn't incorrectly get reset
            expect do
              migration.safely_acquire_lock_for_table("foo")
            end.to raise_error(
              PgHaMigrations::InvalidMigrationError,
              "Nested lock detected! Cannot acquire lock on \"public\".\"foo\" while \"public\".\"bogus_table\" is locked."
            )

            migration.safely_acquire_lock_for_table("foo")
          end
        end.to raise_error(
          PgHaMigrations::InvalidMigrationError,
          "Nested lock detected! Cannot acquire lock on \"public\".\"foo\" while \"public\".\"bogus_table\" is locked."
        )

        expect(TestHelpers.locks_for_table(table_name, connection: alternate_connection)).to be_empty
      end
    end
  end
end
