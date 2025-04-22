# PgHaMigrations

[![Build Status](https://github.com/braintree/pg_ha_migrations/actions/workflows/ci.yml/badge.svg)](https://github.com/braintree/pg_ha_migrations/actions/workflows/ci.yml?query=branch%3Amaster+)

We've documented our learned best practices for applying schema changes without downtime in the post [PostgreSQL at Scale: Database Schema Changes Without Downtime](https://medium.com/paypal-tech/postgresql-at-scale-database-schema-changes-without-downtime-20d3749ed680) on the [PayPal Technology Blog](https://medium.com/paypal-tech). Many of the approaches we take and choices we've made are explained in much greater depth there than in this README.

Internally we apply those best practices to our Rails applications through this gem which updates ActiveRecord migrations to clearly delineate safe and unsafe DDL as well as provide safe alternatives where possible.

Some projects attempt to hide complexity by having code determine the intent and magically do the right series of operations. But we (and by extension this gem) take the approach that it's better to understand exactly what the database is doing so that (particularly long running) operations are not a surprise during your deploy cycle.

Provided functionality:
- [Migrations](#migrations)
- [Utilities](#utilities)
- [Rake Tasks](#rake-tasks)


## Installation

Add this line to your application's Gemfile:

```ruby
gem 'pg_ha_migrations'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install pg_ha_migrations

## Migration Safety

There are two major classes of concerns we try to handle in the API:

- Database safety (e.g., long-held locks)
- Application safety (e.g., dropping columns the app uses)

### Migration Method Renaming

We rename migration methods with prefixes to explicitly denote their safety level:

- `safe_*`: These methods check for both application and database safety concerns, prefer concurrent operations where available, set low lock timeouts where appropriate, and decompose operations into multiple safe steps.
- `unsafe_*`: Using these methods is a signal that the DDL operation is not necessarily safe for a running application. They include basic safety features like safe lock acquisition and dependent object checking, but otherwise dispatch directly to the native ActiveRecord migration method.
- `raw_*`: These methods are a direct dispatch to the native ActiveRecord migration method.

Calling the original migration methods without a prefix will raise an error.

The API is designed to be explicit yet remain flexible. There may be situations where invoking the `unsafe_*` method is preferred (or the only option available for definitionally unsafe operations).

While `unsafe_*` methods were historically (before 2.0) pure wrappers for invoking the native ActiveRecord migration method, there is a class of problems that we can't handle easily without breaking that design rule a bit. For example, dropping a column is unsafe from an application perspective, so we make the application safety concerns explicit by using an `unsafe_` prefix. Using `unsafe_remove_column` calls out the need to audit the application to confirm the migration won't break the application. Because there are no safe alternatives we don't define a `safe_remove_column` analogue. However there are still conditions we'd like to assert before dropping a column. For example, dropping an unused column that's used in one or more indexes may be safe from an application perspective, but the cascading drop of the index won't use a `CONCURRENT` operation to drop the dependent indexes and is therefore unsafe from a database perspective.

For `unsafe_*` migration methods which support checks of this type you can bypass the checks by passing an `:allow_dependent_objects` key in the method's `options` hash containing an array of dependent object types you'd like to allow. These checks will run by default, but you can opt-out by setting `config.check_for_dependent_objects = false` [in your configuration initializer](#configuration).

### Disallowed Migration Methods

We disallow the use of `unsafe_change_table`, as the equivalent operation can be composed with explicit `safe_*` / `unsafe_*` methods. If you _must_ use `change_table`, it is still available as `raw_change_table`.

### Migration Method Arguments

We believe the `force: true` option to ActiveRecord's `create_table` method is always unsafe because it's not possible to denote exactly how the current state will change. Therefore we disallow using `force: true` even when calling `unsafe_create_table`. This option is enabled by default, but you can opt-out by setting `config.allow_force_create_table = true` [in your configuration initializer](#configuration).

### Rollback

Because we require that ["Rollback strategies do not involve reverting the database schema to its previous version"](https://medium.com/paypal-tech/postgresql-at-scale-database-schema-changes-without-downtime-20d3749ed680#360a), PgHaMigrations does not support ActiveRecord's automatic migration rollback capability.

Instead we write all of our migrations with only an `def up` method like:

```
def up
  safe_add_column :table, :column
end
```

and never use `def change`. We believe that this is the only safe approach in production environments. For development environments we iterate by recreating the database from scratch every time we make a change.

### Transactional DDL

Individual DDL statements in PostgreSQL are transactional by default (as are all Postgres statements). Concurrent index creation and removal are two exceptions: these utility commands manage their own transaction state (and each uses multiple transactions to achieve the desired concurrency).

We [disable ActiveRecord's DDL transactions](./lib/pg_ha_migrations/hacks/disable_ddl_transaction.rb) (which wrap the entire migration file in a transaction) by default for the following reasons:

* [Running multiple DDL statements inside a transaction acquires exclusive locks on all of the modified objects](https://medium.com/paypal-tech/postgresql-at-scale-database-schema-changes-without-downtime-20d3749ed680#cc22).
* Acquired locks are held until the end of the transaction.
* Multiple locks creates the possibility of deadlocks.
* Increased exposure to long waits:
  * Each newly acquired lock has its own timeout applied (so total lock time is additive).
  * [Safe lock acquisition](#safely_acquire_lock_for_table) (which is used in each migration method where locks will be acquired) can issue multiple lock attempts on lock timeouts (with sleep delays between attempts).

Because of the above issues attempting to re-enable transaction migrations forfeits many of the safety guarantees this library provides and may even break certain functionally. If you'd like to experiment with it anyway you can re-enable transactional migrations by adding `self.disable_ddl_transaction = false` to your migration class definition.

## Usage

### Unsupported ActiveRecord Features

The following functionality is currently unsupported:

- [Rollback methods in migrations](#rollback)
- Generators
- schema.rb

### Compatibility Notes

- While some features may work with other versions, this gem is currently tested against PostgreSQL 13+ and Partman 4.x

### Migration Methods

#### safe\_create\_table

Safely creates a new table.

```ruby
safe_create_table :table do |t|
  t.type :column
end
```

#### safe\_create\_enum\_type

Safely create a new enum without values.

```ruby
safe_create_enum_type :enum
```
Or, safely create the enum with values.
```ruby
safe_create_enum_type :enum, ["value1", "value2"]
```

#### safe\_add\_enum\_value

Safely add a new enum value.

```ruby
safe_add_enum_value :enum, "value"
```

#### unsafe\_rename\_enum\_value

Unsafely change the value of an enum type entry.

```ruby
unsafe_rename_enum_value(:enum, "old_value", "new_value")
```

> **Note:** Changing an enum value does not issue any long-running scans or acquire locks on usages of the enum type. Therefore multiple queries within a transaction concurrent with the change may see both the old and new values. To highlight these potential pitfalls no `safe_rename_enum_value` equivalent exists. Before modifying an enum type entry you should verify that no concurrently executing queries will attempt to write the old value and that read queries understand the new value.

#### safe\_add\_column

Safely add a column.

```ruby
safe_add_column :table, :column, :type
```

#### unsafe\_add\_column

Unsafely add a column, but do so with a lock that is safely acquired.

```ruby
unsafe_add_column :table, :column, :type
```

#### safe\_change\_column\_default

Safely change the default value for a column.

```ruby
# Constant value:
safe_change_column_default :table, :column, "value"
safe_change_column_default :table, :column, DateTime.new(...)
# Functional expression evaluated at row insert time:
safe_change_column_default :table, :column, -> { "NOW()" }
# Functional expression evaluated at migration time:
safe_change_column_default :table, :column, -> { "'NOW()'" }
```

> **Note:** On Postgres 11+ adding a column with a constant default value does not rewrite or scan the table (under a lock or otherwise). In that case a migration adding a column with a default should do so in a single operation rather than the two-step `safe_add_column` followed by `safe_change_column_default`. We enforce this best practice with the error `PgHaMigrations::BestPracticeError`, but if your prefer otherwise (or are running in a mixed Postgres version environment), you may opt out by setting `config.prefer_single_step_column_addition_with_default = false` [in your configuration initializer](#configuration).

#### safe\_make\_column\_nullable

Safely make the column nullable.

```ruby
safe_make_column_nullable :table, :column
```
#### safe\_make\_column\_not\_nullable

Safely make the column not nullable. This method uses a `CHECK column IS NOT NULL` constraint to validate no values are null before altering the column. If such a constraint exists already, it is re-used, if it does not, a temporary constraint is added. Whether or not the constraint already existed, the constraint will be validated, if necessary, and removed after the column is marked `NOT NULL`.

```ruby
safe_make_column_not_nullable :table, :column
```

> **Note:**
> - This method may perform a full table scan to validate that no NULL values exist in the column. While no exclusive lock is held for this scan, on large tables the scan may take a long time.
> - The method runs multiple DDL statements non-transactionally. Validating the constraint can fail. In such cases an INVALID constraint will be left on the table. Calling `safe_make_column_not_nullable` again is safe.

If you want to avoid a full table scan and have already added and validated a suitable CHECK constraint, consider using [`safe_make_column_not_nullable_from_check_constraint`](#safe_make_column_not_nullable_from_check_constraint) instead.

#### unsafe\_make\_column\_not\_nullable

Unsafely make a column not nullable.

```ruby
unsafe_make_column_not_nullable :table, :column
```

#### safe\_make\_column\_not\_nullable\_from\_check\_constraint

Variant of `safe_make_column_not_nullable` that safely makes a column NOT NULL using an existing validated CHECK constraint that enforces non-null values for the column. This method is expected to always be fast because it avoids a full table scan.

```ruby
safe_make_column_not_nullable_from_check_constraint :table, :column, constraint_name: :constraint_name
```

- `constraint_name` (required): The name of a validated CHECK constraint that enforces `column IS NOT NULL`.
- `drop_constraint:` (optional, default: true): Whether to drop the constraint after making the column NOT NULL.

You should use [`safe_make_column_not_nullable`](#safe_make_column_not_nullable) when neither a CHECK constraint or a NOT NULL constraint exists already. You should use this method when you already have an equivalent CHECK constraint on the table.

This method will raise an error if the constraint does not exist, is not validated, or does not strictly enforce non-null values for the column.

> **Note:** We do not attempt to catch all possible proofs of `column IS NOT NULL` by means of an existing constraint; only a constraint with the exact definition `column IS NOT NULL` will be recognized.

#### safe\_add\_index\_on\_empty\_table

Safely add an index on a table with zero rows. This will raise an error if the table contains data.

```ruby
safe_add_index_on_empty_table :table, :column
```

#### safe\_add\_concurrent\_index

Add an index concurrently.

```ruby
safe_add_concurrent_index :table, :column
```

Add a composite btree index.

```ruby
safe_add_concurrent_index :table, [:column1, :column2], name: "index_name", using: :btree
```

#### safe\_remove\_concurrent\_index

Safely remove an index. Migrations that contain this statement must also include `disable_ddl_transaction!`.

```ruby
safe_remove_concurrent_index :table, :name => :index_name
```

#### safe\_add\_concurrent\_partitioned\_index

Add an index to a natively partitioned table concurrently, as described in the [table partitioning docs](https://www.postgresql.org/docs/current/ddl-partitioning.html):

> To avoid long lock times, it is possible to use `CREATE INDEX ON ONLY` the partitioned table; such an index is marked invalid, and the partitions do not get the index applied automatically.
> The indexes on partitions can be created individually using `CONCURRENTLY`, and then attached to the index on the parent using `ALTER INDEX .. ATTACH PARTITION`.
> Once indexes for all partitions are attached to the parent index, the parent index is marked valid automatically.

```ruby
# Assuming this table has partitions child1 and child2, the following indexes will be created:
#   - index_partitioned_table_on_column
#   - index_child1_on_column (attached to index_partitioned_table_on_column)
#   - index_child2_on_column (attached to index_partitioned_table_on_column)
safe_add_concurrent_partitioned_index :partitioned_table, :column
```

Add a composite index using the `hash` index type with custom name for the parent index when the parent table contains sub-partitions.

```ruby
# Assuming this table has partitions child1 and child2, and child1 has sub-partitions sub1 and sub2,
# the following indexes will be created:
#   - custom_name_idx
#   - index_child1_on_column1_column2 (attached to custom_name_idx)
#   - index_sub1_on_column1_column2 (attached to index_child1_on_column1_column2)
#   - index_sub2_on_column1_column2 (attached to index_child1_on_column1_column2)
#   - index_child2_on_column1_column2 (attached to custom_name_idx)
safe_add_concurrent_partitioned_index :partitioned_table, [:column1, :column2], name: "custom_name_idx", using: :hash
```

> **Note:**
> This method runs multiple DDL statements non-transactionally.
> Creating or attaching an index on a child table could fail.
> In such cases an exception will be raised, and an `INVALID` index will be left on the parent table.

#### safe\_add\_unvalidated\_check\_constraint

Safely add a `CHECK` constraint. The constraint will not be immediately validated on existing rows to avoid a full table scan while holding an exclusive lock. After adding the constraint, you'll need to use `safe_validate_check_constraint` to validate existing rows.

```ruby
safe_add_unvalidated_check_constraint :table, "column LIKE 'example%'", name: :constraint_table_on_column_like_example
```

#### safe\_validate\_check\_constraint

Safely validate (without acquiring an exclusive lock) existing rows for a newly added but as-yet unvalidated `CHECK` constraint.

```ruby
safe_validate_check_constraint :table, name: :constraint_table_on_column_like_example
```

#### safe\_rename\_constraint

Safely rename any (not just `CHECK`) constraint.

```ruby
safe_rename_constraint :table, from: :constraint_table_on_column_like_typo, to: :constraint_table_on_column_like_example
```

#### unsafe\_remove\_constraint

Drop any (not just `CHECK`) constraint.

```ruby
unsafe_remove_constraint :table, name: :constraint_table_on_column_like_example
```

#### safe\_create\_partitioned\_table

Safely create a new partitioned table using [declaritive partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE).

```ruby
# list partitioned table using single column as partition key
safe_create_partitioned_table :table, type: :list, partition_key: :example_column do |t|
  t.text :example_column, null: false
end

# range partitioned table using multiple columns as partition key
safe_create_partitioned_table :table, type: :range, partition_key: [:example_column_a, :example_column_b] do |t|
  t.integer :example_column_a, null: false
  t.integer :example_column_b, null: false
end

# hash partitioned table using expression as partition key
safe_create_partitioned_table :table, :type: :hash, partition_key: ->{ "(example_column::date)" } do |t|
  t.datetime :example_column, null: false
end
```

The identifier column type is `bigserial` by default. This can be overridden, as you would in `safe_create_table`, by setting the `id` argument:

```ruby
safe_create_partitioned_table :table, id: :serial, type: :range, partition_key: :example_column do |t|
  t.date :example_column, null: false
end
```

In PostgreSQL 11+, primary key constraints are supported on partitioned tables given the partition key is included. On supported versions, the primary key is inferred by default (see [available options](#available-options)). This functionality can be overridden by setting the `infer_primary_key` argument.

```ruby
# primary key will be (id, example_column)
safe_create_partitioned_table :table, type: :range, partition_key: :example_column do |t|
  t.date :example_column, null: false
end

# primary key will not be created
safe_create_partitioned_table :table, type: :range, partition_key: :example_column, infer_primary_key: false do |t|
  t.date :example_column, null: false
end
```

#### safe\_partman\_create\_parent

Safely configure a partitioned table to be managed by [pg\_partman](https://github.com/pgpartman/pg_partman).

This method calls the [create\_parent](https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md#creation-functions) partman function with some reasonable defaults and a subset of user-defined overrides.

The first (and only) positional argument maps to `p_parent_table` in the `create_parent` function.

The rest are keyword args with the following mappings:

- `partition_key` -> `p_control`. Required: `true`
- `interval` -> `p_interval`. Required: `true`
- `template_table` -> `p_template_table`. Required: `false`. Partman will create a template table if not defined.
- `premake` -> `p_premake`. Required: `false`. Partman defaults to `4`.
- `start_partition` -> `p_start_partition`. Required: `false`. Partman defaults to the current timestamp.

> **Note:** We have chosen to require PostgreSQL 11+ and hardcode `p_type` to `native` for simplicity, as previous PostgreSQL versions are end-of-life.

Additionally, this method allows you to configure a subset of attributes on the record stored in the [part\_config](https://github.com/pgpartman/pg_partman/blob/master/doc/pg_partman.md#tables) table.
These options are delegated to the `unsafe_partman_update_config` method to update the record:

- `infinite_time_partitions`. Partman defaults this to `false` but we default to `true`
- `inherit_privileges`. Partman defaults this to `false` but we default to `true`
- `retention`. Partman defaults this to `null`
- `retention_keep_table`. Partman defaults this to `true`

With only the required args:

```ruby
safe_create_partitioned_table :table, type: :range, partition_key: :created_at do |t|
  t.timestamps null: false
end

safe_partman_create_parent :table, partition_key: :created_at, interval: "weekly"
```

With custom overrides:

```ruby
safe_create_partitioned_table :table, type: :range, partition_key: :created_at do |t|
  t.timestamps null: false
  t.text :some_column
end

# Partman will reference the template table to create unique indexes on child tables
safe_create_table :table_template, id: false do |t|
  t.text :some_column, index: {unique: true}
end

safe_partman_create_parent :table,
  partition_key: :created_at,
  interval: "weekly",
  template_table: :table_template,
  premake: 10,
  start_partition: Time.current + 1.month,
  infinite_time_partitions: false,
  inherit_privileges: false,
  retention: "60 days",
  retention_keep_table: false
```

#### safe\_partman\_update\_config

There are some partitioning options that cannot be set in the call to `create_parent` and are only available in the `part_config` table.
As mentioned previously, you can specify these args in the call to `safe_partman_create_parent` which will be delegated to this method.
Calling this method directly will be useful if you need to modify your partitioned table after the fact.

Allowed keyword args:

- `infinite_time_partitions`
- `inherit_privileges`
- `premake`
- `retention`
- `retention_keep_table`

> **Note:** If `inherit_privileges` will change then `safe_partman_reapply_privileges` will be automatically called to ensure permissions are propagated to existing child partitions.

```ruby
safe_partman_update_config :table,
  infinite_time_partitions: false,
  inherit_privileges: false,
  premake: 10
```

#### unsafe\_partman\_update\_config

We have chosen to flag the use of `retention` and `retention_keep_table` as an unsafe operation.
While we recognize that these options are useful, changing these values fits in the same category as `drop_table` and `rename_table`, and is therefore unsafe from an application perspective.
If you wish to change these options, you must use this method.

```ruby
unsafe_partman_update_config :table,
  retention: "60 days",
  retention_keep_table: false
```

#### safe\_partman\_reapply\_privileges

If your partitioned table is configured with `inherit_privileges` set to `true`, use this method after granting new roles / privileges on the parent table to ensure permissions are propagated to existing child partitions.

```ruby
safe_partman_reapply_privileges :table
```

### Utilities

#### safely\_acquire\_lock\_for\_table

Acquires a lock (in `ACCESS EXCLUSIVE` mode by default) on a table using the following algorithm:

1. Verify that no long-running queries are using the table.
    - If long-running queries are currently using the table, sleep `PgHaMigrations::LOCK_TIMEOUT_SECONDS` and check again.
2. If no long-running queries are currently using the table, optimistically attempt to lock the table (with a timeout of `PgHaMigrations::LOCK_TIMEOUT_SECONDS`).
    - If the lock is not acquired, sleep `PgHaMigrations::LOCK_FAILURE_RETRY_DELAY_MULTLIPLIER * PgHaMigrations::LOCK_TIMEOUT_SECONDS`, and start again at step 1.
3. If the lock is acquired, proceed to run the given block.

```ruby
safely_acquire_lock_for_table(:table) do
  ...
end
```

Safely acquire a lock on a table in `SHARE` mode.

```ruby
safely_acquire_lock_for_table(:table, mode: :share) do
  ...
end
```

Safely acquire a lock on multiple tables in `EXCLUSIVE` mode.

```ruby
safely_acquire_lock_for_table(:table_a, :table_b, mode: :exclusive) do
  ...
end
```

> **Note:** We enforce that only one set of tables can be locked at a time.
> Attempting to acquire a nested lock on a different set of tables will result in an error.

#### adjust\_lock\_timeout

Adjust lock timeout.

```ruby
adjust_lock_timeout(seconds) do
  ...
end
```

#### adjust\_statement\_timeout

Adjust statement timeout.

```ruby
adjust_statement_timeout(seconds) do
  ...
end
```

#### safe\_set\_maintenance\_work\_mem\_gb

Set maintenance work mem.

```ruby
safe_set_maintenance_work_mem_gb 1
```

#### ensure\_small\_table!

Ensure a table on disk is below the default threshold (10 megabytes).
This will raise an error if the table is too large.

```ruby
ensure_small_table! :table
```

Ensure a table on disk is below a custom threshold and is empty.
This will raise an error if the table is too large and/or contains data.

```ruby
ensure_small_table! :table, empty: true, threshold: 100.megabytes
```

### Configuration

The gem can be configured in an initializer.

```ruby
PgHaMigrations.configure do |config|
  # ...
end
```

#### Available options

- `disable_default_migration_methods`: If true, the default implementations of DDL changes in `ActiveRecord::Migration` and the PostgreSQL adapter will be overridden by implementations that raise a `PgHaMigrations::UnsafeMigrationError`. Default: `true`
- `check_for_dependent_objects`: If true, some `unsafe_*` migration methods will raise a `PgHaMigrations::UnsafeMigrationError` if any dependent objects exist. Default: `true`
- `prefer_single_step_column_addition_with_default`: If true, raise an error when adding a column and separately setting a constant default value for that column in the same migration. Default: `true`
- `allow_force_create_table`: If false, the `force: true` option to ActiveRecord's `create_table` method is disallowed. Default: `false`
- `infer_primary_key_on_partitioned_tables`: If true, the primary key for partitioned tables will be inferred on PostgreSQL 11+ databases (identifier column + partition key columns). Default: `true`

### Rake Tasks

Use this to check for blocking transactions before migrating.

    $ bundle exec rake pg_ha_migrations:check_blocking_database_transactions

This rake task expects that you already have a connection open to your database. We suggest that you add another rake task to open the connection and then add that as a prerequisite for `pg_ha_migrations:check_blocking_database_transactions`.

```ruby
namespace :db do
  desc "Establish a database connection"
  task :establish_connection do
    ActiveRecord::Base.establish_connection
  end
end

Rake::Task["pg_ha_migrations:check_blocking_database_transactions"].enhance ["db:establish_connection"]
```


## Development

After checking out the repo, run `bin/setup` to install dependencies and start a postgres docker container. Then, run `bundle exec rspec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment. This project uses Appraisal to test against multiple versions of ActiveRecord; you can run the tests against all supported version with `bundle exec appraisal rspec`.

> **Warning**: If you rebuild the Docker container _without_ using `docker-compose build` (or the `--build` flag), it will not respect the `PGVERSION` environment variable that you've set if image layers from a different version exist. The Dockerfile uses a build-time argument that's only evaluated during the initial build. To change the Postgres version, you should explicitly provide the build argument: `docker-compose build --build-arg PGVERSION=15`. **Using `bin/setup` handles this for you.**

> **Warning**: The Postgres Dockerfile automatically creates an anonymous volume for the data directory. When changing the specified `PGVERSION` environment variable this volume must be reset using `--renew-anon-volumes` or booting Postgres will fail.  **Using `bin/setup` handles this for you.**

Running tests will automatically create a test database in the locally running Postgres server. You can find the connection parameters in `spec/spec_helper.rb`, but setting the environment variables `PGHOST`, `PGPORT`, `PGUSER`, and `PGPASSWORD` will override the defaults.

To install this gem onto your local machine, run `bundle exec rake install`.

To release a new version, update the version number in `version.rb`, commit the change, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

> **Note:** If while releasing the gem you get the error ``Your rubygems.org credentials aren't set. Run `gem push` to set them.`` you can more simply run `gem signin`.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/braintreeps/pg_ha_migrations. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the PgHaMigrations project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/braintreeps/pg_ha_migrations/blob/master/CODE_OF_CONDUCT.md).
