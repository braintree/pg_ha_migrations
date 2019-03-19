# PgHaMigrations

[![Build Status](https://travis-ci.org/braintree/pg_ha_migrations.svg?branch=master)](https://travis-ci.org/braintree/pg_ha_migrations/)

We've documented our learned best practices for applying schema changes without downtime in the post [PostgreSQL at Scale: Database Schema Changes Without Downtime](https://medium.com/braintree-product-technology/postgresql-at-scale-database-schema-changes-without-downtime-20d3749ed680) on the [Braintree Product and Technology Blog](https://medium.com/braintree-product-technology). Many of the approaches we take and choices we've made are explained in much greater depth there than in this README.

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

## Usage

### Rollback

Because we require that ["Rollback strategies do not involve reverting the database schema to its previous version"](https://medium.com/braintree-product-technology/postgresql-at-scale-database-schema-changes-without-downtime-20d3749ed680#360a), PgHaMigrations does not support ActiveRecord's automatic migration rollback capability.

Instead we write all of our migrations with only an `def up` method like:

```
def up
  safe_add_column :table, :column
end
```

and never use `def change`. We believe that this is the only safe approach in production environments. For development environments we iterate by recreating the database from scratch every time we make a change.

### Migrations

There are two major classes of concerns we try to handle in the API:

- Database safety (e.g., long-held locks)
- Application safety (e.g., dropping columns the app uses)

We rename migration methods with prefixes denoting their safety level:

- `safe_*`: These methods check for both application and database safety concerns prefer concurrent operations where available, set low lock timeouts where appropriate, and decompose operations into multiple safe steps.
- `unsafe_*`: These methods are generally a direct dispatch to the native ActiveRecord migration method.

Calling the original migration methods without a prefix will raise an error.

The API is designed to be explicit yet remain flexible. There may be situations where invoking the `unsafe_*` method is preferred (or the only option available for definitionally unsafe operations).

While `unsafe_*` methods were historically (through 1.0) pure wrappers for invoking the native ActiveRecord migration method, there is a class of problems that we can't handle easily without breaking that design rule a bit. For example, dropping a column is unsafe from an application perspective, so we make the application safety concerns explicit by using an `unsafe_` prefix. Using `unsafe_remove_column` calls out the need to audit the application to confirm the migration won't break the application. Because there are no safe alternatives we don't define a `safe_remove_column` analogue. However there are still conditions we'd like to assert before dropping a column. For example, dropping an unused column that's used in one or more indexes may be safe from an application perspective, but the cascading drop of the index won't use a `CONCURRENT` operation to drop the dependent indexes and is therefore unsafe from a database perspective.

When `unsafe_*` migration methods support checks of this type you can bypass the checks by passing an `:allow_dependent_objects` key in the method's `options` hash containing an array of dependent object types you'd like to allow. Until 2.0 none of these checks will run by default, but you can opt-in by setting `config.check_for_dependent_objects = true` [in your configuration initializer](#configuration).

[Running multiple DDL statements inside a transaction acquires exclusive locks on all of the modified objects](https://medium.com/braintree-product-technology/postgresql-at-scale-database-schema-changes-without-downtime-20d3749ed680#cc22). For that reason, this gem [disables DDL transactions](./lib/pg_ha_migrations.rb:8) by default. You can change this by resetting `ActiveRecord::Migration.disable_ddl_transaction` in your application.

The following functionality is currently unsupported:

- Rollbacks
- Generators
- schema.rb

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
safe_change_column_default :table, :column, "value"
```

#### safe\_make\_column\_nullable

Safely make the column nullable.

```ruby
safe_make_column_nullable :table, :column
```

#### unsafe\_make\_column\_not\_nullable

Unsafely make a column not nullable.

```ruby
unsafe_make_column_not_nullable :table, :column
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


### Utilities

#### safely\_acquire\_lock\_for\_table

Safely acquire a lock for a table.

```ruby
safely_acquire_lock_for_table(:table) do
  ...
end
```

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

### Configuration

The gem can be configured in an initializer.

```ruby
PgHaMigrations.configure do |config|
  # ...
end
```

#### Available options

- `disable_default_migration_methods`: If true, the default implementations of DDL changes in `ActiveRecord::Migration` and the PostgreSQL adapter will be overridden by implementations that raise a `PgHaMigrations::UnsafeMigrationError`. Default: `true`
- `check_for_dependent_objects`: If true, some `unsafe_*` migration methods will raise a `PgHaMigrations::UnsafeMigrationError` if any dependent objects exist. Default: `false`

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

After checking out the repo, run `bin/setup` to install dependencies and start a postgres docker container. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/braintreeps/pg_ha_migrations. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the PgHaMigrations project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/braintreeps/pg_ha_migrations/blob/master/CODE_OF_CONDUCT.md).
