# This is an internal class that is not meant to be used directly
class PgHaMigrations::PartmanConfig < ActiveRecord::Base
  SUPPORTED_PARTITION_TYPES = %w[native range]

  delegate :connection, to: :class

  self.primary_key = :parent_table

  def self.find(parent_table, partman_extension:)
    unless partman_extension.installed?
      raise PgHaMigrations::MissingExtensionError, "The pg_partman extension is not installed"
    end

    self.table_name = "#{partman_extension.quoted_schema}.part_config"

    super(parent_table)
  end

  # The actual column type is TEXT and the value is determined by the
  # intervalstyle in Postgres at the time create_parent is called.
  # Rails hard codes this config when it builds connections for ease
  # of parsing by ActiveSupport::Duration.parse. So in theory, we
  # really only need to do the interval casting, but we're doing the
  # SET LOCAL to be absolutely sure intervalstyle is correct.
  #
  # https://github.com/rails/rails/blob/v8.0.3/activerecord/lib/active_record/connection_adapters/postgresql_adapter.rb#L979-L980
  def partition_interval_iso_8601
    transaction do
      connection.execute("SET LOCAL intervalstyle TO 'iso_8601'")
      connection.select_value("SELECT #{connection.quote(partition_interval)}::interval")
    end
  end

  def partition_rename_adapter
    unless SUPPORTED_PARTITION_TYPES.include?(partition_type)
      raise PgHaMigrations::InvalidPartmanConfigError,
        "Expected partition_type to be in #{SUPPORTED_PARTITION_TYPES.inspect} " \
        "but received #{partition_type.inspect}"
    end

    duration = ActiveSupport::Duration.parse(partition_interval_iso_8601)

    if duration.parts.size != 1
      raise PgHaMigrations::InvalidPartmanConfigError,
        "Partition renaming for complex partition_interval #{duration.iso8601.inspect} not supported"
    end

    # Quarterly and weekly have special meaning in Partman 4 with
    # specific datetime strings that need to be handled separately.
    #
    # The intervals "1 week" and "3 months" will not match the first
    # two conditionals and will fallthrough to standard adapters below.
    if duration == 1.week && datetime_string == "IYYY\"w\"IW"
      PgHaMigrations::WeeklyPartmanRenameAdapter.new(self)
    elsif duration == 3.months && datetime_string == "YYYY\"q\"Q"
      PgHaMigrations::QuarterlyPartmanRenameAdapter.new(self)
    elsif duration >= 1.year
      PgHaMigrations::YearToForeverPartmanRenameAdapter.new(self)
    elsif duration >= 1.month && duration < 1.year
      PgHaMigrations::MonthToYearPartmanRenameAdapter.new(self)
    elsif duration >= 1.day && duration < 1.month
      PgHaMigrations::DayToMonthPartmanRenameAdapter.new(self)
    elsif duration >= 1.minute && duration < 1.day
      PgHaMigrations::MinuteToDayPartmanRenameAdapter.new(self)
    elsif duration >= 1.second && duration < 1.minute
      PgHaMigrations::SecondToMinutePartmanRenameAdapter.new(self)
    else
      raise PgHaMigrations::InvalidPartmanConfigError,
        "Expected partition_interval to be greater than 1 second " \
        "but received #{duration.iso8601.inspect}"
    end
  end
end
