# This is an internal class that is not meant to be used directly
class PgHaMigrations::PartmanConfig < ActiveRecord::Base
  SUPPORTED_PARTITION_TYPES = %w[native range]

  delegate :table_name, to: :class

  self.primary_key = :parent_table

  def self.find(parent_table, partman_extension:)
    unless partman_extension.installed?
      raise PgHaMigrations::MissingExtensionError, "The pg_partman extension is not installed"
    end

    self.table_name = "#{partman_extension.quoted_schema}.part_config"

    super(parent_table)
  end

  def partition_rename_adapter
    unless SUPPORTED_PARTITION_TYPES.include?(partition_type)
      raise PgHaMigrations::InvalidPartConfigError,
        "Expected partition_type to be in #{SUPPORTED_PARTITION_TYPES.inspect} " \
        "but received #{partition_type.inspect}"
    end

    duration = ActiveSupport::Duration.parse(partition_interval)

    if duration.parts.size != 1
      raise PgHaMigrations::InvalidPartConfigError,
        "Partition renaming for complex partition_interval #{partition_interval.inspect} not supported"
    end

    if partition_interval == "P7D"
      PgHaMigrations::WeeklyPartmanRenameAdapter.new(self)
    elsif partition_interval == "P3M"
      PgHaMigrations::QuarterlyPartmanRenameAdapter.new(self)
    elsif duration >= 1.year
      PgHaMigrations::YearToForeverPartmanRenameAdapter.new(self)
    elsif duration >= 1.month && duration < 1.year
      PgHaMigrations::MonthToYearPartmanRenameAdapter.new(self)
    elsif duration >= 1.day && duration < 1.month
      PgHaMigrations::DayToMonthPartmanRenameAdapter.new(self)
    elsif duration >= 1.hour && duration < 1.day
      PgHaMigrations::HourToDayPartmanRenameAdapter.new(self)
    elsif duration >= 1.minute && duration < 1.hour
      PgHaMigrations::MinuteToHourPartmanRenameAdapter.new(self)
    elsif duration >= 1.second && duration < 1.minute
      PgHaMigrations::SecondToMinutePartmanRenameAdapter.new(self)
    else
      raise PgHaMigrations::InvalidPartConfigError,
        "Expected partition_interval to be greater than 1 second " \
        "but received #{partition_interval.inspect}"
    end
  end
end
