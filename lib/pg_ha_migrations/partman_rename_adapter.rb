module PgHaMigrations
  class AbstractPartmanRenameAdapter
    def initialize(part_config)
      if part_config.datetime_string != source_datetime_string
        raise PgHaMigrations::InvalidPartmanConfigError,
          "Expected datetime_string to be #{source_datetime_string.inspect} " \
          "but received #{part_config.datetime_string.inspect}"
      end
    end

    def alter_table_sql(partitions)
      sql = partitions.filter_map do |partition|
        next if partition.name =~ /\A.+_default\z/

        if partition.name !~ source_name_suffix_pattern
          raise PgHaMigrations::InvalidIdentifierError,
            "Expected #{partition.name.inspect} to match #{source_name_suffix_pattern.inspect}"
        end

        begin
          "ALTER TABLE #{partition.fully_qualified_name} RENAME TO #{target_table_name(partition.name)};"
        rescue Date::Error
          raise PgHaMigrations::InvalidIdentifierError,
            "Expected #{partition.name.inspect} suffix to be a parseable DateTime"
        end
      end.join("\n")

      # This wraps the SQL in an anonymous function such that
      # the statement timeout would apply to the entire batch of
      # statements instead of each individual statement
      "DO $$ BEGIN #{sql} END; $$;"
    end

    def target_table_name(table_name)
      raise "#{__method__} should be implemented in subclass"
    end

    def source_datetime_string
      raise "#{__method__} should be implemented in subclass"
    end

    def source_name_suffix_pattern
      raise "#{__method__} should be implemented in subclass"
    end

    def target_datetime_string
      raise "#{__method__} should be implemented in subclass"
    end
  end

  class YearToForeverPartmanRenameAdapter < AbstractPartmanRenameAdapter
    def target_table_name(table_name)
      table_name + "0101"
    end

    def source_datetime_string
      "YYYY"
    end

    def source_name_suffix_pattern
      /\A.+_p\d{4}\z/
    end

    def target_datetime_string
      "YYYYMMDD"
    end
  end

   class QuarterlyPartmanRenameAdapter < AbstractPartmanRenameAdapter
    QUARTER_MONTH_MAPPING = {
      "1" => "01",
      "2" => "04",
      "3" => "07",
      "4" => "10",
    }

    def target_table_name(table_name)
      base_name = table_name[0...-6]

      year = table_name.last(6).first(4)

      month = QUARTER_MONTH_MAPPING.fetch(table_name.last(1))

      base_name + year + month + "01"
    end

    def source_datetime_string
      "YYYY\"q\"Q"
    end

    def source_name_suffix_pattern
      /\A.+_p\d{4}q(1|2|3|4)\z/
    end

    def target_datetime_string
      "YYYYMMDD"
    end
  end

   class MonthToYearPartmanRenameAdapter < AbstractPartmanRenameAdapter
    def target_table_name(table_name)
      base_name = table_name[0...-7]

      partition_datetime = DateTime.strptime(table_name.last(7), "%Y_%m")

      base_name + partition_datetime.strftime("%Y%m%d")
    end

    def source_datetime_string
      "YYYY_MM"
    end

    def source_name_suffix_pattern
      /\A.+_p\d{4}_\d{2}\z/
    end

    def target_datetime_string
      "YYYYMMDD"
    end
  end

   class WeeklyPartmanRenameAdapter < AbstractPartmanRenameAdapter
    def target_table_name(table_name)
      base_name = table_name[0...-7]

      partition_datetime = DateTime.strptime(table_name.last(7), "%Gw%V")

      base_name + partition_datetime.strftime("%Y%m%d")
    end

    def source_datetime_string
      "IYYY\"w\"IW"
    end

    def source_name_suffix_pattern
      /\A.+_p\d{4}w\d{2}\z/
    end

    def target_datetime_string
      "YYYYMMDD"
    end
  end

   class DayToMonthPartmanRenameAdapter < AbstractPartmanRenameAdapter
    def target_table_name(table_name)
      base_name = table_name[0...-10]

      partition_datetime = DateTime.strptime(table_name.last(10), "%Y_%m_%d")

      base_name + partition_datetime.strftime("%Y%m%d")
    end

    def source_datetime_string
      "YYYY_MM_DD"
    end

    def source_name_suffix_pattern
      /\A.+_p\d{4}_\d{2}_\d{2}\z/
    end

    def target_datetime_string
      "YYYYMMDD"
    end
  end

   class MinuteToDayPartmanRenameAdapter < AbstractPartmanRenameAdapter
    def target_table_name(table_name)
      base_name = table_name[0...-15]

      partition_datetime = DateTime.strptime(table_name.last(15), "%Y_%m_%d_%H%M")

      base_name + partition_datetime.strftime("%Y%m%d_%H%M%S")
    end

    def source_datetime_string
      "YYYY_MM_DD_HH24MI"
    end

    def source_name_suffix_pattern
      /\A.+_p\d{4}_\d{2}_\d{2}_\d{4}\z/
    end

    def target_datetime_string
      "YYYYMMDD_HH24MISS"
    end
  end

   class SecondToMinutePartmanRenameAdapter < AbstractPartmanRenameAdapter
    def target_table_name(table_name)
      base_name = table_name[0...-17]

      partition_datetime = DateTime.strptime(table_name.last(17), "%Y_%m_%d_%H%M%S")

      base_name + partition_datetime.strftime("%Y%m%d_%H%M%S")
    end

    def source_datetime_string
      "YYYY_MM_DD_HH24MISS"
    end

    def source_name_suffix_pattern
      /\A.+_p\d{4}_\d{2}_\d{2}_\d{6}\z/
    end

    def target_datetime_string
      "YYYYMMDD_HH24MISS"
    end
  end
end
