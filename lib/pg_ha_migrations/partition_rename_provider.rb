module PgHaMigrations
  class AbstractRenamePartitionSqlProvider
    attr_reader :new_datetime_string

    def initialize(part_config)
      @part_config = part_config
      @new_datetime_string = "YYYYMMDD"
    end

    def get_sql(partition)
      raise "boom" unless partition.name.match?(regex_matcher)

      "ALTER TABLE #{partition.fully_qualified_name} RENAME TO #{get_new_name(partition)};"
    end
  end

  class RenameWeeklyPartitionSqlProvider < AbstractRenamePartitionSqlProvider
    def regex_matcher
      /\A.+_p\d{4}w\d{2}\z/
    end

    def get_new_name(partition)
      base_name = partition.name[0...-7]
      partition_date = DateTime.strptime(partition.name.last(7), "%Gw%V")
      new_suffix = partition_date.strftime("%Y%m%d")

      base_name + new_suffix
    end
  end

  class RenameQuarterlyPartitionSqlProvider < AbstractRenamePartitionSqlProvider
    QUARTER_MONTH_MAPPING = {
      "1" => "01",
      "2" => "04",
      "3" => "07",
      "4" => "10",
    }

    def regex_matcher
      /\A.+_p\d{4}q\d{1}\z/
    end

    def get_new_name(partition)
      base_name = partition.name[0...-6]

      year = partition.name.last(6).first(4)
      quarter = partition.name.last(1)

      month = QUARTER_MONTH_MAPPING.fetch(quarter) do
        raise "unexpected quarter"
      end

      base_name + year + month + "01"
    end
  end
end

