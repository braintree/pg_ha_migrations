module PgHaMigrations
  class LockMode
    include Comparable

    MODES = %i[
      access_share
      row_share
      row_exclusive
      share_update_exclusive
      share
      share_row_exclusive
      exclusive
      access_exclusive
    ]

    attr_reader :mode

    delegate :inspect, :to_s, to: :mode

    def initialize(mode)
      @mode = mode.to_sym

      raise ArgumentError, "Unrecognized lock mode #{@mode.inspect}. Valid modes: #{MODES}" unless MODES.include?(@mode)
    end

    def to_sql
      to_s
        .upcase
        .split("_")
        .join(" ")
    end

    def <=>(other)
      MODES.index(mode) <=> MODES.index(other.mode)
    end
  end
end
