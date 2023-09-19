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
      @mode = mode
        .to_s
        .underscore
        .delete_suffix("_lock")
        .to_sym

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

    def conflicts_with?(other)
      conflicting_modes.include?(other.mode)
    end

    def conflicting_modes
      case mode
      when :access_share
        %i[access_exclusive]
      when :row_share
        %i[exclusive access_exclusive]
      when :row_exclusive
        %i[share share_row_exclusive exclusive access_exclusive]
      when :share_update_exclusive
        %i[share_update_exclusive share share_row_exclusive exclusive access_exclusive]
      when :share
        %i[row_exclusive share_update_exclusive share_row_exclusive exclusive access_exclusive]
      when :share_row_exclusive
        %i[row_exclusive share_update_exclusive share share_row_exclusive exclusive access_exclusive]
      when :exclusive
        %i[row_share row_exclusive share_update_exclusive share share_row_exclusive exclusive access_exclusive]
      when :access_exclusive
        MODES
      end
    end
  end
end
