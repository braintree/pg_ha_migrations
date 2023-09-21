module PgHaMigrations
  class LockMode
    include Comparable

    MODE_CONFLICTS = ActiveSupport::OrderedHash.new

    MODE_CONFLICTS[:access_share] = %i[
      access_exclusive
    ]

    MODE_CONFLICTS[:row_share] = %i[
      exclusive
      access_exclusive
    ]

    MODE_CONFLICTS[:row_exclusive] = %i[
      share
      share_row_exclusive
      exclusive
      access_exclusive
    ]

    MODE_CONFLICTS[:share_update_exclusive] = %i[
      share_update_exclusive
      share
      share_row_exclusive
      exclusive
      access_exclusive
    ]

    MODE_CONFLICTS[:share] = %i[
      row_exclusive
      share_update_exclusive
      share_row_exclusive
      exclusive
      access_exclusive
    ]

    MODE_CONFLICTS[:share_row_exclusive] = %i[
      row_exclusive
      share_update_exclusive
      share
      share_row_exclusive
      exclusive
      access_exclusive
    ]

    MODE_CONFLICTS[:exclusive] = %i[
      row_share
      row_exclusive
      share_update_exclusive
      share
      share_row_exclusive
      exclusive
      access_exclusive
    ]

    MODE_CONFLICTS[:access_exclusive] = %i[
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

      if !MODE_CONFLICTS.keys.include?(@mode)
        raise ArgumentError, "Unrecognized lock mode #{@mode.inspect}. Valid modes: #{MODE_CONFLICTS.keys}"
      end
    end

    def to_sql
      to_s
        .upcase
        .split("_")
        .join(" ")
    end

    def <=>(other)
      MODE_CONFLICTS.keys.index(mode) <=> MODE_CONFLICTS.keys.index(other.mode)
    end

    def conflicts_with?(other)
      MODE_CONFLICTS[mode].include?(other.mode)
    end
  end
end
