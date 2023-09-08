module PgHaMigrations
  class LockMode
    include Comparable

    MODES = ActiveSupport::OrderedHash.new

    MODES[:access_share] = "ACCESS SHARE"
    MODES[:row_share] = "ROW SHARE"
    MODES[:row_exclusive] = "ROW EXCLUSIVE"
    MODES[:share_update_exclusive] = "SHARE UPDATE EXCLUSIVE"
    MODES[:share] = "SHARE"
    MODES[:share_row_exclusive] = "SHARE ROW EXCLUSIVE"
    MODES[:exclusive] = "EXCLUSIVE"
    MODES[:access_exclusive] = "ACCESS EXCLUSIVE"

    attr_reader :mode

    def initialize(mode)
      @mode = mode.to_sym

      raise ArgumentError, "Unrecognized lock mode #{@mode.inspect}. Valid modes: #{MODES.keys}" unless MODES.has_key?(@mode)
    end

    def inspect
      mode.inspect
    end

    def to_sql
      MODES[mode]
    end

    def <=>(other)
      MODES.keys.index(mode) <=> MODES.keys.index(other.mode)
    end
  end
end
