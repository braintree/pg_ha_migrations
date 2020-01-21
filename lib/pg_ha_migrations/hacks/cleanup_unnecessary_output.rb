require "active_record/migration/compatibility"

module PgHaMigrations
  module ActiveRecordHacks
    module CleanupUnnecessaryOutput
      # This is fixed in Rails 6+, but previously there were several
      # places where #adapter_name was called directly which implicitly
      # delegated to the connection through #method_missing. That
      # delegation though results in wrapping the call in #say_with_time
      # which unnecessarily outputs a bunch of calls to #adapter_name.
      # The easiest way to clean this up retroactively is to just patch
      # in a direct dispatch to the connection's method.
      #
      # See: https://github.com/rails/rails/commit/eb7c71bcd3d0c7e079dffdb11e43fb466eec06aa
      def adapter_name
        connection.adapter_name
      end
    end
  end
end

patchable_module = [
 defined?(ActiveRecord::Migration::Compatibility::V5_2) ? ActiveRecord::Migration::Compatibility::V5_2 : nil,
 defined?(ActiveRecord::Migration::Compatibility::V5_1) ? ActiveRecord::Migration::Compatibility::V5_1 : nil,
 defined?(ActiveRecord::Migration::Compatibility::V5_0) ? ActiveRecord::Migration::Compatibility::V5_0 : nil,
].detect { |m| m }
if patchable_module
  patchable_module.prepend(PgHaMigrations::ActiveRecordHacks::CleanupUnnecessaryOutput)
end
