require "active_record/migration/compatibility"

module PgHaMigrations::AllowedVersions
  ALLOWED_VERSIONS = [4.2, 5.0, 5.1, 5.2, 6.0, 6.1, 7.0, 7.1].map do |v|
    begin
      ActiveRecord::Migration[v]
    rescue ArgumentError
      nil
    end
  end.compact

  def inherited(subclass)
    super
    unless ALLOWED_VERSIONS.include?(subclass.superclass)
      raise StandardError, "#{subclass.superclass} is not a permitted migration class\n" \
        "\n" \
        "To add a new version update the ALLOWED_VERSIONS constant in #{__FILE__}\n" \
        "Currently allowed versions: #{ALLOWED_VERSIONS.map { |v| "ActiveRecord::Migration[#{v.current_version}]" }.join(', ')}"
    end
  end
end

ActiveRecord::Migration.singleton_class.send(:prepend, PgHaMigrations::AllowedVersions)
