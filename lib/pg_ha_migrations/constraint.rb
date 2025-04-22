PgHaMigrations::CheckConstraint = Struct.new(:name, :definition, :validated) do
  def initialize(name, definition, validated)
    # pg_get_constraintdef includes NOT VALID in the definition,
    # but we return that as a separate attribute.
    definition = definition&.gsub(/ NOT VALID\Z/, "")
    super(name, definition, validated)
  end
end
