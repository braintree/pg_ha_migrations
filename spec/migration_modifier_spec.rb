require "spec_helper"

RSpec.describe PgHaMigrations::MigrationModifier do
  describe "#modify!" do
    Dir.new('spec/fixtures').each do |fixture|
      file_path = File.join('spec/fixtures', fixture)
      next unless File.file?(file_path)

      fixture_name = fixture.split('.')[0]

      it "changes #{fixture_name} to be safe" do
        Tempfile.create(fixture_name.to_s, "spec/tmp") do |f|
          f.write(File.read("spec/fixtures/#{fixture_name}.rb"))
          f.rewind

          PgHaMigrations::MigrationModifier.new(File.dirname(f.path)).modify!

          expect(f.read).to eq(File.read("spec/expected/#{fixture_name}.rb"))
        end
      end
    end
  end
end
