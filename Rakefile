require "bundler/gem_tasks"
require "rspec/core/rake_task"
require "appraisal"
require "logger"
require_relative File.join("lib", "pg_ha_migrations")

RSpec::Core::RakeTask.new(:spec)

if !ENV["APPRAISAL_INITIALIZED"] && !ENV["TRAVIS"]
    task :default => :appraisal
end

task :default => :spec

