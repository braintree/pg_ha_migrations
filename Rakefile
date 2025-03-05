require "bundler/gem_tasks"
require "rspec/core/rake_task"
require "appraisal"
# In Rails 6 this isn't required in the right order and worked by accident; fixed in rails@0f5e7a66143
require "logger"
require_relative File.join("lib", "pg_ha_migrations")

RSpec::Core::RakeTask.new(:spec)

if !ENV["APPRAISAL_INITIALIZED"] && !ENV["TRAVIS"]
    task :default => :appraisal
end

task :default => :spec

