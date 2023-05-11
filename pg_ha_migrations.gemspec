# coding: utf-8
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "pg_ha_migrations/version"

Gem::Specification.new do |spec|
  spec.name          = "pg_ha_migrations"
  spec.version       = PgHaMigrations::VERSION
  spec.authors       = %w{
    celeen
    cosgroveb
    jaronkk
    jcoleman
    kexline4710
    mgates
    redneckbeard
  }
  spec.email         = ["code@getbraintree.com"]

  spec.summary       = %q{Enforces DDL/migration safety in Ruby on Rails project with an emphasis on explicitly choosing trade-offs and avoiding unnecessary magic.}
  spec.description   = %q{Enforces DDL/migration safety in Ruby on Rails project with an emphasis on explicitly choosing trade-offs and avoiding unnecessary magic.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "rake", ">= 12.3.3"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "pg"
  spec.add_development_dependency "db-query-matchers", "~> 0.11.0"
  spec.add_development_dependency "pry"
  spec.add_development_dependency "pry-byebug"
  spec.add_development_dependency "appraisal", "~> 2.2.0"

  spec.add_dependency "rails", ">= 6.1", "< 7.1"
  spec.add_dependency "relation_to_struct", ">= 1.5.1"
  spec.add_dependency "ruby2_keywords"
end
