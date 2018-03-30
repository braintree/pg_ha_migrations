# coding: utf-8
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "pg_ha_migrations/version"

Gem::Specification.new do |spec|
  spec.name          = "pg_ha_migrations"
  spec.version       = PgHaMigrations::VERSION
  spec.authors       = ["jcoleman"]
  spec.email         = ["code@getbraintree.com"]

  spec.summary       = %q{}
  spec.description   = %q{}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.15"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "pg"
  spec.add_development_dependency "db-query-matchers", "~> 0.9.0"


  spec.add_dependency "rails", ">= 5.0", "< 5.2"
  spec.add_dependency "relation_to_struct"
end
