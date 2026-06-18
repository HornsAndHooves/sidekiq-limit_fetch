# frozen_string_literal: true

# require_relative "lib/sidekiq/limit_fetch/version"

Gem::Specification.new do |gem|
  gem.name          = "HornsAndHooves-sidekiq-limit_fetch"
  gem.license       = "MIT"
  gem.authors       = ["HornsAndHooves", "Peter Maneykowski"]
  gem.email         = ["maneyko@integracredit.com"]
  gem.summary       = "Sidekiq strategy to support queue limits"
  gem.homepage      = "https://github.com/HornsAndHooves/sidekiq-limit_fetch"
  gem.description   = "Sidekiq strategy to restrict number of workers which are able to run specified queues simultaneously."

  gem.metadata["homepage_uri"]    = gem.homepage
  gem.metadata["changelog_uri"]   = gem.homepage + "/blob/master/CHANGELOG.md"
  gem.metadata["source_code_uri"] = gem.homepage

  gem.files         = %w[CHANGELOG.md LICENSE README.md HornsAndHooves-sidekiq-limit_fetch.gemspec] + `git ls-files`.split($INPUT_RECORD_SEPARATOR)
  gem.require_paths = %w[lib]

  gem.version = "5.0.0"
  gem.required_ruby_version = ">= 2.7.0"

  gem.add_dependency "sidekiq", ">= 8"

  gem.add_development_dependency "rake"
  gem.add_development_dependency "rspec"
  gem.add_development_dependency "rubocop"
  gem.add_development_dependency "simplecov"
end
