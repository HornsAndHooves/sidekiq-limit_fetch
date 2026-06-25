# frozen_string_literal: true

require "simplecov"
SimpleCov.start do
  add_filter "/spec/"
end

require "sidekiq"
require "sidekiq/fetch"
require "sidekiq-limit_fetch"

RSpec.configure do |config|
  config.order = :random
  config.filter_run_when_matching :focus
end
