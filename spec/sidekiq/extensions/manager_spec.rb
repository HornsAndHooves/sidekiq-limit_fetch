# frozen_string_literal: true

RSpec.describe Sidekiq::Manager do
  let(:capsule_or_options) do
    Sidekiq.default_configuration.default_capsule
  end

  it 'can be instantiated' do
    expect(described_class).to be < Sidekiq::Manager::InitLimitFetch
    manager = described_class.new(capsule_or_options)
    expect(manager).to respond_to(:start)
  end
end
