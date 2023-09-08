require "spec_helper"

RSpec.describe PgHaMigrations::LockMode do
  describe "#<=>" do
    {
      access_share: {
        above: %i[
          row_share
          row_exclusive
          share_update_exclusive
          share
          share_row_exclusive
          exclusive
          access_exclusive
        ],
        below: [],
      },
      row_share: {
        above: %i[
          row_exclusive
          share_update_exclusive
          share
          share_row_exclusive
          exclusive
          access_exclusive
        ],
        below: %i[access_share],
      },
      row_exclusive: {
        above: %i[
          share_update_exclusive
          share
          share_row_exclusive
          exclusive
          access_exclusive
        ],
        below: %i[
          access_share
          row_share
        ],
      },
      share_update_exclusive: {
        above: %i[
          share
          share_row_exclusive
          exclusive
          access_exclusive
        ],
        below: %i[
          access_share
          row_share
          row_exclusive
        ],
      },
      share: {
        above: %i[
          share_row_exclusive
          exclusive
          access_exclusive
        ],
        below: %i[
          access_share
          row_share
          row_exclusive
          share_update_exclusive
        ],
      },
      share_row_exclusive: {
        above: %i[
          exclusive
          access_exclusive
        ],
        below: %i[
          access_share
          row_share
          row_exclusive
          share_update_exclusive
          share
        ],
      },
      exclusive: {
        above: %i[access_exclusive],
        below: %i[
          access_share
          row_share
          row_exclusive
          share_update_exclusive
          share
          share_row_exclusive
        ],
      },
      access_exclusive: {
        above: [],
        below: %i[
          access_share
          row_share
          row_exclusive
          share_update_exclusive
          share
          share_row_exclusive
          exclusive
        ],
      },
    }.each do |mode, other_modes|
      it "returns correct comparisons for #{mode}" do
        subject = described_class.new(mode)

        aggregate_failures do
          expect(subject == described_class.new(mode)).to eq(true)

          other_modes[:above].each do |other_mode|
            expect(subject > described_class.new(other_mode)).to eq(false)
            expect(subject < described_class.new(other_mode)).to eq(true)
          end

          other_modes[:below].each do |other_mode|
            expect(subject > described_class.new(other_mode)).to eq(true)
            expect(subject < described_class.new(other_mode)).to eq(false)
          end
        end
      end
    end
  end
end
