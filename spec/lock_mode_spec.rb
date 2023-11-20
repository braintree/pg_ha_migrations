require "spec_helper"

RSpec.describe PgHaMigrations::LockMode do
  subject { described_class.new(mode) }

  context "when mode is access_share" do
    let(:mode) { :access_share }

    it "returns correct comparisons" do
      aggregate_failures do
        expect(subject).to eq(described_class.new(mode))

        %i[row_share row_exclusive share_update_exclusive share share_row_exclusive exclusive access_exclusive].each do |other_mode|
          expect(subject).to be < described_class.new(other_mode),
            "lock mode #{mode} is not less than #{other_mode}"
        end
      end
    end

    it "returns correct conflicts" do
      aggregate_failures do
        %i[access_exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(true),
            "lock mode #{mode} does not conflict with #{other_mode}"
        end

        %i[access_share row_share row_exclusive share_update_exclusive share share_row_exclusive exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(false),
            "lock mode #{mode} conflicts with #{other_mode}"
        end
      end
    end
  end

  context "when mode is row_share" do
    let(:mode) { :row_share }

    it "returns correct comparisons" do
      aggregate_failures do
        expect(subject).to eq(described_class.new(mode))

        %i[row_exclusive share_update_exclusive share share_row_exclusive exclusive access_exclusive].each do |other_mode|
          expect(subject).to be < described_class.new(other_mode),
            "lock mode #{mode} is not less than #{other_mode}"
        end

        %i[access_share].each do |other_mode|
          expect(subject).to be > described_class.new(other_mode),
            "lock mode #{mode} is not greater than #{other_mode}"
        end
      end
    end

    it "returns correct conflicts" do
      aggregate_failures do
        %i[exclusive access_exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(true),
            "lock mode #{mode} does not conflict with #{other_mode}"
        end

        %i[access_share row_share row_exclusive share_update_exclusive share share_row_exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(false),
            "lock mode #{mode} conflicts with #{other_mode}"
        end
      end
    end
  end

  context "when mode is row_exclusive" do
    let(:mode) { :row_exclusive }

    it "returns correct comparisons" do
      aggregate_failures do
        expect(subject).to eq(described_class.new(mode))

        %i[share_update_exclusive share share_row_exclusive exclusive access_exclusive].each do |other_mode|
          expect(subject).to be < described_class.new(other_mode),
            "lock mode #{mode} is not less than #{other_mode}"
        end

        %i[access_share row_share].each do |other_mode|
          expect(subject).to be > described_class.new(other_mode),
            "lock mode #{mode} is not greater than #{other_mode}"
        end
      end
    end

    it "returns correct conflicts" do
      aggregate_failures do
        %i[share share_row_exclusive exclusive access_exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(true),
            "lock mode #{mode} does not conflict with #{other_mode}"
        end

        %i[access_share row_share row_exclusive share_update_exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(false),
            "lock mode #{mode} conflicts with #{other_mode}"
        end
      end
    end
  end

  context "when mode is share_update_exclusive" do
    let(:mode) { :share_update_exclusive }

    it "returns correct comparisons" do
      aggregate_failures do
        expect(subject).to eq(described_class.new(mode))

        %i[share share_row_exclusive exclusive access_exclusive].each do |other_mode|
          expect(subject).to be < described_class.new(other_mode),
            "lock mode #{mode} is not less than #{other_mode}"
        end

        %i[access_share row_share row_exclusive].each do |other_mode|
          expect(subject).to be > described_class.new(other_mode),
            "lock mode #{mode} is not greater than #{other_mode}"
        end
      end
    end

    it "returns correct conflicts" do
      aggregate_failures do
        %i[share_update_exclusive share share_row_exclusive exclusive access_exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(true),
            "lock mode #{mode} does not conflict with #{other_mode}"
        end

        %i[access_share row_share row_exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(false),
            "lock mode #{mode} conflicts with #{other_mode}"
        end
      end
    end
  end

  context "when mode is share" do
    let(:mode) { :share }

    it "returns correct comparisons" do
      aggregate_failures do
        expect(subject).to eq(described_class.new(mode))

        %i[share_row_exclusive exclusive access_exclusive].each do |other_mode|
          expect(subject).to be < described_class.new(other_mode),
            "lock mode #{mode} is not less than #{other_mode}"
        end

        %i[access_share row_share row_exclusive share_update_exclusive].each do |other_mode|
          expect(subject).to be > described_class.new(other_mode),
            "lock mode #{mode} is not greater than #{other_mode}"
        end
      end
    end

    it "returns correct conflicts" do
      aggregate_failures do
        %i[row_exclusive share_update_exclusive share_row_exclusive exclusive access_exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(true),
            "lock mode #{mode} does not conflict with #{other_mode}"
        end

        %i[access_share row_share share].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(false),
            "lock mode #{mode} conflicts with #{other_mode}"
        end
      end
    end
  end

  context "when mode is share_row_exclusive" do
    let(:mode) { :share_row_exclusive }

    it "returns correct comparisons" do
      aggregate_failures do
        expect(subject).to eq(described_class.new(mode))

        %i[exclusive access_exclusive].each do |other_mode|
          expect(subject).to be < described_class.new(other_mode),
            "lock mode #{mode} is not less than #{other_mode}"
        end

        %i[access_share row_share row_exclusive share_update_exclusive share].each do |other_mode|
          expect(subject).to be > described_class.new(other_mode),
            "lock mode #{mode} is not greater than #{other_mode}"
        end
      end
    end

    it "returns correct conflicts" do
      aggregate_failures do
        %i[row_exclusive share_update_exclusive share share_row_exclusive exclusive access_exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(true),
            "lock mode #{mode} does not conflict with #{other_mode}"
        end

        %i[access_share row_share].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(false),
            "lock mode #{mode} conflicts with #{other_mode}"
        end
      end
    end
  end

  context "when mode is exclusive" do
    let(:mode) { :exclusive }

    it "returns correct comparisons" do
      aggregate_failures do
        expect(subject).to eq(described_class.new(mode))

        %i[access_exclusive].each do |other_mode|
          expect(subject).to be < described_class.new(other_mode),
            "lock mode #{mode} is not less than #{other_mode}"
        end

        %i[access_share row_share row_exclusive share_update_exclusive share share_row_exclusive].each do |other_mode|
          expect(subject).to be > described_class.new(other_mode),
            "lock mode #{mode} is not greater than #{other_mode}"
        end
      end
    end

    it "returns correct conflicts" do
      aggregate_failures do
        %i[row_share row_exclusive share_update_exclusive share share_row_exclusive exclusive access_exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(true),
            "lock mode #{mode} does not conflict with #{other_mode}"
        end

        %i[access_share].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(false),
            "lock mode #{mode} conflicts with #{other_mode}"
        end
      end
    end
  end

  context "when mode is access_exclusive" do
    let(:mode) { :access_exclusive }

    it "returns correct comparisons" do
      aggregate_failures do
        expect(subject).to eq(described_class.new(mode))

        %i[access_share row_share row_exclusive share_update_exclusive share share_row_exclusive exclusive].each do |other_mode|
          expect(subject).to be > described_class.new(other_mode),
            "lock mode #{mode} is not greater than #{other_mode}"
        end
      end
    end

    it "returns correct conflicts" do
      aggregate_failures do
        %i[access_share row_share row_exclusive share_update_exclusive share share_row_exclusive exclusive access_exclusive].each do |other_mode|
          expect(subject.conflicts_with?(described_class.new(other_mode))).to eq(true),
            "lock mode #{mode} does not conflict with #{other_mode}"
        end
      end
    end
  end
end
