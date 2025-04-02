RSpec.describe ClickHouse::Extend::ConnectionExplaining do
  subject do
    ClickHouse.connection
  end

  before do
    subject.execute <<~SQL
      CREATE TABLE rspec(id Int64) ENGINE TinyLog
    SQL
  end

  let(:expectation) do
    <<~TXT
      Expression ((Projection + Before ORDER BY))
        Join (JOIN)
          Expression (Before JOIN)
            ReadFromStorage (TinyLog)
          Expression ((Joined actions + (Rename joined columns + (Projection + Before ORDER BY))))
            ReadFromStorage (TinyLog)
    TXT
  end

  context 'when normal query' do
    it 'works' do
      buffer = StringIO.new
      subject.explain('SELECT 1 FROM rspec CROSS JOIN rspec', io: buffer)
      output = buffer.string

      expect(output).to include('Join (JOIN')
      expect(output).to include('ReadFromStorage (TinyLog)')
    end
  end

  context 'when EXPLAIN query' do
    it 'works' do
      buffer = StringIO.new
      subject.explain('SELECT 1 FROM rspec CROSS JOIN rspec', io: buffer)

      output = buffer.string

      expect(output).to include('Join (JOIN')
      expect(output).to include('ReadFromStorage (TinyLog)')
    end
  end
end
