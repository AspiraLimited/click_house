RSpec.describe ClickHouse::Extend::ConnectionSelective do
  subject { ClickHouse.connection }

  before do
    subject.execute <<~SQL
      CREATE TABLE rspec (date Date, id UInt32, user String) ENGINE = MergeTree(date, (id, date), 8192)
    SQL

    subject.execute <<~SQL
      INSERT INTO rspec (date, id, user) VALUES('2000-01-01', 1, 'Alice'), ('2000-01-02', 2, 'Bob'), ('2000-01-03', 3, 'Charlie')
    SQL
  end

  describe '#select_value' do
    context 'when exists' do
      it 'works' do
        expect(subject.select_value('SELECT 13')).to eq(13)
      end

      context 'with params' do
        it 'returns requested value' do
          expect(subject.select_value('SELECT user FROM rspec WHERE id = {id:UInt32}', params: { id: 3 })).to eq('Charlie')
        end
      end
    end

    context 'when not exists' do
      it 'works' do
        expect(subject.select_value('SELECT null')).to eq(nil)
      end

      context 'with params' do
        it 'returns nil' do
          expect(subject.select_value('SELECT user FROM rspec WHERE id = {id:UInt32}', params: { id: 100 })).to be_nil
        end
      end
    end

    context 'when multiple columns' do
      it 'works' do
        expect(subject.select_value('SELECT 1, 2, 3, 4, 5')).to eq(1)
      end
    end
  end

  describe '#select_one' do
    context 'when exists' do
      it 'works' do
        expect(subject.select_one('SELECT 1 AS foo, 2 AS bar')).to eq({ 'foo' => 1, 'bar' => 2 })
      end

      context 'with params' do
        it 'returns requested row' do
          response = subject.select_one(
            'SELECT user, date FROM rspec WHERE id = {id:UInt32}',
            params: { id: 2 }
          )

          expect(response).to eq(
            'user' => 'Bob',
            'date' => Date.new(2000, 1, 2)
          )
        end
      end
    end

    context 'when not exists' do
      it 'works' do
        expect(subject.select_one('SELECT NULL')).to eq({ 'NULL' => nil })
      end

      context 'with params' do
        it 'returns nil' do
          response = subject.select_one(
            'SELECT user, date FROM rspec WHERE id = {id:UInt32}',
            params: { id: 100 }
          )

          expect(response).to eq(
            nil
          )
        end
      end
    end
  end

  describe '#select_all' do
    context 'when empty' do
      it 'works' do
        expect(subject.select_all('SELECT * FROM rspec WHERE id = 100').to_a).to eq([])
      end
    end

    context 'when exists' do
      let(:expectation) do
        [
          { 'date' => Date.new(2000, 1, 2), 'id' => 2, 'user' => 'Bob' },
          { 'date' => Date.new(2000, 1, 3), 'id' => 3, 'user' => 'Charlie' }
        ]
      end

      it 'works' do
        response = subject.select_all(
          'SELECT * FROM rspec WHERE date > {date:Date}',
          params: { date: Date.new(2000, 1, 1) }
        )

        expect(response.to_a).to match_array(expectation)
      end
    end
  end
end
