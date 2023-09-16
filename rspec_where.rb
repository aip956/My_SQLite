require 'rspec'
require File.expand_path('../my_sqlite_request', __FILE__)

describe MySqliteRequest do
  context 'with a WHERE condition' do
    let(:request) { MySqliteRequest.new }

    it 'selects rows where college is "University of California, Los Angeles"' do
        request = MySqliteRequest.new
        request.from('/Users/antheaip/Documents/Coding/Coding_Git/Ruby/mySQLite/nba_player_data_test.csv')
        request.where('college', 'University of California, Los Angeles')
        selected_rows = request.run
      
      
      # Assuming you know the expected number of rows that match the condition
      expect(request.run.count).to eq(1)

      # You can also check the specific rows if you know their contents
      expected_row = {
        :name => "Kareem Abdul-Jabbar",
        :year_start => 1970,
        :year_end => 1989,
        :position => "C",
        :height => "7-2",
        :weight => 225,
        :birth_date => "April 16, 1947",
        :college => "University of California, Los Angeles"
      }

      # Check if the expected row is in the selected_rows
      expect(selected_rows.map(&:to_h)).to include(expected_row)

    end
  end
end