require 'rspec'
require File.expand_path('../my_sqlite_request', __FILE__)

describe MySqliteRequest do
  context 'with a JOIN operation' do
    let(:request) { MySqliteRequest.new }
    it 'selects birth_state where college is "University of California, Los Angeles"' do
        request = MySqliteRequest.new
        main_table_path = ('/Users/antheaip/Documents/Coding/Coding_Git/Ruby/mySQLite/nba_player_data_test.csv')
        join_table_path = ('/Users/antheaip/Documents/Coding/Coding_Git/Ruby/mySQLite/nba_player_test.csv')

        join_column_on_main = 'name'
        join_column_on_join = 'Player'

        request.from(main_table_path)
        request.where('college', 'University of California, Los Angeles')
        request.join(join_column_on_main, join_table_path, join_column_on_join)
           

        request.select('birth_state')
        selected_rows = request.run

      # Assuming you know the expected number of rows that match the condition
    #   expect(request.run.count).to eq(1)

      # You can also check the specific rows if you know their contents
      expected_birth_state = 'New York' 

      # Check if the expected row is in the selected_rows
      puts "Debug - selected_rows: #{selected_rows.inspect}"
      expect(selected_rows[0][:birth_state]).to eq(expected_birth_state)
    end
  end
end
