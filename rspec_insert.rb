# Tests the insert method; works

require 'rspec'
require File.expand_path('../my_sqlite_request', __FILE__)
  # Include your class file here
   # my_sqlite_request

RSpec.describe MySqliteRequest do
  describe '#insert' do
    it 'inserts data into nba_player_data_test.csv' do
      # Create an instance of your MySqliteRequest class
      request = MySqliteRequest.new
      # Add debugging output for @table_name
        puts "Table name before insert: #{request.instance_variable_get(:@table_name)}"


        # Specify the file to insert data into
        test_file = 'nba_player_data_test.csv'

        data_to_insert = {
        name: 'Matt Zunic',
        year_start: '1949',
        year_end: '1949',
        position: 'G-F',
        height: '6-3',
        weight: '195',
        birth_: 'December 19, 1919',
        college: 'George Washington University'
      }

      # Use the insert method to insert the data
      request.insert(test_file).values(data_to_insert).run

      # Now, you can check the content of the test file to verify the insertion
      # Read the contents of the test file
      test_file_contents = File.read(test_file)

      # Split the contents into lines
      lines = test_file_contents.split("\n")

      # Get the last line to check if the data was inserted
      last_line = lines[-1]

      # Define the expected last line
      expected_last_line = 'Matt Zunic,1949,1949,G-F,6-3,195,"December 19, 1919",George Washington University'

      puts "Last line in file:"
      puts last_line

      # Check if the last line matches the expected data
      expect(last_line).to eq(expected_last_line)
    end
  end
end
