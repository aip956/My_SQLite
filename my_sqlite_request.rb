require 'csv'


class MySqliteRequest
    def initialize
        @table_name = ''
        @select_columns = []
        @where_conditions = {}
        @join_conditions = nil
        @selected_rows = []
        @join_type = nil
        @order_by = nil
        @insert_table_name = ''
        @insert_values = []
        @update_set_values = {}
        @delete_conditions = {}
        @insertInto = []
        @mode = "r"
    end


    def from(table_name) 
        @table_name = table_name
        puts "23table: #{@table_name}"
        self
    end

    def select(columns)
        if columns.is_a?(Array)
            @select_columns += columns
        else
            @select_columns << columns
        end
        self
    end

    def where(column, value)
        @where_conditions[column.to_sym] = value 
        puts "39SQLWhere: #{@where_conditions}"
        self
    end

    def join(column_on_db_a, filename_db_b, column_on_db_b, join_type='inner')
        if @join_conditions.nil?
            @join_conditions = { column_on_db_a => { file: filename_db_b, column: column_on_db_b} }
            puts "54join_cond: #{@join_conditions}"
            @join_type = join_type
        else
            raise "Join operation already defined"
        end
        self
    end

# Check that order param is asc/desc
    def order(order, column_name)
        order = order.downcase
        unless %w[asc desc].include?(order)
            puts "Error: Invalid order direction '#{order}'. Use 'asc' or 'desc'."
            return self
        end
        @order_by = "#{column_name} #{order.downcase}" 
        puts "Order: #{@order_by}"
        self
    end

    def insert(table_name)
        file_path = File.expand_path(table_name)
        puts "File path: #{file_path}"
        @table_name = table_name
        @mode = "a+" # Set the mode to write, which replaces the file content
        @insert_table_name = table_name
        puts "insert_tbl_name: #{@insert_table_name}"
        self
    end
    def perform_insert
        return unless File.exist?(@insert_table_name)
        puts "71: #{@insert_table_name}" # Add this line for debugging
        if File.writable?(@insert_table_name)
            puts "File is writable" # Add this line for debugging
            begin
                CSV.open(@insert_table_name, 'a+', headers: true) do |dest_csv|
                # If file is empty, add headers
                if dest_csv.count.zero? && dest_csv.header_row.nil?
                    puts "77Before condition - dest_csv.count: #{dest_csv.count}"
                    dest_csv << @insert_values.first.keys
                    puts "Inside condition - Adding headers"
                    puts "Headers added"  # Add this line for debugging
                end
                    puts "83After condition"
                    #Append the data to the CSV
                    @insert_values.each do |data|
                        puts "86 inserting values"
                        puts "79: #{@insert_values}"
                        dest_csv << data.values
                        puts "81: #{dest_csv}"
                    end
                end
                puts "Insertion successful" # Add this line for debugging
            rescue StandardError => e
                puts "Error: #{e.message}"
            end
        else
            puts "File is not writable" # Add this line for debugging
        end
        @mode = "r"
    end

    # def insertInto(table_name)
    #     @insertInto = CSV.read(table_name, headers: true)
    #     @file_path = table_name
    #     # puts "File exists: #{File.exist?(@insertInto)}"
    #     puts "insertInto: #{@insertInto}"
    #     puts "75: #{!@insertInto.empty?}"
    #     self
    # end

    def values(data)
        # receives data, a has of data on formant key => value
        @insert_values << data
        puts "insert_values: #{@insert_values}"
        puts "84: #{!@insert_values.empty?}"
        self
    end

    def update(table_name)
        @update_table_name = table_name
        self
    end

    def set(data)
        @update_set_values = data
        self
    end

    def delete
        @delete_conditions = @where_conditions
        self
    end

 

    def run
        puts "143start run"
        @selected_rows = []

        if @mode == "r"
            puts "140dba: "
            @selected_rows.each { |row| puts row }
        elsif @mode == "a"
            puts "run method"
            puts "table_name: #{@table_name}"
        end

        
        matched_rows = []
        # selected_rows = @selected_rows.dup
        puts "150dba:"
        puts "106table_name: #{@table_name}"
        puts "107Script started. Current directory: #{Dir.pwd}"
        puts "108@selected_rows before: #{@selected_rows.inspect}"
        

        #Load data from main table
        db_a = []
        puts "160is !empty? #{!@table_name.empty?}"
        puts "160is File.exist? #{File.exist?(@table_name)}"
        if !@table_name.empty? && File.exist?(@table_name)
            puts "160is !empty? #{!@table_name.empty?}"
            begin
                db_a = CSV.table(@table_name, headers: true, header_converters: :symbol)
                puts "115Successfully loaded CSV data."
            rescue Errno::ENOENT
                # Handle the case where the table does not exist
                puts "Error: Table '#{@table_name}' does not exist."
                return @selected_rows
            end
        end
        

        #Insert data from one file to another
        if !@insert_table_name.empty? && !@insert_values.empty?
            perform_insert
        end
                
        #Check if there are WHERE conditions
        if @where_conditions.empty?
            puts "No WHERE conditions"
            return @selected_rows
        end
        puts "SQL181Where conditions: #{@where_conditions}"
        puts "Debug - db_a: #{db_a.inspect}"
        puts "CSV file path: #{@table_name}"
        puts "Line 2 from db_a: #{db_a[1]}"

        #Filter rows based on WHERE conditions
        db_a.each do |row|
            puts "196SQLRow: #{row}"
            if match_where_conditions?(row, @where_conditions, @table)
                matched_rows << row
                puts "SQL199: #{matched_rows}"
            end
        end
        # Print out matching rows
        # puts "Matched rows: "
        # matched_rows.each do |key, value|
        #     puts  "#{key}: #{value}"
        # end
        # # end
        # @selected_rows = matched_rows

       
        
        #join if there are join conditions
        if !@join_conditions.empty?
            join_table_name, join_column_data = @join_conditions.first
            join_filename = join_column_data[:file]
            join_column_name = join_column_data[:column]
            
            begin
                puts "220: join conditions.first: #{@join_conditions.first}"
                db_b = CSV.table(join_filename, headers: :true, header_converters: :symbol)
            rescue Errno:: ENOENT
                #Handle the case where the joined table does not exist
                puts "Error: table '#{join_filename}' for JOIN does not exist."
                return []
            end

            #Perform join
            matched_rows = db_a.map do |row|
                puts "229 #{row}"
                joined_rows = db_b.select do |b_row|
                    # puts "231 #{b_row}"
                    #Case-insensitive column name matching
                    column_index = b_row.headers.index { |header| header.to_s.downcase == join_column_name.downcase }
                    
                    if column_index
                        compare_result = b_row[column_index] == row[join_table_name.to_sym]
                        puts "237: b_row[#{column_index}]: #{b_row[column_index]}"
                        puts "238: b_row[0]: #{b_row[0]}"
                        puts "239compresult #{compare_result}" 
                        if compare_result
                            merged_hash = row.to_h.merge(b_row.to_h)
                            merged_row = CSV::Row.new(merged_hash.keys, merged_hash.values)
                            merged_row = match_where_conditions?(merged_row, @where_conditions, @table_name, join_table_name, join_filename)
                            @selected_rows << merged_row if merged_row
                        end
                        puts "246" 
                    else
                        puts "222 Warning: Join column '#{join_column_name}' not found in headers"
                    end
                end

                # if joined_rows.empty?
                #     match_where_conditions?(row, @where_conditions, @table_name, join_table_name, join_filename)
                # else
                #     false
                # end
            end
        else
            puts "No join conditions specified"
        end  
                
        #Sort the result if there is an ORDER BY
        if @order_by
            puts "there is an order by"
            order_column, order_direction = @order_by.split(' ')
            order_column = order_column.to_sym
            order_direction = order_direction.downcase.to_sym

            unless %i[asc desc].include?(order_direction)
                puts "Error: Invalid order direction '#{order_direction}'. Use ':asc' or ':desc'."
                return []
            end
        
            # Check if the specified column exists in the result set
            if @selected_rows.first && @selected_rows.first.key?(order_column)
                @selected_rows = @selected_rows.sort_by do |row|
                    row[order_column]
                end

                # Apply descending order if specified
                @selected_rows.reverse! if order_direction == :desc
            else
                puts "Warning: Column '#{order_column}' does not exist for sorting"
            end
        else
            # Apply a default sorting order (e.g. ascending by ID) if no ORDER by class provided
            @selected_rows.sort_by! { |row| row[:id] }
        end

        # Print the sorted result
        puts "292Selected rows: #{@selected_rows}"
        @selected_rows = matched_rows
    end



    

    
    private
    def match_where_conditions?(row, where_condition, table_name, join_table_name=nil, join_filename=nil)
        return true if where_condition.empty?
        # puts "241In match where method"
        # puts "242row: #{row}"
        # puts "243where_condition: #{where_condition}"
        # puts "table_name: #{table_name}"
        # puts "join_table_name: #{join_table_name}"
        # puts "join_filename: #{join_filename}"
        conditions_met = where_condition.map do |column_name, value|
            if column_name.to_s.include?('.')
                column_parts = column_name.to_s.split('.')
                table = column_parts.first
                column = column_parts.last.to_sym

                if table == table_name.to_s
                    row[column] == value
                elsif table == join_table_name.to_s
                    joined_rows = CSV.table(join_filename, headers: true, header_converters: :symbol)
                    joined_rows.any? { |b_row| b_row[column] == value }
                else
                    false
                end
            else
                #Handle column names without table prefix
                row[column_name.to_sym] == value
            end
        end
        conditions_met.all?
    end
end





# create instances
# request = MySqliteRequest.new("string")
# has access to all methods

# request.where("col1", "val2")
# request.select("col_name")
# request.from("'nba_player_data2.csv")

# MySqliteRequest.from('nba_player_data2.csv').where('college', 'DePaul University').select('name').run

# request = MySqliteRequest.new
# # request = request.from('nba_player_data.csv')
# request = request.from(File.expand_path('nba_player_data.csv'))
# request = request.select('name')
# request = request.where('college', 'DePaul University')
# request.run

# Test join
# request = MySqliteRequest.new
# request = request.from('nba_player_data2.csv')
# request = request.join('name', 'nba_players2.csv', 'Player')
# request = request.select('name')
# request = request.where('birth_state', 'Illinois')
# request.run

# Test order
# request = MySqliteRequest.new
# request = request.from('nba_player_data.csv')
# request = request.select('name')
# request = request.where('college', 'DePaul University')
# request = request.order('DESC', 'name')
# request.run

# request = MySqliteRequest.new
# request = request.from('nba_player_data.csv')
# request = request.select('name')
# request = request.where('college', 'DePaul University')
# request = request.order('ASC', 'name')
# request.run

# Test Insert; works with nba_player_data
# request = MySqliteRequest.new
# request = request.from('nba_players2.csv')
# request = request.insert('nba_players3.csv')
# request.run
# NBA_player_data test.csv; Matt Zunic,1949,1949,G-F,6-3,195,"December 19, 1919",George Washington University



# Values, update, set
# name,year_start,year_end,position,height,weight,birth_, college
# Alex Abrines,2017,2018,G-F,6-6,190,"August 1, 1993",

# request = request.values({ column1: 'value3', column2: 'value4' })

# request = MySqliteRequest.new
# request = request.insertInto('nba_player_test.csv')
# request = request.values({name: 'Alex Abrines',year_start: '2017', year_end: '2018', position: 'G-F', height: '6-6', weight: '190', birth_:  "August 1, 1993"})
# request.run