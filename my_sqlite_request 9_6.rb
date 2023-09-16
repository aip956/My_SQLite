require 'csv'


class MySqliteRequest
    def initialize
        @table_name = ''
        @select_columns = []
        @where_conditions = {}
        @join_conditions = {}
        @selected_rows = []
        @join_type = ''
        @order_by = nil
        @insert_table_name = ''
        @insert_values = []
        @update_set_values = {}
        @delete_conditions = {}
    end


    def from(table_name) 
        # gets called on mySQL.from
        # self.new(table_name)
        @table_name = table_name
        puts "table: #{@table_name}"
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

    # def select(*column_names)
    #     @select_columns += column_names
    #     puts "Select: #{@select_columns}"
    #     self
    # end

    def where(column_name, criteria)
        @where_conditions << { column_name: column_name, critera: criteria }
        self
    end
    #     [column.to_sym] = value 
    #     puts "Where: #{@where_conditions}"
    #     self
    # end


    def where(column, value)
        @where_conditions[column.to_sym] = value 
        puts "Where: #{@where_conditions}"
        self
    end

    def join(column_on_db_a, filename_db_b, column_on_db_b, join_type='inner')
        # join method which loads another fildename_db and 
        # will join both with 'on' column
        @join_conditions = { column_on_db_a => { file: filename_db_b, column: column_on_db_b} }
        puts "54join_cond: #{@join_conditions}"
        @join_type = join_type
        # puts "Matched row: #{row}"
        # puts "column_on_db_a: "
        self
    end

    def order(order, column_name)
        # sort depending on the order base on the column_name
        # selected_rows = @selected_rows
        @order_by = "#{column_name} #{order.downcase}" 
        puts "Order: #{@order_by}"
        self
    end

    def insert(table_name)
        # insert a tablename
        file_path = File.expand_path(table_name)
        puts "File path: #{file_path}"
        @insert_table_name = table_name
        puts "insert_tbl_name: #{@insert_table_name}"
        self
    end

    def insertInto(table_name)
        @insertInto = CSV.read(table_name, headers: true)
        @file_path = table_name
        # puts "File exists: #{File.exist?(@insertInto)}"
        puts "insertInto: #{@insertInto}"
        puts "75: #{!@insertInto.empty?}"
        self
    end

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
        # receives data, a has of data on formant key => value
        # updae of attributes on all matching row
        # might be associated with a where request
        @update_set_values = data
        self
    end

    def delete
        # request to delete on all matching row
        # might be associated with a where request
        @delete_conditions = @where_conditions
        self
    end

 

    def run
        puts "run method"
        matched_rows = []
        selected_rows = @selected_rows.dup
        puts "dba:"
        puts "#{!@table_name.empty?}"
        if !@table_name.empty?
            begin
                db_a = CSV.table(@table_name, headers: true, header_converters: :symbol)
            rescue Errno::ENOENT
                # Handle the case where the table does not exist
                puts "Error: Table '#{@table_name}' does not exist."
                return []
            end
        end
        #puts "CSV Table: #{db_a}"
        #puts "115"
        #puts "115: #{!@insert_table_name.empty?}"
        # Handle INSERT operation 
        if !@insert_table_name.empty? 
            begin
            #puts "104"
                CSV.open(@table_name.to_s, 'a') do |dest_csv|
                    CSV.foreach(@insert_table_name, headers: true) do |row|
                        dest_csv << row.fields
                    end
                end
            rescue Errno::ENOENT
                # Handle the case where the table does not exist
                puts "Error: Table '#{@table_name}' for insertion does not exist."
            rescue StandardError => e
                # Handle other potential erros during insertion
                puts "Error: #{e.message}"
            end
        end



            # puts "db_a after insert: #{db_a}"
        #end

        puts "122: #{@insertInto}"
        puts "123: #{@insert_values}"
        puts "124: #{@file_path}"

        if !@insertInto.empty? && !@insert_values.empty?
            puts "114"
            CSV.open(@file_path, 'a') do |dest_csv|
                if dest_csv.count.zero?
                    dest_csv << @insert_values.first.keys
                end
                @insert_values.each do |data|
                    # new_row = CSV::Row.new(dest_csv.headers, data.values)
                    dest_csv << data.values
                end
            end
        matched_rows
    end

        
        #join if there are join conditions
        if !@join_conditions.empty?
            join_table_name, join_column_data = @join_conditions.first
            join_filename = join_column_data[:file]
            join_column_name = join_column_data[:column]
            
            begin
            # puts "119: join conditions: #{@join_conditions}"
            puts "120: join conditions.first: #{@join_conditions.first}"
            #join_table_name, join_column_data = @join_conditions.first
            #join_filename = join_column_data[:file]
            #join_column_name = join_column_data[:column]
                db_b = CSV.table(join_filename, headers: :true, header_converters: :symbol)
            rescue Errno:: ENOENT
                #Handle the case where the joined table does not exist
                puts "Error: table '#{join_filename}' for JOIN does not exist."
                return []
            end

            db_a = db_a.map do |row|
                joined_rows = db_b.select do |b_row|
                    #Case-insensitive column name matching
                    column_index = b_row.headers.index { |header| header.to_s.downcase == join_column_name.downcase }
                        
                    if column_index
                        # column_name = b_row.headers[column_index].to_s
                        compare_result = b_row[column_index] == row[join_table_name.to_sym]
                        puts "131: b_row[#{column_index}]: #{b_row[column_index]}"
                        puts "134: b_row[0]: #{b_row[0]}"

                        if compare_result
                            merged_hash = row.to_h.merge(b_row.to_h)
                            merged_row = CSV::Row.new(merged_hash.keys, merged_hash.values)
                            matched_rows << merged_row if match_where_conditions?(merged_row, @where_conditions, @table_name, join_table_name, join_filename)
                        end
                    else
                        puts "222 Warning: Join column '#{join_column_name}' not found in headers"
                    end
                end

                matched_rows << row if joined_rows.empty? && match_where_conditions?(row, @where_conditions, @table_name, join_table_name, join_filename)
                row
            end
        else
            puts "No join conditions specified"
            matched_rows = db_a.select { |row| match_where_conditions?(row, @where_conditions, @table_name, join_table_name, join_filename) }
        end  
                
        #matched_rows.each do |row|
        #     selected_data = {}
        #     @select_columns.each do |column_name|
        #         selected_data[column_name.to_sym] = row[column_name.to_sym]
        #     end
        #     @selected_rows << selected_data
        # end
  
        # puts "149Matched_rows: #{matched_rows}"
        # puts "159Order by: #{@order_by}"

        if @order_by
            puts "there is an order by"
            order_column, order_direction = @order_by.split(' ')
            order_column = order_column.to_sym
            order_direction = order_direction.downcase.to_sym
            # Check if the specified column exists in the result set
            if @selected_rows.first && @sel_rows.first.key?(order_column)
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
        puts "Selected rows: #{@selected_rows}"
        @selected_rows


            # order_column = @order_by.split(' ').first.to_sym
            # order_direction = @order_by.split(' ').last.downcase.to_sym
            # puts "order_col: #{order_column}"
            # puts "order_col: #{order_direction}"
        #     puts "@sel_rows: #{@selected_rows}"
        #     @selected_rows = @selected_rows.sort do |row1, row2|
        #         value1 = row1[order_column]
        #         value2 = row2[order_column]

        #         if order_direction == :asc 
        #             # puts "Order asc"
        #             value1 <=> value2
        #             # puts "vsal1: #{value1}"
        #             # puts "val1: #{value2}"
        #         else
        #             # puts "Order desc"
        #             value2 <=> value1
        #         end
        #     end
        # end 
        # puts "Selected rows: #{@selected_rows}"
        # @selected_rows
        
    end



    

    
    private
    def match_where_conditions?(row, where_condition, table_name, join_table_name=nil, join_filename=nil)
        return true if where_condition.empty?
        # puts "In match where method"
        # puts "row: #{row}"
        # puts "where_condition: #{where_condition}"
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
# request = request.from('nba_player_data.csv')
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


# Values, update, set
# name,year_start,year_end,position,height,weight,birth_, college
# Alex Abrines,2017,2018,G-F,6-6,190,"August 1, 1993",

# request = request.values({ column1: 'value3', column2: 'value4' })

request = MySqliteRequest.new
request = request.insertInto('nba_player_test.csv')
request = request.values({name: 'Alex Abrines',year_start: '2017', year_end: '2018', position: 'G-F', height: '6-6', weight: '190', birth_:  "August 1, 1993"})
request.run