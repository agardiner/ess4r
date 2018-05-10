class Essbase

    # Represents a grid of results returned by MaxL.
    class MaxlResultSet < Base

        # @!visibility private
        #
        # Creates a MaxL result set.
        #
        # Note: Should not be instantiated directly
        #
        # @param maxl [Maxl] A Maxl session object
        # @param resultset [IEssMaxlResultSet] The Maxl result set JAPI object
        #   to be wrapped.
        def initialize(maxl, result_set)
            super(maxl.log, '@result_set', result_set)
        end


        # @return [Array] of the available column names in this result set.
        def column_names
            (1..(@result_set.column_count)).map do |col|
                @result_set.column_name(col)
            end
        end


        # @param cols [Array] An optional list of column names whose info is
        #   required. If not specified, column info for all columns is returned.
        # @return [Hash<String, Hash>] details about the columns in this result set.
        #   The keys are the column names (String), and the values are themselves
        #   hashes with +:index+ and +:type+ entries. The :index entry holds the
        #   index of the column in this result set, and the :type entry indicates
        #   the type of data in that column (+:string+, +:int+, +:long+, +:boolean+,
        #   +:double+, etc)
        def column_info(cols=[])
            col_info = {}
            (1..(@result_set.column_count)).each do |col|
                col_name = @result_set.column_name(col)
                if cols.empty? || cols.include?(col_name)
                    ct = @result_set.column_type(col)
                    col_info[col_name] = {
                        :index => col,
                        :type => (ct && ct.string_value.downcase.intern) || :long
                    }
                end
            end
            unknown = cols - col_info.keys
            if unknown.size > 0
                log.warning "The following column(s) were not recognized: #{unknown.join(', ')}"
            end
            col_info
        end


        # Processes a MaxL resultset, returning the results as an Array of Hashes
        # containing the result records.
        #
        # @param cols [Array<String>] An optional list of columns to return. If
        #    specified, only these columns are returned.
        def to_hash(cols=[])
            col_info = column_info(cols)
            rows = []
            while try { @result_set.next } do
                row = []
                col_info.each do |col, info|
                    obj = try{ @result_set.object(info[:index]) }
                    case info[:type]
                    when :string
                        row[col] = obj.to_s
                    when :boolean
                        row[col] = (obj.to_s =~ /true/i ? true : false)
                    when :long
                        row[col] = obj.to_i
                    when :double
                        row[col] = obj.to_f
                    else
                        log.warning "Unkown type: #{info[:type]} for #{col}"
                        row[col] = obj.to_s
                    end
                end
                rows << row
            end
            rows
        end


        # Processes a MaxL resultset, returning the results as an Array of Arrays
        # containing the result records.
        #
        # @param cols [Array<String>] An optional list of columns to return. If
        #    specified, only these columns are returned.
        def to_a(cols=[])
            col_info = column_info(cols)
            rows = []
            rows << col_info.keys
            while try { @result_set.next } do
                row = []
                col_info.each do |col, info|
                    obj = try{ @result_set.object(info[:index]) }
                    case info[:type]
                    when :string
                        row << obj.to_s
                    when :boolean
                        row << (obj.to_s =~ /true/i ? true : false)
                    when :long
                        row << obj.to_i
                    when :double
                        row << obj.to_f
                    else
                        log.warning "Unkown type: #{info[:type]} for #{col}"
                        row << obj.to_s
                    end
                end
                rows << row
            end
            rows
        end


        # Returns the MaxL rsults as a string.
        #
        # TODO: Determine appropriate column widths and format so columns line up
        def to_s
            self.to_a.map{ |row| row.join('  ') }.join("\n")
        end

    end

end
