require 'bigdecimal'


class Essbase

    # Represents the results of an MDX query.
    class MdxDataSet < Base

        # Determines whether to return rows that contain only zeros (or missing).
        # Default is true.
        attr_accessor :suppress_zeros

        # Control the number of decimal places output
        attr_accessor :decimals
        alias_method :decimal_places, :decimals
        alias_method :decimal_places=, :decimals=

        attr_reader :suppress_members, :map_members

        # @!attribute suppress_members
        #
        #   Gets/sets any members that should be suppressed in the output returned
        #   via #each or #to_file. When a suppressed member is encountered in a
        #   column, row, or page, that column, row, or page is skipped (i.e. not
        #   output).
        #
        #   @param mbrs [Array<String>] An array of member names that should be
        #     suppressed.
        def suppress_members=(mbrs)
            @suppress_members = mbrs.map(&:upcase)
            @suppress_cols = Array.new(column_count)
            (0...column_count).each do |col_num|
                @suppress_cols[col_num] = skip_tuple?(@cols, col_num)
            end
        end


        # @!attribute map_members
        #
        #   Gets/sets mappings for converting a source member name to a different
        #   name on output.
        #
        #   @param hsh [Hash<String, String>] A hash giving source -> target maps.
        def map_members=(hsh)
            @map_members = Hash[hsh.map{ |k, v| [k.upcase, v] }]
        end


        # Creates a new MdxDataSet object.
        #
        # @private
        def initialize(data_set)
            super("@data_set", data_set)
            axes = try{ @data_set.get_all_axes.to_a }
            # If a slicer (i.e. a WHERE clause) is specified, the slicer members
            # are the first axis; otherwise, columns are first.
            if axes[0].is_slicer_axis
                @slicer = axes.shift
            end
            @cols, @rows, @pages = axes
        end


        # @!group Axis Dimensions

        # Returns the number of dimensions in the slicer (aka POV) axis.
        #
        # @note The slicer axis is a special axis corresponding to the POV in
        #   a SmartView retrieval. It has a single member for each dimension
        #   that is specified in the WHERE clause of an MDX query.
        #
        # @return [Integer] a count of the number of slicer dimensions.
        def slicer_dimension_count
            @slicer && try{ @slicer.dimension_count } || 0
        end

        # Returns the names of the dimensions on the slicer (aka POV) axis.
        #
        # @return [Array] the names of the slicer dimension(s).
        def slicer_dimensions
            @slicer && map_names(try{ @slicer.get_all_dimensions }) || []
        end

        # @note The page axis is rarely used; it is the axis on which multiple
        #   members for a dimension can appear, but the member names do not
        #   appear on the rows or columns of the output data set. It is mostly
        #   used with the +suppress_members+ option; multiple page members are
        #   included in the query (usually to satisfy an MDX calculation), but
        #   only one member is ultimately yielded due to the +suppress_members+
        #   setting.
        #
        # @return [Integer] a count of the number of page dimensions.
        def page_dimension_count
            @pages && try{ @pages.dimension_count } || 0
        end

        # Returns the names of the dimensions on the page axis.
        #
        # @return [Array] the names of the page dimension(s).
        def page_dimensions
            @pages && map_names(try{ @pages.get_all_dimensions }) || []
        end

        # @return [Integer] a count of the number of row dimensions.
        def row_dimension_count
            @rows && try{ @rows.dimension_count } || 0
        end

        # Returns the names of the dimensions on the row axis.
        #
        # @return [Array] the names of the row dimension(s).
        def row_dimensions
            @rows && map_names(try{ @rows.get_all_dimensions }) || []
        end

        # @return [Integer] a count of the number of column dimensions.
        def column_dimension_count
            @cols && try{ @cols.dimension_count } || 0
        end

        # Returns the names of the dimensions on the column axis.
        #
        # @return [Array] the names of the column dimension(s).
        def column_dimensions
            @cols && map_names(try{ @cols.get_all_dimensions }) || []
        end

        # @!endgroup


        # @!group Axis Members

        alias_method :slicer_count, :slicer_dimension_count

        # @return [Array<String>] an array containing an entry for each member
        #   on the slicer axis.
        def slicer_members
            @slicer && map_names(try{ @slicer.get_all_tuple_members(0) }) || []
        end

        # @return [Integer] a count of the number of pages of data. Returns 0 if
        #   no page dimensions were specified.
        def page_count
            @pages && try{ @pages.get_tuple_count } || 0
        end

        # @return [Array<String>] an array containing an entry for each dimension
        #   member on the specified page.
        def page_members(page_num)
            @pages && map_names(try{ @pages.get_all_tuple_members(page_num) }) || []
        end

        # @return [Integer] a count of the number of rows of data *per page*.
        #
        # @see #record_count
        def row_count
            @rows && try{ @rows.get_tuple_count } || 0
        end

        # @return [Integer] a count of the number of rows across all pages.
        #
        # @see #row_count
        def record_count
            @record_count || try{ @data_set.get_cell_count / column_count } || 0
        end

        # @return [Array<String>] an array containing an entry for each row member
        #   on the specified +row_num+.
        def row_members(row_num)
            @rows && map_names(try{ @rows.get_all_tuple_members(row_num) }) || []
        end

        # @return [Integer] a count of the number of columns of data.
        def column_count
            @cols && try{ @cols.get_tuple_count } || 0
        end

        # @return [Array<String>] an array containing an entry for each column
        #   member on the specified +col_num+.
        def column_members(col_num)
            @cols && map_names(try{ @cols.get_all_tuple_members(col_num) }) || []
        end

        # @!endgroup


        # Returns the column ordinal for the column with the specified header(s)
        # in a grid that consists of column and row headers plus data.
        #
        # @param names One or more column header labels identifying the column
        #   to locate.
        def column_ordinal(*names)
            if names.length == 1
                if idx = row_dimensions.map(&:downcase).index(names.first.downcase)
                    return idx
                end
            end
            (0...column_count).each do |col|
                if column_members(col).map(&:downcase) == names.map(&:downcase)
                    return row_dimension_count + col
                end
            end
            raise ArgumentError, "No column in grid matches #{names.join(', ')}.\n" +
                "Available columns are: #{column_headers(:file).join(', ')}"
        end


        # Returns an array of arrays, representing the column headers of the
        # grid. The array contains an array for each row of column headers.
        #
        # @param header_style [Symbol] A symbol identifying the style of header
        #   to be returned. Valid values are as follows:
        #   - :grid, indicating a SmartView style where each column dimension is
        #     on a different line, and blank cells sit above the row axis
        #     columns
        #   - :file, indicating an extract file style, where a single header row
        #     is required, which should also include the dimension names for the
        #     row axes.
        def column_headers(header_style = :grid)
            headers = []
            if header_style == :file
                row = row_dimensions
                (0...column_count).each do |col_num|
                    unless @suppress_cols && @suppress_cols[col_num]
                        cell = (0...column_dimension_count).map do |col_dim|
                            column_members(col_num)[col_dim]
                        end
                        row << cell.join(':')
                    end
                end
                headers << row
            elsif header_style == :grid
                row_spacers = Array.new(row_dimension_count)
                (0...column_dimension_count).each do |col_dim|
                    row = row_spacers.dup
                    (0...column_count).each do |col_num|
                        unless @suppress_cols && @suppress_cols[col_num]
                            row << column_members(col_num)[col_dim]
                        end
                    end
                    headers << row
                end
            end
            headers
        end


        # Returns the cell contents in the grid at the specified row and column
        # (0-based).
        #
        # @param row The ordinal of the row
        # @param col The ordinal of the column
        # @param page The ordinal of the page
        def [](row, col, page=0)
            if row < column_dimension_count
                # In column headers
                if col >= row_dimension_count
                    row_dimensions[col - row_dimension_count]
                else
                    nil
                end
            else
                row -= row_dimension_count
                if col < row_dimension_count
                    # In row headers
                    row_members[row]
                else
                    # In data section
                    col -= row_dimension_count
                    ord = row * column_count + col
                    ord += row_count * col_count * page if page > 0
                    val = (try{ @data_set.is_missing_cell(ord) } ?
                           nil : try{ @data_set.cell_value(ord) })
                    val = val.round(@decimals) if val && @decimals
                    val
                end
            end
        end


        # Yields each row of the data set to the supplied block as an array of
        # field values. Each field yielded contains a value as would be seen in
        # an Essbase retrieval, i.e. a grid with column and row headers. If the
        # data set consists of multiple pages, and +include_page_headers+ is true,
        # then the grid for each page will be preceded by a single row that
        # contains an array of each page member.
        #
        # @example Grid without Column Headers
        #   [Row 1a, Row 1b, 100, 100, 100]
        #   [Row 1a, Row 2b, 100, 100, 100]
        #
        # @example Grid with Column Headers
        #   [nil, nil, Col A, Col B, Col C]
        #   [Row 1a, Row 1b, 100, 100, 100]
        #   [Row 1a, Row 2b, 100, 100, 100]
        #
        # @example Grid with Column and Page Headers
        #   [Page 1]
        #   [nil, nil, Col A, Col B, Col C]
        #   [Row 1a, Row 1b, 100, 100, 100]
        #   [Row 1a, Row 2b, 100, 100, 100]
        #   [Page 2]
        #   [nil, nil, Col A, Col B, Col C]
        #   [Row 1a, Row 1b, 100, 100, 100]
        #   [Row 1a, Row 2b, 100, 100, 100]
        #
        # @param include_col_headers [Boolean] A flag controlling whether column
        #   headers should be output/yielded.
        # @param include_page_headers [Boolean] A flag controlling whether page
        #   headers should be output/yielded. If true, at the start of each page,
        #   an Array will be yielded containing the member(s) for the current page.
        # @yield [row] Each row of the data set will be yielded as an Array.
        # @yieldparam row [Array] An array containing the member names and data
        #   values for the current row. May also include blank cells if a column
        #   header row is being yielded.
        def each(include_headers = true, header_style = :grid)
            col_count = column_count
            cells_per_page = row_count * col_count
            row = nil
            ord = 0
            limit = try{ @data_set.get_cell_count }
            @record_count = 0
            while ord < limit || ord == 0
                if cells_per_page
                    # Data set has pages
                    if cells_per_page == 0
                        page_num, page_rem = 0, 0
                    else
                        page_num, page_rem = ord.divmod(cells_per_page)
                    end
                    if page_rem == 0
                        # New page
                        if skip_tuple?(@pages, page_num)
                            ord += cells_per_page
                            next
                        end
                        # Page and column headers
                        if include_headers
                            yield page_members(page_num) if page_dimension_count > 0
                            column_headers(header_style).each{ |header_row| yield header_row }
                        end
                    end
                else
                    page_rem = ord
                end
                break if ord == 0 && limit == 0

                row_num = page_rem / col_count
                unless skip_tuple?(@rows, row_num)
                    row = row_members(row_num)
                    non_zero = !@suppress_zeros
                    (0...col_count).each do |col_num|
                        next if @suppress_cols && @suppress_cols[col_num]
                        val = (try{ @data_set.is_missing_cell(ord + col_num) } ?
                               nil : try{ @data_set.cell_value(ord + col_num) })
                        val = val.round(@decimals) if val && @decimals
                        row << val
                        non_zero ||= (val && val != 0)
                    end
                    if non_zero
                        yield row
                        @record_count += 1
                    end
                end
                ord += col_count
            end
            @record_count
        end


        # Writes the contents of this MDX query result set to +file_name+.
        #
        # @param file_name [String] The path to the file to be generated.
        # @param options [Hash] An options hash for controlling various aspects
        #   of the generated file.
        # @option options [String] :delimiter The character(s) to be used to
        #   separate each column in the generated file. Defaults to tab.
        # @option options [Boolean] :include_headers Whether or not to output
        #   headers at the top of the file. Defaults to true.
        # @option options [Boolean] :header_style The style of headers to output
        #   if headers are included; :grid or :file (@see #column_headers).
        #   Defaults to :file.
        # @option options [Boolean] :quote_members Whether or not to enclose
        #   member names in quotes. Defaults to nil, which means member names are
        #   are only quoted if they contain the delimiter character(s).
        # @option options [String] :missing_val the value to be output when a
        #   #Missing value exists in a cell. Defaults to '-'.
        # @option options [String] :file_mode the mode to open the output file
        #   in. Defaults to 'w'.
        # @option options [Integer] :decimals the maximum number of decimal places
        #   of precision. If not specified, values are output at full precision.
        def to_file(file_name, options = {})
            delimiter = options.fetch(:delimiter, "\t")
            include_headers = options.fetch(:include_headers, true)
            header_style = options.fetch(:header_style, :file)
            quote_mbrs = options.fetch(:quote_members, nil)
            missing_val = options.fetch(:missing_val, '-')
            file_mode = options.fetch(:file_mode, "w")
            decimals = (options[:decimals] || options.fetch(:decimal_places, @decimals))
            decimals = "%.#{decimals}f" if decimals

            file = File.new(file_name, file_mode)
            begin
                line_count = 0
                self.each(include_headers, header_style) do |row|
                    row = row.map do |cell|
                        case cell
                        when String
                            quote_mbrs || (quote_mbrs.nil? && cell.include?(delimiter)) ?
                                %Q{"#{cell}"} :
                                cell
                        when Fixnum, BigDecimal, Float
                            decimals ? decimals % cell : cell
                        else missing_val
                        end
                    end
                    file.puts row.join(delimiter)
                    line_count += 1
                end
            ensure
                file.close
            end
            line_count
        end


        # Dumps this data set to a String; useful for debugging.
        def to_s
            rows = []
            rows << "# POV: #{slicer_members.join("\t")}" if slicer_members
            self.each do |row|
                rows << row.join("\t")
            end
            rows.join("\n")
        end


        private


        # Determines whether a particular tuple should be skipped when iterating
        # over the grid. This is only true if the tuple contains a member from
        # the +suppress_members+ set.
        #
        # @param axis [IEssMdAxis] The MDX axis to be checked.
        # @param tuple_num [Integer] The tuple number on that axis to be checked.
        # @return [Boolean] True if the tuple should be skipped, false otherwise.
        def skip_tuple?(axis, tuple_num)
            skip = false
            if axis && @suppress_members
                try{ axis.get_all_tuple_members(tuple_num) }.each do |member|
                    skip = @suppress_members.include?(member.name.upcase)
                    break if skip
                end
            end
            skip
        end


        # Give a set of member names, converts these using the +map_members+
        # defined. If a member is not in the map_members map, it is returned
        # as is.
        #
        # @param mbrs [Array<String>] A list of members to map using the current
        #   member maps.
        # @return [Array<String>] The same set of members converted using member
        #   maps.
        def map_names(mbrs)
            mbrs && mbrs.map do |mbr|
                (@map_members && @map_members[mbr.name.upcase]) || mbr.name
            end
        end

    end

end
