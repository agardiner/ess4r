class Essbase

    # Extracts data from Essbase using the Grid API.
    class GridExtract < Extract

        # Create a grid extract instance
        #
        # @param cube [Cube] The cube from which to run the extract.
        # @param extract_spec [Hash] A hash containing a grid specification plus
        #   other options to control the extract process.
        def initialize(cube, extract_spec, options = {})
            super(cube, extract_spec)
        end


        # Create and run a grid API query to export data.
        #
        # Note: If you receive an error about exceeding the maximum number of rows
        #   of 1000, you can create an essbase.properties file in the working dir
        #   and add the following entry:
        #
        #       service.olap.dataQuery.grid.maxRows = 50000
        #
        # @param output_file [String] A path to where the extract should be saved.
        # @param options [Hash] An options hash for controlling various facets of
        #   the extract process.
        def extract_data(output_file, options = {})
            log.info "Extracting #{@cube} data via Grid API..."
            cv = @cube.open_cube_view
            begin
                grid = try{ cv.getGridView() }
                generate_grid(gird, options)
                cv.retrieve(options)
                save_output_to_file(grid, output_file, options)
            ensure
                cv.close
            end
        end


        # Generates a retrieve grid from +grid+.
        #
        # @param grid [Array<Array>] An array-of-arrays, where the outer array
        #   is the rows of the grid, and each inner array are the cells for a
        #   single row.
        # @param options [Hash] An options hash.
        def generate_grid(grid, options)
            pov_count = extract_spec[:pov] && extract_spec[:pov].size
            pov_offset = pov_count ? 1 : 0
            row_dims = extract_spec[:grid].first.find_index{ |c| !c.nil? }
            row_count = extract_spec[:grid].size
            col_count = extract_spec[:grid].max_by(&:size).size
            if pov_count
                row_count += 1
                pov_len = row_dims + pov_count
                col_count = pov_len if pov_len > col_count
            end
            extract_spec[:grid].each_with_index do |row, row_i|
                row_i += pov_offset
                row.each_with_index do |cell, col_i|
                    if cell
                        try{ grid.setValue(row_i, col_i, cell) }
                    end
                end
            end
        end


        # Saves the grid to a CSV file.
        #
        # @param grid [GridView] The grid to be saved to the file.
        # @param file_name [String] The path to save the grid to.
        # @param options [Hash] An options hash.
        # @option options [String] :separator The column separator to use.
        def save_output_to_file(grid, file_name, options)
            require 'csv'
            rows = grid.getCountRows()
            cols = grid.getCountColumns()
            row_count = 0
            cb = options[:output_handler]
            CSV.open(file_name, "wb", col_sep: options.fetch(:separator, ',')) do |csv|
                (0...rows).each do |row_i|
                    cells = []
                    (0...cols).each do |col_i|
                        #cell = try{ grid.getCell(row_i, col_i) }
                        cells << try{ grid.getStringValue(row_i, col_i) }
                    end
                    cb.call(cells) if cb
                    csv << cells
                    row_count += 1
                end
            end
            log.fine "Output #{row_count} records to #{file_name}"
            row_count
        end

    end

end
