require_relative 'extract'
require 'csv'


class Essbase

    # Handles the layout and population of a report from an Essbase::Extract.
    #
    # Although the data in a report comes from an Essbase extract, there are a
    # number of additional features provided in this class for formatting one or
    # more extracts into a report:
    # - Members may need to be mapped to report-specific labels which are not
    #   the same as the Essbase alias.
    # - Data may need to be scaled, formatted, and sign-flipped appropriately
    #   for the measure.
    # - A dimension can only appear once in an extract, but a report may need
    #   to populate multiple columns from a single dimension.
    # - Extracts are symmetrical, but reports may be asymmetrical.
    # - Data may need to be sorted and/or filtered.
    # - Formatting may need to be overridden for some combinations of row/column.
    class Report < Base

        # The name of the report
        attr_reader :name
        # The cube from which the report is produced
        attr_reader :cube
        # The extract definition used to extract the data in the report
        attr_reader :extract

        # Whether to include additional columns and rows showing unmapped member
        # names.
        attr_reader :include_names
        # Whether to include additional columns and rows showing unmapped member
        # aliases.
        attr_reader :include_aliases
        # Whether to include rows containing only missing or zero values in the
        # report.
        # Note: Missing values can also be suppressed in the extract, but values
        # may be returned from the extract that become zero after scaling in the
        # report. This option controls whether these rows should be suppressed.
        attr_reader :include_missing_and_zero

        #attr_reader :filter_count


        # Creates a new Report specification.
        #
        # @param name [String] The name of the report.
        # @param cube [Cube] The cube from which the report will be run.
        # @param extract [Hash] An extract specification identifying the data
        #   from which the report will be populated.
        # @param layout [Hash] A layout specification, identifying how the
        #   report should be laid out, formatted, etc.
        # @param options [Hash] An options hash.
        # @option options [Boolean] :include_names Rows/columns that contain
        #   members will be duplicated with an identical row/column beside them
        #   that contain the member name, rather than the mapped name. Useful
        #   for debugging.
        # @option options [Boolean] :include_aliases Rows/columns that contain
        #   members will be duplicated with an identical row/column beside them
        #   that contain the member alias, rather than the mapped name. Useful
        #   for debugging.
        # @option options [Hash] :maps Any global maps to apply to members in
        #   the absence of a specific map in the +layout+.
        # @option options [Fixnum] :filter The maximum number of records to
        #   include for common values in the filter column(s). For example,
        #   if an Entity column is marked as a filter column, and :filter is
        #   set to 50, then a maximum of 50 records will be returned for any
        #   individual entity.
        def initialize(name, cube, extract, layout, options = {})
            super('report')
            @name = name
            @cube = cube
            @extract = extract.is_a?(Extract) ? extract :
                MdxExtract.new(cube, extract, options)

            @include_names = options.fetch(:include_names, false)
            @include_aliases = options.fetch(:include_aliases, false)
            @suppress_missing = options.fetch(:suppress_missing, true)
            @suppress_zero = !options.fetch(:suppress_zero, true)
            @filter = options[:filter]
            all_maps = options.fetch(:maps, {})

            # Create columns for output/layout
            idx = 0
            @columns = []
            layout.each do |col|
                ci = Column.new(self, col, all_maps, idx)
                if ci.axis == :row
                    if @include_names
                        ci_tc = Column.new(self, col, nil, idx)
                        @columns << ci_tc
                        idx += 1
                    end
                    if @include_aliases
                        ci_al = Column.new(self, col, :alias, idx)
                        @columns << ci_al
                        idx += 1
                    end
                end
                @columns << ci
                idx += 1
                @sort_required ||= ci.sort_order
                @filter_required ||= ci.filter_key?
            end
            @filter_count = @filter_required && spec.filter
            @row_count = 0
        end


        def sort_required?
            @sort_required
        end


        def filter_required?
            @filter_required
        end


        # Returns the names of the columns in the sheet
        def column_headers
            @columns.map(&:header)
        end


        # Returns the content specification for each column in the sheet
        def column_content
            @columns.map{ |col| [col.content].flatten.join(' / ') }
        end


        # Populate data for the report via the associated Extract.
        def populate(options = {})
            opts = options.merge(
                suppress_missing: @suppress_missing,
                output_handler: lambda{ |data, chunk|
                    if file_name = options[:response_file]
                        data.to_file(file_name,
                                     file_mode: chunk == 0 ? 'w' : 'a',
                                     include_col_headers: chunk == 0)
                    end
                    process_data(data, chunk)
                }
            )
            extract_specs = [extract_specs] unless extract_specs.is_a?(Array)
            @data = []
            extract_specs.each do |extract_spec|
                @layout_mapped = false
                @extract.extract_data(nil, opts)
            end
            sort_data if @sort_required
            filter_data if @filter_required
            @data
        end


        def row_count
            @data && @data.length
        end


        def clear
            @data = nil
        end


        def save(path)
            CSV.open(path, "w") do |csv|
                csv << column_headers
                @data.each do |row|
                    csv << row
                end
            end
            log.fine "Report saved to #{path}"
        end


        def load(path)
            if File.exists?(path)
                @data = CSV.read(path)
                @data.shift(1)
            else
                log.warn "No data file found at #{path}"
                @data = []
            end
            @data
        end


        def missing_maps
            mm = @columns.map{ |ci| ci.missing_maps.values }.flatten.sort
            mm.each{ |m| m.report = self.name }
            mm
        end


        def to_s
            @name
        end


        private

        # Processes a data set returned from an MDX query.
        def process_data(data, chunk)
            if data.row_count > 0
                unless @layout_mapped
                    @columns.each{ |col| col.find_data_set_col(data) }
                    @layout_mapped = true
                end
                data.each(false) do |row|
                    out_row = []
                    @columns.each do |ci|
                        out_row << ci.populate_cell(row)
                    end
                    if !@suppress_missing || !@suppress_zero || out_row.find{ |cell| Numeric === cell && cell != 0 }
                        @data << out_row
                    end
                end
            end
        end


        # Sorts the data according to any column sort_order specifications.
        def sort_data
            sort_cols = @columns.select{ |col| col.sort_order }.sort_by{ |col| col.sort_order }
            if sort_cols.length > 0
                log.fine "Sorting by #{sort_cols.map(&:header).to_sentence}"
                @data.sort! do |a, b|
                    ord = nil
                    sort_cols.each do |col|
                        if col.sort_ascending
                            a_val = a[col.column_index]
                            b_val = b[col.column_index]
                        else
                            a_val = b[col.column_index]
                            b_val = a[col.column_index]
                        end
                        ord = a_val <=> b_val || (a_val && 1) || (b_val && -1) || 0
                        break unless ord == 0
                    end
                    ord
                end
            end
        end


        # Keeps only the first @filter_count records, based on unique combinations
        # of filter_key columns.
        def filter_data
            filter_cols = @columns.select{ |col| col.filter_key? }
            if filter_cols.length > 0
                log.fine "Filtering top #{@filter_count} rows for combinations of #{
                    filter_cols.map(&:header).to_sentence}"
                combo_count = Hash.new{ |h, k| h[k] = 0 }
                @data.reject! do |row|
                    key = filter_cols.map{ |col| row[col.column_index] }.join('|')
                    count = combo_count[key] += 1
                    count > @filter_count
                end
                log.fine "Retained #{@data.length} rows"
            end
        end

    end

end

require_relative 'report/column'
require_relative 'report/missing_map'
