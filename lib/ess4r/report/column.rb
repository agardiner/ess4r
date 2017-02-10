class Essbase

    class Report

        # Holds specifications for what should appear in a single column of a
        # report/schedule.
        class Column < Base

            attr_reader :header, :content, :map_name, :scale, :sign, :decimal_places,
                :sort_order, :sort_ascending
            attr_reader :column_index, :axis, :dimension, :missing_maps


            # Creates a new column for the report.
            #
            # @param report [Report] A reference to the report to which this
            #   column belongs.
            # @param col_cfg [Hash] Configuration details for this column.
            # @param all_maps [Hash] Any global maps to use for unmapped members
            #   of this column (assuming this is a member name column).
            # @param idx [Fixnum] The column number at which this column will
            #   appear (0-based).
            def initialize(report, col_cfg, all_maps, idx)
                super('report')
                @report = report
                content = get_hash_val(col_cfg, :content)
                case content
                when /^([^.]+)(?:\.(.+))?/
                    @content = $1
                when Array, NilClass
                    @content = content
                else
                    raise ArgumentError, "Column content specification #{content.inspect
                        } for column #{idx} is invalid; must be nil, a member name or an #{
                        }Array of member names (with optional expansion macro suffixes)"
                end

                @header = get_hash_val(col_cfg, :header)
                @scale = get_hash_val(col_cfg, :scale, 1)
                @sign = get_hash_val(col_cfg, :sign, 1)
                @decimal_places = get_hash_val(col_cfg, :decimal_places, 2)
                @sort_order = get_hash_val(col_cfg, :sort_order)
                @sort_ascending = !!(get_hash_val(col_cfg, :sort_direction, 'ASC') =~ /^asc/i)
                @filter_key = get_hash_val(col_cfg, :filter_key)

                # Determine the axis the column content will be found on in the MDX resultset
                # Note: We don't try to determine the column at this point, as it is liable to
                # move due to suppression etc; instead, we will locate the actual column once
                # data is returned.
                case @content
                when NilClass
                    @axis = :none
                when /^\s+$/
                    @axis = :none
                when String
                    if @report.extract.row_dims.find{ |dim| dim.downcase == @content.downcase }
                        @axis = :row
                    elsif @report.extract.col_dims.find{ |dim|
                            @report.extract.extract_members[dim].find{ |mbr|
                                mbr.downcase == @report.extract.quote_mbr(@content.downcase)
                            }
                        }
                        @axis = :column
                    else
                        raise ArgumentError, "Cannot locate #{@content} on rows (#{
                            @report.extract.row_dims.join(', ')}) or columns (#{
                            @report.extract.col_dims.join(', ')}) of extract"
                    end
                when Array
                    all_col_mbrs = @report.extract.col_dims.reduce([]) do |col_mbrs, col_dim|
                        col_mbrs.concat(@report.extract.extract_members[col_dim].map(&:downcase))
                    end
                    unk = (@content.map(&:downcase) - all_col_mbrs).map{ |m| @content.find{ |c| m == c.downcase} }
                    if unk.size > 0
                        raise ArgumentError, "Could not locate any column in the extract for report column #{
                            idx + 1} '#{@header}'. Unknown members are: #{unk.join(', ')}"
                    end
                    @axis = :column
                else
                    raise "Unexpected content specification: #{@content} (#{@content.class.name})"
                end
                @column_index = idx
                @dimension = nil
                @missing_maps = {}

                # Setup the mappings to be used for this column
                map = get_hash_val(col_cfg, :map)
                case
                when all_maps && (all_maps == :alias || map == :alias)
                    @map_name = :alias
                when all_maps && map.is_a?(Hash)
                    @map_name = "#{@header} inline map"
                    @column_maps = map
                when all_maps && map.is_a?(String)
                    @map_name = map
                    dim_maps = all_maps[@content]
                    @column_maps = dim_maps && dim_maps[@map_name]
                    if dim_maps && !@column_maps
                        @missing_maps[@map_name] = MissingMap.new(content, @map_name, nil,
                                                                  'No mapping table definition found')
                    end
                end

                # Setup any row-override formatting. This consists of a hash of members
                # from the specified row dimension, where the values are the format
                # overrides.
                if ov = get_hash_val(col_cfg, :row_override_formats)
                    @row_override_dim = ov.dimension? ? ov.dimension : @content
                    @row_override_mbrs = {}
                    ov.formats.each do |lbl, defn|
                        mbrs = defn.dup.delete(:members) || []
                        defn.scale = @scale unless defn.scale?
                        defn.decimal_places = @decimal_places unless defn.decimal_places?
                        defn.sign = @sign unless defn.sign?
                        mbrs.each do |mbr_lbl|
                            case mbr_lbl
                            when /^["\[]?(.+)["\]]?\.(Level0|Leaves|I?Children|I?Descendants|I?Ancestors)$/i
                                # Memer name with expansion macro
                                mbr = @report.cube[@row_override_dim][$1]
                                raise "Unrecognised #{@row_override_dim} member '#{$1}'" unless mbr
                                exp_mbrs = mbr.send($2.downcase.intern)
                                exp_mbrs.each do |exp_mbr|
                                    @row_override_mbrs[exp_mbr.name.upcase] = defn
                                end
                            else
                                @row_override_mbrs[mbr_lbl.upcase] = defn
                            end
                        end
                    end
                end
            end


            # Whether this column is a key column when grouping to filter
            def filter_key?
                @filter_key
            end


            # Locates the column in an MDX data set that contains the content
            # for this schedule column.
            def find_data_set_col(data_set)
                unless @axis == :none
                    case @axis
                    when :row
                        # Find the index of the row dimension that contains this column
                        @data_set_col = data_set.row_dimensions.index do |dim|
                            dim.upcase == @content.upcase
                        end
                        @dimension = data_set.row_dimensions[@data_set_col] if @data_set_col
                    when :column
                        # Find the index of the column containing this combination of column members
                        content = [@content].flatten.map(&:upcase).sort
                        col_headers = data_set.column_headers
                        (0...(col_headers.first.size)).each do |col|
                            col_mbrs = col_headers.map{ |row| row[col] && row[col].upcase }.sort
                            if content == col_mbrs
                                @data_set_col = col
                                @dimension = data_set.column_dimensions
                                break
                            end
                        end
                        if @row_override_dim
                            # Locate override dimension column
                            @row_override_col = data_set.row_dimensions.index do |dim|
                                dim.upcase == @row_override_dim.upcase
                            end
                            raise "Cannot locate #{@row_override_dim} in rows of retrieval data set" unless @row_override_col
                        end
                    end
                    raise "Cannot locate #{@content} in rows or columns of retrieval data set" unless @data_set_col
                end
            end


            # Populates a cell of this column in the report schedule
            def populate_cell(row)
                if @data_set_col
                    cell = row[@data_set_col]
                    if @axis == :row
                        # This is a row member column
                        map_member(cell)
                    elsif @row_override_col && (fmt = @row_override_mbrs[row[@row_override_col].upcase])
                        # Apply the row-override formatting
                        val = cell && (cell * fmt.scale * fmt.sign).round(fmt.decimal_places)
                        val == 0 ? nil : val
                    else
                        # Apply column formatting
                        val = cell && (cell * @scale * @sign).round(@decimal_places)
                        val == 0 ? nil : val
                    end
                end
            end


            # Maps a member to the appropriate schedule label.
            #
            # Every Essbase member from every dimension used on a schedule layout
            # must have a mapping from the technical code used in Essbase to a
            # schedule label enumeration.
            #
            # The mapping may come from either a mapping table, or the Essbase member
            # alias. In many cases, the Essbase member alias will be the enumeration
            # value, so we support this as a default option if no other mapping is
            # specified.
            def map_member(mbr)
                desc = nil
                if @column_maps
                    desc = @column_maps[mbr]
                    unless desc || @missing_maps[mbr]
                        @missing_maps[mbr] = MissingMap.new(@content, @map_name, mbr)
                    end
                end
                if @map_name == :alias || (@map_name && !desc)
                    member = @report.cube[@content][mbr]
                    desc = member && member.alias
                end
                desc == :empty ? nil : desc || mbr
            end


            # @return [String] the content definition for this column.
            def to_s
                @content
            end

        end

    end

end

