require 'set'
require 'fileutils'


class Essbase

    # Provides a means for extracting data from an Essbase cube, given a
    # specification of what to extract.
    #
    # @abstract
    class Extract < Base

        # The cube against which the extract will be performed
        attr_reader :cube
        # The extract specification as supplied by the caller
        attr_reader :extract_spec

        # The extract specification after macro expansion
        attr_reader :extract_members
        # The dimensions assigned to the POV (aka Slicer) axis
        attr_reader :pov_dims
        alias_method :slicer_dims, :pov_dims
        # The dimensions assigned to the page axis
        attr_reader :page_dims
        # The dimensions assigned to the row axis
        attr_reader :row_dims
        # The dimensions assigned to the column axis
        attr_reader :col_dims
        alias_method :column_dims, :col_dims

        # The set of sparse dynamic calc members found in the extract spec
        attr_reader :sparse_dynamic_calcs
        # The generated extract query
        attr_reader :query



        # Create an extract instance.
        #
        # @param cube [Cube] The cube from which to run the extract.
        # @param extract_spec [Hash] A hash containing a list of member
        #   specifications for each dimension. The hash may be provided in one
        #   of two different forms:
        #   - The keys of the hash are the dimensions, and the values are the
        #     member specifications (which will be expanded via
        #     Dimension#expand_members).
        #   - The keys of the hash are axis specifiers containing :pov (optional),
        #     :page (optional), :row (required) and :column (required) keys, and
        #     the values are Hashes of dimension => member specifications (which
        #     will be expanded via Dimension#expand_members).
        def initialize(cube, extract_spec)
            super('extract')
            @cube = cube
            @extract_spec = extract_spec
        end


        # Takes an +extract_spec+, and converts that to a hash of members to be
        # extracted for each dimension in the cube.
        #
        # The contents of +extract_spec+ form the basis of the resulting hash,
        # but the following logic is applied to flesh the +extract_spec+ out to
        # a full listing of all members to be extracted:
        # - If a dimension contains one or more member specifications that
        #   include expansion macros, such as <mbr>.Level0 or <mbr>.Children,
        #   these are converted to the corresponding list of members.
        # - If a dimension is not specified in the extract_spec, and the option
        #   :default_missing_dims is specified, then the appropriate member(s)
        #   of the dimension (top or leaves) are added to the hash.
        #
        # This function is designed to generate member lists in the appropriate
        # format for each extract type (e.g. "mbr" for REP or CSC extracts vs
        # [mbr] for MDX extracts), and delegates to a subclass quote_mbrs method
        # to handle the quoting appropriate for the extract method. As such,
        # member specifications can (and should) be given in extract agnostic
        # format as an array of unquoted names, and use the expansion macros
        # where suitable. However, extract method specific functions (e.g.
        # @RELATIVE, <DESCENDANTS, etc) can be used, and will be passed through
        # unaltered. If these functions are used, the appropriate member quoting
        # will be the responsibility of the caller.
        #
        # Finally, as the use of dynamic calc members on sparse dimensions
        # greatly slows down the extract process, we check and warn if sparse
        # dynamic calc members are found in the extract spec.
        #
        # @param options [Hash] An options hash.
        # @option options [Symbol] :default_missing_dims Determines what to do
        #   for non-attribute dimensions in the cube where no member specification
        #   has been supplied. Possible values are:
        #   - :top Use the top member from the dimension
        #   - :leaves/:level0 Include all non-shared leaf members from the dimension
        #   - :none Raise an exception if a dimension is not specified.
        #   Default is :top.
        # @option options [Boolean] :include_dynamic_calcs If false, dynamic
        #   calc members will be excluded from the extract. Useful when an
        #   expansion macro expands to these members, but you don't need them in
        #   the extract. Default is true - dynamic members will be included.
        # @option options [Boolean] :include_sparse_dynamic_calcs If false,
        #   sparse dynamic calc members will be excluded from the extract. Useful
        #   when an expansion macro expands to these members, but you don't need
        #   them in the extract. Default is false - sparse dynamic members will
        #   be excluded.
        #
        # @return [Hash] An expanded extract specification, with all dimensions
        #   specified, and all expansion macros replaced with the matching
        #   members.
        def convert_extract_spec_to_members(options = {})
            default_missing_dims = options.fetch(:default_missing_dims, :top)
            include_dynamic_calcs = options.fetch(:include_dynamic_calcs, true)
            include_sparse_dynamic_calcs = options.fetch(:include_sparse_dynamic_calcs, false)

            log.fine "Validating extract specification against #{@cube}"
            if get_hash_val(@extract_spec, :rows) && get_hash_val(@extract_spec, :columns)
                ext_mbrs = get_hash_val(@extract_spec, :rows).clone.
                    merge(get_hash_val(@extract_spec, :columns)).
                    merge(get_hash_val(@extract_spec, :pages, {})).
                    merge(get_hash_val(@extract_spec, :pov, {}))
            else
                ext_mbrs = @extract_spec.clone
            end
            @sparse_dynamic_calcs = Set.new

            @cube.dimensions.each do |dim|
                # Get the member selection for each dimension
                if mbr_specs = get_hash_val(ext_mbrs, dim.name)
                    unless mbr_specs.is_a?(String) || mbr_specs.is_a?(Array)
                        raise ArgumentError, "extract_spec returned invalid result for #{dim.name}: #{
                            mbr_specs.inspect}"
                    end
                    # Dimension specification as per +extract_spec+
                    mbr_list = []
                    [mbr_specs].flatten.each do |mbr_spec|
                        case mbr_spec
                        when /\[/
                            # A [quoted] MDX member - pass through as is, as it may be a calculated mbr
                            mbr_list << mbr_spec
                        else
                            mbrs = @cube[dim.name].expand_members(mbr_spec, raise_if_empty: false)
                            mbrs.map! do |mbr|
                                mbr.unique_name
                            end
                            mbr_list.concat(quote_mbrs(mbrs))
                        end
                    end
                elsif dim.non_attribute_dimension?
                    # No dimension specification
                    if default_missing_dims == :leaves || default_missing_dims == :level0
                        # Default missing dimension to level 0 members of dimension
                        log.warning "No members specified for #{dim.name}; defaulting to #{dim.name}.Level0"
                        mbrs = dim[dim.name].leaves
                        mbrs.map! do |mbr|
                            mbr.unique_name
                        end
                        mbr_list = quote_mbrs(mbrs)
                    elsif default_missing_dims == :top
                        # Default missing dimension to top member
                        log.warning "No members specified for #{dim.name}; defaulting to #{dim.name}"
                        mbr = dim[dim.name]
                        mbr_list = quote_mbrs([dim.name])
                    else
                        # No defaulting missig dims, so error
                        raise ArgumentError, "No member selection specified for #{dim.name} dimension"
                    end
                end

                if mbr_list
                    mbr_list.uniq!
                    dyn_calc = dim.select(&:dynamic_calc?).select do |mbr|
                        mbr_list.include?(quote_mbr(mbr.unique_name))
                    end
                    if dyn_calc.size > 0 && (!include_dynamic_calcs ||
                                             (!include_sparse_dynamic_calcs && dim.sparse?))
                        mbr_list = mbr_list - quote_mbrs(dyn_calc.map(&:unique_name))
                        log.warning "Removed #{dyn_calc.size} dynamic calc members from #{dim.name} member set"
                        if mbr_list.size == 0
                            raise ArgumentError, "Removing dynamic calc members from #{dim.name} leaves an empty set"
                        end
                    elsif dim.storage_type.to_s == 'Sparse' && dyn_calc.size > 0
                        @sparse_dynamic_calcs.merge(dyn_calc)
                    end
                    ext_mbrs[dim.name] = mbr_list.uniq
                end
            end
            @extract_members = ext_mbrs
        end


        # Quotes a list of member names for a query specification.
        def quote_mbrs(mbrs)
            mbrs.map{ |mbr| quote_mbr(mbr) }
        end


        # A default implementation for quoting members, which simply surrounds
        # individual member names with double-quotes.
        def quote_mbr(mbr)
            %{"#{mbr}"}
        end


        # Handles reporting of instances of sparse dynamic calc members that are
        # included in an extract specification. These members greatly slow down
        # the speed of the extract process, and can often be omitted or replaced
        # with members calculated on-the-fly in the extract process. This method
        # determines if the sparse dynamic calc members are likely to cause a
        # large slowdown, and offers alternatives if possible.
        def report_sparse_dynamic_calcs(extract_method, options)
            return if @sparse_dynamic_calcs.size == 0 || options[:suppress_sparse_dynamic_calc_warnings]

            # Calculate number of potential blocks
            sparse_dims = cube.dimensions.select(&:sparse?).map(&:name)
            sparse_combinations = sparse_dims.reduce(1) do |prod, dim|
                prod * @extract_members[dim].size
            end
            potential_bytes = sparse_combinations * cube.actual_block_size * 8

            # Return if potential bytes to be processed is less than 100MB
            return if (extract_method != :calc) && (potential_bytes < 100 * 1024 * 1024)

            # Report on sparse dynamic calc members, as these will slow the
            # extract down by a factor proportional to the sparse density
            case extract_method
            when :mdx
                log.warning <<-EOT.strip.gsub(/\s{2,}/, ' ')
                    The extract specification evaluates to #{sparse_combinations}
                    potential blocks (#{potential_bytes / (1 * 1024 * 1024)} MB);
                    the inclusion of #{@sparse_dynamic_calcs.size} sparse dynamic
                    calc members prevents the use of the NONEMPTYBLOCK optimisation,
                    which will cause the extract to run considerably slower.
                EOT
            when :calc
                log.severe <<-EOT.strip.gsub(/\s{2,}/, ' ')
                    The extract specification includes #{@sparse_dynamic_calcs.size}
                    sparse dynamic calc members, but calc extracts ignore dynamic calc members.
                EOT
            when :report
                log.warning <<-EOT.gsub(/\s{2,}/, ' ')
                    The extract specification includes #{@sparse_dynamic_calcs.size}
                    dynamic calc members on sparse dimensions, which will slow down the extract
                    process (possibly considerably).
                EOT
            end

            @sparse_dynamic_calcs.each do |mbr|
                msg = "#{mbr.dimension.name} member #{mbr.name} is a sparse dynamic calc"
                if options.fetch(:suggest_dynamic_calc_alternatives, true)
                    unless mbr.formula
                        # Check if member could be replaced by expansion macro
                        cf = mbr.children.find do |child|
                            child.consolidation_type != '+' || child.dynamic_calc?
                        end
                        lf = mbr.rdescendants.find do |desc|
                            desc.consolidation_type != '+' ||
                            (desc.level == 0 && desc.dynamic_calc?)
                        end
                        case
                        when !lf && !cf
                            msg += "; it may be replaceable by an expansion macro (e.g. #{
                                mbr.name}.Children or #{mbr.name}.RLeaves)"
                        when !cf
                            msg += "; it may be replaceable by the expansion macro #{mbr.name}.Children"
                        when !lf
                            msg += "; it may be replaceable by the expansion macro #{mbr.name}.RLeaves"
                        end
                    else
                        msg += "; it may be replaceable by an MDX calculation" if extract_method == :mdx
                    end
                end
                log.warning msg
            end
        end


        # Process the extract spec, and assigns dimensions to axes
        def assign_axes(options)
            @pov_dims = []
            @page_dims = []
            @row_dims = []
            @col_dims = []

            if get_hash_val(@extract_spec, :rows) && get_hash_val(@extract_spec, :columns)
                assign_axis(:pov, @pov_dims)
                assign_axis(:pages, @page_dims)
                assign_axis(:rows, @row_dims)
                assign_axis(:columns, @col_dims)
            else
                @col_dims = [get_hash_val(options, :column_dim, @cube.dense_dimensions.last.name)]
                @row_dims = (@cube.get_data_export_dimension_order).map(&:name) - @col_dims
                # TODO: Consider any attribute dimensions in @extract_spec
            end
            raise ArgumentError, "No column dimension specified" unless @col_dims.size > 0
        end


        # Add the names of each dimension that is specified in @extract_spec to
        # the supplied +dim_set+.
        def assign_axis(axis, dim_set)
            if axis = get_hash_val(@extract_spec, axis)
                axis.keys.each do |dim_key|
                    if dim = @cube[dim_key]
                        dim_set << dim.name
                    else
                        raise ArgumentError, "No dimension named #{dim_key} exists"
                    end
                end
            end
        end


        # Saves the generated extract query specification to a query file for
        # debugging.
        #
        # @param query [String] The extract query text (i.e. report spec, MDX
        #   etc)
        # @param file_name [String] The path of the file to save the query to.
        # @param extension [String] The appropriate extension for the query,
        #   based on the type of extract performed.
        # @param query_num [Integer] An optional count of the number of queries
        #   performed so far in this extract. Determines if a new query file is
        #   created, or an existing file is appended to.
        def save_query_to_file(query, file_name, extension, query_num = 0)
            if file_name
                log.finest "Extract script:\n#{query}"
                ext_re = Regexp.new("\\#{extension}$", Regexp::IGNORECASE)
                file_name += extension unless file_name =~ ext_re
                FileUtils.mkdir_p(File.dirname(file_name))
                File.open(file_name, query_num == 0 ? 'w' : 'a') do |f|
                    f.puts "\n---\n\n" if query_num > 0
                    f.puts(query)
                end
            end
        end

    end

end


# Include the sub-classes that define the different query methods
require_relative 'extract/mdx_extract'
require_relative 'extract/report_extract'
require_relative 'extract/calc_extract'
