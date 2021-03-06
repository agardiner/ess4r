class Essbase

    # Extracts data from Essbase using MDX queries. This method is the most
    # flexible, since it supports the inclusion of calculated members that don't
    # exist in the outline, can map members to new names, offers excellent
    # performance - provided the extract specification does not include sparse
    # dynamic  calculations.
    class MdxExtract < Extract

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
        def initialize(cube, extract_spec, options = {})
            super(cube, extract_spec)

            # Convert extract spec to member and axis assignments
            convert_extract_spec_to_members(options)
            mdx_setup_calcs(options)
            report_sparse_dynamic_calcs(:mdx, options)
            assign_axes(options)

            @pov_dims.clone.each do |dim|
                if @extract_members[dim].size > 1
                    # Move dimension from slicer to page axis, since it now has multiple members
                    log.finer "Moving dimension #{dim} onto page axis"
                    @page_dims.unshift(dim)
                    @pov_dims.delete(dim)
                end
            end

            # Add any member-name mappings
            if options[:member_maps]
                reverse_maps = Hash[@member_name_maps.invert.map{ |k, v| [k.upcase, v] }]
                options[:member_maps].each do |name, map|
                    if src = reverse_maps[name.to_s.upcase]
                        # Existing map from calc member to dynamic sparse member needs re-mapping
                        @member_name_maps[src] = map
                    else
                        @member_name_maps[name.to_s] = map
                    end
                end
            end

        end


        # Create and run one or more MDX queries to export data.
        #
        # @param output_file [String] A path to where the output should be saved.
        # @param options [Hash] An options hash for controlling various facets
        #   of the extract process.
        # @option options [String] :partition_dim The name of the dimension to
        #   partition the query on.
        # @option options [Integer] :partition_size The number of members of the
        #   partition dimension to include in each MDX query.
        def extract_data(output_file, options = {})
            log.info "Exporting #{@cube} data via MDX..."

            # Split query into chunks that don't use too much memory
            partition_dim = options[:partition_dim] && @cube[options[:partition_dim]].name
            partition_size = options.fetch(:partition_size, 50)
            if partition_dim
                partition_sets = @extract_members[partition_dim].each_slice(partition_size).to_a
                log.fine "Partitioning extract by #{partition_dim}; #{partition_sets.size} " +
                    "partition#{partition_sets.size > 1 ? 's' : ''} created"
            else
                partition_sets = [nil]
            end

            unless options.has_key?(:include_sparse_dynamic_calcs)
                options[:include_sparse_dynamic_calcs] = @sparse_dynamic_calcs.size > 0
            end

            template = generate_query(partition_dim, options)
            output_options = options.dup
            output_options[:file_mode] = 'w' unless output_options[:file_mode]

            count = 0
            cv = @cube.open_cube_view
            begin
                partition_sets.each_with_index do |mbr_set, i|
                    yield @cube, i, partition_sets.size if block_given?
                    @extract_members[partition_dim] = mbr_set if partition_dim
                    if mbr_set
                        mdx_script = template.gsub("%{#{partition_dim}}", mbr_set.join(', '))
                    else
                        mdx_script = template
                    end
                    save_query_to_file(mdx_script, options[:query_file], '.mdx', i)
                    data = cv.mdx_query(mdx_script)
                    if data.record_count > 0
                        data.suppress_zeros = options[:suppress_zeros]
                        data.decimals = options[:decimals] || options[:decimal_places]
                        data.suppress_members = @suppress_members
                        data.map_members = @member_name_maps
                        if cb = options[:output_handler]
                            cb.call(data, i)
                        end
                    end
                    if output_file
                        log.fine "Writing data to #{output_file}" if i == 0
                        data.to_file(output_file, output_options)
                        output_options[:file_mode] = 'a'
                        output_options[:include_headers] = false
                    end
                    count += data.record_count
                end
            ensure
                cv.close
            end
            log.fine "Output #{count} records"
        end


        # A function for quoting members, which simply surrounds individual
        # member names with square brackets.
        def quote_mbr(mbr)
            mbr[0] == '[' ? mbr : %Q{[#{mbr}]}
        end


        # Handle any sparse dynamic calc substitutions for MDX calcs, plus any
        # additional MDX calculations.
        # Calculations are defined in an extract specification via two options:
        #  - dynamic_calc_substitutions: Define MDX calculations that can be used
        #    to replace a sparse dynamic calc member in a query. Each dynamic
        #    calc substitution must be defined under a key for the dimension to
        #    which it belongs, and specify the data member(s) that are used in
        #    the calculation (as these members must be included in the query, but
        #    filtered out of the results) under a +data_members+ key, as well as
        #    the MDX formula to use.
        # - mdx_calculations: Define additional calculations that can be referenced
        #   in member specifications for a dimension.
        #
        # @param options [Hash] An options hash.
        # @option options [Boolean] :diable_mdx_calcs If true, turns off the use
        #   of any MDX calculations that have been created as alternatives to
        #   sparse dynamic calc members.
        # @option options [
        def mdx_setup_calcs(options)
            @suppress_members = []
            @member_name_maps = {}
            @mdx_calcs = Hash.new{ |h, k| h[k] = {} }
            subs = !options[:disable_mdx_calcs] && options[:dynamic_calc_substitutions]
            calcs = !options[:disable_mdx_calcs] && options[:mdx_calculations]
            subs && subs.each do |dim, dtls|
                dim_calcs = @mdx_calcs[dim]
                dtls.each do |mbr, calcs|
                    # Only add calc if +mbr+ is used in this extract
                    if dyn_mbr = @sparse_dynamic_calcs.find{ |dyn_mbr| dyn_mbr.name.upcase == mbr.to_s.upcase }
                        log.fine "Replacing sparse dynamic calc member #{dyn_mbr} with MDX calculation"
                        # Map calculated member name back to substituted member name
                        @member_name_maps["#{dyn_mbr}_calc"] = dyn_mbr.name
                        # Replace substituted member in retrieval member set
                        @extract_members[dim].map!{ |ext_mbr| "[#{dyn_mbr}]" == ext_mbr ? "[#{dyn_mbr}_calc]" : ext_mbr }
                        # Add data members used in MDX calculation to the retrieval
                        data_mbrs = @cube[dim].expand_members(calcs.data_members).map(&:name)
                        data_mbrs.each do |mbr|
                            quoted_mbr = "[#{mbr}]"
                            unless @extract_memmbers[dim].include?(quoted_mbr)
                                log.finer "Adding #{dim} #{mbr} to extract to provide data for MDX calculation"
                                @extract_members[dim] << quoted_mbr
                                # Suppress data member not in extract, but needed for calc
                                @suppress_members << mbr
                            end
                        end
                        # Add calculation for dynamic calc substitution
                        if calcs.formula?
                            formula = calcs.formula
                        else
                            formula = data_mbrs.map{ |m| "[#{m}]" }.join(' + ')
                        end
                        log.finer "MDX formula used is: #{formula}"
                        dim_calcs["#{dyn_mbr}_calc"] = formula
                        @sparse_dynamic_calcs.delete(dyn_mbr)
                    end
                end
            end
            # Add any other MDX calcs specified
            calcs && calcs.each do |dim, dtls|
                dim_calcs = @mdx_calcs[dim]
                dtls.each do |mbr, formula|
                    log.fine "Adding MDX calculated member #{mbr}"
                    log.finer "MDX formula used is: #{formula}"
                    dim_calcs[mbr] = formula
                end
            end
        end


        # Create an MDX query consisting of a series of CrossJoin's to combine the
        # members we want from each dimension on each axis.
        #
        # @param partition_dim [String|NilClass] An optional dimension by which
        #   the query will be partitioned.
        # @param options [Hash] An options hash
        # @option options [Boolean] :suppress_missing Whether missing rows should
        #   be suppressed (true) or included in the output (false).
        # @option options [Boolean] :include_sparse_dynamic_calcs Whether sparse
        #   dynamic calcs should be included. The presence of a sparse dynamic calc
        #   prevents the use of the NONEMPTYBLOCK optimisation.
        def generate_query(partition_dim, options)
            assign_axes(options)

            # Place dimensions on each axis
            pov_spec = @pov_dims.length > 0 ?
                "WHERE (#{@pov_dims.map{ |dim| @extract_members[dim].first }.join(', ')})" : ''
            page_spec = cross_join_dims(@page_dims)
            row_spec = cross_join_dims(@row_dims)
            col_spec = cross_join_dims(@col_dims)

            # Add any custom member calculations
            calc_mbrs = []
            calc_mbrs = @mdx_calcs.map do |dim, mbrs|
                mbrs.map do |mbr, formula|
                    "  MEMBER [#{dim}].[#{mbr}] AS '#{formula}'"
                end
            end.flatten
            mbr_sets = (@page_dims + @row_dims + @col_dims).map do |dim|
                if dim == partition_dim
                    # Dimension name will be replaced later by current set of members in partition
                    "  SET [#{dim}Set] AS '{%{#{dim}}}'"
                else
                    "  SET [#{dim}Set] AS '{#{@extract_members[dim].join(', ')}}'"
                end
            end
            with_stmts = (calc_mbrs + mbr_sets).join("\n")

            # Determine what suppression options to use
            non_empty = options.fetch(:suppress_missing, true)
            non_empty_blocks = non_empty && @cube.cube_type.to_s == 'Normal' &&
                                    (!options.fetch(:include_sparse_dynamic_calcs, false) ||
                                     @sparse_dynamic_calcs.size == 0)
            non_empty_block_spec = non_empty_blocks ? 'NONEMPTYBLOCK ' : ''
            non_empty_spec = non_empty ? 'NON EMPTY ' : ''

            mdx = <<-EOQ.gsub(/^ {16}/, '')
                WITH
                #{with_stmts}
                SELECT #{non_empty_block_spec}#{page_spec.length > 0 ? " #{non_empty_spec}#{page_spec} ON PAGES," : ''}
                  #{non_empty_spec}#{row_spec} ON ROWS,
                  #{col_spec} ON COLUMNS
                #{pov_spec}
            EOQ
        end


        # Takes an array of dimensions, and combines them using CrossJoin.
        def cross_join_dims(dims)
            dims.reduce("") do |spec, dim|
                spec.length > 0 ? "CrossJoin(#{spec}, [#{dim}Set])" : "[#{dim}Set]"
            end
        end

    end

end
