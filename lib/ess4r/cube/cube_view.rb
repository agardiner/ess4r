class Essbase

    # Wraps an IEssCubeView object from the Essbase JAPI. A CubeView is used to
    # generate grid-based retrievals of data from an Essbase database.
    class CubeView < Base

        include_package 'com.essbase.api.dataquery'


        # Creates a new CubeView object to wrap the supplied +cube_view+ JAPI
        # object.
        #
        # Note: CubeView objects should be instantiated via Cube#open_cube_view.
        #
        # @private
        def initialize(cube_view)
            super("@cube_view", cube_view)
        end


        # Closes this CubeView onto the database. Once closed, a CubeView can
        # no longer be used.
        def close
            try{ @cube_view.close }
            @cube_view = nil
        end


        # Executes the specified MDX query against the Essbase database to which
        # this cube view is connected.
        #
        # @param mdx_stmt [String] An MDX statement to be executed against the
        #   database.
        # @param options [Hash] An options hash.
        # @option options [Boolean] :unique_names If true, unique member names
        #   are returned instead of names. This option is only valid on more
        #   recent versions of Essbase.
        # @option options [Boolean] :aliases If true, aliases from the current
        #   alias table are returned instead of names.
        # @option options [String] :alias_table The name of the alias table to
        #   use.
        # @return [DataSet] An MDX data set object that contains the returned
        #   data.
        def mdx_query(mdx_stmt, options = {})
            require_relative 'mdx_data_set'

            op = try{ @cube_view.createIEssOpMdxQuery() }
            if options[:unique_names] && IEssOpMdxQuery::EEssMemberIdentifierType.sm_values.size > 2
                try{ op.setMemberIdentifierType(IEssOpMdxQuery::EEssMemberIdentifierType::UNIQUENAME) }
            elsif options[:aliases]
                try{ op.setMemberIdentifierType(IEssOpMdxQuery::EEssMemberIdentifierType::ALIAS) }
                if options[:alias_table] && options[:alias_table] != try{ @cube_view.getAliasTable() }
                    try{
                        @cube_view.setAliasTable(options[:alias_table])
                        @cube_view.updatePropertyValues()
                    }
                end
            end
            try{ op.setQuerySpec(mdx_stmt) }
            instrument "mdx_query", mdx: mdx_stmt do
                try{ @cube_view.performOperation(op) }
            end
            MdxDataSet.new(try{ @cube_view.getMdDataSet })
        end


        # Executes a grid retrieve against the Essbase database to which this
        # cube view is connected. The grid must already have been defined before
        # calling this method.
        #
        # @param options [Hash] An options hash.
        # @option options [Boolean] :aliases If true, aliases from the active
        #   alias table are returned instead of names. Note that if a given
        #   member does not have an alias in the active alias table, the member
        #   name will be returned instead.
        # @option options [String] :alias_table The name of the alias table to
        #   use.
        # @option options [Boolean] :suppress_missing If true, suppresses missing
        #   records from the retrieve.
        # @option options [Boolean] :suppress_zero If true, suppresses zero
        #   records from the retrieve.
        def retrieve(options = {})
            if options[:aliases]
                try{ @cube_view.setAliasNames(true) }
                if options[:alias_table] && options[:alias_table] != try{ @cube_view.getAliasTable() }
                    try{ @cube_view.setAliasTable(options[:alias_table]) }
                end
            else
                try{ @cube_view.setAliasNames(false) }
            end
            indent = case options.fetch(:indent_style, :none)
                     when :totals then IEssCubeView::EEssIndentStyle::TOTALS
                     when :sub_items then IEssCubeView::EEssIndentStyle::SUB_ITEMS
                     else IEssCubeView::EEssIndentStyle::NONE
                     end
            try {
                @cube_view.setSuppressMissing(options.fetch(:suppress_missing, false))
                @cube_view.setSuppressMissing(options.fetch(:suppress_zero, false))
                @cube_view.setFormulas(options.fetch(:preserve_formulas, true))
                @cube_view.setIndentStyle(indent)
            }
            try { @cube_view.updatePropertyValues() }
            op = try{ @cube_view.createIEssOpRetrieve() }
            instrument "retrieve" do
                try{ @cube_view.performOperation(op) }
            end
        end


        # Runs the +calc_string+ calculation against the cube.
        #
        # @param calc_str [String] A calculation script to be executed (the
        #   actual calc script code, not a calc script name; see #run_calc for
        #   running an existing calc script that exists as a file).
        def run_calc(calc_str)
            op = @cube_view.createIEssOpCalculate()
            op.set(calc_str, false)
            instrument "calculate", calc: calc_str do
                try{ @cube_view.performOperation(op) }
            end
        end

    end

end
