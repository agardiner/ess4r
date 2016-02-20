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
        # @return [DataSet] An MDX data set object that contains the returned
        #   data.
        def mdx_query(mdx_stmt, options = {})
            require_relative 'mdx_data_set'

            op = try{ @cube_view.createIEssOpMdxQuery() }
            if options[:unique_names] && IEssOpMdxQuery::EEssMemberIdentifierType.sm_values.size > 2
                try{ op.setMemberIdentifierType(IEssOpMdxQuery::EEssMemberIdentifierType::UNIQUENAME) }
            elsif options[:aliases]
                try{ op.setMemberIdentifierType(IEssOpMdxQuery::EEssMemberIdentifierType::ALIAS) }
            end
            try{ op.setQuerySpec(mdx_stmt) }
            instrument "mdx_query", mdx: mdx_stmt do
                try{ @cube_view.performOperation(op) }
            end
            MdxDataSet.new(try{ @cube_view.getMdDataSet })
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
