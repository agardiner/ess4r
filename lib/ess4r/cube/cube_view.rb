class Essbase

    # Wraps an IEssCubeView object from the Essbase JAPI. A CubeView is used to
    # generate grid-based retrievals of data from an Essbase database.
    class CubeView < Base


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
        # @return [DataSet] An MDX data set object that contains the returned
        #   data.
        def mdx_query(mdx_stmt)
            require_relative 'mdx_data_set'

            op = @cube_view.createIEssOpMdxQuery()
            op.set_query_spec mdx_stmt
            instrument "mdx_query", mdx: mdx_stmt do
                try{ @cube_view.perform_operation(op) }
            end
            MdxDataSet.new(@cube_view.get_md_data_set)
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
                try{ @cube_view.perform_operation(op) }
            end
        end

    end

end