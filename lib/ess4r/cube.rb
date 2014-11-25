require_relative 'cube/dimensions'
require_relative 'cube/loads'
require_relative 'cube/rules'
require_relative 'file_transfer'
require_relative 'extract'


class Essbase

    # Represents a connection to an Essbase database.
    class Cube < Base

        include_package 'com.essbase.api.datasource'


        # Extend functionality from modules
        include Dimensions
        include Loads
        include FileTransfer
        include Rules


        # Creates a new Cube object to wrap the supplied +cube+ JAPI object.
        #
        # Note: Cube objects should be instantiated via Server#open_cube.
        #
        # @private
        def initialize(cube)
            super('@cube', cube)
        end


        # Closes this connection to the Essbase database.
        def clear_active
            if @cube
                try{ @cube.clear_active }
                @cube = nil
            end
        end
        alias_method :close, :clear_active


        # Write a message to the Essbase application log.
        def write_to_log(msg)
            try{ @cube.get_application.get_olap_server.write_to_log_file(false, msg) }
        end


        # Returns the value of a substitution variable.
        #
        # @param sub_var [String] The name of the substitution variable to
        #   retrieve.
        # @param inherited [Boolean] If true (the default), and the variable is
        #   not set at the database level, searches the application- and server-
        #   level variables as well.
        def get_substitution_variable_value(sub_var, inherited = true)
            sub_var =~ /^&?(.+)$/
            var_name = $1
            log.finer "Retrieving value for substitution variable #{var_name}"
            val = nil
            begin
                val = try{ @cube.get_substitution_variable_value(var_name) }
            rescue
                raise unless inherited
                app = @cube.get_application
                begin
                    val = try{ app.get_substitution_variable_value(var_name) }
                rescue
                    val = try{ app.get_olap_server.get_substitution_variable_value(var_name) }
                end
            end
            val
        end


        # @!group Calculation Methods

        # Runs the +calc_string+ calculation against the cube.
        #
        # @param calc_str [String] A calculation script to be executed (the
        #   actual calc script code, not a calc script name; see #run_calc for
        #   running an existing calc script that exists as a file).
        def calculate(calc_str)
            instrument "calculate", calc: calc_str do
                try{ @cube.calculate(calc_str, false) }
            end
        end


        # Executes the specified +calc_script+ against this cube.
        #
        # @param calc_script [String] The name of a calculation script to be
        #   executed against this cube.
        def run_calc(calc_script)
            instrument "calculate", calc_script: calc_script do
                try{ @cube.calculate(false, calc_script) }
            end
        end

        # @!endgroup


        # Open a CubeView object for retrieving data from the Essbase database.
        #
        # @yield If a block is supplied, the opened CubeView object is yielded to
        #   the block, and then closed when the block returns.
        # @yieldparam cube [Cube] The CubeView object representing the Essbase
        #   database.
        # @return [CubeView] A CubeView for executing retrieves etc against this
        #   cube.
        def open_cube_view(label = 'CubeView')
            require_relative 'cube/cube_view'

            cube_view = nil
            instrument "open_cube_view" do
                cube_view = CubeView.new(try{ @cube.open_cube_view(label) })
            end
            if block_given?
                begin
                    yield cube_view
                    cube_view.close
                    nil
                end
            else
                cube_view
            end
        end


        # Extract data from the cube to an +output_file+.
        def extract(extract_spec, output_file, options = {})
            extractor = case options.fetch(:extract_method, :mdx)
            when :report
                Essbase::ReportExtract.new(self, extract_spec, options)
            when :calc
                Essbase::CalcExtract.new(self, extract_spec, options)
            when :mdx
                Essbase::MdxExtract.new(self, extract_spec, options)
            else
                raise ArgumentError, "Unrecognised extract_method: #{options[:extract_method]}"
            end
            extractor.extract_data(output_file, options)
        end


        # Returns the application and database name for the cube connection.
        def to_s
            "#{@cube.application.name}:#{@cube.name}"
        end


        # Check that we are connected before attempting to delegate.
        def method_missing(meth_name, *args)
            if @cube
                super
            else
                raise "Essbase database connection was closed"
            end
        end

    end

end
