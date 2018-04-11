


class Essbase

    # @!visibility private
    # To retreive the log file, we need access to the private m_olapCxt on
    # EssOlapApplication.
    class Java::ComEssbaseApiDatasource::EssOlapApplication

        field_reader :m_olapCxt

    end


    # Wraps an IEssOlapApplication implementing class returned from {Server#open_app}.
    class Application < Base

        # @!visibility private
        #
        # Create an instance of this class, wrapping the supplied IEssOlapApplication
        # instance.
        #
        # @see Server#open_app
        #
        # @param app [IEssOlapApplication] The JAPI application object to wrap.
        def initialize(app)
            super('@app', app)
        end


        # Return each of the databases in this application (as a Cube object).
        #
        # @return [Array<Cube>] An array of Cube instances representing each
        #   database in this application.
        def cubes
            require_relative 'cube'
            try{ @app.getCubes.getAll().to_a }.map{ |c| Cube.new(c) }
        end


        # Download the application log file from the server.
        #
        # @param local_file [String] The path to the local file into which the
        #   server application log should be written.
        # @param start_time [Date, Time, DateTime, Fixnum, NilClass] A start time
        #   from which log records should be retrieved. Supports all Ruby date/time
        #   types, as well as a Fixnum, which is assumed to be seconds since 1/1/70.
        def get_log_file(local_file, start_time = nil)
            case start_time
            when Date, DateTime
                start_time = start_time.to_time.to_i
            when Time
                start_time = start_time.to_i
            when NilClass
                start_time = 0
            when Fixnum
                # Pass as is
            else
                raise ArgumentError, "Expected a Date, Time, or DateTime object; got #{start_time.class}"
            end
            try{ m_olapCxt.orbPlugin.essMainGetLogFile(m_olapCxt.hCtx, getName(), start_time, local_file) }
        end

    end

end
