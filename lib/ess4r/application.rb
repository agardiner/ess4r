


class Essbase

    # To retrieve the log file, we need access to the m_olapCxt
    class Java::ComEssbaseApiDatasource::EssOlapApplication

        field_reader :m_olapCxt

    end


    class Application < Base

        def initialize(app)
            super('@app', app)
        end


        # Retrieves the application log file, optionally from the specified date.
        #
        # @param local_file [String] The path to the local file to save the log to
        # @param start_time [Date, Time, DateTime] The earliest time from which to
        #    return log entries; if omitted, the entire log file is returned.
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
