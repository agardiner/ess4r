require_relative 'maxl/maxl_result_set'


class Essbase

    # Represents a MaxL session
    class Maxl < Base

        # A count of the number of records in any result set from the last query
        # submitted.
        attr_reader :count


        # Instantiate a MaxL session.
        #
        # Note: This method should not be invoked directly; see instead
        # Server#open_maxl.
        #
        # @private
        def initialize(maxl_session, message_handler)
            super("@maxl", maxl_session)
            @message_handler = message_handler
            @count = nil
        end


        # Close the MaxL session.
        def close
            try{ @maxl.close }
            @maxl = nil
        end


        # Executes the specified Maxl +stmt+.
        #
        # Note: this seems not to work for data loads or dimension builds that
        # reference local files or rules objects.
        #
        # @return [MaxlResultSet, NilClass] If the command is one that returns a
        #   grid, then a MaxlResultSet object is returned from which the contents
        #   can be obtained. Otherwise, nil is returned.
        def execute(stmt)
            orig_stmt = stmt
            stmt = stmt.sub(/;\s*$/, '').gsub(/\n/, ' ').gsub(/\s{2,}/, ' ').strip
            log.finer "Executing MAXL statement: #{orig_stmt}"
            begin
                instrument 'maxl', statement: stmt do
                    try{ @maxl.execute(stmt) }
                end
            rescue
                log.severe "Error in Maxl statement: #{stmt}"
                raise
            end
            if @count = process_messages
                result_set
            end
        end


        # Returns a ResultSet object representing the result set from the last
        # Maxl command.
        def result_set
            MaxlResultSet.new(@maxl.result_set)
        end


        private


        # Retrieve the messages from executing the MaxL statement, and pass them
        # on to the message handler.
        def process_messages
            count = nil
            try{ @maxl.get_messages }.each do |msg|
                msg =~ /(\w+) - (\d+) - (.+)/
                level, msg_num, msg_text = $1, $2.to_i, $3
                msg_lvl = case level
                          when /info/i then MessageHandler::MSG_LVL_INFO
                          when /warn/i then MessageHandler::MSG_LVL_WARN
                          when /error/i then MessageHandler::MSG_LVL_ERROR
                          when /fatal/i then MessageHandler::MSG_LVL_FATAL
                          when /debug/i then MessageHandler::MSG_LVL_DEBUG
                          end
                @message_handler.process_message(msg_num, msg_lvl, msg_text)
                count = (/Records returned: \[(\d+)\]/.match(msg_text)[1].to_i) if msg_num == 1241044
            end
            count
        end

    end

end
