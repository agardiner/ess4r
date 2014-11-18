class Essbase


    # Implementation of IEssCustomMessageHandler to process Essbase messages.
    # This implementation uses the logging capability in the Base superclass to
    # log Essbase messages received from the server.
    #
    # By default certain less useful messages are suppressed from logging; this
    # can be controlled via the #suppress_message_nums property.
    #
    # The default behaviour is to log just the message text at a corresponding
    # message level on the ess4r.server logger (but with Essbase INFO messages
    # being logged at FINE level). If desired, the message number can be included
    # at the end of the message, and an optional connection id can be added to
    # the start of the message.
    class MessageHandler < Base

        include com.essbase.api.session.IEssCustomMessageHandler


        # Essbase message level constants
        MSG_LVL_DEBUG = 1
        MSG_LVL_INFO = 2
        MSG_LVL_WARN = 3
        MSG_LVL_ERROR = 4
        MSG_LVL_FATAL = 5

        # We don't echo every Essbase message, since some are just noise and
        # don't tell us anything useful. Messages with the following numbers
        # will be suppressed.
        DEFAULT_SUPPRESS_MSGS = [
            1003040, 1003051,
            1012055, 1012675, 1012677, 1012693,
            1019018, 1019025, 1019061,
            1021000, 1021002, 1021004,
            1051083, 1053012, 1053013, 1053014,
            1243002, 1243003
        ]


        # Flag indicating whether to include the Essbase message number at the
        # end of the message.
        attr_accessor :include_message_num
        # Optional string to be prepended to each message to identify which
        # connection the message relates to.
        attr_accessor :connection_id
        # Access the array of message numbers to be suppressed.
        attr_reader :suppress_message_nums


        # Create a new message handler for processing log messages from Essbase
        def initialize
            super
            @suppress_message_nums = DEFAULT_SUPPRESS_MSGS
        end


        # Callback that will be called by Essbase for each message written to
        # the server or application log.
        java_signature 'int(int, int, string)'
        def process_message(msg_num, msg_lvl, msg_txt)
            unless @suppress_message_nums.include?(msg_num)
                msg_parts = []
                msg_parts << @connection_id if @connection_id
                msg_parts << msg_txt
                msg_parts << " (#{msg_num})" if @include_message_num
                msg = msg_parts.join(' ')
                # Sometimes we get duplicate messages logged
                if msg != @last_msg
                    case msg_lvl
                    when MSG_LVL_DEBUG
                        log.finest(msg)
                    when MSG_LVL_INFO
                        log.fine(msg)
                    when MSG_LVL_WARN
                        log.warn(msg)
                    when MSG_LVL_ERROR
                        log.severe(msg)
                    when MSG_LVL_FATAL
                        log.severe(msg)
                    else
                        raise ArgumentError, "Unrecognised Essbase message level: #{msg_lvl}"
                    end
                    @last_msg = msg
                end
            end
            msg_num
        end

    end

end
