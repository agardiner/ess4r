require_relative 'message_handler'


class Essbase

    # Wrapper for an Essbase server JAPI object.
    #
    # Note: An Essbase server connection can only be used by one thread at a
    # time. If concurrent Essbase operations are to be performed, a separate
    # connection will be required for each concurrent operation.
    class Server < Base

        # Access to the MessageHandler used to process messages from this Essbase
        # server
        attr_reader :message_handler


        # Create a connection to an Essbase server.
        #
        # Note: This method should not be called directly - instead, instantiate
        # a server connection via Essbase.connect.
        #
        # @private
        def initialize(user, password, server, aps_url)
            super("@server")

            log.fine "Connecting to Essbase server #{server} as #{user}"
            instrument "connect.ess4r", :server => server, :user_id => user, :aps_url => aps_url do
                @server = try{ Essbase.instance.sign_on(user, password, false, nil, aps_url, server) }
                @message_handler = MessageHandler.new
                try{ @server.set_message_handler(@message_handler) }
            end
        end

    end

end
