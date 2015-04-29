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
            instrument "sign_on", :server => server, :user_id => user, :aps_url => aps_url do
                @server = try{ Essbase.instance.sign_on(user, password, false, nil, aps_url, server) }
                @message_handler = MessageHandler.new
                try{ @server.set_message_handler(@message_handler) }
            end
        end


        def disconnect
            try{ @server.disconnect }
            @server = nil
        end


        # Open the specified application/database, and return a Cube object for
        # interacting with it.
        #
        # @param ess_app [String] Essbase application name.
        # @param ess_db [String] Essbase database name.
        # @yield If a block is supplied, the opened Cube object is yielded to
        #   the block, and then closed when the block returns.
        # @yieldparam cube [Cube] The Cube object representing the Essbase
        #   database.
        # @return [Cube] A Cube object for interacting with the specified
        #   database.
        def open_cube(ess_app, ess_db)
            require_relative 'cube'

            log.fine "Opening Essbase database #{ess_app}:#{ess_db}"
            cube = nil
            instrument "open_cube", app: ess_app, db: ess_db do
                cube = Cube.new(try{ @server.get_application(ess_app).get_cube(ess_db) })
            end
            if block_given?
                begin
                    yield cube
                    cube.close
                    nil
                end
            else
                cube
            end
        end


        # Open a MaxL session against this Essbase server.
        #
        # @param session_name [String] Name to give the MaxL session.
        # @yield If supplied, the block will be passed a Maxl object with which
        #   to execute commands. The Maxl session will then be closed when the
        #   block returns.
        # @yieldparam maxl [Maxl] A Maxl object from which Maxl commands can be
        #   issued.
        # @return [Maxl] A Maxl object from which Maxl commands can be issued.
        def open_maxl_session(session_name = 'Maxl')
            require_relative 'maxl'

            maxl = Maxl.new(try{ @server.open_maxl_session(session_name) }, @message_handler)
            if block_given?
                begin
                    yield maxl
                ensure
                    maxl.close
                    nil
                end
            else
                maxl
            end
        end

    end

end
