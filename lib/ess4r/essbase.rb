require_relative 'epm_jar_finder'


# Main class for wrapping the Essbase JAPI.
class Essbase

    extend EPMJarFinder

    load_jar 'ess_japi.jar'

    include_package 'com.essbase.api.session'
    include_package 'com.essbase.api.datasource'
    include_package 'com.essbase.api.dataquery'
    include_package 'com.essbase.api.metadata'


    # Return a new Essbase API instance; one of these is needed for each
    # thread that will concurrently interact with Essbase.
    #
    # It's usually not necessary to call this method directly, since it is
    # called automatically when you connect to an Essbase server (via
    # Essbase.connect). You would therefore only need to call this method if you
    # wished to call a JAPI method on IEssbase.
    #
    # @note This method registers an at_exit handler to disconnect automatically
    #   when the Ruby program exits (if the instance is still signed-on).
    #
    # @return [IEssbase] A native Java class that implements IEssbase.
    def self.instance
        instance = IEssbase::Home.create(IEssbase.JAPI_VERSION)
        at_exit do
            instance.sign_off if instance.is_signed_on
        end
        instance
    end


    # Connect to an Essbase server using a user id and password.
    #
    # @param user [String] A userid with login rights to Essbase. Use a value of
    #   +nil+ if authenticating via a CSS token.
    # @param password [String] The password for +user+, or a CSS token.
    # @param server [String] The Essbase server to connect to. May include an
    #   optional port number for when the Essbase server is not listening on the
    #   default port; use the form <server>:<port>.
    #   To connect in embedded mode to the currently active server in an active/
    #   passive cluster, you can specify the URL to the APS provider in the
    #   following form:
    #     http[s]://<aps_server>[:<port>]/aps/Essbase?ClusterName=<cluster>[&SecureMode=yes]
    #   This will cause the JAPI to query the APS provider for the address of
    #   the active Essbase node in the cluster, and then connect to that
    #   server directly if the +aps_url+ is 'embedded' (the default).
    # @param aps_url [String] A URL to the APS server; usually of the form:
    #   http[s]://<server>:<port>/aps/JAPI
    #   If omitted, a direct connection to the Essbase server will be created
    #   using embedded mode. Embedded mode is the most flexible, since operations
    #   such as data loads can use local files with a direct connection, but not
    #   via APS. However, an direct connection requires that the Essbase server
    #   JAPI jar files are avaiable, uses more memory, and requires that the Essbase
    #   server port is reachable from the client. A connection via provider
    #   services (APS) is lighter-weight, and more likely to be usable from
    #   client machines outside the data centre, since it uses the same server
    #   as SmartView.
    # @param options [Hash] An options hash; see MessageHandler#new.
    #
    # @return [Server] A Server instance representing a connection to the requested
    #   Essbase server.
    def self.connect(user, password, server = 'localhost', aps_url = 'embedded', options = {})
        if !options
            if server.is_a?(Hash)
                options = server
                server = 'localhost'
                aps_url = 'embedded'
            elsif aps_url.is_a?(Hash)
                options = aps_url
                aps_url = 'embedded'
            end
        end

        if aps_url =~ /^embedded$/i && !@jars_loaded
            # Load additional jar files required in embedded mode
            load_jars 'ess_es_server.jar', 'ojdl.jar'
            @jars_loaded = true

            # Stop APS from dumping product info and internal logs to STDOUT
            java.lang.System.setProperty("suppressAPSProductInfo", "true")

            # Disable framework logging, since we setup our own log handler
            ol = Java::OracleCoreOjdlLogging::ODLLogger.getODLLogger("oracle.EPMOHPS")
            ol.set_level(Java::JavaUtilLogging::Level::OFF)
        end
        Server.new(user, password, server, aps_url, options)
    end

end


# Require core Essbase objects
require_relative 'base'
require_relative 'server'

