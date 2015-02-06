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
    def self.instance
        instance = IEssbase::Home.create(IEssbase.JAPI_VERSION)
        at_exit do
            instance.sign_off if instance.is_signed_on
        end
        instance
    end


    # Connect to an Essbase server using a user id and password.
    #
    # @param user [String] A userid with login rights to Essbase
    # @param password [String] The password for the user
    # @param server [String] The Essbase server to connect to. May include an
    #   optional port number for when the Essbase server is not listening on the
    #   default port; use the form <server>:<port>.
    # @param aps_url [String] A URL to the APS server; usually of the form:
    #   http(s)://<server>:<port>/aps/JAPI
    #   If omitted, a direct connection to the Essbase server will be created
    #   using embedded mode. This is the most flexible, since operations such
    #   as data loads can use local files with a direct connection, but not via
    #   APS. However, a direct connection requires that the Essbase server JAPI
    #   jar files are avaiable, uses more memory, and requires that the Essbase
    #   server port is reachable from the client. A connection via provider
    #   services (APS) is lighter-weight, and more likely to be usable from
    #   client machines outside the data centre, since it uses the same server
    #   as SmartView.
    def self.connect(user, password, server = 'localhost', aps_url = 'embedded')
        # Load additional jar files required in embedded mode
        if aps_url =~ /^embedded$/i && !@jars_loaded
            load_jars 'ess_es_server.jar', 'ojdl.jar'
            @jars_loaded = true

            # Stop APS from dumping product info and internal logs to STDOUT
            java.lang.System.setProperty("suppressAPSProductInfo", "true")

            # Disable framework logging, since we setup our own log handler
            ol = Java::OracleCoreOjdlLogging::ODLLogger.getODLLogger("oracle.EPMOHPS")
            ol.set_level(Java::JavaUtilLogging::Level::OFF)
        end
        Server.new(user, password, server, aps_url)
    end

end


# Require core Essbase objects
require_relative 'base'
require_relative 'server'

