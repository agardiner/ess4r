require 'java'


# This is intended to make it easier to use the Essbase JAPI, since configuring
# the necessary classpath and environment settings can be tricky and is poorly
# documented.
module EPMJarFinder

    # Represents a classpath or environment configuration error that prevents
    # initialisation of the Essbase JAPI.
    class ConfigurationError < StandardError; end


    # Standard locations in an EPM install for various needed JAR files.
    STANDARD_JAR_LOCATIONS = {
        'ess_japi.jar' => 'common/EssbaseJavaAPI',
        'ess_es_server.jar' => 'common/EssbaseJavaAPI',
        'ojdl.jar' => 'common/loggers/ODL'
    }


    # @return [Logger] a java.util.logging.Logger object for logging the
    #   initialisation process.
    def log
        @log = Java::JavaUtilLogging::Logger.getLogger("ess4r.essbase")
    end


    # Attempts to locate the specified JAR(s) and require them.
    # First we try to load the jar with the current load path, as this allows
    # a library user to control where the JARs should be loaded from by setting
    # the classpath or JRuby load path. Only if that fails do we fallback to
    # attempting to locate the JARs on our own.
    #
    # @param jars [String] The name(s) of JARs to be loaded.
    def load_jars(*jars)
        jars.each do |jar|
            unless $LOADED_FEATURES.find{ |f| f =~ Regexp.new(jar) }
                log.finest "Loading jar #{jar}..."
                begin
                    loaded = require(jar)
                rescue LoadError
                    loaded = find_jar(jar)
                end
                unless loaded
                    raise ConfigurationError, "Could not load JAR file #{jar}"
                end
            end
        end
    end
    alias_method :load_jar, :load_jars


    # Find the specified jar using EPM_ORACLE_HOME as a starting point.
    #
    # @param jar [String] The name of the JAR file to attempt to add to the
    #   class path.
    def find_jar(jar)
        oh = java.lang.System.getProperty('EPM_ORACLE_HOME') || ENV['EPM_ORACLE_HOME']
        unless oh
            raise ConfigurationError, "No EPM_ORACLE_HOME defined. Set this as an environment " +
                "variable or Java system property, or else add #{jar} to the classpath."
        end
        log.finest "EPM_ORACLE_HOME is #{oh}"

        sub_dir = STANDARD_JAR_LOCATIONS[jar]
        if (ver_dirs = Dir["#{oh}/#{sub_dir}/*"]) && !ver_dirs.empty?
            ver_dir = ver_dirs.first
            log.finest "Adding #{ver_dir} dir to load path"
            $LOAD_PATH << "#{ver_dir}/lib"
            require jar
        else
            raise ConfigurationError, "Cannot locate #{jar} under #{oh}/#{sub_dir}"
        end
    end

end
