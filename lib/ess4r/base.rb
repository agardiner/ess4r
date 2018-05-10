class Essbase


    # A Ruby exception class for Essbase errors.
    class EssbaseError < StandardError

        # Accessor for the underlying EssException instance that was thrown by
        # the Essbase JAPI
        attr_reader :ess_exception


        # Create a new EssbaseError instance that wraps the supplied EssException.
        def initialize(ess_exc)
            @ess_exception = ess_exc
            super(ess_exc.message)
        end

    end


    # Base class for all Essbase JAPI wrapper objects; provides logging and
    # instrumentsion support, and a {#try} method for wrapping interactions via
    # the Java API, so that failures return a Ruby exception instead of a Java
    # one.
    #
    # The wrapper uses {#method_missing} to delegate to the underlying Java method
    # (using normal JRuby to Java name conversion) if no Ruby implementation is
    # provided for a method.
    #
    # Logging support is provided by a {#log} property, which returns a Logger
    # from the java.util.logging framework.
    #
    # Finally, an {#instrument} method is provided for capturing instrumentation
    # about Essbase method calls. This is a no-op unless ActiveSupport
    # notifications are available.
    class Base

        include_package 'com.essbase.api.base'

        # Provides access to a java.util.logging.Logger instance under the ess4r
        # namespace. This logger can be used to log activity on the same log
        # handlers used by the Essbase message hanlder callback.
        # @return [java.util.logging.Logger] A logger named for the sub-class,
        #   e.g. ess4r.server, ess4r.cube, etc.
        attr_reader :log


        # Create an Essbase JAPI object wrapper, with logging etc
        #
        # @param log [Java::JavaUtilLogging::Logger] The log instance to use
        #   when logging messages.
        # @param instance_var_name [String] The name of the instance variable to
        #   use for the wrapped JAPI object.
        # @param wrapped_obj [Object] A JAPI object that is to be wrapped and
        #   delegated to for unimplemented method calls.
        def initialize(log, instance_var_name = nil, wrapped_obj = nil)
            @log = log

            if instance_var_name
                @japi_instance_var_name = instance_var_name.intern
                instance_variable_set(@japi_instance_var_name, wrapped_obj) if wrapped_obj
            end

            if defined?(ActiveSupport::Notifications)
                @instrument = ActiveSupport::Notifications
            end
        end


        # Attempts to make an Essbase API call, and converts any JAPI EssException
        # Java exceptions to Ruby {EssbaseError} exceptions.
        def try
            begin
                yield
            rescue com.essbase.api.base.EssException => ex
                raise EssbaseError, ex
            end
        end


        # Instrument an Essbase operation, using ActiveSupport::Notifications
        # (if available). Calls to methods that might take some time to complete
        # can utilise this method to track start/end times, as well as various
        # other items of interest about the method call. Interested parties can
        # register to receive notifications of these calls via the
        # ActiveSupport#Notifications framework from Rails. If ActiveSupport is
        # not available and +require+d, this becomes a no-op (although the
        # instrumented call still takes place).
        #
        # @param operation [String] A name for the operation being performed.
        #   This is placed under the ess4r namespace, using the standard naming
        #   for ActiveSupport::Notifications, i.e. <operation>.ess4r.
        # @param payload [Hash] An optional payload to send along with the
        #   notification. Can contain additional parameters of interest.
        # @yield A block containing the action(s) to be instrumented.
        def instrument(operation, payload = {}, &blk)
            if @instrument
                @instrument.instrument("#{operation}.ess4r", payload, &blk)
            else
                blk.call payload
            end
        end


        # Finds a matching entry in +hsh+ that matches +key+, ignoring case
        # and Symbol/String differences.
        #
        # @param hsh [Hash] The hash in which to find the value.
        # @param key [String, Symbol] A String or Symbol key to find a match for.
        # @param default [Object] The default value to return if no match is
        #   found for the key.
        # @return [Object] The matching value in +hsh+, or +default+ if no value
        #   was found.
        def get_hash_val(hsh, key, default = nil)
            # Find the key used in the that matches the dimension name
            return hsh[key] if hsh.has_key?(key)
            search_key = key.to_s.downcase
            matched_key = hsh.keys.find{ |k| k.to_s.downcase == search_key }
            matched_key ? hsh[matched_key] : default
        end


        # For any unknown method call, forward it to the underlying wrapped JAPI
        # object (if any). If the JAPI method returns an IEssIterator, converts
        # the content to a Ruby array.
        def method_missing(mthd_name, *args)
            if @japi_instance_var_name &&
                japi_obj = instance_variable_get(@japi_instance_var_name)
                res = try{ japi_obj.send(mthd_name, *args) }
                res = try{ res.getAll }.to_a if res.is_a?(IEssIterator)
                res
            else
                super
            end
        end


        # For any unknown method, check if the underlying object responds to it.
        def respond_to?(mthd_name)
            if @japi_instance_var_name &&
                japi_obj = instance_variable_get(@japi_instance_var_name)
                try{ japi_obj.respond_to?(mthd_name) }
            else
                super
            end
        end

    end

end
