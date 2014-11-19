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
    # instrumentsion support, and a #try method for wrapping interactions via
    # the Java API, so that failures return a Ruby exception instead of a Java
    # one.
    #
    # The wrapper uses #method_missing to delegate to the underlying Java method
    # (using normal JRuby to Java name conversion) if no Ruby implementation is
    # provided for a method.
    #
    # Logging support is provided by a #log property, which returns a Logger
    # from the java.util.logging framework.
    #
    # Finally, an #instrument method is provided for capturing instrumentation
    # about Essbase method calls. This is a no-op unless ActiveSupport
    # notifications are available.
    class Base

        attr_reader :log


        # Create an Essbase JAPI object wrapper, with logging etc
        def initialize(instance_var_name = nil, wrapped_obj = nil)
            @log = Java::JavaUtilLogging::Logger.getLogger("ess4r.#{instance_var_name ?
                                                                    instance_var_name[1, -1] :
                                                                    'base'}")
            if instance_var_name
                @japi_instance_var_name = instance_var_name.intern
                instance_variable_set(@japi_instance_var_name, wrapped_obj) if wrapped_obj
            end

            if defined?(ActiveSupport::Notifications)
                @instrument = ActiveSupprt::Notifications
            end
        end


        # When making an Essbase API call, converts an EssException to a
        # Ruby exception.
        def try
            begin
                yield
            rescue com.essbase.api.base.EssException => ex
                raise EssbaseError, ex
            end
        end


        # Instrument an Essbase operation
        def instrument(operation, payload = {}, &blk)
            if @instrument
                @instrument.instrument("#{operation}.ess4r", payload, &blk)
            else
                blk.call
            end
        end


        # For any unknown method call, forward it to the underlying wrapped JAPI
        # object (if any).
        def method_missing(meth_name, *args)
            if @japi_instance_var_name &&
                japi_obj = instance_variable_get(@japi_instance_var_name)
                try{ japi_obj.send(meth_name, *args) }
            else
                super
            end
        end

    end

end
