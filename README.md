# @markup markdown
# @title Ess4r: A Ruby wrapper for the Hyperion Essbase Java API
# @author Adam Gardiner


# Ess4r: A Ruby wrapper for the Hyperion Essbase Java API

The ess4r gem provides a wrapper over significant portions of the Essbase Java
API (JAPI), and makes interaction with Essbase more natural from Ruby. It does
this by converting Java or Essbase data types to Ruby types, taking blocks for
callbacks where appropriate, and providing a higher-level API for common tasks
such as loading and extracting data.

The structure of the ess4r library closely mirrors the underlying Essbase JAPI
structure:

  * An {Essbase} class provides the API entry point; use {Essbase.connect} to
    initialize the API and return an {Essbase::Server} instance.

  * From an {Essbase::Server} instance, you can use {Essbase::Server#open_app},
    {Essbase::Server#open_cube}, or {Essbase::Server#open_maxl_session} to
    obtain an {Essbase::Application}, {Essbase::Cube}, or {Essbase::Maxl} instance
    respectively.

  * From a {Essbase::Cube} instance, you can:
    - Retrieve {Essbase::Dimension} and {Essbase::Member} objects that represent
      the cube outline metadata.
    - Load data from different sources via the {Essbase::Cube#load_data},
      {Essbase::Cube#load_sql}, and {Essbase::Cube#load_enumerable} methods.
    - Build dimensions via the {Essbase::Cube#build_dimension} and
      {Essbase::Cube#incremental_build} methods.
    - Run calculations via the {Essbase::Cube#calc_default},
     {Essbase::Cube#calculate}, and {Essbase::Cube#run_calc} methods.
    - Extract data using several different methods via the {Essbase::Cube#extract}
      method.
    - Retrieve substitution variable values via {Essbase::Cube#get_substitution_variable_value}.
    - Open a CubeView to run an MDX query via {Essbase::CubeView#mdx_query}, or
      calculation via the {Essbase::CubeView#run_calc} method.

All Ruby classes wrap a corresponding JAPI object, re-implementing commonly used
methods. However, you can also call any methods on the wrapped JAPI class via
these Ruby objects, and the method call will be forwarded to the JAPI object
via {Essbase::Base#method_missing}. This includes automatically wrapping the call
via the {Essbase::Base#try} method, which converts native JAPI exceptions to
{Essbase::EssbaseError} exceptions, which work better with standard Ruby exception
handling.

When connected to Essbase via ess4r, a message handler (IEssCustomMessageHandler)
is registered with Essbase, so that all messages that are logged in the Essbase
server or application logs are also forwarded to the client. These messages are
logged using a java.util.logging.Logger, under the ess4r.essbase log namespace.
