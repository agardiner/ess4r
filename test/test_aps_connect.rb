require 'test/unit'
require 'ess4r'
require_relative 'credentials'

class TestAPSConnect < Test::Unit::TestCase

    def test_connect
        s = Essbase.connect(ESSBASE_USER, ESSBASE_PWD, ESSBASE_SERVER, APS_JAPI_URL)
        assert(s.connections.size > 0)
        s.disconnect
    end

end
