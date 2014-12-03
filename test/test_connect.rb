require 'test/unit'
require 'ess4r'
require_relative 'credentials'


class TestConnect < Test::Unit::TestCase

    def setup
        @srv = Essbase.connect(ESSBASE_USER, ESSBASE_PWD, ESSBASE_SERVER)
    end

    def test_connect
        assert_equal(Essbase::Server, @srv.class)
    end

end

