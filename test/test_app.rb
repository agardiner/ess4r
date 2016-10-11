require 'test/unit'
require 'ess4r'
require_relative 'credentials'

require 'date'
require 'active_support/core_ext/date'


class TestConnect < Test::Unit::TestCase

    def setup
        @srv = Essbase.connect(ESSBASE_USER, ESSBASE_PWD, ESSBASE_SERVER)
    end

    def test_open
        @srv.open_app('CCAR')
    end

    def test_get_log_file
        app = @srv.open_app('CCAR')
        app.get_log_file('./ccar.log')
    end


    def test_get_log_from_date
        app = @srv.open_app('CCAR')
        app.get_log_file('./ccar_recent.log', Date.today.beginning_of_month)
    end

end

