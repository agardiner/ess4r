require 'test/unit'
require 'ess4r'
require_relative 'credentials'


class TestConnect < Test::Unit::TestCase

    SAMPLE_DIMS = %w{Year Measures Product Market Scenario}
    SAMPLE_ATTR_DIMS = ['Caffeinated', 'Ounces', 'Pkg Type', 'Population',
                        'Intro Date', 'Attribute Calculations']
    PRODUCT_LEAVES = %w{100-10 100-20 100-30
                        200-10 200-20 200-30 200-40
                        300-10 300-20 300-30
                        400-10 400-20 400-30
                        100-20 200-20 300-30}

    def setup
        srv = Essbase.connect(ESSBASE_USER, ESSBASE_PWD, ESSBASE_SERVER)
        @cube = srv.open_cube('Sample', 'Basic')
    end


    def test_dimension_list
        dims = @cube.dimensions.map(&:to_s)
        assert_equal(SAMPLE_DIMS + SAMPLE_ATTR_DIMS, dims)
    end


    def test_dimension_members
        msrs = @cube['Measures']
        assert_equal('Profit', msrs['Profit'].name)
        assert_equal(true, msrs['Profit'].dynamic_calc?)
        assert_equal('Margin % Sales;', msrs['Margin %'].formula)
        assert_equal('Gross Margin', msrs['Margin'].alias('Long Names'))
    end


    def test_member_relations
        prod = @cube['Product']
        assert_equal(PRODUCT_LEAVES, prod['Product'].level0.map(&:name))
        assert_equal(prod['100-20'], prod['Diet'].children.first.non_shared_member)
        assert_equal(%w{100 Product}, prod['100-20'].ancestors.map(&:name))
        assert_equal(%w{100 Product Diet}, prod['100-20'].rancestors.map(&:name))
    end

end

