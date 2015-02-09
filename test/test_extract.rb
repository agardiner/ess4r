require 'test/unit'
require 'ess4r'
require_relative 'credentials'
require 'fileutils'


class TestExtract < Test::Unit::TestCase

    OUTPUT_DIR = File.dirname(__FILE__) + '/../data/'

    FileUtils.mkdir_p(OUTPUT_DIR)


    EXTRACT_SPEC = {
        rows: {
            Accounts: 'Accounts.Level0',
            Product: 'Product.Level0',
            Market: 'East.Children',
            Scenario: 'Actual'
        },
        columns: {
            Year: 'Year.Level0'
        }
    }


    def setup
        srv = Essbase.connect(ESSBASE_USER, ESSBASE_PWD, ESSBASE_SERVER)
        @cube = srv.open_cube('Demo', 'Basic')
    end

    def test_mdx_extract
        @cube.extract(EXTRACT_SPEC, OUTPUT_DIR + 'mdx_extract.txt',
                      include_headers: true, query_file: OUTPUT_DIR + 'mdx_extract.mdx')
    end

    def test_rep_extract
        @cube.extract(EXTRACT_SPEC, OUTPUT_DIR + 'rep_extract.txt',
                      extract_method: :report,
                      include_headers: true, query_file: OUTPUT_DIR + 'rep_extract.rep')
    end

    def test_mdx_map
        @cube.extract(EXTRACT_SPEC, OUTPUT_DIR + 'mdx_map_extract.txt',
                      member_maps: {'Actual' => 'ACT'},
                      include_headers: true, query_file: OUTPUT_DIR + 'mdx_map_extract.mdx')
    end

end

