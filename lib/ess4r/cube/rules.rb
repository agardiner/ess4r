class Essbase

    # Defines methods for working with Essbase load rules.
    module Rules

        include_package 'com.essbase.api.rulefile'


        # Creates a data load rule for loading a data file containing the
        # columns specified in +fields+.
        #
        # @param file_name [String] A path to where the data load rule should be
        #   saved.
        # @param fields [Array<String>] An array specifying the field names in
        #   the data file.
        # @param [Hash] An options hash.
        # @options opts [Symbol] :delimiter The field delimiter used in the data
        #   file. One of :tab, :comma, or :space.
        # @options opts [Boolean] :use_header_for_field_names If true, the load
        #   rule will be configured to use the header record from the data file
        #   to set the columns when loading data.
        def create_data_load_rule(file_name, fields, opts = {})
            delimiter = opts.fetch(:delimiter, :tab)
            use_header_for_field_names = opts.fetch(:use_header_for_field_names, false)

            rule_file = EssRFRulesFile.new
            locale = self && self.application.locale
            if locale =~ /^UTF/
                rule_file.setEncoding(EssRFRulesFile::ENCODING_UTF8)
            elsif locale
                rule_file.setEncoding(EssRFRulesFile::ENCODING_NONUNICODE)
                rule_file.setLocale(locale)
            end

            ff = EssRFFlatFile.new
            case delimiter
            when :tab
                ff.setColumnDelimiter(EssRFFlatFile::TAB)
            when :comma
                ff.setColumnDelimiter(EssRFFlatFile::COMMA)
            when :space, :whitespace
                ff.setColumnDelimiter(EssRFFlatFile::WHITESPACE)
            end

            if use_header_for_field_names
                ff.setDataLoadRecordNumber(1)
            end

            ds = EssRFDataSource.new
            ds.setFileProperties(ff)
            rule_file.setDatasource(ds)

            fields.each do |fld|
                f = EssRFField.new
                f.setName(fld) unless use_header_for_field_names
                rule_file.addField(f)
            end

            rule_file.write(file_name)
        end

    end

end
