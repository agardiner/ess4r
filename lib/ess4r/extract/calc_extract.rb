class Essbase

    # Extracts data from Essbase using a calc script. This method provides the
    # the best performance on large extracts where no dynamic calcs are present.
    class CalcExtract < Extract

        # Create a calc script to export data to a server file, and then download
        # the file to the local machine. The generated calc script consists of a
        # series of FIXes to select the members we want from each dimension.
        #
        # @param extract_spec [Hash] A hash containing a list of member
        #   specifications for each dimension. The keys of the hash are the
        #   dimensions, and the values are the member specifications (which will
        #   be expanded via Dimension#expand_members).
        # @param output_file [String] A path to where the output should be saved.
        # @param options [Hash] An options hash for controlling various facets
        #   of the extract process.
        def extract_data(extract_spec, output_file, options = {})
            log.info "Exporting #{@cube} data via calc script..."

            # Create the calc script
            convert_extract_spec_to_members(extract_spec, options)
            report_sparse_dynamic_calcs(:calc, options)
            out_file, calc_script = generate_script(output_file, options)
            save_query_to_file(calc_script, options[:query_file], '.csc')

            # Submit the calc
            FileUtils.rm_f(output_file) rescue nil
            @cube.delete_olap_file_object(Essbase::IEssOlapFileObject.TYPE_TEXT, out_file) rescue nil
            instrument 'calc.extract', script: calc_script, output_file: output_file do
                @cube.calculate(calc_script)
                @cube.copy_olap_file_object_from_server(Essbase::IEssOlapFileObject.TYPE_TEXT,
                                                        out_file, output_file, false)
            end
        end


        # Returns a calc script template for exporting a slice of data.
        #
        # @param options [Hash] An options hash.
        # @option options [String] :column_dim The dimension to appear on the
        #   column axis in the extract. Defaults to the last dense dimension
        #   if :cols is not specified in the extract specification.
        # @option options [String] :delimiter The character to use as the
        #   field delimiter. Defaults to "\t" (i.e. tab).
        # @option options [Integer] :decimals The number of decimal places to
        #   use when outputting data. Default is 2 decimal places.
        # @option options [String] :missing_val The text to output in place of
        #   missing values.
        # @return [String, String] Returns two values in an array: the name of
        #   the server file to extract to, and the calculation script to
        #   generate the extract.
        def generate_script(output_file, options)
            col_format = options.fetch(:column_format, true)
            level = options.fetch(:export_level, 'LEVEL0')
            decimals = options.fetch(:decimal_places, 2)
            include_header = options.fetch(:include_header, false)
            field_sep = options.fetch(:field_sep, "\t")
            missing_val = options.fetch(:missing_val, "NULL")
            include_dynamic_calcs = options.fetch(:include_dynamic_calcs, true)
            include_sparse_dynamic_calcs = options.fetch(:include_sparse_dynamic_calcs, false)

            assign_axes(options)
            raise ArgumentError, "POV axis is not supported for calc extracts" if @pov_dims.size > 0
            raise ArgumentError, "Page axis is not supported for calc extracts" if @page_dims.size > 0
            raise ArgumentError, "Only one column dimension may be specified for calc extracts" if @col_dims.size > 1

            dims = @row_dims + @col_dims
            fixes = dims.each_with_index.map do |dim, i|
                '  ' * i + "FIX(#{@extract_members[dim].join(', ')})"
            end.join("\n                ")
            end_fixes = dims.each_with_index.map do |dim, i|
                '  ' * i + "ENDFIX;"
            end.reverse.join("\n                ")
            file_name = File.basename(output_file, File.extname(output_file))
            extract_path = "./#{@cube.application.name}/#{@cube.name}/#{file_name}.txt"

            @query = <<-EOQ.gsub(/^ {16}/, '')
                //ESS_LOCALE #{@cube.application.locale}
                SET EMPTYMEMBERSETS ON;

                SET DATAEXPORTOPTIONS
                {
                    DataExportLevel #{level};
                    DataExportDynamicCalc #{include_dynamic_calcs ? 'ON' : 'OFF'};
                    DataExportNonExistingBlocks #{include_sparse_dynamic_calcs ? 'ON' : 'OFF'};
                    DataExportColFormat #{col_format ? 'ON' : 'OFF'};
                    DataExportDimHeader #{include_header ? 'ON' : 'OFF'};
                    DataExportColHeader "#{@col_dims.first}";
                    DataExportRelationalFile #{col_format ? 'ON' : 'OFF'};
                    DataExportOverwriteFile ON;
                    DataExportDecimal #{decimals};
                };


                #{fixes}

                    DATAEXPORT "File" "#{field_sep}" "#{extract_path}" "#{missing_val}"

                #{end_fixes}
            EOQ
            [file_name, @query]
        end

    end

end

