class Essbase

    # Extracts data from Essbase using the report writer. This method provides
    # the best performance when sparse dynamic calc members are required in the
    # extract.
    class ReportExtract < Extract

        # Create and run a report script to export data.
        #
        # @param extract_spec [Hash] A hash containing a list of member
        #   specifications for each dimension. The keys of the hash are the
        #   dimensions, and the values are the member specifications (which will
        #   be expanded via Dimension#expand_members).
        # @param output_file [String] A path to where the output should be saved.
        # @param options [Hash] An options hash for controlling various facets
        #   of the extract process.
        def extract_data(extract_spec, output_file, options = {})
            log.info "Exporting #{@cube} data via report script..."

            # Create the report spec
            convert_extract_spec_to_members(extract_spec, options)
            report_sparse_dynamic_calcs(:report, options)
            rep_script = generate_script(options)
            save_query_to_file(rep_script, options[:query_file], '.rep')

            # Submit the report
            iter = nil
            instrument 'report.extract', script: rep_script, output_file: output_file do
                iter = cube.report(rep_script, false, false, true, false)
            end

            # Write the returned data to the file
            log.fine "Writing data to #{output_file}"
            file = File.new(output_file, "w")
            count = 0
            begin
                # TODO: Handle header, non-tab delimiters
                while !try{ iter.is_end_of_data } do
                    chunk = try{ iter.next_string }
                    file.puts chunk
                    count += chunk.lines.count
                end
            ensure
                file.close
            end
            log.fine "Output #{count} records"
        end


        # Returns a report script for a data extract.
        #
        # @param options [Hash] An options hash.
        # @option options [String] :column_dim The dimension to appear on the
        #   column axis in the extract. Defaults to the last dense dimension
        #   if :cols is not specified in the extract specification.
        # @option options [String] :delimiter The character to use as the
        #   field delimiter. Defaults to "\t" (i.e. tab).
        # @option options [Integer] :decimals The number of decimal places to
        #   use when outputting data. Default is no rounding.
        # @option options [String] :missing_val The text to output in place of
        #   missing values.
        def generate_script(options)
            delimiter = options.fetch(:delimiter, "\t")
            missing_val = options[:missing_val]
            decimals = options[:decimals] || options[:decimal_places]

            # Determine layout
            assign_axes(options)
            raise ArgumentError, "POV axis is not supported for report extracts" if @pov_dims.size > 0
            layout = []
            layout << %{<PAGE(#{quote_mbrs(@page_dims).join(', ')})} if @page_dims.size > 0
            layout << %{<ROW(#{quote_mbrs(@row_dims).join(', ')})}
            layout << %{<COL(#{quote_mbrs(@col_dims).join(', ')})}

            # Create member sets
            mbr_sets = (@page_dims + @row_dims + @col_dims).map do |dim|
                @extract_members[dim].join("\n")
            end

            # Setup formatting
            format_options = %w{
                NOINDENTGEN
                BLOCKHEADERS
                ROWREPEAT
                SUPFEED
                SUPBRACKETS
                SUPCOMMAS
                SUPEMPTYROWS
                SUPMISSINGROWS
                SUPZEROROWS
            }
            format_options << "DECIMAL #{decimals}" if decimals
            format_options << %Q{MISSINGTEXT "#{missing_val}"} if missing_val
            format_options << 'TABDELIMIT' if delimiter

            @query = <<-EOQ.gsub(/^ {16}/, '')
                //ESS_LOCALE #{@cube.application.locale}

                // Format section
                {
                #{format_options.join("\n")}
                }

                // Layout section
                #{layout.join("\n")}

                // Member selections
                #{mbr_sets.join("\n\n")}
                !
            EOQ
        end

    end

end
