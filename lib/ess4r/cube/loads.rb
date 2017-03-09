require_relative 'load_errors'


class Essbase

    # Define methods for loading data and building dimensions.
    module Loads

        include_package 'com.essbase.api.datasource'


        # @!macro [new] load_notes
        #
        #    If records are rejected during the load, they can be written to the
        #    specified +error_file+ and/or yielded to a supplied block for further
        #    processing.
        #
        #    If no +rules_file+ parameter is specified, the load will be attempted
        #    without using a rules file. Essbase has fairly strict requirements for
        #    loading data without a rules file; in short, all dimensions must be
        #    identified before the first data value.
        #
        #    Additionally, if no load rule is used, the +abort_on_error+ param
        #    is ignored, and acts as though it were set to +true+, i.e. the load
        #    will terminate on the first error, and an exception will be thrown.
        #
        #    @param rules_file [String] The name of a local or server Essbase load
        #      rule file. A server rule file object should be specified without any
        #      file extension, while a local file should include the extension.
        #      If no +rules_file+ value is specified, the data load will be attempted
        #      without using a load rule.
        #    @param error_file [String] The name or path to a local file in which
        #      errors should be recorded. The file will only be created if there
        #      are rejected records.
        #    @param abort_on_error [Boolean] Whether the load should be terminated
        #      on the first error, or continue until the end.
        #
        #    @yield [msg, mbr, line] If a block is supplied, it will be called once
        #      for each rejected record.
        #    @yieldparam msg [String] The error message indicating the reason why
        #      the record was rejected.
        #    @yieldparam mbr [String] The name of the member that caused the problem.
        #    @yieldparam line [String] The line of input data that was rejected.
        #
        #    @return [Integer] a count of the number of rejected records.

        # @!group Data Load Methods

        # Loads data from a local or server +data_file+.
        #
        # @param data_file [String] The name of a local or server data file. A
        #   server data file should be specified without any file extension,
        #   while a local data file should include the extension and path.
        #
        # @macro load_notes
        def load_data(data_file, rules_file = nil, error_file = nil, abort_on_error = false, &block)
            reject_count = 0
            instrument "load_data", :data_file => data_file,
                :rules_file => rules_file, :error_file => error_file,
                :abort_on_error => abort_on_error do |payload|
                rejects = try { @cube.load_data(IEssOlapFileObject.TYPE_RULES, rules_file,
                                IEssOlapFileObject.TYPE_TEXT, data_file, abort_on_error) }
                reject_count = payload[:rejects] = process_rejects(rejects, error_file, &block)
            end
            reject_count
        end


        # Loads data from a SQL query, details of which are defined in the
        # +rules_file+.
        #
        # @param sql_user [String] SQL user id with which to connect to the SQL
        #   datasource.
        # @param sql_pwd [String] SQL password with which to connect to the SQL
        #   datasource.
        #
        # @macro load_notes
        def load_sql(sql_user, sql_pwd, rules_file, error_file = nil, abort_on_error = false, &block)
            reject_count = 0
            instrument "load_data", :sql_user => sql_user,
                :rules_file => rules_file, :error_file => error_file,
                :abort_on_error => abort_on_error do |payload|
                rejects = try { @cube.load_data(IEssOlapFileObject.TYPE_RULES, rules_file,
                                # IEssOlapFileObject does not define a constant for TYPE_SQL (16384)
                                16384, "", abort_on_error, sql_user, sql_pwd) }
                reject_count = payload[:rejects] = process_rejects(rejects, error_file, &block)
            end
            reject_count
        end


        # Streams data to the cube from +data+ using the optional +rules_file+.
        #
        # @param [Enumerable] data The object that will yield up lines of data
        #   to send to Essbase. This can be any object on which #each can be
        #   called, e.g. an Array or File.
        #
        # @macro load_notes
        def load_enumerable(data, rules_file = nil, error_file = nil, abort_on_error = false, &block)
            reject_count = 0
            instrument "data_load",
                :rules_file => rules_file, :error_file => error_file,
                :abort_on_error => abort_on_error do |payload|
                try{ @cube.begin_dataload(true, false, abort_on_error, rules_file,
                                           IEssOlapFileObject.TYPE_RULES) }
                data.each do |line|
                    line = line.join("\t") + "\n" if line.is_a?(Array)
                    try{ @cube.send_string(line) }
                end
                rejects = try{ @cube.end_dataload() }
                reject_count = payload[:rejects] = process_rejects(rejects, error_file, &block)
            end
            reject_count
        end


        # Processes the reject results from {#load_data}, #load_sql, or
        # #load_enumerable.
        #
        # @param rejects [Array<Array<String>>] An array of rejected records,
        #   where each record consists of an Array of +message+, +member+, and
        #   +source_line+.
        # @param error_file [String] Optional path to a local file to receive
        #   the rejected records.
        #
        # @yield If supplied, the block will be called for each rejection.
        # @yieldparam message [String] An error message indicating the reason
        #   the record was rejected.
        # @yieldparam member [String] The member that led to the rejection.
        # @yieldparam source_line [String] The data record that was rejected.
        #
        # @return [Integer] A count of the number of rejected records. Note that
        #   this may be less than the total number of records actually rejected
        #   by the load if the number of rejections exceeds the DATAERRORLIMIT
        #   value configured on the Essbase server.
        def process_rejects(rejects, error_file)
            reject_count = (rejects && rejects.length) || 0
            if rejects && (error_file || block_given?)
                err_file = File.new(error_file, "w") if error_file
                begin
                    rejects.each do |message, member, source_line|
                        err_num = message.match(/(\d+)$/)[1].to_i
                        message = (DATA_LOAD_ERROR_CODES[err_num] || message) % member
                        yield message, member, source_line if block_given?
                        if err_file
                            err_file.puts "\\\\ #{message}"
                            err_file.puts source_line
                            err_file.puts
                        end
                    end
                ensure
                    err_file.close if err_file
                end
            end
            log.warning "There were #{reject_count} rejected records" if reject_count > 0
            reject_count
        end

        # @!endgroup


        # @!group Dimension Build Methods

        # Perform a dimension build using the specified +rules_file+ and
        # +build_file+.
        #
        # @param rules_file [String] The name of a local or server Essbase load
        #   rule file. A server rule file object should be specified without any
        #   file extension, while a local file should include the extension.
        # @param error_file [String] The name or path to a local file in which
        #   errors should be recorded. The file will only be created if there
        #   are rejected records.
        # @yield [msg, mbr, line] If a block is supplied, it will be called once
        #   for each rejected record.
        # @yieldparam msg [String] The error message indicating the reason why
        #   the record was rejected.
        # @yieldparam mbr [String] The name of the member that caused the problem.
        # @yieldparam line [String] The line of input data that was rejected.
        #
        # @return [Integer] a count of the number of rejected records.
        def build_dimension(rules_file, build_file, error_file = nil)
            error_count = 0
            instrument "dimension_build", :rules_file => rules_file,
                :build_file => build_file, :error_file => error_file do |payload|
                errors = try{ @cube.build_dimension(rules_file, IEssOlapFileObject.TYPE_RULES,
                                                     build_file, IEssOlapFileObject.TYPE_TEXT,
                                                     error_file) }
                # Process errors
                error_count = payload[:errors] = process_build_errors(errors, build_file, &block)
            end
            error_count
        end


        # Performs an incremental outline build for each pair of data file/rule
        # combinations in +arr_pairs+.
        #
        # @param arr_pairs [Array<Array<String>>] An Array of 2-item Arrays,
        #   where the first item in inner arrays is the data file name, and the
        #   second item in the inner array is the name of the dimension build
        #   rule to use with that file.
        # @param err_file [String] The name or path to a local file in which
        #   errors should be recorded. The file will only be created if there
        #   are rejected records.
        # @param restruct_opt [:all_data, :no_data, :level0, :input] The cube
        #   restructure option for data retention.
        #
        # @yield [msg, mbr, line] If a block is supplied, it will be called once
        #   for each rejected record.
        # @yieldparam msg [String] The error message indicating the reason why
        #   the record was rejected.
        # @yieldparam mbr [String] The name of the member that caused the problem.
        # @yieldparam line [String] The line of input data that was rejected.
        #
        # @return [Integer] a count of the number of rejected records.
        def incremental_build(arr_pairs, err_file, restruct_opt = :all_data, &block)
            restruct_opt = case restruct_opt
            when :all_data then IEssCube.ESS_DOR_ALLDATA
            when :no_data then IEssCube.ESS_DOR_NODATA
            when :level0 then IEssCube.ESS_DOR_LOWDATA
            when :input then IEssCube.ESS_DOR_INDATA
            else raise "Unrecognised restructure option #{restruct_opt}; use :all_data, :no_data, :level0, or :input"
            end
            tmp_otl = 'tmpotl'
            error_count = 0

            log.fine "Commencing incremental outline build"
            try { @cube.begin_incremental_build_dim }
            arr_pairs.each do |file, rule|
                log.fine "Loading dimension file #{file} using #{rule}"
                instrument "incremental_build_dim", :rules_file => rule,
                    :build_file => file, :error_file => err_file do |payload|
                    errors = try{ @cube.incremental_build_dim(rule, IEssOlapFileObject.TYPE_RULES,
                                                               file, IEssOlapFileObject.TYPE_TEXT,
                                                               nil, nil,
                                                               IEssCube.ESS_INCDIMBUILD_BUILD, tmp_otl, err_file) }
                    # Process errors
                    error_count += payload[:errors] = process_build_errors(errors, file, &block)
                end
            end
            log.fine "Saving outline..."
            instrument "restructure" do |payload|
                errors = try{ @cube.end_incremental_build_dim(restruct_opt, tmp_otl, err_file, false) }
                error_count += payload[:errors] = process_build_errors(errors, tmp_otl, &block)
            end
            error_count
        end


        # Process the errors from dimension_build or incremental_dimension_build.
        # The JAPI dimension build methods return a Java StringBuffer containing
        # details of the errors, so we need to parse the contents of this to
        # determine the details of the errors.
        # Processes the reject results from #dimension_build or
        # #incremental_dimension_build
        #
        # @param errors [Array<StringBuffer>] An array of rejected record
        #   errors, where each record consists of an error message or source
        #   record.
        # @param error_file [String] Optional path to a local file to receive
        #   the rejected records.
        #
        # @yield If supplied, the block will be called for each rejection.
        # @yieldparam message [String] An error message indicating the reason
        #   the record was rejected.
        # @yieldparam member [String] The member that led to the rejection.
        # @yieldparam source_line [String] The data record that was rejected.
        #
        # @return [Integer] A count of the number of rejected records. Note that
        #   this may be less than the total number of records actually rejected
        #   by the load if the number of rejections exceeds the DATAERRORLIMIT
        #   value configured on the Essbase server.
        def process_build_errors(errors, error_file)
            error_count = 0
            if errors && errors.length > 0
                err_file = File.new(error_file, "w") if error_file
                begin
                    msg, msg_num, source_line, mbr = nil, nil, nil, nil
                    errors.to_s.each_line do |line|
                        err_file.puts(line) if err_file
                        if line =~ /^\\\\(?:Record #\d+ - )?(.+) \((\d+)\)/
                            yield msg, mbr, source_line if msg && block_given?
                            msg = $1
                            msg_num = $2.to_i
                            msg_template = DATA_LOAD_ERROR_CODES[msg_num]
                            if msg_template
                                re = Regexp.new(msg_template.gsub('%s', '(.+)'), Regexp::IGNORECASE)
                                mbr = re.match(msg)[1]
                            end
                            error_count += 1
                        else
                            source_line = line
                            yield msg, mbr, source_line if block_given?
                            msg, msg_num, source_line, mbr = nil, nil, nil, nil
                        end
                    end
                    yield msg, mbr, source_line if msg && block_given?
                ensure
                    err_file.close if err_file
                end
            end
            log.warning "There were #{error_count} build errors" if error_count > 0
            error_count
        end

        # @!endgroup

    end

end
