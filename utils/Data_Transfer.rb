require 'batch/job'
require 'color_console/java_util_logger'
require 'ess4r'


class DataTransfer < Batch::Job::Base

    title 'Essbase Data Transfer Utility'
    purpose 'A utility for extracting data from one or more source Essbase cubes, and ' +
        'loading it to one or more targets. The transfer process consists of a series of ' +
        'steps executed in sequence; each step may perform an extract, load, or calculation. ' +
        'By defining the steps and sequence, complicated transfers can be scripted.'

    positional_arg :spec, 'The path to a transfer specification file'
    rest_arg :subs, 'Values for any placeholder variables used in the transfer spec. ' +
        'Placeholder values should be entered in the form <variable>:<value> [<variable>:<value> ...]',
        on_parse: lambda{ |val, arg, hsh| Hash[val.map{ |pair| pair.split(':') }] }
    flag_arg :extract, 'Run extract step(s)', default: true
    flag_arg :load, 'Run load step(s)', default: true
    flag_arg :calc, 'Run calculate step(s)', default: true


    # Set log level to FINE
    Console.replace_console_logger(level: :fine)


    desc 'Extracts data from a cube to a file'
    task :extract_data, instance_expr: '${0}' do |lbl, cube, step|
        log.info "Extracting data from #{cube}..."
        default_options = Batch::Config.new({
            missing_val: '#MI', include_col_headers: true
        })
        options = default_options.merge(@spec.cubes[step.cube].fetch(:extract_options, {}))
        cube.extract(step, step.data_file, options)
    end


    desc 'Loads data to a cube from a file'
    task :load_data, instance_expr: '${0}' do |lbl, cube, step|
        data_files = step.data_files? ? step.data_files : [step.data_file]
        data_files.each do |file|
            log.info "Loading data to #{cube} from #{file}..."
            cube.load_data(file, step[:load_rule], step[:error_file])
        end
    end


    desc 'Executes an Essbase calculation'
    task :calc, instance_expr: '${0}' do |lbl, cube, step|
        log.info "Executing calculation against #{cube}..."
        case
        when step.calc_template?
            calc_str = @subs.read_template(step.calc_template)
            cube.calculate(calc_str)
        when step.calc_script?
            cube.run_calc(step.calc_script)
        else
            cube.calc_default
        end
    end


    desc 'Runs a sequence of extract, load, and calc steps in a spec file'
    job instance_expr: '${File.basename(arguments.spec, File.extname(arguments.spec))}' do
        @subs = Batch::Config.new(arguments.subs)
        @spec = Batch::Config.load(arguments.spec, @subs, true)
        @conns = Batch::Config.new

        log.info "Connecting to Essbase cubes..."
        @spec.cubes.each do |lbl, cfg|
            srv = Essbase.connect(cfg.essbase_user, cfg.essbase_password, cfg.essbase_server)
            @conns[lbl] = srv.open_cube(cfg.essbase_application, cfg.essbase_database)
        end

        @spec.steps.each do |lbl, step|
            if step.disabled? && step.disabled
                log.detail "Skipping disabled step #{lbl}"
                next
            end
            cube = @conns[step.cube]
            raise ArgumentError, "No cube with label #{step.cube} has been defined" unless cube

            case step.action
            when :extract
                extract_data(lbl, cube, step) if arguments.extract
            when :calc, :calculate
                calc(lbl, cube, step) if arguments.calc
            when :load
                load_data(lbl, cube, step) if arguments.load
            else
                raise ArgumentError, "Unrecognised action: #{step.action}"
            end
        end

    end

end

DataTransfer.run

