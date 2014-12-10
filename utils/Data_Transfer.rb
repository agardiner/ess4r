require 'batch/job'


class DataTransfer < Batch::Job::Base

    title 'Essbase Data Transfer Utility'
    purpose 'A utility for extracting data from one or more source Essbase cubes, and ' +
        'loading it to one or more targets. The transfer process consists of a series of ' +
        'steps executed in sequence. Each step may perform an extract, load, or calculation.' +
        'By defining the steps and sequence, complicated transfers can be scripted.'

    positional_arg :spec, 'The path to a transfer specification file'
    rest_arg :subs, 'Values for any placeholder variables used in the transfer spec. ' +
        'Placeholder values should be entered in the form <variable>:<value> [<variable>:<value> ...]',
        on_parse: lambda{ |val, arg, hsh| Hash[val.map{ |pair| pair.split(':') }] }
    flag_arg :extract, 'Run extract step(s)', default: true
    flag_arg :load, 'Run load step(s)', default: true
    flag_arg :calc, 'Run calculate step(s)', default: true


    desc 'Extracts data from a cube to a file'
    task :extract_data, instance_expr: '${0}' do |lbl, cube, spec|
        puts "In extract"
    end


    desc 'Loads data to a cube from a file'
    task :load_data, instance_expr: '${0}' do |lbl, cube, spec|
        puts "In load"
    end


    desc 'Executes an Essbase calculation'
    task :calc, instance_expr: '${0}' do |lbl, cube, spec|
        puts "In calc"
    end


    desc 'Runs a sequence of extract, load, and calc steps in a spec file'
    job instance_expr: '${File.basename(arguments.spec, File.extname(arguments.spec))}' do
        extract_data 't', nil, nil
        load_data 't', nil, nil
        calc 't', nil, nil
    end

end

dt = DataTransfer.new
dt.parse_arguments
dt.execute

