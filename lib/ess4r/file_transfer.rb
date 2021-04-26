class Essbase


    # Module for file transfer functionality for copying files to/from the
    # Essbase server.
    module FileTransfer


        # Wrapper for an IEssOlapFileObject object. Handles conversion to Ruby
        # Time etc.
        class FileObject < Base

            # Convert a file object type to a file extension
            def self.extension_for(obj_type)
                ext = case obj_type
                when Essbase::IEssOlapFileObject.TYPE_ALIAS then '.alt'
                when Essbase::IEssOlapFileObject.TYPE_ASCBACKUP then '.bka'
                when Essbase::IEssOlapFileObject.TYPE_BINBACKUP then '.bkb'
                when Essbase::IEssOlapFileObject.TYPE_CALCSCRIPT then '.csc'
                when Essbase::IEssOlapFileObject.TYPE_EQD then '.eqd'
                when Essbase::IEssOlapFileObject.TYPE_EXCEL then '.xls'
                when 536870912 then '.jar'
                when Essbase::IEssOlapFileObject.TYPE_LOTUS2 then '.wk1'
                when Essbase::IEssOlapFileObject.TYPE_LOTUS3 then '.wk3'
                when Essbase::IEssOlapFileObject.TYPE_LOTUS4 then '.wk4'
                when Essbase::IEssOlapFileObject.TYPE_LRO then '.lro'
                when Essbase::IEssOlapFileObject.TYPE_OUTLINE then '.otl'
                when Essbase::IEssOlapFileObject.TYPE_PARTITION then '.ddb'
                when Essbase::IEssOlapFileObject.TYPE_REPORT then '.rep'
                when Essbase::IEssOlapFileObject.TYPE_RULES then '.rul'
                when Essbase::IEssOlapFileObject.TYPE_SELECTION then '.sel'
                when Essbase::IEssOlapFileObject.TYPE_STRUCTURE then '.str'
                when Essbase::IEssOlapFileObject.TYPE_TEXT then '.txt'
                when Essbase::IEssOlapFileObject.TYPE_WIZARD then '.wiz'
                when Essbase::IEssOlapFileObject.TYPE_XML then '.xml'
                when Essbase::IEssOlapFileObject.TYPE_ALL then '.*'
                else ".??? (#{obj_type})"
                end
            end


            # Converts a file extension to a file object type
            def self.file_type_for(ext)
                obj_type = case ext
                when /\.alt$/ then Essbase::IEssOlapFileObject.TYPE_ALIAS
                when /\.bka$/ then Essbase::IEssOlapFileObject.TYPE_ASCBACKUP
                when /\.bkb$/ then Essbase::IEssOlapFileObject.TYPE_BINBACKUP
                when /\.csc$/ then Essbase::IEssOlapFileObject.TYPE_CALCSCRIPT
                when /\.eqd$/ then Essbase::IEssOlapFileObject.TYPE_EQD
                when /\.xls$/ then Essbase::IEssOlapFileObject.TYPE_EXCEL
                when /\.jar$/ then 536870912
                when /\.wk1$/ then Essbase::IEssOlapFileObject.TYPE_LOTUS2
                when /\.wk3$/ then Essbase::IEssOlapFileObject.TYPE_LOTUS3
                when /\.wk4$/ then Essbase::IEssOlapFileObject.TYPE_LOTUS4
                when /\.lro$/ then Essbase::IEssOlapFileObject.TYPE_LRO
                when /\.otl$/ then Essbase::IEssOlapFileObject.TYPE_OUTLINE
                when /\.ddb$/ then Essbase::IEssOlapFileObject.TYPE_PARTITION
                when /\.rep$/ then Essbase::IEssOlapFileObject.TYPE_REPORT
                when /\.rul$/ then Essbase::IEssOlapFileObject.TYPE_RULES
                when /\.sel$/ then Essbase::IEssOlapFileObject.TYPE_SELECTION
                when /\.str$/ then Essbase::IEssOlapFileObject.TYPE_STRUCTURE
                when /\.txt$/ then Essbase::IEssOlapFileObject.TYPE_TEXT
                when /\.wiz$/ then Essbase::IEssOlapFileObject.TYPE_WIZARD
                when /\.xml$/ then Essbase::IEssOlapFileObject.TYPE_XML
                else Essbase::IEssOlapFileObject.TYPE_ALL
                end
            end


            # Creates a FileObject
            #
            # @!visibility private
            def initialize(cube, file_obj)
                super(cube.log, '@file', file_obj)
            end


            # @return [Time] the time at which the file was last modified.
            def time_modified
                Time.at(try{ @file.getTimeModified() }.to_a[0])
            end


            # @return [Time] the time at which the file was locked, or nil if it
            #   is not locked.
            def time_stamp
                ts = try{ @file.getTimeStamp() }
                Time.at(ts) if ts > 0
            end


            # @return [String] the name and extension for the file object.
            def file_name
                "#{self.name}#{self.class.extension_for(self.type)}"
            end


            # @return [Integer] the size of the file.
            def size
                try{ @file.getFileSizeLong() } rescue try{ @file.getFileSize() }
            end


            # @return [String] The file name.
            def to_s
                self.file_name
            end


            # @return [String] details about the file.
            def inspect
                "%-12s  %s  %dKB" % [
                    self.file_name,
                    self.time_modified.strftime('%d-%m-%Y %H:%M:%S'),
                    self.size / 1024
                ]
            end

        end



        # Returns a listing of files matching +filter+.
        #
        # @param file_spec [String] A file name pattern that identifies file(s)
        #   to upload from +local_dir+.
        # @return [Array<String>] A list of file names matching +file_spec+.
        def list_files(file_spec = '*')
            file_type = FileObject.file_type_for(file_spec)
            filter = Regexp.new("^#{File.basename(file_spec, File.extname(file_spec)).
                                gsub('.', '\.').gsub('?', '.').gsub('*', '.*')}$",
                                Regexp::IGNORECASE)
            items = self.get_olap_file_objects(Essbase::IEssOlapFileObject.TYPE_ALL).sort_by(&:name)
            files = []
            items.each do |item|
                next unless item.type != 8192 && (filter.nil? || item.name =~ filter)
                files << FileObject.new(self, item)
            end
            files
        end


        # Downloads files from Essbase to the local directory.
        #
        # @param local_dir [String] The path to the local directory to which
        #   files should be downloaded.
        # @param file_spec [String] A file name pattern that identifies file(s)
        #   to download to +local_dir+.
        # @return [Integer] A count of the number of files downloaded.
        def download_files(local_dir, file_spec)
            files = list_files(file_spec)
            instrument 'download_files', local_dir: local_dir, files: files, source: self do
                files.each do |item|
                    local_file = "#{local_dir}/#{item.file_name}"
                    self.copy_olap_file_object_from_server(item.type, item.name, local_file, false)
                    File.utime(File.atime(local_file), item.time_modified, local_file)
                    log.fine "Downloaded #{item.file_name}"
                end
            end
            files.size
        end


        # Uploads files to the Essbase server from the local directory.
        #
        # @param local_dir [String] The path to the local directory from which
        #   files should be uploaded.
        # @param file_spec [String] A file name pattern that identifies file(s)
        #   to upload from +local_dir+.
        # @return [Integer] A count of the number of files uploaded.
        def upload_files(local_dir, file_spec)
            paths = Dir["#{local_dir}/#{file_spec}"]
            files = paths.map{ |p| File.basename(p) }
            instrument 'upload_files', local_dir: local_dir, files: files, target: self do
                paths.each do |path|
                    ext = File.extname(path)
                    name = File.basename(path, ext)
                    obj_type = FileObject.file_type_for(ext)
                    self.delete_olap_file_object(obj_type, name) rescue nil
                    self.copy_olap_file_object_to_server(obj_type, name, path, true)
                    log.finer "Uploaded #{name}.#{ext}"
                end
            end
            files.size
        end


        # Deletes files from the Essbase server database directory.
        #
        # @param file_spec [String] A file name pattern that identifies file(s)
        #   to delete.
        # @return [Integer] A count of the number of files deleted.
        def delete_files(file_spec)
            files = list_files(file_spec)
            instrument 'delete_files', files: files, source: self do
                files.each do |item|
                    self.delete_olap_file_object(item.type, item.name)
                    log.finer "Deleted #{item.file_name}"
                end
            end
            files.size
        end

    end

end

