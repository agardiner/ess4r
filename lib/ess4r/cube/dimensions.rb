class Essbase

    # Defines methods for obtaining {Dimension} objects from a {Cube}.
    # This module is used to extend {Cube} with methods for working with
    # dimensions.
    module Dimensions

        # Returns a an Array of Dimension objects, representing the available
        # dimensions in this cube.
        #
        # @return [Array<Dimension>] An array of Dimension objects, one for each
        #   dimension in this cube.
        def dimensions
            retrieve_dimensions unless @dimensions
            @dimensions.values
        end


        # @return [Array<Dimension>] An array containing all the sparse
        #   dimensions in this cube (in outline order).
        def sparse_dimensions
            dimensions.select{ |d| d.sparse? }
        end


        # @return [Array<Dimension>] An array containing all the dense
        #   dimensions in this cube (in outline order).
        def dense_dimensions
            dimensions.select{ |d| d.dense? }
        end


        # @return [Array<Dimension>] An array containing all the non-attribute
        #   dimensions in this cube (in outline order).
        def non_attribute_dimensions
            dimensions.reject{ |d| d.attribute_dimension? }
        end


        # @return [Array<Dimension>] An array containing all the attribute
        #   dimensions in this cube (in outline order).
        def attribute_dimensions
            dimensions.select{ |d| d.attribute_dimension? }
        end


        # Returns a Dimension or Member object matching +name+.
        #
        # @param name [String] The name of the dimension or member to return.
        # @return [Dimension|Member] A {Dimension} object representing the requested
        #   dimension, or a Member object if the name corresponds to a member.
        def [](name)
            retrieve_dimensions unless @dimensions
            dim = @dimensions[name.to_s.upcase]
            dim or get_member(name)
        end

        # Return a Dimension object for the specified +dim_name+.
        #
        # @param dim_name [String] The name of the dimension to be returned.
        # @return [Dimension] A {Dimension} object representing the requested
        #   dimension.
        def get_dimension(dim_name)
            retrieve_dimensions unless @dimensions
            dim = @dimensions[dim_name.to_s.upcase]
            dim or raise ArgumentError, "No dimension named '#{dim_name}' exists in #{self}"
        end

        # Return a Member object for the specifed +mbr_name+.
        #
        # @param mbr_name [String] The name of the member to be returned.
        # @return [MemberLite] A {MemberLite} object representing the requested
        #   member.
        def get_member(mbr_name)
            mbr = try{ @cube.getMember(mbr_name) }
            dim = self[try{ mbr.getDimensionName() }]
            MemberLite.new(dim, mbr)
        end


        # Run a member query against this cube; a member query is any valid
        # calc syntax member selection expression.
        #
        # @param spec [String] the query to find the member(s) of interest
        # @return [Array<MemberLite>] an array of member(s) satisfying the
        #   query.
        def member_query(spec)
            mbrs = []
            mbr_sel = try{ @cube.open_member_selection("MemberQuery") }
            begin
                mbr_sel.execute_query(<<-EOQ.strip, spec)
                    <OutputType Binary
                    <SelectMbrInfo(MemberName, ParentMemberName, DimensionName)
                EOQ
                mbr_sel.get_members && mbr_sel.get_members.get_all.each do |ess_mbr|
                    dim = self[ess_mbr.getDimensionName()]
                    mbr = MemberLite.new(dim, ess_mbr)
                    mbrs << mbr
                end
            ensure
                mbr_sel.close
            end
            mbrs
        end


        # Returns the dimension names in the order they appear in a Data Export
        # run from a calc script; i.e. sparse dimensions in outline order,
        # followed by dense dimensions in outline order. This is the most
        # efficient means of extracting data when the order of records is not
        # important.
        #
        # @return [Array<Dimension>] A list of dimensions in the order they will
        #   appear in a database export.
        def get_data_export_dimension_order
            sparse_dimensions + dense_dimensions
        end


        # Retrieve (or refresh) the available dimensions from the cube. Causes
        # the cube to be (re-)queried for metadata about available dimensions.
        def retrieve_dimensions
            require_relative 'dimension'
            @dimensions = {}
            try{ @cube.get_dimensions.get_all }.each do |ess_dim|
                dim = Dimension.new(self, ess_dim)
                @dimensions[dim.name.upcase] = dim
            end
        end

    end

end
