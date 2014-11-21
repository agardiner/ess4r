class Essbase

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


        # Returns a Dimension object containing the members of the +dim_name+
        # dimension. The Dimension object is cached for re-use, and Member
        # objects include useful methods for navigating through a hierarchy.
        def [](dim_name)
            retrieve_dimensions unless @dimensions
            dim = @dimensions[dim_name.to_s.upcase]
            dim or raise ArgumentError, "No dimension named '#{dim_name}' exists in #{self}"
        end


        # Returns the dimension names in the order they appear in a Data Export
        # run from a calc script; i.e. sparse dimensions in outline order,
        # followed by dense dimensions in outline order. This is the most
        # efficient means of extracting data when the order of records is not
        # important.
        #
        # @return [Array] A list of dimension names in the order they will
        #   appear in a database export.
        def get_data_export_dimension_order
            sparse_dimensions + dense_dimensions
        end



        # Retrieve the available dimensions from the cube.
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
