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
        # followed by dense dimensions in outline order.
        #
        # @return [Array] A list of dimension names in the order they will
        #   appear in a database export.
        def get_data_export_dimension_order
            sparse_dims, dense_dims = [], []
            self.dimensions.each do |dim|
                if dim.sparse?
                    sparse_dims << dim.name
                elsif dim.dense?
                    dense_dims << dim.name
                end
            end
            sparse_dims + dense_dims
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
