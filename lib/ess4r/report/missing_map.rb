class Essbase

    class Report

        # Structure used to hold details of missing maps encountered while
        # populating a report.
        class MissingMap

            attr_reader :dimension, :map_name, :member, :reason
            attr_accessor :report


            # Constructs a MissingMap instance for a member of a dimension with
            # no entry in the specified map_name enumeration.
            #
            # @param dimension The content definition for which no map was found.
            # @param map_name [String|Symbol] The name of the map table from which
            #   no map entry was found.
            # @param member [Member] The member for which no map was found.
            # @param reason [String] An optional reason for which this missing
            #   map has been created.
            def initialize(dimension, map_name, member, reason = nil)
                @dimension = dimension
                @map_name = map_name
                @member = member
                @reason = reason || "No mapping found"
            end


            # Comparison operator for comparing MissingMap entries.
            def <=>(other)
                "#{@dimension}:#{@member}" <=> "#{other.dimension}:#{other.member}"
            end


            # @return [String] A description of the missing map cause.
            def to_s
                "#{@reason} for#{@member ? " member #{@member} in" : ''
                    } #{@dimension} mapping table #{@map_name}"
            end

        end

    end

end
