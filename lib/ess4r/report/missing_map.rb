class Essbase

    class Report

        # Structure used to hold details of missing maps encountered while
        # populating a schedule.
        class MissingMap

            attr_reader :dimension, :map_name, :member, :reason
            attr_accessor :schedule

            def initialize(dimension, map_name, member, reason = nil)
                @dimension = dimension
                @map_name = map_name
                @member = member
                @reason = reason || "No mapping found"
            end

            def <=>(other)
                "#{@dimension}:#{@member}" <=> "#{other.dimension}:#{other.member}"
            end

            def to_s
                "#{@reason} for#{@member ? " member #{@member} in" : ''
                    } #{@dimension} mapping table #{@map_name}"
            end

        end

    end

end
