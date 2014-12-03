class Essbase

    # Holds information about a single member. This class does not wrap the JAPI
    # EssMember object, since we need to release the resources from the query
    # used to obtain the members. Instead, we cache the information we need in
    # instances of this class.
    class Member

        # @return [Dimension] The Dimension object to which this member belongs.
        attr_reader :dimension
        # @return [String] The name of the member.
        attr_reader :name
        # @return [String] A qualified name that uniquely identifies this member
        #   in the outline.
        attr_reader :unique_name
        # @return [String] The type of storage for this member, e.g. Store Data,
        #   Shared Member etc
        attr_reader :share_option
        # @return [String] The consolidation operation to perform when aggregating
        #   this member.
        attr_reader :consolidation_type
        # @return [Fixnum] The generation number of the member. Generations count
        #   upwards from 1 as you descend the hierarchy from the dimension top
        #   member.
        attr_reader :generation_number
        # @return [Fixnum] The level number of the member. Levels count upwards
        #   from 0 starting from the leaf nodes and moving up the hierarchy.
        attr_reader :level_number
        # @return [String] The formula for this member in the outline.
        attr_reader :formula
        # @return [Member] The Member object that is the parent of this member.
        #   Nil if member is the root of the dimension.
        attr_reader :parent
        # @return [Member] The Member object that is the main (or non-shared)
        #  instance of this shared member. Nil if this member is not shared.
        attr_reader :non_shared_member

        alias_method :storage_type, :share_option
        alias_method :level, :level_number
        alias_method :generation, :generation_number


        # Creates a new Member object for the specified +dim+ from the IEssMember
        # +mbr+.
        #
        # @private
        def initialize(dim, mbr, alias_tbls)
            @dimension = dim
            @name = mbr.name
            @unique_name = mbr.unique_name
            @aliases = {}
            alias_tbls.each{ |tbl| @aliases[tbl] = mbr.get_alias(tbl) }
            @share_option = mbr.share_option.to_s
            @consolidation_type = mbr.consolidation_type.to_s[0]
            @generation_number = mbr.generation_number
            @level_number = mbr.level_number
            @formula = mbr.formula
            @parent = mbr.parent_member_name
            @udas = mbr.getUDAs.to_a
            @children = []
            @shared_members = []
        end


        # Returns true if the member is a shared member.
        def shared?
            !!(share_option =~ /Shared/i)
        end


        # Returns true if the member is non-shared, but has other shared
        # instances of itself elsewhere in the dimension.
        def has_shared_members?
            @shared_members.size > 0
        end


        # Returns true if the member is a dynamic calc member.
        def dynamic_calc?
            !!(share_option =~ /Dynamic calc/i)
        end


        # Returns true if the member is an XREF member.
        def xref?
            !!(dynamic_calc? && formula =~ /@XREF/i)
        end


        # Returns true if the member is a leaf member.
        def leaf?
            @level_number == 0
        end


        # Returns the alias for the member for the specified alias table.
        #
        # @param alias_tbl [String] The name of the alias table from which to
        #   return the alias. If not specified, the Default alias table alias is
        #   returned.
        def alias(alias_tbl = 'Default')
            @aliases[alias_tbl]
        end


        # Returns a hash containing the aliases for this member.
        def aliases
            @aliases.clone
        end


        # Returns true if the member has the specified UDA.
        def has_uda?(uda)
            @udas.find{ |mbr_uda| mbr_uda.downcase == uda.downcase }
        end


        # Returns the children of the member.
        #
        # @return [Array<Member>] An array of the child members of this member.
        def children
            @children.clone
        end


        # Returns the other shared instances of this (non-shared) member.
        def shared_members
            @shared_members.clone
        end


        # Returns the UDAs that this member has been assigned.
        def udas
            @udas.clone
        end


        # Returns the ancestors of this member.
        def ancestors
            anc = iancestors
            anc.shift
            anc
        end


        # Returns the ancestors of this member plus the member itself.
        def iancestors
            anc = [self]
            mbr = self
            while par = mbr.parent do
                anc << par
                mbr = par
            end
            anc
        end


        # Returns all ancestors of all instances of this member.
        def rancestors
            anc = irancestors
            anc.shift
            anc
        end


        # Returns all ancestors of all instances of this member plus the member.
        def irancestors
            anc = iancestors
            anc.clone.each do |ambr|
                ambr.shared_members.each do |smbr|
                    anc.concat(smbr.rancestors)
                end
            end
            anc.uniq
        end


        # Returns all the members descended from this member.
        def descendants
            desc = idescendants
            desc.shift
            desc
        end


        # Returns all descendants of this member, including the descendants
        # of other shared instances of this member.
        def rdescendants
            desc = irdescendants
            desc.shift
            desc
        end


        # Returns all the descendants of this member plus the member itself.
        def idescendants
            desc = [self]
            @children.each do |child|
                desc.concat(child.idescendants)
            end
            desc
        end


        # Returns all descendants of this member, including this member and
        # the descendants of shared members that are descendants
        def irdescendants
            me = @non_shared_member || self
            desc = [me]
            @children.each do |child|
                child = child.non_shared_member if child.shared?
                desc.concat(child.irdescendants)
            end
            desc
        end


        # Returns the level 0 descendants of this member.
        def leaves
            idescendants.select{ |mbr| mbr.leaf? }
        end
        alias_method :level0, :leaves


        # Returns the level 0 descendants of this member and all other shared
        # instances of this member.
        def rleaves
            irdescendants.select{ |mbr| mbr.leaf? }
        end
        alias_method :rlevel0, :rleaves


        # Return members related to this member at the specified generation or
        # level. Like the @RELATIVE function, +gen_or_lvl+ denotes a level by a
        # negative or zero value, and a generation by positive values.
        def relative(gen_or_lvl, follow_shared = false)
            case
            when gen_or_lvl <= 0 && gen_or_lvl.abs <= @level_number
                rels = follow_shared ? irdescendants : idescendants
            when gen_or_lvl <= 0 && gen_or_lvl.abs > @level_number
                rels = follow_shared ? rancestors : ancestors
            when gen_or_lvl > 0 && gen_or_lvl >= @generation_number
                rels = follow_shared ? irdescendants : idescendants
            when gen_or_lvl > 0 && gen_or_lvl < @generation_number
                rels = follow_shared ? rancestors : ancestors
            else
                raise ArgumentError, "gen_or_lvl must be numeric"
            end
            if gen_or_lvl <= 0
                rels.select{ |mbr| mbr.level_number == gen_or_lvl.abs }
            else
                rels.select{ |mbr| mbr.generation_number == gen_or_lvl }
            end
        end


        # Performs a left-most depth-first traversal of the descendants of this
        # member.
        def traverse(visitation_num = 0, options = {}, &blk)
            if options.fetch(:pre_traversal, true)
                blk.call(self, :pre, visitation_num += 1)
            end
            self.children.each do |mbr|
                visitation_num = mbr.traverse(visitation_num, options, &blk)
            end
            if options.fetch(:post_traversal, false)
                blk.call(self, :post, visitation_num += 1)
            end
            visitation_num
        end


        # Returns the name of the member
        def to_s
            @name
        end


        # Return a string showing pertinent details of this member
        def inspect
            %Q{<Member @name="#{@name}" @dimension="#{@dimension}" @aliases=#{
                @aliases.inspect} @children=#{@children.size} >}
        end

    end

end
