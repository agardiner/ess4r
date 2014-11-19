require_relative 'member'


class Essbase

    # Holds a set of Member objects representing the members in a dimension.
    class Dimension < Base

        # The dimension name
        attr_reader :name
        # The storage type, e.g. Dense, Sparse
        attr_reader :storage_type
        # @return The dimension tag, e.g. Accounts, Time, Attribute, etc
        attr_reader :tag


        # Creates a new Dimension object, populated from the supplied IEssDimension
        # object.
        def initialize(cube, ess_dim)
            super()
            @name = ess_dim.name
            @storage_type = ess_dim.storage_type.to_s
            @tag = ess_dim.tag.to_s
            @cube = cube
            @dim_mbr = nil
        end


        # Returns true if this dimension is dense
        def dense?
            @storage_type == 'Dense'
        end


        # Returns true if this dimension is sparse
        def sparse?
            @storage_type == 'Sparse' && !(@tag =~ /^Attr/)
        end


        # Returns true if this dimension is an attribute dimension
        def attribute_dimension?
            @storage_type == 'Sparse' && (@tag =~ /^Attr/)
        end


        # Returns a Member object containing details about the dimension member
        # +mbr_name+.
        def [](mbr_name)
            retrieve_members unless @members
            case mbr_name
            when /^['"\[]?&(.+?)['"\]]?$/
                # Substitution variable
                mbr_name = @cube.get_substitution_variable_value($1)
            when /^['"\[]?(.+?)['"\]]?$/
                # Quoted
                mbr_name = $1
            end
            mbr = @member_lookup[mbr_name.upcase]
            unless mbr
                # Search by alias
                @cube.get_alias_table_names.each do |tbl|
                    mbr = @members.find{ |mbr| (al = mbr.alias(tbl)) &&
                                               (al.upcase == mbr_name.upcase) }
                    break if mbr
                end
            end
            mbr
        end


        # Takes a +mbr_spec+ and expands it into an Array of matching Member
        # objects.
        #
        # Member names may be followed by various macros indicating relations of
        # the member to return, or alternatively, an Essbase calc function may be
        # used to query the outline for matching members.
        #
        # Relationship macros cause a member to be replaced by an expansion set
        # of members that have that relationship to the member. A macro is specified
        # by the addition of a .<Relation> suffix, e.g. <Member>.Children.
        # Additionally, the relationship in question may be further qualified by I
        # and/or R modifiers, where I means include the member itself in the expansion
        # set, and R means consider all hierarchies when expanding the member.
        #
        # The following relationship macros are supported:
        # - .Parent returns the member parent
        # - .[I,R]Ancestors returns the member's ancestors
        # - .[I]Children returns the member's children; supports the I modifier
        # - .[I,R]Descendants returns the member's descendants
        # - .[R]Leaves or .[R]Level0 returns the leaf/level 0 descendants of the member
        # - .[R]Level<N> returns the related members at level N
        # - .[R]Generation<N> returns the related members at generation N
        # - .UDA(<uda>) returns descendants of the member that have the UDA <uda>
        #
        # All of the above do not require querying of Essbase, and so are cheap
        # to evaluate. Alternatively, Essbase calc functions can be used to express
        # more complicated relationships, such as compound relationships.
        #
        # @param mbr_spec [Array, String] A string or array of strings containing
        #   member names, optionally followed by an expansion macro, such as
        #   .Children, .Leaves, .Descendants, etc
        # @return [Array<Member>] An array of Member objects representing the
        #   members that satisfy +mbr_spec+.
        def expand_members(mbr_spec)
            retrieve_members unless @members
            mbr_spec = [mbr_spec].flatten
            mbrs = []
            mbr_spec.each do |spec|
                case spec
                when /^['"\[]?(.+?)['"\]]?\.(Parent|I?Children|I?R?Descendants|I?R?Ancestors|R?Level0|R?Leaves)$/i
                    # Memer name with expansion macro
                    mbr = self[$1]
                    raise ArgumentError, "Unrecognised #{self.name} member '#{$1}' in #{spec}" unless mbr
                    rels = mbr.send($2.downcase.intern)
                    mbrs.concat(rels.is_a?(Array) ? rels : [rels])
                when /^['"\[]?(.+?)['"\]]?\.(R)?(Level|Generation|Relative)\(?(\d+)\)?$/i
                    # Memer name with level/generation expansion macro
                    mbr = self[$1]
                    sign = $3.downcase == 'level' ? -1 : 1
                    raise ArgumentError, "Unrecognised #{self.name} member '#{$1}' in #{spec}" unless mbr
                    rels = mbr.relative($4.to_i * sign, !!$2)
                    mbrs.concat(rels)
                when /^['"\[]?(.+?)['"\]]?\.UDA\(['"]?(.+)['"]?\)$/i
                    # Memer name with UDA expansion macro
                    mbr = self[$1]
                    raise ArgumentError, "Unrecognised #{self.name} member '#{$1}' in #{spec}" unless mbr
                    rels = mbr.idescendants.select{ |mbr| mbr.has_uda?($2) }
                    mbrs.concat(rels)
                when /[@:,]/
                    # An Essbase calc function or range - execute query and use results to find Member objects
                    mbr_sel = @cube.open_member_selection("MemberQuery")
                    begin
                        mbr_sel.execute_query(<<-EOQ.strip, spec)
                            <OutputType Binary
                            <SelectMbrInfo(MemberName, ParentMemberName)
                        EOQ
                        mbr_sel.get_members && mbr_sel.get_members.get_all.each do |ess_mbr|
                            mbr = self[ess_mbr.name]
                            raise ArgumentError, "No member in #{self.name} named '#{ess_mbr.name}'" unless mbr
                            if mbr.parent && mbr.parent.name != ess_mbr.parent_member_name
                                mbr = mbr.shared_members.find{ |mbr| mbr.parent.name == ess_mbr.parent_member_name }
                                raise "Cannot locate #{ess_mbr.name} with parent #{ess_mbr.parent_member_name}" unless mbr
                            end
                            mbrs << mbr
                        end
                    ensure
                        mbr_sel.close
                    end
                when /^['"\[]?(.+?)['"\]]?$/
                    # Plain member name
                    mbr = self[$1]
                    raise ArgumentError, "Unrecognised #{self.name} member '#{$1}'" unless mbr
                    mbrs << mbr
                else
                    raise ArgumentError, "Unrecognised #{self.name} member '#{spec}'"
                end
            end
            mbrs
        end


        # Performs a traversal of the dimension hierarchy, yielding the name and
        # Member object for each member in the dimension to the supplied block.
        # Note: By default, this iteration of members excludes shared members,
        # but if these are desired, specify false in the :exclude_shared option.
        def each(opts = {})
            retrieve_members unless @members
            exclude_shared = opts.fetch(:exclude_shared, true)
            @members.each do |mbr|
                yield mbr.name, mbr unless mbr.shared? && exclude_shared
            end
        end


        # Performs a depth-first traversal of the dimension, yielding members
        # to the supplied block. A depth-first traversal yields members in
        # outline order.
        #
        # @yield Yields each member of the dimension as it is visited
        # @yieldparam mbr [Member] A member of the dimension
        def walk(options = {}, &blk)
            retrieve_members unless @dim_mbr
            @dim_mbr.traverse(0, options, &blk)
        end


        # Retrieves the current set of members for this dimension.
        #
        # Note: this method is called automatically the first time a member is
        # requested from a dimension, and so would not normally need to be called
        # directly unless you wish to refresh the dimension members, e.g. after
        # a dimension build.
        def retrieve_members
            @dim_mbr = nil
            @members = []
            shared = []
            @member_lookup = {}
            log.finer "Retrieving members of dimension '#{@name}'"
            alias_tbls = try{ @cube.get_alias_table_names.to_a }
            puts alias_tbls
            mbr_sel = try{ @cube.open_member_selection("MemberQuery") }
            begin
                spec = %Q{@IDESCENDANTS("#{self.name}")}
                query = <<-EOQ.strip
                    <OutputType Binary
                    <SelectMbrInfo(MemberName, MemberAliasName, ParentMemberName,
                                   MemberGeneration, MemberLevel, Consolidation,
                                   ShareOption, MemberFormula, UDAList)
                EOQ
                @cube.instrument 'retrieve_members.ess4r', dimension: self do
                    try{ mbr_sel.execute_query(query, spec) }
                end
                mbr_sel.get_members.get_all.each do |ess_mbr|
                    mbr = Member.new(self, ess_mbr, alias_tbls)
                    @members << mbr
                    if mbr.shared?
                        shared << mbr
                    else
                        @member_lookup[mbr.name.upcase] = mbr
                    end
                end
                # Link shared members to non-shared member (and vice versa)
                shared.each do |smbr|
                    mbr = @member_lookup[smbr.name.upcase]
                    smbr.instance_variable_set(:@non_shared_member, mbr)
                    mbr.instance_variable_get(:@shared_members) << smbr
                end
                @dim_mbr = @member_lookup[self.name.upcase]
                # Convert parent names to references to the parent Member object
                # This can only be done after we've seen all members, since the
                # member selection query returns parents after children
                @members.each do |mbr|
                    par = @member_lookup[mbr.parent.upcase]
                    mbr.instance_variable_set(:@parent, par)
                    par.instance_variable_get(:@children) << mbr if par
                end
            ensure
                try{ mbr_sel.close }
            end
            log.finer "Retrieved #{@members.size} members"
        end


        # Returns the name of the dimension
        def to_s
            @name
        end

    end

end
