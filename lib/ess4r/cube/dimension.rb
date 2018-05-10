require_relative 'member'


class Essbase

    # Represents an Essbase dimension, and holds a set of {Member} objects
    # representing the members in a dimension.
    #
    # To obtain a Dimension instance, use the {Cube#[] Cube#[]} method.
    class Dimension < Base

        include Enumerable

        # The {Cube} to which this dimension belongs
        attr_reader :cube
        # @return [String] the name of the dimension.
        attr_reader :name
        # @return [String] the storage type: Dense or Sparse.
        attr_reader :storage_type
        # @return the dimension tag: Accounts, Time, Attribute, etc.
        attr_reader :tag


        # @!visibility private
        #
        # Creates a new Dimension object, populated from the supplied IEssDimension
        # object.
        #
        # @param cube [Cube] The cube to which this dimension belongs,
        # @param ess_dim [IEssDimension] The JAPI IEssDimension object this class
        #   is to wrap.
        def initialize(cube, ess_dim)
            super(cube.log)
            @name = ess_dim.name
            @storage_type = ess_dim.storage_type.to_s
            @tag = ess_dim.tag.to_s
            @cube = cube
            @root_member = nil
        end


        # @return [Boolean] true if this dimension is dense, false if it is not.
        def dense?
            @storage_type == 'Dense'
        end


        # @return [Boolean] true if this dimension is sparse or an attribute
        #   dimension.
        def sparse?
            @storage_type == 'Sparse' && !(@tag =~ /^Attr/)
        end


        # @return [Boolean] true if this dimension is an attribute dimension,
        #   false otherwise.
        def attribute_dimension?
            @storage_type == 'Sparse' && (@tag =~ /^Attr/)
        end


        # @return [Boolean] true unless this dimension is an attribute dimension.
        def non_attribute_dimension?
            !attribute_dimension?
        end


        # @return [Member] The top-most member in the dimension (i.e. the member
        #   with the same name as the dimension).
        def root_member
            retrieve_members unless @members
            @root_member
        end


        # Returns a {Member} object containing details about the dimension member
        # +mbr_name+.
        #
        # @param mbr_name [String] A name or alias for the member to be returned.
        #   An Essbase substitution variable can also be passed, in which case
        #   the variable value is retrieved, and the corresponding {Member} object
        #   returned.
        #
        # @return [Member] if the named member was found in this dimension.
        # @return [NilClass] if no matching member can be found.
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
        # by the addition of a +.<Relation>+ suffix, e.g. <Member>.Children.
        # Additionally, the relationship in question may be further qualified by +I+
        # and/or +R+ modifiers, where +I+ means include the member itself in the
        # expansion set, and +R+ means consider all hierarchies when expanding the
        # member.
        #
        # The following relationship macros are supported:
        #   - +.Parent+ returns the member parent
        #   - +.[I,R]Ancestors+ returns the member's ancestors
        #   - +.[I]Children+ returns the member's children; supports the I modifier
        #   - +.[I,R]Descendants+ returns the member's descendants
        #   - +.[R]Leaves+ or +.[R]Level0+ returns the leaf/level 0 descendants of
        #     the member
        #   - +.[R]Level<N>+ returns the related members at level _N_
        #   - +.[R]Generation<N>+ returns the related members at generation _N_
        #   - +.UDA(<uda>)+ returns descendants of the member that have the UDA <uda>
        #
        # All of the above do not require querying of Essbase, and so are cheap
        # to evaluate. Alternatively, Essbase calc functions can be used to express
        # more complicated relationships, such as compound relationships.
        #
        # The +mbr_spec+ can be a single specification (using the macros or calc
        # syntax described), or it can an array of specifications, each of which
        # is expanded to the corresponding set of member(s). By default, each such
        # set of member(s) is appended to the result, but several other possibilities
        # are supported, using an optional operation type prefix:
        #   - :add <spec> or + <spec>: The member set is appended to the results;
        #     this is the default behaviour when no operation type is specified.
        #   - :minus <spec> or - <spec>: The resulting member(s) are removed from
        #     the results. Members in this set that are not in the results are
        #     ignored.
        #   - :filter <ruby expr>: The current result set is filtered to only
        #     include members that satisy <ruby expr>. The results are always
        #     Member object instances, so the filter expression can use any
        #     properties of the members to define the filter condition.
        #   - :map <ruby expor>: The current result set is mapped using <ruby expr>,
        #     e.g. ":map parent" would convert the results to the parents of the
        #     members currently in the result set. The result of the :map operation
        #     should be Member objects.
        #   - :unique: Removes duplicates from the result set.
        #
        # @param mbr_spec [Array|String] A string or array of strings containing
        #   member names, optionally followed by an expansion macro, such as
        #   .Children, .Leaves, .Descendants, etc
        # @return [Array<Member>] An array of Member objects representing the
        def expand_members(mbr_spec, options={})
            retrieve_members unless @members
            mbr_spec = [mbr_spec].flatten
            all_mbrs = []
            mbr_spec.each do |spec|
                spec.strip!
                case spec
                when /^(?:\-|[:!](?:minus|not)\s)\s*(.+)/
                    all_mbrs -= process_member_spec($1)
                when /^(?:\+|[:!](?:add|or)\s)\s*(.+)/
                    all_mbrs += process_member_spec($1)
                when /^[:!]and\s+(.+)/  # Can't use & as a shortcut, since that identifies a subvar
                    all_mbrs = all_mbrs & process_member_spec($1)
                when /^[:!]filter\s+(.+)/
                    spec = eval("lambda{ #{$1} }")
                    all_mbrs.select!{ |mbr| mbr.instance_exec(&spec) }
                when /^[:!]map\s+(.+)/
                    spec = eval("lambda{ #{$1} }")
                    all_mbrs.map!{ |mbr| mbr.instance_exec(&spec) }.flatten!
                when /^[!:]uniq(ue)?\s*$/
                    all_mbrs.uniq!
                else
                    all_mbrs.concat(process_member_spec(spec))
                end
            end
            if all_mbrs.size == 0 && options.fetch(:raise_if_empty, true)
                raise ArgumentError, "Member specification #{mbr_spec} for #{self.name} returned no members"
            end
            all_mbrs
        end


        # Takes a +spec+ and expands it into an Array of matching Member objects.
        #
        # Member names may be followed by various macros indicating relations of
        # the member to return, or alternatively, an Essbase calc function may be
        # used to query the outline for matching members.
        #
        # Relationship macros cause a member to be replaced by an expansion set
        # of members that have that relationship to the member. A macro is specified
        # by the addition of a +.<Relation>+ suffix, e.g. <Member>.Children.
        # Additionally, the relationship in question may be further qualified by +I+
        # and/or +R+ modifiers, where +I+ means include the member itself in the
        # expansion set, and +R+ means consider all hierarchies when expanding the
        # member.
        #
        # The following relationship macros are supported:
        #   - +.Parent+ returns the member parent
        #   - +.[I,R]Ancestors+ returns the member's ancestors
        #   - +.[I]Children+ returns the member's children; supports the I modifier
        #   - +.[I,R]Descendants+ returns the member's descendants
        #   - +.[R]Leaves+ or +.[R]Level0+ returns the leaf/level 0 descendants of
        #     the member
        #   - +.[R]Level<N>+ returns the related members at level _N_
        #   - +.[R]Generation<N>+ returns the related members at generation _N_
        #   - + .[I]Shared returns all other instances of the member
        #   - +.UDA(<uda>)+ returns descendants of the member that have the UDA <uda>
        #
        # All of the above do not require querying of Essbase, and so are cheap
        # to evaluate. Alternatively, Essbase calc functions can be used to express
        # more complicated relationships, such as compound relationships.
        def process_member_spec(spec)
        case spec.strip
            when /^['"\[]?(.+?)['"\]]?\.(Parent|I?Children|I?R?Descendants|I?R?Ancestors|R?Level0|R?Leaves|I?Shared)$/i
                # Memer name with expansion macro
                mbr = self[$1]
                raise ArgumentError, "Unrecognised #{self.name} member '#{$1}' in #{spec}" unless mbr
                mthd = $2.downcase
                mthd += '_members' if mthd =~ /shared$/
                rels = mbr.send(mthd.intern)
                mbrs = rels.is_a?(Array) ? rels : [rels]
            when /^['"\[]?(.+?)['"\]]?\.(R)?(Level|Generation|Relative)\(?(\d+)\)?$/i
                # Memer name with level/generation expansion macro
                mbr = self[$1]
                sign = $3.downcase == 'level' ? -1 : 1
                raise ArgumentError, "Unrecognised #{self.name} member '#{$1}' in #{spec}" unless mbr
                mbrs = mbr.relative($4.to_i * sign, !!$2)
            when /^['"\[]?(.+?)['"\]]?\.UDA\(['"]?(.+?)['"]?\)$/i
                # Memer name with UDA expansion macro
                mbr = self[$1]
                raise ArgumentError, "Unrecognised #{self.name} member '#{$1}' in #{spec}" unless mbr
                mbrs = mbr.idescendants.select{ |mbr| mbr.has_uda?($2) }
            when /[@:,]/
                # An Essbase calc function or range - execute query and use results to find Member objects
                mbrs = []
                mbr_sel = try{ @cube.open_member_selection("MemberQuery") }
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
                mbrs = [mbr]
            else
                raise ArgumentError, "Unrecognised #{self.name} member '#{spec}'"
            end
        end


        # Performs a traversal of the dimension hierarchy, yielding each Member
        # in the dimension to the supplied block.
        #
        # @note By default, this iteration of members excludes shared members,
        # but if these are desired, specify true in the :include_shared option.
        #
        # @param opts [Hash] An options hash.
        # @option opts [Boolean] :include_shared If true, includes shared members
        #   when iterating over the dimension. If false (the default), shared
        #   members are not yielded.
        #
        # @yield Yields a {Member} object for each member in the dimension.
        # @yieldparam mbr [Member] The current member in the iteration.
        def each(opts = {})
            retrieve_members unless @members
            include_shared = opts.fetch(:include_shared, false)
            @members.each do |mbr|
                yield mbr unless mbr.shared? || include_shared
            end
        end


        # Performs a depth-first traversal of the dimension, yielding members
        # to the supplied block. A depth-first traversal yields members in
        # outline order.
        #
        # @yield Yields each member of the dimension as it is visited
        # @yieldparam mbr [Member] A member of the dimension
        def walk(options = {}, &blk)
            retrieve_members unless @members
            @root_member.traverse(0, options, &blk)
        end


        # Retrieves the current set of members for this dimension.
        #
        # Note: this method is called automatically the first time a member is
        # requested from a dimension, and so would not normally need to be called
        # directly unless you wish to refresh the dimension members, e.g. after
        # a dimension build.
        def retrieve_members
            @root_member = nil
            @members = []
            shared = []
            @member_lookup = {}
            log.finer "Retrieving members of dimension '#{@name}'"
            alias_tbls = try{ @cube.get_alias_table_names.to_a }
            mbr_sel = try{ @cube.open_member_selection("MemberQuery") }
            begin
                spec = %Q{@IDESCENDANTS("#{self.name}")}
                query = <<-EOQ.strip
                    <OutputType Binary
                    <SelectMbrInfo(MemberName, MemberAliasName, ParentMemberName,
                                   MemberGeneration, MemberLevel, Consolidation,
                                   ShareOption, MemberFormula, UDAList)
                EOQ
                @cube.instrument 'retrieve_members', dimension: self do
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
                @root_member = @member_lookup[self.name.upcase]
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
