package Class::ReluctantORM::Relationship::HasManyMany;

=head1 NAME

Class::ReluctantORM::Relationship::HasManyMany

=head1 SYNOPSIS

  # Add many-to-many relationships to a ReluctantORM Class

  # May use has_many if you provide join_table
  Pirate->has_many(
                   class       => 'Booty'
                   join_table => 'booties2pirates',
                  );
  Pirate->has_many_many(
                        class       => 'Booty'
                        method_name => 'booties',
                   # New in 0.4: multi-column keys allowed via arrayrefs
                        local_key   => 'pirate_id',
                        remote_key  => 'booty_id',
                   # New in 0.4: keys can have different names in the join table
                        join_local_key   => 'pirate_id',
                        join_remote_key  => 'booty_id',
                        join_table  => 'booties2pirates',
                        join_schema => 'caribbean',
                );

  # Now you have:
  $booties_collection  = $pirate->booties();

  # New in 0.4: in array context, implicitly do $booties_collection->all_items
  @loot = $pirate->booties();

  # Fetchers defined automatically
  $pirate   = Pirate->fetch_with_booties($pirate_id);
  @bipeds   = Pirate->fetch_by_leg_count_with_booties(2);

  # Get info about the relationship
  $rel = Pirate->relationships('booties');
  $str = $rel->type();                 # 'has_many_many';
  $str = $rel->linked_class();         # 'Booty';
  $str = $rel->linking_class();        # 'Pirate';
  @fields = $rel->local_key_fields();  # fields in Pirate that link to join table
  @fields = $rel->remote_key_fields(); # fields in Booty that link to join table

  $int = $rel->join_depth();           # 2

  # Class::ReluctantORM::SQL integration
  @sql_cols = $rel->additional_output_sql_columns();
  @cols     = $rel->local_key_sql_columns();
  @cols     = $rel->remote_key_sql_columns();
  @empty    = $rel->join_local_key_sql_columns();
  @empty    = $rel->join_remote_key_sql_columns();


=head1 DESCRIPTION

=head1 CREATING A RELATIONSHIP

=head2 $tb_class->has_many(class => 'OtherClass', join_table => 'join_table', ....);

=head2 $tb_class->has_many_many(class => 'OtherClass', join_table => 'join table', ...);

Initiates a many-to-many relationship between two classes/tables.
Results are handled with assistance of a simple container class, 
Class::ReluctantORM::Collection::ManyMany (documented below in this file).

An accessor will be created named other_classes (or method_name).  Note that this 
should be plural for readability.  The accessor will return a Collection object.

Additionally, a new constructor is created, named $class->fetch_with_METHOD.
This constructor has the special feature that it performs an outer join and
prepopulates the Collection.  Thus, Pirate->fetch_with_booties(23) is only
one DB query.

Finally, additional constructors named $class->fetch_by_ATTRIBUTE_with_METHOD 
will also be available via AUTOLOAD.

Obtaining the Collection object does NOT result in trips to the database.  Operations 
on the Collection object DO require trips to the database.

Note that a many-to-many relationship does not imply a reciprocal has_many_many relationship going the other way.
It's OK to set that up manually, though.

The first form is an alias for the second form.  Some users find it more readable.  That
alias is actually provided by the HasMany module.

In the first form, a relationship is setup to OtherClass using defaults, described below.

In the second form, options are made explicit:

=over

=item class (required)

The linked class.  This is the class on the remote end of the many-to-many.

=item join_table (required)

The name of the join table in the database.

=item join_schema (optional)

The schema of the join table if different than the local class.  Default: $tb_class->schema_name().

=item method_name (optional)

The name of the method that will be used to access the relationship.  This is also the name for the relationship, which you can pass to $tb_class->relationships.  Default is the lower-cased and pluralized OtherClass.  For example, if you say Pirate->has_many_many(class => 'Booty', ...), you'll get $pirate->booties().  Pluralization is performed using Lingua.

=item local_key (optional string or arrayref)

Name or names of columns in the local table acting as keys in the link between the local table and the join table.
Defaults to $tb_class->primary_key_columns().

=item remote_key (optional string or arrayref)

Name or names of columns in the remote table acting as keys in the link between the remote table and the join table.
Defaults to OtherClass->primary_key_columns().

=item join_local_key (optional string or arrayref)

Name or names of columns in the join table acting as keys in the link between the join table and the local table.
Defaults to $tb_class->primary_key_columns().

=item join_remote_key (optional string or arrayref)

Name or names of columns in the join table acting as keys in the link between the join table and the remote table.
Defaults to OtherClass->primary_key_columns().

=item join_extra_columns (optional arrayref)

Extra columns from the join table that will be fetched.

=back

=cut


use strict;
use warnings;

use Data::Dumper;
use Scalar::Util qw(blessed);
use Class::ReluctantORM::Utilities qw(install_method conditional_load array_shallow_eq check_args);
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::SQL::Aliases;

our $DEBUG = 0;

use base 'Class::ReluctantORM::Relationship';

sub _initialize {
    my $class = shift;
    install_method('Class::ReluctantORM::Relationship', 'is_has_many_many', sub { return 0; });
    install_method('Class::ReluctantORM', 'has_many_many', \&__setup_has_many_many);
    install_method('Class::ReluctantORM', 'is_field_has_many_many', \&is_field_has_many_many);
}

=head2 $str = $rel->type();

Returns 'has_many_many'.

=cut

sub type { return 'has_many_many'; }

=head2 $int = $rel->join_depth();

Returns 2.

=cut

sub join_depth { 2; }

=head2 $str = $rel->join_type();

Returns 'LEFT OUTER'.

This is the type of the first of the two joins - from the base table to the join table.  The next join, from the join table to the remote table, is always an INNER.

=cut

sub join_type { return 'LEFT OUTER'; }

=head2 $bool = $rel->is_has_many_many();

Returns true.

=cut

sub is_has_many_many { return 1; }


=head2 $int = $rel->lower_multiplicity()

Returns 0.

=cut

sub lower_multiplicity { return 0; }

=head2 $int = $rel->upper_multiplicity()

Returns undef.

=cut

sub upper_multiplicity { return undef; }


=begin devdocs

Not sure this is public.... or if that calling pattern is right.

=head2 $bool = $cro_obj->is_field_has_many_many('field');

Returns true if the given field is a HasMany field.

=cut

sub is_field_has_many_many {
    my $inv = shift;
    my $field = shift;
    my $tb_class = ref($inv) ? ref($inv) : $inv;
    my $rel = $tb_class->relationships($field);
    return $rel ? $rel->is_has_many_many() : undef;
}

=head2 $bool = $rel->is_populated_in_object($cro_obj);

Returns true if the CRO object has had this relationship fetched.

=cut

sub is_populated_in_object {
    my $rel = shift;
    my $cro_obj = shift;

    # Obtain the underlying collection
    my $collection = $cro_obj->get($rel->method_name());
    unless ($collection) {
        return 0;
    }

    return $collection->is_populated();
}

sub _mark_unpopulated_in_object {
    my $rel = shift;
    my $cro_obj = shift;

    # Obtain the underlying collection
    my $collection = $cro_obj->get($rel->method_name());
    unless ($collection) { return; }
    $collection->depopulate();

}


# Called from ReluctantORM::new()
sub _handle_implicit_new {
    my $rel = shift;
    my $linking_object = shift;
    my $new_args = shift;

    my $relation = $rel->method_name;
    my $children = $new_args->{$relation} || undef; # Default to unpopulated

    my $all_exist = 1;
    for my $c (@{$children || []}) { $all_exist &&= $c->is_inserted; }
    unless ($all_exist) {
        Class::ReluctantORM::Exception::Data::UnsupportedCascade->croak('Cascading imports not supported');
    }

    my $collection = Class::ReluctantORM::Collection::ManyToMany->_new(
                                                                       relationship => $rel,
                                                                       linking_object => $linking_object,
                                                                      );
    # If children were provided, that's great; unfortunately we can't
    # save them to the join table yet because we don't have keys on the parent yet
    # So, save them to the attach queue, and save the queue later in _handle_implicit_create
    if ($children) {
        # So, ahhhh... is this consdiered pouplated?
        $collection->{_populated} = 1; # guess so

        foreach my $child (@$children) {
            $collection->attach($child, 1);
        }
        $collection->{_count} ||= 0;
    }


    $linking_object->set($relation, $collection);
    delete $new_args->{$relation};
}

# Ick....  this verges on cascading inserts.  Blech.
# Also, this logic might be better served to be under _notify_key_change_on_linking_object
# (that would catch save()s as well)
sub _handle_implicit_create {
    my $rel = shift;
    my $linking_object = shift;
    my $create_args = shift;

    my $method = $rel->method_name;
    my $collection = $linking_object->$method;

    $collection->commit_pending_attachments();
}

sub _notify_key_change_on_linking_object {
    my $rel = shift;
    my $changed_linking_object = shift;
    if ($Class::ReluctantORM::SOFT_TODO_MESSAGES) {
        print STDERR __PACKAGE__ . ':' . __LINE__ . " - soft TODO - HasManyMany::_notify_key_change_on_linking_object()\n";
    }
}


sub __setup_has_many_many {
    my $cro_base_class = shift;
    my $hmm_class = __PACKAGE__;
    my %args = ();

    if (@_ == 1) {
        %args = (class => shift());
    } else {
        if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
        %args = check_args(
                           args => \@_,
                           required => [qw(class join_table)],
                           optional => [qw(
                                              method_name
                                              join_schema
                                              remote_key
                                              local_key
                                              join_local_key
                                              join_remote_key
                                              join_extra_columns
                                         )],
                          );
    }

    # Validate Args
    $args{method_name} ||= Class::ReluctantORM::Utilities::pluralize(Class::ReluctantORM::Utilities::camel_case_to_underscore_case((split('::', $args{class}))[-1]));
    $args{join_schema} ||= $cro_base_class->schema_name;

    # Coerce local and foreign keys to be arrayrefs
    $args{remote_key} ||= $args{class}->primary_key_columns();
    $args{remote_key}   = ref($args{remote_key}) eq 'ARRAY' ? $args{remote_key} : [ $args{remote_key} ];

    $args{local_key}  ||= $cro_base_class->primary_key_columns();
    $args{local_key}    = ref($args{local_key}) eq 'ARRAY' ? $args{local_key} : [ $args{local_key} ];

    $args{join_remote_key} ||= $args{class}->primary_key_columns();
    $args{join_remote_key}   = ref($args{join_remote_key}) eq 'ARRAY' ? $args{join_remote_key} : [ $args{join_remote_key} ];

    $args{join_local_key} ||= $cro_base_class->primary_key_columns();
    $args{join_local_key}   = ref($args{join_local_key}) eq 'ARRAY' ? $args{join_local_key} : [ $args{join_local_key} ];

    $args{join_extra_columns} ||= [];

    conditional_load($args{class});
    $hmm_class->delay_until_class_is_available
      ($args{class}, $hmm_class->__relationship_installer(%args, cro_base_class => $cro_base_class));
    $hmm_class->delay_until_class_is_available
      ($args{class}, $hmm_class->__inverse_relationship_finder(%args, cro_base_class => $cro_base_class));

}

sub __relationship_installer {
    my $hmm_class = shift;
    my %args = @_;
    return sub {
        if ($DEBUG > 1) {
            print STDERR __PACKAGE__ . ':' . __LINE__ . " - in HasManyMany setup callback\n";
        }
        my $rel = $hmm_class->new();
        $rel->method_name($args{method_name});
        $rel->linked_class($args{class});
        $rel->linking_class($args{cro_base_class});
        $rel->local_key_fields($args{cro_base_class}->field_name(@{$args{local_key}}));
        $rel->remote_key_fields($args{class}->field_name(@{$args{remote_key}}));

        my $jt = Table->new(
                            table => $args{join_table},
                            schema => $args{join_schema},
                            columns => [@{$args{join_remote_key}}, @{$args{join_local_key}}, @{$args{join_extra_columns}}],
                           );
        $rel->{_join_sql_table} = $jt;
        $rel->{_join_remote_sql_cols} = [ map { Column->new(table => $jt, column => $_) } @{$args{join_remote_key}} ];
        $rel->{_join_local_sql_cols}  = [ map { Column->new(table => $jt, column => $_) } @{$args{join_local_key}} ];
        $rel->remote_key_fields($args{class}->field_name(@{$args{remote_key}}));

        install_method($args{cro_base_class}, $args{method_name}, $rel->__make_has_many_many_accessor());
        install_method($args{cro_base_class}, 'fetch_' . $args{method_name}, $rel->__make_has_many_many_fetch_accessor());

        $rel->_install_search_by_with_methods();

        my @args_copy = map { ($_, $args{$_} ) } grep { $_ ne 'cro_base_class' } keys %args;
        $rel->_original_args_arrayref(\@args_copy);

        $args{cro_base_class}->register_relationship($rel);
    };
}

sub __inverse_relationship_finder {
    my $hmm_class = shift;
    my %args = @_;
    return sub {
        my $cro_local_class = $args{cro_base_class};
        my $cro_remote_class = $args{class};
        my $local_relname = $args{method_name};
        my $local_rel = $cro_local_class->relationships($local_relname);
        unless ($local_rel && $local_rel->is_has_many_many) { return; }
        if ($local_rel->inverse_relationship()) {
            # Assume we already found it
            return;
        }

        # Unlike HO and HM, HMM is self-inverting
        # So we look for other HMM relations

        # List the has_many_many relationships on the linked class
        # that point to this class
        my @remote_hmm_rels =
          grep { $_->linked_class eq $cro_local_class }
            grep { $_->is_has_many_many } $cro_remote_class->relationships();
        unless (@remote_hmm_rels) { return; }

        my @matches = ();
        foreach my $remote_rel (@remote_hmm_rels) {

            # These are lists of keys that should be on the local table, 
            # and should be identical
            my @local_keys1 = $remote_rel->remote_key_fields();
            my @local_keys2 = $local_rel->local_key_fields();
            next unless (array_shallow_eq(\@local_keys1, \@local_keys2));

            # Keys on the local side of the join table
            my @join_local_keys1 = $remote_rel->join_remote_key_columns();
            my @join_local_keys2 = $local_rel->join_local_key_columns();
            next unless (array_shallow_eq(\@join_local_keys1, \@join_local_keys2));

            # Keys on the remote side of the join table
            my @join_remote_keys1 = $remote_rel->join_local_key_columns();
            my @join_remote_keys2 = $local_rel->join_remote_key_columns();
            next unless (array_shallow_eq(\@join_remote_keys1, \@join_remote_keys2));

            # These are lists of keys that should be on the remote table,
            # and should be identical
            my @remote_keys1 = $remote_rel->local_key_fields();
            my @remote_keys2 = $local_rel->remote_key_fields();
            next unless (array_shallow_eq(\@remote_keys1, \@remote_keys2));

            push @matches, $remote_rel;
        }

        if (@matches == 1) {
            $local_rel->inverse_relationship($matches[0]);
            $matches[0]->inverse_relationship($local_rel);
        } else {
            # Not touching that with a 10-foot pole
        }

    };
}


=head2 @names = $rel->join_remote_key_columns();

Returns the names of the columns on the join table that are used in the relationship to the remote table.

=cut

sub join_remote_key_columns { return map { $_->column } shift->join_remote_key_sql_columns(); }

=head2 @names = $rel->join_local_key_columns();

Returns the names of the columns on the join table that are used in the relationship to the local table.

=cut

sub join_local_key_columns { return map { $_->column } shift->join_local_key_sql_columns(); }


=head2 @cols = $rel->join_remote_key_sql_columns();

Returns the columns (as Class::ReluctantORM::SQL::Column objects) on the join table that are used in the relationship to the remote table.

=cut

sub join_remote_key_sql_columns { return @{shift->{_join_remote_sql_cols}}; }

=head2 @cols = $rel->join_local_key_sql_columns();

Returns the columns (as Class::ReluctantORM::SQL::Column objects) on the join table that are used in the relationship to the local table.

=cut

sub join_local_key_sql_columns  { return @{shift->{_join_local_sql_cols}}; }

=head2 $table = $rel->join_sql_table();

Returns the linking table as a Class::ReluctantORM::SQL::Table.

=cut

sub join_sql_table { return shift->{_join_sql_table}; }

sub __make_has_many_many_accessor {
    my $rel = shift;

    # Setup accessor
    my $code = sub {
        my $tb_obj = shift;
        my $collection = $tb_obj->get($rel->method_name);
        unless (defined $collection) {
            $collection = Class::ReluctantORM::Collection::ManyToMany->_new(
                                                                      relationship => $rel,
                                                                      linking_object => $tb_obj
                                                                     );
            $tb_obj->set($rel->method_name, $collection);
        }
        # New feature
        return wantarray ? $collection->all() : $collection;
    };
    return $code;
}


sub __make_has_many_many_fetch_accessor {
    my $rel = shift;
    return sub {
        my $cro_obj = shift;
        my $method = $rel->method_name();
        $cro_obj->$method->fetch_all();
        my $coll = $cro_obj->$method();
        return wantarray ? $coll->all() : $coll;
    };
}


# Make SQL to insert one row
sub __make_insert_sql {
    my $rel = shift;
    my $sql = SQL->new('INSERT');
    $sql->table($rel->join_sql_table());

    foreach my $keycol ($rel->__join_keys()) {
        $sql->add_input($keycol, Param->new());
    }

    return $sql;
}

# Make SQL to delete one row
sub __make_delete_sql {
    my $rel = shift;
    my $sql = SQL->new('DELETE');

    my $join_table = $rel->join_sql_table();
    $sql->table($join_table);

    my $root_crit;
    foreach my $keycol ($rel->__join_keys()) {
        my $crit = Criterion->new(
                                  '=',
                                  $keycol,
                                  Param->new(),
                                 );
        $root_crit = $root_crit ? Criterion->new('AND', $root_crit, $crit) : $crit;
    }
    $sql->where(Where->new($root_crit));

    return $sql;
}


sub __join_keys {
    my $rel = shift;
    my $sql = SQL->new('INSERT');
    $sql->table($rel->join_sql_table());

    my @locals = 
      sort { $a->column cmp $b->column } 
        $rel->join_local_key_sql_columns();

    my @remotes = 
      sort { $a->column cmp $b->column } 
        $rel->join_remote_key_sql_columns();

    return (@locals, @remotes);
}


# Return array of raw values needed to be bound to execute a single-row insert or delete
# should be in order needed by the SQL returned by __make_insert_sql/__make_delete_sql
sub __make_join_binds {
    my ($rel, $parent, $child) = @_;
    my @binds;
    my $use_child = 0;
    foreach my $keycol ($rel->__join_keys) {
        $use_child ||= !$parent->field_name($keycol->column);
        my $obj = $use_child ? $child : $parent;
        push @binds, $obj->raw_field_value($obj->field_name($keycol->column()));
    }
    return @binds;
}

1;


#=============================================================================#
#=============================================================================#
#                               Collection Subclass
#=============================================================================#
#=============================================================================#

package Class::ReluctantORM::Collection::ManyToMany;
use strict;
use warnings;
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::SQL::Aliases;
use Class::ReluctantORM::Utilities qw(nz check_args);
use Class::ReluctantORM::FetchDeep::Results qw(fd_inflate);
use Scalar::Util qw(weaken blessed);

our $DEBUG = 0;
use Data::Dumper;

use base 'Class::ReluctantORM::Collection';

sub rel { return shift->{relationship}; }
sub linking_object { return shift->{linking_object}; }

sub _new {
    my ($class, %args) = @_;
    foreach my $f (qw(left_class left_key_value right_class join_table join_table_schema) ) {
        if (exists $args{$f}) { Class::ReluctantORM::Exception::Call::Deprecated->croak("May not use param $f for Collection::ManyToMany::_new in 0.4 code"); }
    }
    foreach my $f (qw(relationship linking_object)) {
        unless (exists $args{$f}) { Class::ReluctantORM::Exception::Param::Missing->croak(param => $f); }
    }

    my $self = bless \%args, $class;
    weaken($self->{linking_object});

    $self->{_attach_queue} = [];
    $self->{_remove_queue} = [];

    if ($args{children}) {
        $self->{_children} = $args{children};
        $self->{_populated} = 1;
        $self->{_count} = scalar @{$args{children}};
    } else {
        $self->{_populated} = 0;
        $self->{_count} = undef;
        $self->{_children} = [];
    }

    return $self;
}

sub _check_correct_child_class {
    my ($self, $object) = @_;
    unless (defined($object)) {
        Class::ReluctantORM::Exception::Param::Missing->croak(param => 'object', value => undef, error => "Cannot add an undef entry to a Has-Many-Many collection", frames => 2);
    }
    unless ($object->isa($self->rel->linked_class)) {
        Class::ReluctantORM::Exception::Data::WrongType->croak(param => 'object', expected => $self->rel->linked_class, frames => 2);
    }
}

sub all_items {
    my $self = shift;
    if ($self->is_populated) {
        return @{$self->{_children}};
    } else {
        Class::ReluctantORM::Exception::Data::FetchRequired->croak(called => 'all_items', call_instead => 'fetch_all', fetch_locations => [ $self->linking_object->all_origin_traces ]);
    }
}

sub all { goto &all_items; }


sub is_populated { return shift->{_populated}; }
sub depopulate {
    my $self = shift;
    $self->{_populated} = 0;
    $self->{_count} = undef;
    $self->{_children} = [];
}

sub count {
    my $self = shift;
    if ($self->is_populated || defined($self->{_count})) {
        return $self->{_count};
    } else {
        Class::ReluctantORM::Exception::Data::FetchRequired->croak(called => 'count', call_instead => 'fetch_count', fetch_locations => [ $self->linking_object->all_origin_traces ]);
    }
}

sub __make_link_where {

    # TODO - this is duplicate code with make_link_crit

    my $collection = shift;
    my $use_alias_macro = shift || 0;
    my $rel = $collection->rel;

    my @where  = ();
    my @execargs = ();

    # Create criteria with the join local keys as cols and the local keys as params
    my @local_key_cols = $rel->local_key_columns();
    my @join_local_key_cols = $rel->join_local_key_columns();

    foreach my $index (0..$#local_key_cols) {
        my $join_local_column_name = $join_local_key_cols[$index];
        my $local_field = $rel->linking_class->field_name($local_key_cols[$index]);

        my $crit;
        if ($use_alias_macro) {
            $crit = 'MACRO__parent__' . $rel->method_name() . '__.' . $join_local_column_name . ' = ?';
        } else {
            $crit = $join_local_column_name . ' = ?';
        }

        push @where, $crit;
        push @execargs, $collection->linking_object->raw_field_value($local_field);
    }
    return (where => (join ' AND ', @where), execargs => \@execargs);
}

sub __make_link_crit {
    my $collection = shift;
    my $use_alias_macro = shift;
    my $rel = $collection->rel();
    my $linking_obj =$collection->linking_object();
    my $linking_class = $rel->linking_class();

    # Create criteria with the join local keys as cols and the local keys as params
    my @local_key_cols = $rel->local_key_sql_columns();
    my @join_local_key_cols = $rel->join_local_key_sql_columns();

    my $where = Where->new(
                           Criterion->new(
                                          '=',
                                          $join_local_key_cols[0],
                                          Param->new($linking_obj->raw_field_value($linking_class->field_name($local_key_cols[0]->column))),
                                         )
                          );

    foreach my $index (1..$#local_key_cols) {
        my $crit = Criterion->new(
                                  '=',
                                  $join_local_key_cols[$index],
                                  Param->new($linking_obj->raw_field_value($linking_class->field_name($local_key_cols[$index]->column))));
        $where->and($crit);
    }

    return $where->root_criterion();
}

=for devnotes

=head2 $collection->_set_contents(@children);

Assuming you know what you are doing, this method replaces the in-memory guts of the collection.  The populated flag is set to true, and the count is set to the new count, but keys are not updated, dirtiness is not changed, and no db activity occurs.

=cut

sub _set_contents {
    my $self = shift;
    my @children = @_;

    $self->{_children} = \@children;
    $self->{_populated} = 1;
    $self->{_count} = scalar @children;

}

sub fetch_count {
    my $collection = shift;
    my $rel = $collection->rel();
    my $parent_obj = $collection->linking_object();

    my $sql = SQL->new('SELECT');
    $sql->from(From->new($rel->join_sql_table));
    $sql->where(Where->new($collection->__make_link_crit(0)));
    my $column = ($rel->join_remote_key_sql_columns)[0];
    my $output =  OutputColumn->new
      (expression => FunctionCall->new('COUNT', $column), alias => 'hmm_count');
    $sql->add_output($output);

    $parent_obj->driver->run_sql($sql);
    $collection->{_count} = $output->output_value();
    return $collection->count();
}

sub __remote_join_crit {
    my $coll = shift;
    my $rel = $coll->rel();

    my @jrc = $rel->join_remote_key_sql_columns();
    my @rc = $rel->remote_key_sql_columns();

    my $crit;
    foreach my $idx (0..$#rc) {
        my $this_crit =
          Criterion->new(
                         '=',
                         $jrc[$idx],
                         $rc[$idx],
                        );
        $crit = $crit ? Criterion->new('AND', $crit, $this_crit) : $this_crit;
    }

    return $crit;

}

sub __where_crit_on_join {
    my $coll = shift;
    my $rel = $coll->rel();

    my $obj = $coll->linking_object();

    my @jlc = $rel->join_local_key_sql_columns();
    my @pkf = $obj->primary_key_fields();

    my $crit;
    foreach my $idx (0..$#jlc) {
        my $this_crit =
          Criterion->new(
                         '=',
                         $jlc[$idx],
                         Param->new($obj->raw_field_value($pkf[$idx])),
                        );
        $crit = $crit ? Criterion->new('AND', $crit, $this_crit) : $this_crit;
    }
    return $crit;
}

sub fetch_all {
    my $coll = shift;
    my $rel = $coll->rel();

    my $sql = SQL->new('SELECT');
    my $join = Join->new(
                         'INNER',
                         $rel->remote_sql_table(),
                         $rel->join_sql_table(),
                         $coll->__remote_join_crit(),
                        );
    $join->relationship($rel);
    $sql->from(From->new($join));
    $sql->where(Where->new($coll->__where_crit_on_join()));

    $sql->make_inflatable();

    my @children = fd_inflate($sql);
    $coll->linking_object->capture_origin();

    $coll->{_children} = \@children;
    $coll->{_populated} = 1;
    $coll->{_count} = scalar @children;
    return @children;
}

sub fetch_deep {
    my $self = shift;
    my %args = check_args
      (
       args => \@_,
       required => [ qw(with) ], # As of CRO 0.5, no where, limit, or ordering permitted
      );


    # Rely on fetch_deep in parent
    # By "refetching" the parent
    my %where_args = $self->__make_link_where();
    my $method_name = $self->rel->method_name;
    my $parent = $self->rel->linking_class->fetch_deep(
                                                       %where_args,
                                                       with => { $method_name => $args{with} },
                                                      );
    my @children = $parent->$method_name->all();
    $self->linking_object->capture_origin();

    $self->{_children} = \@children;
    $self->{_populated} = 1;
    $self->{_count} = scalar @children;
    return @children;
}

=head2 $collection->attach($child);

Attach the child object to the parent in memory.  Unlike HasMany,
HasManyMany does not detach it from any other collections based
on this relationship.

Both the parent and the child must already be inserted in the database.  This operation
adds to an internal list of pairings to be inserted into the join
table later.  Use $collection->commit_pending_attachments() to
send the changes to the database.

If the collection is populated, the count will be updated.

The child will not become dirty.  No database activity occurs.  To attach
and immediately commit the change, use $collection->add().

=cut

sub attach {
    my ($collection, $child, $allow_uninserted_parent) = @_;
    $collection->_check_able_to_attach($child, $allow_uninserted_parent);
    push @{$collection->{_attach_queue}}, $child;
    $collection->__attach_bidirectional_in_memory($child);
}

sub _check_able_to_attach {
    my ($collection, $child, $allow_uninserted_parent) = @_;
    $collection->_check_correct_child_class($child);
    unless ($child->is_inserted()) {
        Class::ReluctantORM::Exception::Data::UnsupportedCascade->croak("Related object must be already inserted in the DB to be attached to a HasManyMany relationship");
    }
    unless ($allow_uninserted_parent) {
        unless ($collection->linking_object->is_inserted()) {
            Class::ReluctantORM::Exception::Data::UnsupportedCascade->croak("Parent object must be already inserted in the DB to be attached to a HasManyMany relationship");
        }
    }
    if ($collection->is_populated()) {
        if (grep { $_->id eq $child->id() } $collection->all()) {
            Class::ReluctantORM::Exception::Data::UniquenessViolation->croak("The child with ID " . $child->id() . " appears to already exist in the " . $collection->rel->method_name() . " relation");
        }
    }
}

=head2 $collection->commit_pending_attachments();

Inserts any pending rows into the join table.  Call this once after calling attach() repeatedly.  It is hoped that database drivers will be able to optimize this into one INSERT, though it may be as many INSERTs as there are rows to insert.

=cut

sub commit_pending_attachments {
    my $collection = shift;
    my @pending = @{$collection->{_attach_queue}};
    unless (@pending) { return; }

    # TODO OPTIMIZE With most DB drivers should be possible to send a VALUES table, allowing this to be done in one INSERT
    my $rel = $collection->rel;
    my $sql = $rel->__make_insert_sql();

    my $driver = $collection->linking_object->driver();
    foreach my $child (@pending) {
        my @binds = $rel->__make_join_binds($collection->linking_object(), $child);
        $sql->set_bind_values(@binds);
        $driver->run_sql($sql);      # TODO OPTIMIZE add prepare-execute
    }

    $collection->{_attach_queue} = [];
    return 1;
}




=head2 $collection->add($child);

Inserts a row in the join table linking the parent object and the child object.  

Unlike HasMany, HasManyMany does not remove the child from any other collections.

Calling add() directly does not affect the attach queue - in other words, if you
call attach($child1) then add($child2), $child1 will still not be committed.  Neither
the parent nor the child is becomes dirty during this operation.

Note that if you are adding many children, it is more efficient to call attach()
repeatedly, then call commit_pending_attachments().

=cut

sub add {
    my ($collection, $child) = @_;
    $collection->_check_able_to_attach($child);

    my $rel = $collection->rel;
    my $sql = $rel->__make_insert_sql();

    my $driver = $collection->linking_object->driver();
    my @binds = $rel->__make_join_binds($collection->linking_object(), $child);
    $sql->set_bind_values(@binds);
    $driver->run_sql($sql);

    $collection->__attach_bidirectional_in_memory($child);
}

sub __attach_in_memory {
    my ($coll, $child) = @_;
    if ($coll->is_populated()) {
        push @{$coll->{_children}}, $child;
        $coll->{_count}++;
    }

}

sub __attach_bidirectional_in_memory {
    my ($local_coll, $child) = @_;
    $local_coll->__attach_in_memory($child);

    my $inv_rel = $local_coll->rel->inverse_relationship();
    if ($inv_rel) {
        my $inv_method = $inv_rel->method_name();
        my $inv_coll = $child->$inv_method;
        $inv_coll->__attach_in_memory($local_coll->linking_object());
    }
}


=head2 $collection->remove($child);

Removes the child from the collection in memory, and removes 
the parent from the inverse collection if available.  No database activity occurs.

The child is then placed in a removal queue in the collection.  Call 
commit_pending_removals() to delete the associations from the join table.

To delete from memory and DB at once, use delete().  To delete using a SQL query, use delete_where().

=cut

sub remove {
    my ($collection, $child) = @_;
    $collection->_check_able_to_remove($child);
    push @{$collection->{_remove_queue}}, $child;
    $collection->__remove_bidirectional_in_memory($child);
}


=head2 $collection->commit_pending_removals();

Deletes any pending rows into the join table.  Call this once after calling remove() repeatedly.  It is hoped that database drivers will be able to optimize this into one DELETE.

=cut

sub commit_pending_removals {
    my $collection = shift;
    my @pending = @{$collection->{_remove_queue}};
    unless (@pending) { return; }

    # TODO OPTIMIZE With most DB drivers should be possible to send a VALUES table, allowing this to be done in one DELETE
    my $rel = $collection->rel;
    my $sql = $rel->__make_delete_sql();

    my $driver = $collection->linking_object->driver();
    foreach my $child (@pending) {
        my @binds = $rel->__make_join_binds($collection->linking_object(), $child);
        $sql->set_bind_values(@binds);
        $driver->run_sql($sql);      # TODO OPTIMIZE add prepare-execute
    }

    $collection->{_delete_queue} = [];
    return 1;
}




=head2 $collection->delete($child);

Deletes all rows, if any, in the join table linking the parent object and the child object.

Unlike HasMany, HasManyMany does not remove the child from any other collections.

Calling delete() directly does not affect the removal queue - in other words, if you
call remove($child1) then delete($child2), $child1 will still not be deleted in the database.  Neither
the parent nor the child is becomes dirty during this operation.

Note that if you are removing many children, it is more efficient to call remove()
repeatedly, then call commit_pending_attachments();  alternatively, use SQL and call delete_where (note that delete_where() depopulates the collection, whereas commit_pending_removals() does not.

=cut

sub delete {
    my ($collection, $child) = @_;
    $collection->_check_able_to_remove($child);

    my $rel = $collection->rel;
    my $sql = $rel->__make_delete_sql();

    my $driver = $collection->linking_object->driver();
    my @binds = $rel->__make_join_binds($collection->linking_object(), $child);
    $sql->set_bind_values(@binds);
    $driver->run_sql($sql);

    $collection->__remove_bidirectional_in_memory($child);
}

sub _check_able_to_remove {
    my ($collection, $child) = @_;
    $collection->_check_correct_child_class($child);
    unless ($child->is_inserted()) {
        Class::ReluctantORM::Exception::Data::UnsupportedCascade->croak("Related object must be already inserted in the DB to be attached to a HasManyMany relationship");
    }
    unless ($collection->linking_object->is_inserted()) {
        Class::ReluctantORM::Exception::Data::UnsupportedCascade->croak("Parent object must be already inserted in the DB to be attached to a HasManyMany relationship");
    }
    unless ($collection->is_populated()) {
        Class::ReluctantORM::Exception::Data::FetchRequired->croak(called => 'delete', call_instead => 'fetch_all or delete_where', fetch_locations => [ $collection->linking_object->all_origin_traces ]);
    }
}

sub __remove_in_memory {
    my ($coll, $child) = @_;
    if ($coll->is_populated()) {
        $coll->{_children} = [ grep { $_->id ne $child->id } @{$coll->{_children}} ];
        $coll->{_count} = @{$coll->{_children}};
    }
}

sub __remove_bidirectional_in_memory {
    my ($local_coll, $child) = @_;
    $local_coll->__remove_in_memory($child);

    my $inv_rel = $local_coll->rel->inverse_relationship();
    if ($inv_rel) {
        my $inv_method = $inv_rel->method_name();
        my $inv_coll = $child->$inv_method;
        $inv_coll->__remove_in_memory($local_coll->linking_object());
    }
}

=head2 $collection->delete_where(where => $str, execargs => \@args);

=head2 $collection->delete_where(where => $where_obj);

Executes a DELETE against the join table using the provided WHERE clause.  A set of criteria is added to the WHERE clause
ensuring that only records associated with the parent object are deleted.

The where argusment may be either a SQL string or a SQL::Where object.

=cut

sub delete_where {
    my $collection = shift;
    if (@_ == 1) { @_ = (where => $_[0]);  }
    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;
    unless (defined $args{where}) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'where'); }

    my $remote_where;
    if (Scalar::Util::blessed($args{where}) && $args{where}->isa(Where())) {
        $remote_where = $args{where};
    } else {
        my $driver = $collection->rel->linked_class->driver();
        $remote_where = $driver->parse_where($args{where});
        $remote_where->bind_params(@{$args{execargs}});
    }

    # Strategy: Delete from join table with a 2-part where clause:
    #   1.  check for remote key in a squbquery using the provided where clause
    #   2.  Criteria to restrict delete to record associated with the parent
    my $rel = $collection->rel();
    my $subselect_statement = SQL->new('SELECT');
    $subselect_statement->from(From->new($rel->remote_sql_table));
    $subselect_statement->where($remote_where);
    $subselect_statement->add_output
      (FunctionCall->new('KEY_COMPOSITOR_INSIDE_SUBQUERY',
                         $rel->remote_key_sql_columns()));
    my $subquery = SubQuery->new($subselect_statement);
    my $join_key_check = FunctionCall->new(
                                           'KEY_COMPOSITOR_OUTSIDE_SUBQUERY',
                                           $rel->join_remote_key_sql_columns(),
                                          );
    my $subquery_crit = Criterion->new('IN',$join_key_check, $subquery);


    my $link_crit = $collection->__make_link_crit(0);
    my $where = Where->new(
                           Criterion->new('AND',
                                          $subquery_crit,
                                          $link_crit,
                                         )
                          );

    my $sql = SQL->new('DELETE');
    $sql->table($collection->rel->join_sql_table());
    $sql->where($where);

    $collection->linking_object->driver->run_sql($sql);
    $collection->depopulate();

    return;
}

=head2 $collection->delete_all();

Deletes all associations from the collection.

=cut

sub delete_all {
    my $coll = shift;
    $coll->delete_where(where => Where->new());
}


1;
