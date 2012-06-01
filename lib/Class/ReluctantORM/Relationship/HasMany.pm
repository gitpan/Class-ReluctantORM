package Class::ReluctantORM::Relationship::HasMany;
use strict;
use warnings;


=head1 NAME

Class::ReluctantORM::Relationship::HasMany

=head1 SYNOPSIS

  # Add relationships to a ReluctantORM Class
  Ship->has_many('Pirate');
  Ship->has_many(
                 class       => 'Pirate'
                 local_key   => 'ship_id', # New in 0.4: multi-column keys allowed via
                 remote_key  => 'ship_id', # arrayrefs here!
                 method_name => 'pirates',
                );

  # Now you have:
  $pirates_collection  = $ship->pirates();

  # New in 0.4: in array context, implicitly do $pirates_collection->all_items
  @mateys = $ship->pirates();

  # Fetchers defined automatically
  $ship      = Ship->fetch_with_pirates($ship_id);
  @unarmed   = Ship->fetch_by_gun_count_with_pirates(0);

  # Get info about the relationship
  $rel = Ship->relationships('pirates');

  $str = $rel->type();                 # 'has_many';
  $str = $rel->linked_class();         # 'Pirate';
  $str = $rel->linking_class();        # 'Ship';
  @fields = $rel->local_key_fields();  # fields in Ship that link to Pirate
  @fields = $rel->remote_key_fields(); # fields in Pirate that link to Ship
  $int = $rel->join_depth();           # 1

  # Class::ReluctantORM::SQL integration
  @sql_cols = $rel->additional_output_sql_columns();
  @cols     = $rel->local_key_sql_columns();
  @cols     = $rel->remote_key_sql_columns();
  @empty    = $rel->join_local_key_sql_columns();  # always empty for HasMany
  @empty    = $rel->join_remote_key_sql_columns(); # always empty for HasMany


=head1 DESCRIPTION

=head1 CREATING A RELATIONSHIP

=head2 $tb_class->has_many('OtherClass');

=head2 $tb_class->has_many(class => 'OtherClass', local_key => 'key_column', remote_key => 'key_column', method_name => 'other_class');

=head2 $tb_class->has_many(... join_table => 'table_name' ...);

join_table => 'table_name', join_table_schema => 'schema_name', 

Initiates a one-to-many relationship between two classes/tables.
Results are handled with assistance of a simple container class, 
Class::ReluctantORM::Collection.

An accessor will be created named other_classes (or method_name).  Note that this 
should be plural for readability.  The accessor will return a Collection object.

Additionally, a new constructor is created, named $class->fetch_with_METHOD.
This constructor has the special feature that it performs an outer join and
prepopulates the Collection.  Thus, Ship->fetch_with_pirates(23) is only
one DB query.

Finally, additional constructors named $class->fetch_by_ATTRIBUTE_with_METHOD 
will also be available via AUTOLOAD.

Obtaining the Collection object does NOT result in trips to the database.  Operations 
on the Collection object DO require trips to the database.

Note that a one-to-many relationship does not imply a reciprocal has_one relationship going the other way.  
It's OK to set that up manually, though.

In the first form, a relationship is setup to OtherClass using defaults, described below.

In the second form, options are made explicit:

=over

=item class (required)

The linked class.  This is the class on the remote end of the one-to-many.
That means it will have foreign key(s) to the local (linking) class.

=item method_name

The name of the method that will be used to access the relationship.  This is also the name for the relationship, which you can pass to $tb_class->relationships.  Default is the lower-cased and pluralized OtherClass.  For example, if you say Ship->has_many('Pirate'), you'll get $ship->pirates().  Pluralization is performed using Lingua.

=item local_key (optional string or arrayref)

Name or names of columns on the local table acting as keys in the relationship.
Defaults to $tb_class->primary_key_columns().

=item remote_key (optional string or arrayref)

Name or names of columns on the remote table acting as keys in the relationship.
Defaults to looking for columns in OtherClass with the names $tb_class->primary_key_columns().

=item foreign_key

Deprecated synonym for remote_key.

=back

In the third form, all arguments will be passed to Class::ReluctantORM::Relationshipo::HasManyMany.  This form is somewhat discouraged, but remains because some find it more readable.

=cut


use Data::Dumper;
use Scalar::Util qw(blessed);
use Class::ReluctantORM::Utilities qw(install_method conditional_load pluralize array_shallow_eq check_args);
use Class::ReluctantORM::Collection;

our $DEBUG = 0;

use base 'Class::ReluctantORM::Relationship';

sub _initialize {
    my $class = shift;
    install_method('Class::ReluctantORM::Relationship', 'is_has_many', sub { return 0; });
    install_method('Class::ReluctantORM', 'has_many', \&__setup_has_many);
    install_method('Class::ReluctantORM', 'is_field_has_many', \&is_field_has_many);
}

=head2 $str = $rel->type();

Returns 'has_many'.

=cut

sub type { return 'has_many'; }

=head2 $int = $rel->join_depth();

Returns 1.

=cut

sub join_depth { return 1; }

=head2 $str = $rel->join_type();

Returns 'LEFT OUTER'

=cut

sub join_type { return 'LEFT OUTER'; }

=head2 $int = $rel->lower_multiplicity()

Returns 0.

=cut

sub lower_multiplicity { return 0; }

=head2 $int = $rel->upper_multiplicity()

Returns undef.

=cut

sub upper_multiplicity { return undef; }

=head2 $bool = $rel->is_has_many();

Returns true.

=cut

sub is_has_many { return 1; }

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


=begin devdocs

Not sure this is public.... or if that calling pattern is right.

=head2 $bool = $cro_obj->is_field_has_many('field');

Returns true if the given field is a HasOne field.

=cut

sub is_field_has_many {
    my $inv = shift;
    my $field = shift;
    my $tb_class = ref($inv) ? ref($inv) : $inv;
    my $rel = $tb_class->relationships($field);
    return $rel ? $rel->is_has_many() : undef;
}

sub _notify_key_change_on_linking_object {
    my $rel = shift;
    my $parent = shift;
    my $method = $rel->method_name();
    my $collection = $parent->$method();
    if ($collection->is_populated) {
        # Note that $collection already knows $parent via linking_object();
        $collection->__hm_set_keys_on_children_from_parent();
    }
}

# Do nothing
sub _handle_implicit_create { }

# Called from ReluctantORM::new()
sub _handle_implicit_new {
    my $rel = shift;
    my $linking_object = shift;
    my $new_args = shift;

    my $children = $new_args->{$rel->method_name} || undef; # Default to unpopulated

    my $all_exist = 1;
    for my $c (@{$children || []}) { $all_exist &&= $c->is_inserted; }

    unless ($all_exist) {
        Class::ReluctantORM::Exception::Data::UnsupportedCascade->croak('Cascading inserts not supported');
    }

    my $inverse_rel = $rel->inverse_relationship();
    if ($inverse_rel) {
        my $method = $inverse_rel->method_name();
        for my $c (@{$children || []}) {
            # Set parent reference in each child, if backreferences are requested
            if (Class::ReluctantORM->get_global_option('populate_inverse_relationships')) {
                $c->$method($linking_object);
            }
        }
    }

    my $collection = Class::ReluctantORM::Collection::OneToMany->_new(
                                                                      relationship => $rel,
                                                                      linking_object => $linking_object,
                                                                      children => $children,
                                                                     );
    $linking_object->set($rel->method_name, $collection);
    delete $new_args->{$rel->method_name};

    return;
}

sub __setup_has_many {
    my $cro_base_class = shift;
    my $has_many_class = __PACKAGE__;
    my %args = ();

    if (@_ == 1) {
        %args = (class => shift());
    } else {
        if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
        %args = @_;
    }

    %args = check_args(
                       args => \%args,
                       optional => [qw(remote_key local_key method_name)],
                       required => [qw(class)],
                      );


    # Determine method name
    $args{method_name} ||= pluralize(Class::ReluctantORM::Utilities::camel_case_to_underscore_case((split('::', $args{class}))[-1]));

    # Coerce local and foreign keys to be arrayrefs
    $args{remote_key} ||= $cro_base_class->primary_key_columns();
    $args{remote_key} = ref($args{remote_key}) eq 'ARRAY' ? $args{remote_key} : [ $args{remote_key} ];
    $args{local_key}  ||= $cro_base_class->primary_key_columns();
    $args{local_key}  = ref($args{local_key}) eq 'ARRAY' ? $args{local_key} : [ $args{local_key} ];

    conditional_load($args{class});

    $has_many_class->delay_until_class_is_available
      ($args{class}, $has_many_class->__relationship_installer(%args, cro_base_class => $cro_base_class));
    $has_many_class->delay_until_class_is_available
      ($args{class}, $has_many_class->__inverse_relationship_finder(%args, cro_base_class => $cro_base_class));

}

sub __relationship_installer {
    my $has_many_class = shift;
    my %args = @_;
    return sub {
        if ($DEBUG > 1) {
            print STDERR __PACKAGE__ . ':' . __LINE__ . " - in HasMany setup callback\n";
        }
        my $rel = Class::ReluctantORM::Relationship::HasMany->new();
        $rel->method_name($args{method_name});
        $rel->linked_class($args{class});
        $rel->linking_class($args{cro_base_class});
        $rel->local_key_fields($args{cro_base_class}->field_name(@{$args{local_key}}));
        $rel->remote_key_fields($args{class}->field_name(@{$args{remote_key}}));

        install_method($args{cro_base_class}, $rel->method_name, $rel->__make_has_many_accessor());
        install_method($args{cro_base_class}, 'fetch_' . $rel->method_name, $rel->__make_has_many_fetch_accessor());
        $rel->_install_search_by_with_methods();

        my @args_copy = map { ($_, $args{$_} ) } grep { $_ ne 'cro_base_class' } keys %args;
        $rel->_original_args_arrayref(\@args_copy);

        $args{cro_base_class}->register_relationship($rel);
    };
}

sub __inverse_relationship_finder {
    my $has_many_class = shift;
    my %args = @_;
    return sub {
        my $cro_local_class = $args{cro_base_class};
        my $cro_remote_class = $args{class};
        my $local_relname = $args{method_name};
        my $local_rel = $cro_local_class->relationships($local_relname);
        unless ($local_rel && $local_rel->is_has_many) { return; }
        if ($local_rel->inverse_relationship()) {
            # Assume we already found it
            return;
        }

        # List the has_one relationships on the linked class
        # that point to this class
        my @remote_has_one_rels =
          grep { $_->linked_class eq $cro_local_class }
            grep { $_->is_has_one } $cro_remote_class->relationships();
        unless (@remote_has_one_rels) { return; }

        my @matches = ();
        foreach my $remote_rel (@remote_has_one_rels) {

            # These are lists of keys that should be on the local table, 
            # and should be identical
            my @remote_keys1 = $remote_rel->remote_key_fields();
            my @local_keys1 = $local_rel->local_key_fields();
            next unless (array_shallow_eq(\@remote_keys1, \@local_keys1));

            # These are lists of keys that should be on the remote table,
            # and should be identical
            my @remote_keys2 = $remote_rel->local_key_fields();
            my @local_keys2 = $local_rel->remote_key_fields();
            next unless (array_shallow_eq(\@remote_keys2, \@local_keys2));

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



sub __make_has_many_accessor {
    my $rel = shift;

    # Setup accessor
    my $code = sub {
        my $tb_obj = shift;
        my $collection = $tb_obj->get($rel->method_name);
        unless (defined $collection) {
            $collection = Class::ReluctantORM::Collection::OneToMany->_new(
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

sub __make_has_many_fetch_accessor {
    my $rel = shift;

    # Setup accessor
    my $code = sub {
        my $tb_obj = shift;
        my $collection = $tb_obj->get($rel->method_name);
        unless (defined $collection) {
            $collection = Class::ReluctantORM::Collection::OneToMany->_new(
                                                                     relationship => $rel,
                                                                     linking_object => $tb_obj
                                                                    );
            $tb_obj->set($rel->method_name, $collection);
        }
        $collection->depopulate();
        $collection->fetch_all();

        # New feature
        return wantarray ? $collection->all() : $collection;
    };
    return $code;
}


#=============================================================================#
#=============================================================================#
#                               Collection Subclass
#=============================================================================#
#=============================================================================#

package Class::ReluctantORM::Collection::OneToMany;
use strict;
use warnings;

use Scalar::Util qw(blessed);
use Data::Dumper;
use Class::ReluctantORM::SQL::Aliases;
use Class::ReluctantORM::Utilities qw(nz check_args);
use base 'Class::ReluctantORM::Collection';
use Scalar::Util qw(weaken);
our $DEBUG = 0;

my %COLLECTION_REGISTRY_BY_RELATION;

sub rel { return shift->{relationship}; }

sub _new {
    my ($class, %args) = @_;
    foreach my $f (qw(master_class master_key_name master_key_value child_key_name child_class) ) {
        if (exists $args{$f}) { Class::ReluctantORM::Exception::Call::Deprecated->croak("May not use param $f for Collection::OneToMany::_new in 0.4 code"); }
    }
    foreach my $f (qw(relationship linking_object)) {
        unless (exists $args{$f}) { Class::ReluctantORM::Exception::Param::Missing->croak(param => $f); }
    }

    my $self = bless \%args, $class;
    weaken($self->{linking_object});

    if ($args{children}) {
        $self->{_children} = $args{children};
        $self->{_populated} = 1;
        $self->{_count} = scalar @{$args{children}};
    } else {
        $self->{_populated} = 0;
        $self->{_count} = undef;
        $self->{_children} = [];
    }

    # Add to collection registry so that we can find other collections
    # when we need to do a global remove
    $COLLECTION_REGISTRY_BY_RELATION{$args{relationship}} ||= [];
    push @{$COLLECTION_REGISTRY_BY_RELATION{$args{relationship}}}, $self;
    weaken($COLLECTION_REGISTRY_BY_RELATION{$args{relationship}}->[-1]);

    return $self;
}

sub __list_collections_on_relation {
    my $collection = shift;
    my $rel = $collection->rel();
    my @colls = @{$COLLECTION_REGISTRY_BY_RELATION{$rel}}; # Hash lookup by memory address
    return grep { defined($_) } @colls;  # may not be defined because it was weakened
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

sub _check_correct_child_class {
    my ($self, $object) = @_;
    unless (blessed($object) && $object->isa($self->rel->linked_class)) {
        Class::ReluctantORM::Exception::Data::WrongType->croak(param => 'object', expected => $self->rel->linked_class, frames => 2);
    }
}

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

sub fetch_count {
    my $self = shift;

    my $field = $self->rel->linked_class->first_primary_key_field();
    my $method = 'count_of_' . $field;

    # Rely on aggregate mechanism
    $self->{_count} = $self->$method;
    return $self->{_count};
}

# Provides where and execargs options for a fetchdeep on the linked class
#
sub __make_link_where {
    my $collection = shift;
    my $configure_for_join = shift || 0;
    my $rel = $collection->rel;

    my @where  = ();
    my @execargs = ();

    my @remote_key_cols = $rel->remote_key_columns();
    my @local_key_cols = $rel->local_key_columns();

    foreach my $index (0..$#local_key_cols) {
        my $remote_column_name = $remote_key_cols[$index];
        my $local_field = $rel->linked_class->field_name($local_key_cols[$index]);

        my $crit;
        if ($configure_for_join) {
            $crit = 'MACRO__child__' . $rel->method_name() . '__.' . $remote_column_name . ' = ?';
        } else {
            $crit = $remote_column_name . ' = ?';
        }

        push @where, $crit;
        push @execargs, $collection->linking_object->raw_field_value($local_field);
    }
    return (where => (join ' AND ', @where), execargs => \@execargs);
}

sub __make_link_crit {
    my $collection = shift;
    my $configure_for_join = shift;
    my %where_args = $collection->__make_link_where($configure_for_join);
    my $driver = $collection->rel->linked_class->driver();
    my $where = $driver->parse_where($where_args{where});
    $where->bind_params(@{$where_args{execargs}});
    return $where->root_criterion();
}

sub __hm_set_keys_on_children_from_parent {
    my $collection = shift;
    my $child_ref = shift;  # May pass arrayref here to only work on a few childrens
    my @children = $child_ref ? @$child_ref : $collection->all();

    my $parent = $collection->linking_object();
    my $rel = $collection->rel();
    my %parent_key2child_key;
    my @parent_keys = $rel->local_key_fields();
    my @child_keys = $rel->remote_key_fields();
    @parent_key2child_key{@parent_keys} = @child_keys;

    foreach my $child (@children) {
        foreach my $parent_key_field (@parent_keys) {
            my $child_key_field = $parent_key2child_key{$parent_key_field};
            $child->raw_field_value($child_key_field, $parent->raw_field_value($parent_key_field));
        }
    }
}

sub __hm_clear_keys_on_child {
    my $collection = shift;
    my $child = shift;

    my $rel = $collection->rel();
    my %parent_key2child_key;
    my @parent_keys = $rel->local_key_fields();
    my @child_keys = $rel->remote_key_fields();
    @parent_key2child_key{@parent_keys} = @child_keys;

    foreach my $parent_key_field (@parent_keys) {
        my $child_key_field = $parent_key2child_key{$parent_key_field};
        $child->raw_field_value($child_key_field, undef);
    }
}


sub fetch_all {
    my $self = shift;

    my %where_args = $self->__make_link_where(0);
    my $child_class = $self->rel->linked_class();
    my @children = $child_class->search(%where_args);

    $self->linking_object->capture_origin();

    $self->{_children} = \@children;
    $self->{_populated} = 1;
    $self->{_count} = scalar @children;
    return @children;
}

sub fetch_deep {
    my $self = shift;
    my %args = check_args
      (
       args => \@_,
       required => [ qw(with) ], # As of CRO 0.5, no where, limit, or ordering permitted
      );

    my %where_args = $self->__make_link_where(0);
    my $child_class = $self->rel->linked_class();
    my @children = $child_class->search_deep(%where_args, with => $args{with});

    $self->linking_object->capture_origin();

    $self->{_children} = \@children;
    $self->{_populated} = 1;
    $self->{_count} = scalar @children;
    return @children;

}

# Note: AUTOLOAD defined in Collection base class
sub __setup_aggregate_autoload {
    my ($self1, $AUTOLOAD, $method, $args, $agg_type, $agg_field) = @_;

    my $linked_class = $self1->rel->linked_class;

    # Generate a coderef
    my $code = sub {
        my $self = shift;
        my %args = @_;
        my %where_args = $self->__make_link_where(0);

        # Append args
        $where_args{where} .= ' AND ' . ($args{where} || '1=1');
        push @{$where_args{execargs}}, @{$args{execargs} || []};

        # Use aggregate method defined by child class
        return $linked_class->$method(%where_args);
    };

    # Don't install coderef in symbol table
    # The name of this will vary based on the classes linked
    $code->($self1, @$args);
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


=head2 $collection->attach($child);

Attach the child object to the parent in memory, and remove it from
any other collections on the same relationship.

For HasMany collections, this sets the keys in the child.  If the
collection is populated, adds the child to the in-memory collection
and increments the count.

The child is now dirty.  No database activity occurs.  To attach
and immediately commit the change, use $collection->add().

=cut

sub attach {
    my ($collection, $child) = @_;
    $collection->_check_correct_child_class($child);

    $collection->__remove_from_from_all_related_collections($child);

    # Set keys in child object
    $collection->__hm_set_keys_on_children_from_parent([$child]);

    # If populated, adjust the collection
    if ($collection->is_populated()) {
        push @{$collection->{_children}}, $child;
        $collection->{_count}++;
    }
}

=head2 $collection->add($child);

Removes the child object from all other collections based on this relationship, then 
attaches the child object to the collection in memory, and finally saves the child 
object to the database with its new keys.

The child is briefly dirty during this operation, but ends up non-dirty.

=cut

sub add {
    my ($collection, $child) = @_;
    $collection->attach( $child );
    $child->save();
}


=head2 $collection->remove($child);

If the collection is populated, remove the child from the in-memory collection.

Regardless of whether the collection is populated, clear the foreign keys on the child.

The child is marked dirty.  No database activity occurs.

=cut

sub remove {
    my ($collection, $child) = @_;

    if ($collection->is_populated()) {
        $collection->{_children} =
          [ grep { nz($_->id,0) ne nz($child->id,0) } @{$collection->{_children}} ];
        $collection->{_count} = @{$collection->{_children}};
    }

    $collection->__hm_clear_keys_on_child($child);

    return $child;
}

sub __remove_from_from_all_related_collections {
    my $collection = shift;
    my $child = shift;
    my @sisters = $collection->__list_collections_on_relation();
    foreach my $coll (@sisters) {
        $coll->remove($child);
    }
}

=head2 $collection->delete($child);

Removes the child object from the collection in memory, and deletes
the child object from the database.

=cut

sub delete {
    my ($collection, $child) = @_;

    # Not sure this is needed....
    unless ($collection->is_populated) {
        Class::ReluctantORM::Exception::Data::FetchRequired->croak(called => 'delete', call_instead => 'fetch_all or delete_where', fetch_locations => [ $collection->linking_object->all_origin_traces ]);
    }

    $collection->_check_correct_child_class($child);

    unless ($collection->is_present($child)) { return; }

    # Remove collection - should this remove from all collections?
    $collection->remove($child);

    # Delete the child
    $child->delete();

    return;
}

=head2 $collection->delete_where(where => $str, execargs => \@args);

=head2 $collection->delete_where(where => $where_obj);

Executes a DELETE against the child table using the provided WHERE clause.  A set of criteria is added to ensure that only records associated with the parent record.

The where argusment may be either a SQL string or a SQL::Where object.

=cut

sub delete_where {
    my $collection = shift;
    if (@_ == 1) { @_ = (where => $_[0]);  }
    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;
    unless (defined $args{where}) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'where'); }

    my $where;
    if (blessed($args{where}) && $args{where}->isa(Where())) {
        $where = $args{where};
    } else {
        my $driver = $collection->rel->linked_class->driver();
        $where = $driver->parse_where($args{where});
        $where->bind_params(@{$args{execargs}});
    }

    my $link_crit = $collection->__make_link_crit(0);
    $where = Where->new(
                        Criterion->new('AND',
                                       $where->root_criterion(),
                                       $link_crit),
                       );

    my $sql = SQL->new('DELETE');
    $sql->table($collection->rel->remote_sql_table());
    $sql->where($where);

    $collection->linking_object->driver->run_sql($sql);
    $collection->depopulate();

    return;
}


1;





