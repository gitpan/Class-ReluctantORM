package Class::ReluctantORM::Relationship::HasOne;

=head1 NAME

Class::ReluctantORM::Relationship::HasOne

=head1 SYNOPSIS

  # Add relationships to a ReluctantORM Class
  Pirate->has_one('Ship');

  # Now you have:
  $pirate = Pirate->fetch_with_ship($pirate_id);
  @bipeds = Pirate->fetch_by_leg_count_with_ship(2);

  # Get info about the relationship
  $rel = Pirate->relationships('ship');

  $str = $rel->type();                 # 'has_one';
  $str = $rel->linked_class();         # 'Ship';
  $str = $rel->linking_class();        # 'Pirate';
  @fields = $rel->local_key_fields();  # fields in Pirate that link to Ship
  @fields = $rel->remote_key_fields(); # array of fields in Ship that link to Pirate
  $int = $rel->join_depth();           # 1

  # Class::ReluctantORM::SQL integration
  @sql_cols = $rel->additional_output_sql_columns();
  @cols = $rel->local_key_sql_columns();
  @cols = $rel->remote_key_sql_columns();
  @empty = $rel->join_local_key_sql_columns();  # always empty for HasOne
  @empty = $rel->join_remote_key_sql_columns(); # always empty for HasOne


=head1 DESCRIPTION

=head2 $class->has_one('OtherClass');

=head2 $class->has_one(class => 'OtherClass', local_key => [colname,...], remote_key => [colname, ...], => 'key_column', method_name => 'some_name', read_only => 1);

Describes a (possibly optional) relationship between two classes/tables.

The local table should have a column (or columns) that act as foreign keys
into the remote table.  An accessor/mutator wil be created that provides
access to the related object.  

Additionally, a new constructor is created, named $class->fetch_with_METHOD.  
This constructor has the special feature that it performs an outer join and 
pre-fetches the named object.  Finally, additional constructors named 
$class->fetch_by_ATTRIBUTE_with_METHOD will also be available via AUTOLOAD.

In the first form, OtherClass is taken to be the 'class' argument, and all
other arguments are determined from that.

Arguments:

=over

=item class (string classname, required)

The name of the remote ReluctantORM class.

=item local_key (string or arrayref)

The name of the foreign key column (or columns) in the local table.  Optional
- default is OtherClass->primary_key_columns().

=item remote_key (string or arrayref)

The name of the foreign key column (or columns) in the remote table.  Optional
- default is OtherClass->primary_key_columns().

=item method_name (string)

The name of the accessor/mutator method to be created.  Optional - default is 
the lowercased and underscore-spaced version of the class name of OtherClass.

=item foreign_key (string, deprecated)

Deprecated synonym for local_key.

=back


The mutator will set the corresponding local key column.

The accessor will display some behavior intended to help with scalability.
If the value has already been fetched, it will be returned normally.  If a 
trip to the database would be required, the method dies with an
Class::ReluctantORM::Exception::Data::FetchRequired.  You can then actually 
fetch the value using $instance->fetch_METHOD .

=cut

use strict;
use warnings;

use Data::Dumper;
use Scalar::Util qw(blessed);
use Class::ReluctantORM::Utilities qw(install_method conditional_load array_shallow_eq);
use Class::ReluctantORM::Exception;
our $DEBUG ||= 0;

use base 'Class::ReluctantORM::Relationship';

sub _initialize {
    my $class = shift;
    install_method('Class::ReluctantORM::Relationship', 'is_has_one', sub { return 0; });
    install_method('Class::ReluctantORM', 'has_one', \&__setup_has_one);
    install_method('Class::ReluctantORM', 'is_field_has_one', \&is_field_has_one);
}

=head2 $str = $rel->type();

Returns 'has_one'.

=cut

sub type { return 'has_one'; }

=head2 $bool = $rel->is_has_one();

Returns true.

=cut

sub is_has_one { return 1; }

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

Returns 1.

=cut

sub upper_multiplicity { return 1; }


=begin devdocs

Not sure this is public.... or if that calling pattern is right.

=head2 $bool = $cro_obj->is_field_has_one('field');

Returns true if the given field is a HasOne field.

=cut

sub is_field_has_one {
    my $inv = shift;
    my $field = shift;
    my $tb_class = ref($inv) ? ref($inv) : $inv; # wtf
    my $rel = $tb_class->relationships($field);
    return $rel ? $rel->is_has_one() : undef;
}

=head2 @cols = $h1->additional_output_sql_columns();

Returns the essential columns of the linked table.

=cut

sub additional_output_sql_columns {
    my $rel = shift;
    return $rel->linked_class->essential_sql_columns();
}

=begin devnotes

In order to use a Collection, while appearing not to, 
we will actually use a secondary field to store the 
collection.


=cut

sub __setup_has_one {
    my $cro_base_class = shift;
    my $has_one_class = __PACKAGE__;
    my %raw_args = ();

    if (@_ == 1) {
        %raw_args = (class => shift());
    } else {
        if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
        %raw_args = @_;
    }

    # Validate Args
    my %args;

    unless ($raw_args{class}) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'class'); }
    $args{class} = $raw_args{class};
    delete $raw_args{class};

    $args{method_name} = $raw_args{method_name};
    delete $raw_args{method_name};
    $args{method_name} ||= Class::ReluctantORM::Utilities::camel_case_to_underscore_case((split('::', $args{class}))[-1]);


    $args{local_key} = $raw_args{local_key};
    delete $raw_args{local_key};
    $args{local_key} ||= $args{class}->primary_key_columns();
    $args{local_key} = ref($args{local_key}) eq 'ARRAY' ? $args{local_key} : [ $args{local_key} ];
    foreach my $key (@{$args{local_key}}) {
        unless ($cro_base_class->field_name($key)) {
            Class::ReluctantORM::Exception::Param::BadValue->croak
                (
                 param => 'local_key',
                 value => $key,
                 error => "Local key '$key' does not appear to be a column on " . $cro_base_class->table_name,
                );
        }
    }

    $args{remote_key} = $raw_args{remote_key};
    delete $raw_args{remote_key};
    $args{remote_key} ||= $args{class}->primary_key_columns();
    $args{remote_key} = ref($args{remote_key}) eq 'ARRAY' ? $args{remote_key} : [ $args{remote_key} ];
    foreach my $key (@{$args{remote_key}}) {
        unless ($args{class}->field_name($key)) {
            Class::ReluctantORM::Exception::Param::BadValue->croak
                (
                 param => 'remote_key',
                 value => $key,
                 error => "Remote key '$key' does not appear to be a column on " . $args{class}->table_name,
                );
        }
    }

    # Should have no more args at this point
    if (keys %raw_args) {
        Class::ReluctantORM::Exception::Param::Spurious->croak
            (
             param => join(',', keys %raw_args),
             error => "Extra args to 'has_one'",
            );
    }

    # Load class
    conditional_load($args{class});

    $has_one_class->delay_until_class_is_available
      ($args{class}, $has_one_class->__relationship_installer(%args, cro_base_class => $cro_base_class));
    $has_one_class->delay_until_class_is_available
      ($args{class}, $has_one_class->__inverse_relationship_finder(%args, cro_base_class => $cro_base_class));
}

sub __relationship_installer {
    my $has_one_class = shift;
    my %args = @_;
    return sub {
        if ($DEBUG > 1) {
            print STDERR __PACKAGE__ . ':' . __LINE__ . " - in HasOne setup callback\n";
        }
        my $rel = Class::ReluctantORM::Relationship::HasOne->new();
        $rel->method_name($args{method_name});
        $rel->linked_class($args{class});
        $rel->linking_class($args{cro_base_class});
        $rel->local_key_fields($args{cro_base_class}->field_name(@{$args{local_key}}));
        $rel->remote_key_fields($args{class}->field_name(@{$args{remote_key}}));

        install_method($args{cro_base_class}, $rel->method_name, $rel->__make_has_one_accessor());
        install_method($args{cro_base_class}, 'fetch_' . $rel->method_name, $rel->__make_has_one_fetch_accessor());

        $rel->_install_search_by_with_methods();

        my @args_copy = map { ($_, $args{$_} ) } grep { $_ ne 'cro_base_class' } keys %args;
        $rel->_original_args_arrayref(\@args_copy);

        $args{cro_base_class}->register_relationship($rel);
    };
}

sub __inverse_relationship_finder {
    my $has_one_class = shift;
    my %args = @_;
    return sub {
        my $cro_local_class = $args{cro_base_class};
        my $cro_remote_class = $args{class};
        my $local_relname = $args{method_name};
        my $local_rel = $cro_local_class->relationships($local_relname);
        unless ($local_rel && $local_rel->is_has_one) { return; }
        if ($local_rel->inverse_relationship()) {
            # Assume we already found it
            return;
        }

        # List the has_many relationships on the linked class
        # that point to this class
        my @remote_has_many_rels =
          grep { $_->linked_class eq $cro_local_class }
            grep { $_->is_has_many } $cro_remote_class->relationships();
        unless (@remote_has_many_rels) { return; }

        my @matches = ();
        foreach my $remote_rel (@remote_has_many_rels) {

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

=head2 $bool = $rel->is_populated_in_object($cro_obj);

Returns true if the CRO object has had this relationship fetched.

=cut

sub is_populated_in_object {
    my $rel = shift;
    my $cro_obj = shift;

    # Obtain the underlying collection
    my $collection_slot = '_' . $rel->method_name . '_coll';
    my $collection = $cro_obj->get($collection_slot);
    unless ($collection) {
        return 0;
    }

    return $collection->is_populated();
}

sub __make_has_one_accessor {
    my $rel = shift;

    # Setup accessor
    my $code = sub {
        my $cro_obj = shift;
        my $obj_field = $rel->method_name();

        # Fetch the underlying collection
        my $collection_slot = '_' . $rel->method_name . '_coll';
        my $collection = $cro_obj->get($collection_slot);
        unless (defined $collection) {
            $collection = Class::ReluctantORM::Collection::One->_new(
                                                               relationship => $rel,
                                                               linking_object => $cro_obj
                                                              );
            $cro_obj->set($collection_slot, $collection);
        }

        if (@_) {

            # Acting as mutator
            my $raw_linked_object = shift;
            my @local_keys = $rel->local_key_fields;

            if (defined $raw_linked_object) {

                unless (blessed($raw_linked_object) && $raw_linked_object->isa($rel->linked_class)) {
                    Class::ReluctantORM::Exception::Param::WrongType->croak(
                                                               param => 'value',
                                                               expected => $rel->linked_class,
                                                               value => $raw_linked_object
                                                              );
                }

                # Run write filters
                my $cooked_linked_obj = $cro_obj->__apply_field_write_filters($obj_field, $raw_linked_object);

                # Set the keys
                my @remote_keys = $rel->remote_key_fields;
                for my $key_num (0..(@remote_keys -1)) {
                    my $remote_key = $remote_keys[$key_num];
                    my $local_key = $local_keys[$key_num];
                    $cro_obj->$local_key($cooked_linked_obj->$remote_key());
                }

                # Set the collection contents
                $collection->_set_single_value($cooked_linked_obj);

            } else {
                # Clear the keys
                foreach my $key (@local_keys) {
                    $cro_obj->$key(undef);
                }

                # Set the collection to be fetched but empty
                $collection->_set_empty_but_populated();
            }
        }

        if ($collection->is_populated) {
            my $raw_value = $collection->first();
            my $cooked_value = $cro_obj->__apply_field_read_filters($obj_field, $raw_value);
            return $cooked_value;
        } else {

            if ($rel->linked_class->is_static) {
                # Go ahead and fetch
                my @linking_keys = map { $cro_obj->$_() } $rel->local_key_fields;
                my $raw_value = $rel->linked_class->fetch(@linking_keys);
                my $cooked_value = $cro_obj->__apply_field_read_filters($obj_field, $raw_value);
                $collection->_set_single_value($cooked_value);
                return $cooked_value;
            } else {
                Class::ReluctantORM::Exception::Data::FetchRequired->croak(called => $rel->method_name, call_instead => 'fetch_' . $rel->method_name, fetch_locations => [ $cro_obj->all_origin_traces ]);
            }
        }
    };
    return $code;
}


sub _raw_mutator {
    my $rel = shift;
    my $cro_obj = shift;
    my @newval = @_;

    my $has_one_field = $rel->method_name();

    # Fetch the underlying collection
    my $collection_slot = '_' . $rel->method_name . '_coll';
    my $collection = $cro_obj->get($collection_slot);
    unless (defined $collection) {
        $collection = Class::ReluctantORM::Collection::One->_new(
                                                                 relationship => $rel,
                                                                 linking_object => $cro_obj
                                                                );
        $cro_obj->set($collection_slot, $collection);
    }

    if (@newval) {
        # Set the collection contents
        my $newval = $newval[0]; # Only allows one

        if (defined ($newval)) {
            $collection->_set_single_value($newval);
        } else {
            # Set the collection to be fetched but empty
            $collection->_set_empty_but_populated();
        }

        $cro_obj->_mark_field_dirty($has_one_field);
    }

    if ($collection->is_populated) {
        my $raw_value = $collection->first();
        return $raw_value;
    } else {
        Class::ReluctantORM::Exception::Data::FetchRequired->croak
            (
             called => $rel->method_name, 
             call_instead => 'fetch_' . $rel->method_name,
             fetch_locations => [ $cro_obj->all_origin_traces ],
            );
    }
}

sub __make_has_one_fetch_accessor {
    my $rel = shift;
        # Setup accessor
    my $code = sub {
        my $cro_obj = shift;

        # Fetch the underlying collection
        my $collection_slot = '_' . $rel->method_name . '_coll';
        my $collection = $cro_obj->get($collection_slot);
        unless (defined $collection) {
            $collection = Class::ReluctantORM::Collection::One->_new(
                                                               relationship => $rel,
                                                               linking_object => $cro_obj
                                                              );
            $cro_obj->set($collection_slot, $collection);
        }

        $collection->depopulate;
        $collection->fetch_all();
        return $collection->first;
    };
    return $code;
}


# Do nothing
sub _handle_implicit_create { }

# Called from ReluctantORM::new()
sub _handle_implicit_new {
    my $rel = shift;
    my $linking_object = shift;
    my $new_args = shift;

    my @key_fields = $rel->local_key_fields();
    my $rel_field = $rel->method_name();

    my $any_key_present = 0;
    my $all_keys_present = 1;
    for my $key (@key_fields) {
        $any_key_present ||= exists $new_args->{$key};
        $all_keys_present &&= exists $new_args->{$key};
    }

    my $rel_field_present = exists $new_args->{$rel_field};
    my $child_obj;
    if ($rel_field_present) {
        $child_obj =  $new_args->{$rel_field};
        if (ref($child_obj) eq 'ARRAY') {
            # Fetch_deep will build things passing children in array refs - unpack it
            $child_obj = $child_obj->[0];
        }
    }


    # This stanza causes a bunch of tests in 11-has_one to fail
#     if ($rel_field_present && $any_key_present) {
#         Class::ReluctantORM::Exception::Param::Duplicate->croak
#             (
#              error => "You specified both the related field and one or more local keys for a Has-One relationship.  Please specify one or the other.",
#              param => join ',', ($rel_field, @key_fields),
#             );
#     }




    if ($rel_field_present) {
        # The linked object was provided.  Set it.
        # (the keys will be set by the mutator call)
        $linking_object->$rel_field($child_obj);
        my $inverse_rel = $rel->inverse_relationship();
        if ($inverse_rel && $child_obj) {
            my $method = $inverse_rel->method_name();
            $child_obj->$method->attach($linking_object);
        }

    } elsif ($all_keys_present) {
        # They've all already been set by new(), since all the local keys are actual fields.
        # So we have an unfetched relation, which is handled by the fetching accessor.
    } else {
        # Neither object nor keys.  Set it up as a fetched, empty collection.
        my $collection_slot = '_' . $rel->method_name . '_coll';
        my $collection  = Class::ReluctantORM::Collection::One->_new(
                                                               relationship => $rel,
                                                               linking_object => $linking_object,
                                                               children => [],
                                                              );
        $linking_object->set($collection_slot, $collection);
    }

}

sub _mark_unpopulated_in_object {
    my $rel = shift;
    my $cro_obj = shift;

    # Obtain the underlying collection
    my $collection_slot = '_' . $rel->method_name . '_coll';
    my $collection = $cro_obj->get($collection_slot);
    unless ($collection) { return; }

    $collection->depopulate();


}


sub _notify_key_change_on_linking_object {
    my $rel = shift;
    my $changed_linking_object = shift;
    if ($Class::ReluctantORM::SOFT_TODO_MESSAGES) {
        print STDERR __PACKAGE__ . ':' . __LINE__ . " - soft TODO - HasOne::_notify_key_change_on_linking_object()\n";
    }
}

sub _merge_children {
    my $rel = shift;
    my $cro_obj = shift;
    my $children_ref = shift;

    # Nothing to do if children is undef
    return unless (defined $children_ref);

    # Has one should only ever get one child, derp
    my $new_child = $children_ref->[0];

    my $relname = $rel->name();
    my $existing_child = $cro_obj->$relname; # We know this is populated

    if ($new_child->id eq $existing_child->id()) {
        # Recurse into fetched relations and merge?
        foreach my $child_rel ($existing_child->relationships) {
            my $child_rel_name = $child_rel->name();
            if ($existing_child->is_fetched($child_rel_name)) {
                if ($new_child->is_fetched($child_rel_name)) {
                    $child_rel->merge_children($existing_child, [ $new_child->$child_rel_name ]);
                }
            } elsif ($new_child->is_fetched($child_rel_name)) {
                $child_rel->handle_implicit_new($existing_child, [ $new_child->$child_rel_name ]);
            }
        }
    } else {
        # new_child is fresh from the DB, while existing_child is in ram
        # Which is more correct to keep?
        # I'd say keep the existing one, since it may have been messed with
        # So, nothing to do? But what if the fetch deep maps were different?
        Class::ReluctantORM::Exception::NotImplemented->croak("Cannot merge kids, ids don't match");
    }
}

1;



#=============================================================================#
#=============================================================================#
#                               Collection Subclass
#=============================================================================#
#=============================================================================#

package Class::ReluctantORM::Collection::One;
use strict;
use warnings;

use Data::Dumper;
use base 'Class::ReluctantORM::Collection';
use Class::ReluctantORM::SQL::Aliases;
use Scalar::Util qw(weaken);

our $DEBUG = 0;

sub rel { return shift->{relationship}; }

sub _new {
    my ($class, %args) = @_;
    foreach my $f (qw(master_class master_key_name master_key_value child_key_name child_class) ) {
        if (exists $args{$f}) { Class::ReluctantORM::Exception::Call::Deprecated->croak("May not use param $f for Colelction::OneToMany::_new in 0.4 code"); }
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

    return $self;
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
    unless ($object->isa($self->rel->linked_class)) {
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

sub fetch_all {
    my $self = shift;

    # Determine FK values in the parent
    my $parent = $self->linking_object();
    my @fk_values = map { $parent->$_() } $self->rel->local_key_fields();

    my $child = $self->rel->linked_class->fetch(@fk_values);

    # This counts as an origin on the parent
    $parent->capture_origin();

    $self->{_children} = [ $child ];
    $self->{_populated} = 1;
    $self->{_count} = 1;
    my @results = @{$self->{_children}};
    return @results;
}

sub __make_link_where {
    my $self = shift;
    my $linking_class = $self->rel->linking_class();
    my @where;
    my @execargs;

    foreach my $colname ($self->rel->local_key_columns) {
        push @where, 'MACRO__parent__' . $self->rel->method_name() . '__.' . $colname . ' = ?';

        my $f = $linking_class->field_name($colname);
        my $value = $self->linking_object->raw_field_value($f);
        push @execargs, $value;

    }

    return (where => join(' AND ', @where), execargs => \@execargs);
}

sub __make_link_where_sql {
    my $self = shift;
    my $linking_class = $self->rel->linking_class();
    my $crit;

    foreach my $col ($self->rel->local_key_sql_columns) {
        my $f = $linking_class->field_name($col->column);
        my $param = Param->new();
        $param->bind_value($self->linking_object->raw_field_value($f));

        my $this_crit = Criterion->new('=', $col, $param);
        $crit = $crit ? Criterion->new('AND', $crit, $this_crit) : $this_crit;
    }

    return (where => Where->new($crit));
}


# Note: AUTOLOAD defined in Collection base class
sub __setup_aggregate_autoload {
    my ($self1, $AUTOLOAD, $method, $args, $agg_type, $agg_field) = @_;

    my $linked_class = $self1->rel->linked_class;

    # Generate a coderef
    my $code = sub {
        my $self = shift;
        my %args = @_;
        my %where_args = $self->__make_link_where();

        # Append args
        $where_args{where} .= $args{where} || '1=1';
        push @{$where_args{execargs}}, @{$args{execargs} || []};

        # Use aggregate method defined by child class
        return $linked_class->$method(%where_args);
    };

    # Don't install coderef in symbol table
    # The name of this will vary based on the classes linked
    $code->($self1, @$args);
}

sub _set_single_value {
    my $self = shift;
    my $val = shift;
    $self->{_children} = [ $val ];
    $self->{_populated} = 1;
    $self->{_count} = 1;
    return;
}

sub _set_empty_but_populated {
    my $self = shift;
    $self->{_children} = [  ];
    $self->{_populated} = 1;
    $self->{_count} = 0;
    return;

}


1;



