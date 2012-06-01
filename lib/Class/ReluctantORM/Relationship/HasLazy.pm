package Class::ReluctantORM::Relationship::HasLazy;

=head1 NAME

Class::ReluctantORM::Relationship::HasLazy

=head1 SYNOPSIS

  # Declare a column to be lazy
  Pirate->has_lazy('diary');

  # Now you have:
  $pirate = Pirate->fetch_with_diary($pirate_id);
  @bipeds = Pirate->fetch_by_leg_count_with_diary(2);

  # Get info about the relationship
  $rel = Pirate->relationships('diary');

  $str = $rel->type();                 # 'has_lazy';
  $str = $rel->linked_class();         # undef;
  $str = $rel->linking_class();        # 'Pirate';
  @fields = $rel->local_key_fields();  # field in Pirate that is lazy-loaded
                                       # by this relationship, always one
  @fields = $rel->remote_key_fields(); # empty list
  $int = $rel->join_depth();           # 0

  # Class::ReluctantORM::SQL integration
  @sql_cols = $rel->additional_output_sql_columns();
  @cols = $rel->local_key_sql_columns();
  @cols = $rel->remote_key_sql_columns(); # empty list
  @empty = $rel->join_local_key_sql_columns();  # always empty for HasLazy
  @empty = $rel->join_remote_key_sql_columns(); # always empty for HasLazy


=head1 DESCRIPTION

The HasLazy relationship permits a class to be loaded from the database without loading all of its columns.  If a field is declared has_lazy, the column will not be fetched from the database unless it is explicitly mentioned in a fetch_deep 'with' clause.

Unlike other relationships, HasLazy does not link to another CRO class.  Nor does it require a remote table, as it draws its data from the base table.

HasLazy relationships do not have inverse relationships.

=head1 BUILD_CLASS INTEGRATION

HasLazy also provides integration via the build_class method of Class::ReluctantORM.  This is merely for convenience; behind the scenes, has_lazy will be called for you.

=head2 MyClass->build_class(%other_opts,  %lazy_opts);

By providing either of these two options, you can automatically set up many Lazy columns quickly.

=over

=item lazy_fields 

Optional array ref.  List of fields that should be made Lazy.  Mutually exclusive with non_lazy_fields.

=item non_lazy_fields

Optional array ref.  If provided, ALL fields are assumed to be lazy, EXCEPT those listed here and primary and foreign keys. Mutually exclusive with non_lazy_fields.

=back

=cut

# Integration provided in Class::ReluctantORM


=head2 $class->has_lazy('field_name');

Indicates that the given field should be lazy-loaded, meaning that is not automatically fetched during a regular fetch.

You can cause the field to be fetched by using fetch_deep or calling $obj->fetch_FIELD().

The field will not appear on the 'essential_fields' list, but it will appear on the 'field_names' list.

Note that the value passed to has_lazy is a field name, not a column name; for some classes, they may be different.  This is configured by passing a hashref as the value of the 'fields' option to build_class.

The accessor/mutator will behave similarly to a HasOne accessor, in that it will die on access if the value has not been fetched.

=cut

use strict;
use warnings;

use Data::Dumper;
use Class::ReluctantORM::Utilities qw(install_method conditional_load array_shallow_eq check_args);
use Class::ReluctantORM::Exception;
our $DEBUG ||= 0;

use base 'Class::ReluctantORM::Relationship';

sub _initialize {
    my $class = shift;
    install_method('Class::ReluctantORM::Relationship', 'is_has_lazy', sub { return 0; });
    install_method('Class::ReluctantORM', 'has_lazy', \&__setup_has_lazy);
    install_method('Class::ReluctantORM', 'is_field_has_lazy', \&is_field_has_lazy);
}

=head2 $str = $rel->type();

Returns 'has_lazy'.

=cut

sub type { return 'has_lazy'; }

=head2 $bool = $rel->is_has_lazy();

Returns true.

=cut

sub is_has_lazy { return 1; }

=head2 $int = $rel->join_depth();

Returns 0.

=cut

sub join_depth  { return 0; }

=head2 $str = $rel->join_type();

Returns 'NONE'

=cut

=head2 $int = $rel->lower_multiplicity()

Returns 0.

=cut

sub lower_multiplicity { return 0; }

=head2 $int = $rel->upper_multiplicity()

Returns 0 - this is a relationship that doesn't link to another table.

=cut

sub upper_multiplicity { return 0; }

sub join_type   { return 'NONE'; }

=begin devdocs

Not sure this is public.... or if that calling pattern is right.

=head2 $bool = $cro_obj->is_field_has_lazy('field');

Returns true if the given field is a HasLazy field.

=cut

sub is_field_has_lazy {
    my $inv = shift;
    my $field = shift;
    my $tb_class = ref($inv) ? ref($inv) : $inv;
    my $rel = $tb_class->relationships($field);
    return $rel ? $rel->is_has_lazy() : undef;
}

=head2 @cols = $rel->additional_output_sql_columns();

Returns a list of exactly one column, the column to lazy-loaded.

=cut

sub additional_output_sql_columns {
    my $rel = shift;
    return $rel->local_key_sql_columns();
}

=begin devnotes

In order to use a Collection, while appearing not to, 
we will actually use a secondary field to store the 
collection.

=cut

sub __setup_has_lazy {
    my $cro_base_class = shift;
    if (@_ < 1) {
        Class::ReluctantORM::Exception::Param::Missing->croak(param => 'field_name');
    } elsif (@_ > 1) {
        Class::ReluctantORM::Exception::Param::Spurious->croak(error => 'has_lazy expects exactly one arg, the field name');
    }

    my $lazy_field = shift;
    my $lazy_column = $cro_base_class->column_name($lazy_field);
    unless ($lazy_column) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'field_name', error => "Could not find a column for field '$lazy_field' in class '$cro_base_class'");
    }

    # Immediate registration - no need to wait, as we don't depend on a nother class
    my $rel = Class::ReluctantORM::Relationship::HasLazy->new();
    $rel->method_name($lazy_field);
    $rel->linked_class(undef);
    $rel->linking_class($cro_base_class);

    $rel->local_key_fields($lazy_field);

    install_method($cro_base_class, $rel->method_name, $rel->__make_has_lazy_accessor(), 1); # be sure to clobber here
    install_method($cro_base_class, 'fetch_' . $rel->method_name, $rel->__make_has_lazy_fetch_accessor());
    $rel->_install_search_by_with_methods();

    $rel->_original_args_arrayref([$lazy_field]);

    $cro_base_class->register_relationship($rel);

}

# Implements $pirate->diary();
sub __make_has_lazy_accessor {
    my $rel = shift;

    # Setup accessor
    my $code = sub {
        my $cro_obj = shift;
        my $lazy_field = $rel->method_name();

        # Fetch the underlying collection
        my $collection_slot = '_' . $rel->method_name . '_coll';
        my $collection = $cro_obj->get($collection_slot);
        unless (defined $collection) {
            $collection = Class::ReluctantORM::Collection::Lazy->_new(
                                                                      relationship => $rel,
                                                                      linking_object => $cro_obj
                                                                     );
            $cro_obj->set($collection_slot, $collection);
        }

        if (@_) {
            # Acting as mutator
            my $raw_value = shift;

            if (defined $raw_value) {
                my $cooked_value = $cro_obj->__apply_field_write_filters($lazy_field, $raw_value);
                if (ref($cooked_value)) {
                    Class::ReluctantORM::Exception::Param::WrongType->croak(
                                                                            param => 'value',
                                                                            expected => 'plain scalar',
                                                                            error => 'Can only store plain scalar values in HasLazy fields',
                                                                            value => $cooked_value,
                                                                           );
                }
                # Set the collection contents
                $collection->_set_single_value($cooked_value);
            } else {
                # Set the collection to be fetched but empty
                $collection->_set_empty_but_populated();
            }

            $cro_obj->_mark_field_dirty($lazy_field);
        }

        if ($collection->is_populated) {
            my $raw_value = $collection->first();
            my $cooked_value = $cro_obj->__apply_field_read_filters($lazy_field, $raw_value);
            return $cooked_value;
        } else {
            Class::ReluctantORM::Exception::Data::FetchRequired->croak
                (
                 called => $rel->method_name, 
                 call_instead => 'fetch_' . $rel->method_name,
                 fetch_locations => [ $cro_obj->all_origin_traces ],
                );
        }
    };
    return $code;
}


# Implements $pirate->fetch_diary();
sub __make_has_lazy_fetch_accessor {
    my $rel = shift;
        # Setup accessor
    my $code = sub {
        my $cro_obj = shift;

        # Fetch the underlying collection
        my $collection_slot = '_' . $rel->method_name . '_coll';
        my $collection = $cro_obj->get($collection_slot);
        unless (defined $collection) {
            $collection = Class::ReluctantORM::Collection::Lazy->_new(
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

sub _raw_mutator {
    my $rel = shift;
    my $cro_obj = shift;
    my @newval = @_;

    my $lazy_field = $rel->method_name();

    # Fetch the underlying collection
    my $collection_slot = '_' . $rel->method_name . '_coll';
    my $collection = $cro_obj->get($collection_slot);
    unless (defined $collection) {
        $collection = Class::ReluctantORM::Collection::Lazy->_new(
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

        $cro_obj->_mark_field_dirty($lazy_field);
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


# Called from ReluctantORM::new()
sub _handle_implicit_new {
    my $rel = shift;
    my $cro_object = shift;
    my $new_args = shift;

    my $lazy_field = $rel->method_name();

    unless (exists $new_args->{$lazy_field}) {
        return;
    }

    my $value = $new_args->{$lazy_field};
    if (ref($value) eq 'ARRAY') {
        # Fetch_deep will build things passing children in array refs - unpack it
        $value = $value->[0];
    }

    # Rely on mutator to set it
    $cro_object->$lazy_field($value);
}

# Do nothing - handle_implicit_new did everything we needed to do
sub _handle_implicit_create { }

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

sub _notify_key_change_on_linking_object {
    # We don't care, we have no keys
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


# sub merge_children {
#     my $rel = shift;
#     my $cro_obj = shift;
#     my $children_ref = shift;

#     # Nothing to do if shild ren is undef
#     return unless (defined $children_ref);

#     # Has one should only ever get one child, derp
#     my $new_child = $children_ref->[0];

#     my $relname = $rel->name();
#     my $existing_child = $cro_obj->$relname; # We know this is populated

#     if ($new_child->id eq $existing_child->id()) {
#         # Recurse into fetched relations and merge?
#         foreach my $child_rel ($existing_child->relationships) {
#             my $child_rel_name = $child_rel->name();
#             if ($existing_child->is_fetched($child_rel_name)) {
#                 if ($new_child->is_fetched($child_rel_name)) {
#                     $child_rel->merge_children($existing_child, [ $new_child->$child_rel_name ]);
#                 }
#             } elsif ($new_child->is_fetched($child_rel_name)) {
#                 $child_rel->handle_implicit_new($existing_child, [ $new_child->$child_rel_name ]);
#             }
#         }
#     } else {
#         # new_child is fresh from the DB, while existing_child is in ram
#         # Which is more correct to keep?
#         # I'd say keep the existing one, since it may have been messed with
#         # So, nothing to do? But what if the fetch deep maps were different?
#         Class::ReluctantORM::Exception::NotImplemented->croak("Cannot merge kids, ids don't match");
#     }
# }

1;



#=============================================================================#
#=============================================================================#
#                               Collection Subclass
#=============================================================================#
#=============================================================================#

package Class::ReluctantORM::Collection::Lazy;
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
    foreach my $f (qw(relationship linking_object)) {
        unless (exists $args{$f}) { Class::ReluctantORM::Exception::Param::Missing->croak(param => $f); }
    }

    my $self = bless \%args, $class;
    weaken($self->{linking_object});

    if ($args{children}) {
        $self->{_children} = $args{children};
        $self->{_populated} = 1;
    } else {
        $self->{_populated} = 0;
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
    if (ref($object)) {
        Class::ReluctantORM::Exception::Param::WrongType->croak(
                                                                param => 'value',
                                                                expected => 'plain scalar',
                                                                error => 'Can only store plain scalar values in HasLazy fields',
                                                                value => $object,
                                                               );
    }
}

sub is_populated { return shift->{_populated}; }
sub depopulate {
    my $self = shift;
    $self->{_populated} = 0;
    $self->{_children} = [];
}

sub count {
    my $self = shift;

    # No separate count mechanism - if you're populated, it's 1; else, it's an exception
    if ($self->is_populated) {
        return 1;
    } else {
        Class::ReluctantORM::Exception::Data::FetchRequired->croak(called => 'count', call_instead => 'fetch_count', fetch_locations => [ $self->linking_object->all_origin_traces ]);
    }
}

sub fetch_count {
    my $self = shift;
    $self->fetch_all();
    return $self->count();
}

sub fetch_all {
    my $self = shift;

    # Be as gentle as possible
    # (Fetching as few columns as possible)
    my $class = $self->rel->linking_class();
    my $obj = $self->linking_object();
    my $table = Table->new($class);
    my $lazy_field = $self->rel->name();

    my $sql = SQL->new('SELECT');
    $sql->from(From->new($table));
    $sql->where($self->__make_link_where_sql());
    my $output = OutputColumn->new(Column->new(table => $table,
                                               column => $class->column_name($lazy_field)));
    $sql->add_output($output);

    # Run query
    $class->driver->run_sql($sql);

    my $value = $output->output_value();

    $self->linking_object->capture_origin();
    $self->{_children} = [ $value ];
    $self->{_populated} = 1;
    my @results = @{$self->{_children}};
    return @results;
}

sub __make_link_where_sql {
    my $coll = shift;
    my $class = $coll->rel->linking_class();
    my $obj   = $coll->linking_object();
    my $table = Table->new($class);

    my $crit;

    foreach my $col_name ($class->primary_key_columns()) {
        my $field = $class->field_name($col_name);
        my $param = Param->new($obj->raw_field_value($field));
        my $col = Column->new(table => $table, column => $col_name);

        my $this_crit = Criterion->new('=', $col, $param);
        $crit = $crit ? Criterion->new('AND', $crit, $this_crit) : $this_crit;
    }

    return Where->new($crit);
}

sub _set_single_value {
    my $self = shift;
    my $val = shift;
    $self->{_children} = [ $val ];
    $self->{_populated} = 1;
    return;
}

sub _set_empty_but_populated {
    my $self = shift;
    $self->{_children} = [  ];
    $self->{_populated} = 1;
    return;

}


# Note: AUTOLOAD defined in Collection base class
# sub __setup_aggregate_autoload {
#     my ($self1, $AUTOLOAD, $method, $args, $agg_type, $agg_field) = @_;

#     my $linked_class = $self1->rel->linked_class;

#     # Generate a coderef
#     my $code = sub {
#         my $self = shift;
#         my %args = @_;
#         my %where_args = $self->__make_link_where();

#         # Append args
#         $where_args{where} .= $args{where} || '1=1';
#         push @{$where_args{execargs}}, @{$args{execargs} || []};

#         # Use aggregate method defined by child class
#         return $linked_class->$method(%where_args);
#     };

#     # Don't install coderef in symbol table
#     # The name of this will vary based on the classes linked
#     $code->($self1, @$args);
# }



1;



