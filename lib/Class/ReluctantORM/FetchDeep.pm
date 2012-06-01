=begin devnotes

As the fetch_deep code is fairly complex, I thought best to 
pull it out of the CRO main module and place it here.

This is a mix-in module - so set package to be CRO.

=cut

package Class::ReluctantORM;


use strict;
use warnings;
use Scalar::Util qw(blessed refaddr);
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::Utilities qw(check_args);

our $DEBUG_FD ||= 0;
use Class::ReluctantORM::SQL::Aliases;

use Class::ReluctantORM::SQL::From;
use Class::ReluctantORM::SQL::Table;
use Class::ReluctantORM::SQL::Column;
use Class::ReluctantORM::SQL::Param;
use Class::ReluctantORM::SQL::Where;
use Class::ReluctantORM::SQL::Expression::Criterion;
use Class::ReluctantORM::SQL::OrderBy;

=begin devnotes

fetch_deep and search_Deep are really just frontends
for __deep_query.

=cut

sub fetch_deep {
    my $class = shift;
    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;

    return $class->__deep_query(%args, fatal => 1);
}

sub search_deep {
    my $class = shift;
    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;
    return $class->__deep_query(%args, fatal => 0);
}

sub fetch_deep_overlay {
    my $inv = shift;
    my $plural_mode = !ref($inv);
    my $class = ref($inv) ? ref($inv) : $inv;
    my %args;
    if ($plural_mode) {
        %args = check_args(args => \@_, required => [qw(with objects)]);
    } else {
        %args = check_args(args => \@_, required => [qw(with)]);
    }

    # With cannot be empty for an overlay
    unless (keys %{$args{with}}) {
        Class::ReluctantORM::Exception::Param::BadValue->croak
            (
             param => 'with',
             value => '{}',
             error => "'with' cannot be empty for an overlay fetch",
            );
    }

    # Check that 'with' arg with a dummy 'where'
    {
        my %checked_args = $inv->__dq_check_args(with => $args{with}, where => Where->new());
        $args{with} = $checked_args{with};
    }
    # If in plural mode, objects param must be an array ref
    if ($plural_mode && ref($args{objects}) ne 'ARRAY') {
        Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak
            (
             param => 'objects',
             value => $args{objects},
            );
    }
    if ($plural_mode && !@{$args{objects}}) {
        # Zero objects to act on, just return.
        return;
    }

    my @parent_objects = ($plural_mode) ? @{$args{objects}} : ($inv);

    # Ok, start building the actual where clause

    # Assume single-column PKs
    if ($inv->primary_key_column_count() > 1) {
        # TODO
        Class::ReluctantORM::Exception::NotImplemented->croak
            (
             "Multiple-column primary keys are not supported when using fetch_deep_overlay.  Need perl-side KEY_COMPOSITOR support."
            );
    }

    # we want to match primary keys in the objects against the base table PKs
    # two ways to do this: as a $pk_column IN (pkv1, pkv2, ....) (faster, but vulnerable to SQL injection since we can't do placeholders)
    # or as $pk_column IN = pkv1 OR $pk_column IN = pkv2 ... (dead slow but safe)

    # We'll check to see if all the PK values look like integers.  If so, we'll do an IN.  If not, we'll do ORs.
    my $must_use_OR_approach = grep { $_->id !~ /^\d+$/ } @parent_objects;

    my %fetch_args;
    if ($must_use_OR_approach) {
        my $where = Where->new();
        my $pk_column = Column->new(
                                    column => $class->first_primary_key_column(),
                                    table => $class->table_name,
                                   );
        foreach my $obj (@parent_objects) {
            $where = $where->or(Criterion->new('=', $pk_column, Param->new($obj->id)));
        }
        $fetch_args{where} = $where;
    } else {
        # Ugggh, apparently we haven't implemented 'IN'.  Lame.
        $fetch_args{where} =
          $class->table_name . '.' .
            $class->first_primary_key_column() . 
              ' IN (' . join(',', map { $_->id } @parent_objects) . ')';
        $fetch_args{parse_where} = 0;
    }
    $fetch_args{with} = $args{with};

    # Ok, run fetch.
    my @new_copies_of_parents = $class->__deep_query(%fetch_args);

    # Overlay merge.
    my %parent_index = map { $_->id => $_ } @parent_objects;
    foreach my $copy (@new_copies_of_parents) {
        my $id = $copy->id;
        my $original = $parent_index{$id};
        next if refaddr($copy) eq refaddr($original); # Registry has already merged them
        # Loop over top-level relations in the with
        foreach my $relname (grep { $_ !~ /^__/} keys %{$args{with}}) {
            my $rel = $class->relationships($relname);
            my ($lower, $upper) = ($rel->lower_multiplicity, $rel->upper_multiplicity);
            if (defined($lower) && defined($upper) && $lower == 0 && $upper == 0) {
                # Has-Lazy or similar
                $original->$relname($copy->$relname);
                $original->_mark_field_clean($relname);
            } elsif (defined($upper) && $upper == 1) {
                # Has-one, or similar
                $original->$relname($copy->$relname);
                $original->_mark_field_clean($relname);
            } else {
                # has-many, HMM, or similar
                $original->$relname->_set_contents($copy->$relname->all());
            }
        }
        $original->capture_origin();
    }
}


=begin devnotes

=head2 @results = __deep_query(%deep_args, fatal => 0|1)

__deep_query does the hard work.  The fatal flag says whether to
throw an exception if there are no results.

Helper subs are named __dq_* .

=cut

sub __deep_query {
    my ($class, %orig_args) = @_;

    # Normalize args
    if ($DEBUG_FD > 2) { print STDERR __FILE__ . ':' . __LINE__ . " - __deep_query orig args: :" . Dumper(\%orig_args); }
    my %args = $class->__dq_check_args(%orig_args);
    if ($DEBUG_FD > 2) { print STDERR __FILE__ . ':' . __LINE__ . " - __deep_query scrubbed args: :" . Dumper(\%args); }

    # Build SQL
    my $sql  = $class->__dq_build_sql(%args);
    if ($DEBUG_FD > 2) { print STDERR __FILE__ . ':' . __LINE__ . " - __deep_query SQL object:" . Dumper($sql); }

    #  Execute SQL Query
    my $driver = $class->driver();
    my @results = $driver->execute_fetch_deep($sql, $args{with});
    if ($args{fatal} && !@results) {
        Class::ReluctantORM::Exception::Data::NotFound->croak();
    }
    $class->_apply_deep_filter_args($args{filter_info}, \@results);
    return wantarray ? @results : $results[0];
}

sub __dq_check_args {
    my $class = shift;
    my %orig_args = @_;
    my %args;

    if ($orig_args{with}) {
        unless (ref($orig_args{with}) && ref($orig_args{with}) eq 'HASH') {
            Class::ReluctantORM::Exception::Param::ExpectedHashRef->croak
                (param => 'with', frames => 3, );
        }
        $args{with} = $class->__dq_normalize_with($orig_args{with});
        delete $orig_args{with};

        # Go ahead and build From here - we'll need it to process the WHERE
        my $from = From->_new_from_with($class, $args{with});
        $args{from} = $from;
    } else {
        my $t = Table->new($class);
        $args{from} = From->new($t);
        $args{with} = { __upper_table => $t };
    }

    $args{fatal} = $orig_args{fatal};
    delete $orig_args{fatal};

    # Boost order by to be an Order By object if it isn't already
    $args{order_by} = $orig_args{order_by};
    if ($args{order_by}) {
        if (ref($args{order_by}) && !$args{order_by}->isa('Class::ReluctantORM::SQL::OrderBy')) {
            Class::ReluctantORM::Exception::Param::WrongType->croak(
                                                       error => 'order_by must be either a string or an OrderBy object',
                                                       param => 'order_by',
                                                       expected => 'Class::ReluctantORM::SQL::OrderBy',
                                                       frames => 3,
                                                      );
        }
        unless (ref($args{order_by})) {
            $args{order_by} = $class->driver->parse_order_by($args{order_by});
        }
    }
    delete $orig_args{order_by};

    # Pagination args
    if (defined $orig_args{limit}) {
        unless ($args{order_by}) {
            Class::ReluctantORM::Exception::Param::Missing->croak(
                                                     error => 'order_by is required if limit is provided',
                                                     param => 'order_by',
                                                     frames => 3
                                                    );
        }
        unless ($orig_args{limit} =~ /-?\d+/) {
            Class::ReluctantORM::Exception::Param::BadValue->croak(
                                                      error => 'limit must be an integer',
                                                      param => 'limit',
                                                      value => $orig_args{limit},
                                                      frames => 3
                                                     );
        }
        if ($orig_args{limit} < 1) {
            Class::ReluctantORM::Exception::Param->croak(
                                            error => 'when limit is provided, it must be a positive integer',
                                            param => 'limit',
                                            value => $orig_args{limit},
                                            frames => 3
                                           );
        }
        $args{limit} = $orig_args{limit};
        delete $orig_args{limit};
    }
    if (defined $orig_args{offset}) {
        unless ($args{limit}) { 
            Class::ReluctantORM::Exception::Param::Missing->croak(
                                                     error => 'limit is required if offset is provided',
                                                     param => 'limit',
                                                     frames => 3
                                                    );
        }
        unless ($orig_args{offset} =~ /-?\d+/) {
            Class::ReluctantORM::Exception::Param::BadValue->croak(
                                                      error => 'offset must be an integer',
                                                      param => 'offset',
                                                      value => $orig_args{offset},
                                                      frames => 3
                                                     );
        }
        if ($orig_args{offset} < 0) {
            Class::ReluctantORM::Exception::Param::BadValue->croak(
                                                      error => 'when offset is provided, it must be a non-negative integer',
                                                      param => 'offset',
                                                      value => $orig_args{offset},
                                                      frames => 3
                                                     );
        }
        $args{offset} = $orig_args{offset};
        delete $orig_args{offset};
    }

    # Filter args
    $args{filter_info} = $class->_extract_deep_filter_args(\%orig_args);

    # At this point, only other permissible args are a
    # field, where, or execargs
    if ($orig_args{where}) {
        if (blessed($orig_args{where}) && $orig_args{where}->isa(Where)) {
            $args{where_obj} = $orig_args{where};
            delete $orig_args{where};
            delete $orig_args{parse_where}; # Spurious in this case, but just ignore it if present
        } else {
            # Respect parse options
            if ($orig_args{no_re_alias_where}) {
                $orig_args{parse_where} = 0;
            }
            my $should_attempt_parse = 
              defined($orig_args{parse_where}) ?
                $orig_args{parse_where} :
                  $class->get_global_option('parse_where');
            delete $orig_args{parse_where};

            my $parse_failures_are_fatal = $class->get_global_option('parse_where_hard');

            my $raw_where = $orig_args{where};
            my $where_obj;

            if ($should_attempt_parse) {
                my $driver = $class->driver();

                if ($parse_failures_are_fatal) {
                    $where_obj = $driver->parse_where($raw_where);
                } else {
                    # Ignore exception (was asked to!)
                    eval { $where_obj = $driver->parse_where($raw_where); };
                }
            }

            if ($where_obj) {
                $where_obj->bind_params(@{$orig_args{execargs} || []});
                $args{where_obj} = $where_obj;
            } else {
                $args{raw_where} = $raw_where;
                $args{raw_where_execargs} = $orig_args{execargs} || [];
                $args{no_re_alias_where} = $orig_args{no_re_alias_where} || 0;
                delete $orig_args{no_re_alias_where};
            }
        }
        delete $orig_args{where};
        delete $orig_args{execargs};
    } else {
        delete $orig_args{parse_where}; # Spurious in this case, but just ignore it if present
        delete $orig_args{no_re_alias_where}; # Spurious in this case, but just ignore it if present

        # Must have exactly one key left, and it must be a field or column
        if ((keys %orig_args) < 1) {
            Class::ReluctantORM::Exception::Param::Missing->croak(error => 'Must provide either where clause or exactly one field or column argument', param => 'where');
        } elsif ((keys %orig_args) > 1) {
            Class::ReluctantORM::Exception::Param::Spurious->croak(error => 'Must provide either where clause or exactly one field or column argument', param => (join ',', keys %orig_args));
        }
        my $field = (keys %orig_args)[0];
        my ($col_name) = $class->__to_column_name($field);
        unless ($col_name) {
            Class::ReluctantORM::Exception::Param::Spurious->croak(error => "$field is not a field or column", param => $field);
        }

        my $col = Column->new(
                              column => $col_name,
                              table => $args{with}{__upper_table},
                             );
        my $param = Param->new();
        $param->bind_value($orig_args{$field});

        my $crit = Criterion->new('=', $col, $param);
        my $where = Where->new($crit);

        $args{where_obj} = $where;
        delete $orig_args{$field};
    }

    if (%orig_args) {
        # Should have nothing left
        Class::ReluctantORM::Exception::Param::Spurious->croak(error => "Ended up with leftover options to search/fetch_deep", param => join ',', keys %orig_args);
    }

    return %args;
}


=begin devnotes

This forces the with clause to be in 0.4 syntax.

So....
with => {
         pirates => {
                     booties => {}
                    }
        }
becomes
with => {
         pirates => {
                     with => {
                              booties => {}
                             }
                    }
        }


=cut

sub __dq_normalize_with {
    my $class = shift;
    my $with = shift || {};

    # These were all introduced in 0.4
    my %zero_four_attrs = map { $_ => 1 } qw(join_type join_on where execargs with class relationship);

    my %relations = %{$class->relationships};
    if ($DEBUG_FD > 2) { print STDERR __FILE__ . ':' . __LINE__ . " - have relationships for $class:\n" . Dumper([keys %relations]); }
    foreach my $rel_name (keys %$with) {
        # Permit __upper_table annotation
        next if ($rel_name eq '__upper_table');

        # Each key must be the name of a relation
        unless (exists $relations{$rel_name}) {
            Class::ReluctantORM::Exception::Param::BadValue->croak(error => "$rel_name is not a relationship of $class", param => 'with', frames => 3);
        }

        # Boost into 0.4 mode if needed
        my $is_zero_four_mode = 1;
        my $any_zero_four_mode = 0;
        for my $attr (keys %{$with->{$rel_name} || {}}) {
            $is_zero_four_mode  &&= exists $zero_four_attrs{$attr};
            $any_zero_four_mode ||= exists $zero_four_attrs{$attr};
        }
        if ($any_zero_four_mode && !$is_zero_four_mode) {
            Class::ReluctantORM::Exception::Param::BadValue->croak
                (
                 error => "The 'with' argument under '$rel_name' contains a mix of advanced CRO with options and unrecognized other keys (which might be typos, or might be relationships).  Mixing 'simple with' and 'advanced with' syntax is not permitted - either fix your typo, or move the relation down a level under an additional 'with'.  See perldoc Class::ReluctantORM::Manual::Prefetching. ", 
                 param => 'with', 
                 value => join(',', keys %{$with->{$rel_name}}),
                 frames => 3,
                );
        }
        unless ($is_zero_four_mode) {
            # OK, boost to 0.4 mode
            $with->{$rel_name} = { with => ($with->{$rel_name} || {} )};
        }

        # Set relationship hint
        $with->{$rel_name}->{relationship} = $relations{$rel_name};

        # Recurse
        my $linked_class = $relations{$rel_name}->linked_class();
        # HasLazy, possibly others in the future do not have a linked class
        if ($linked_class) {
            $with->{$rel_name}{with} = $linked_class->__dq_normalize_with($with->{$rel_name}{with});
        } elsif (keys %{$with->{$rel_name}{with}}) {
            Class::ReluctantORM::Exception::Param::BadValue->croak(error => "$rel_name is a " . $relations{$rel_name}->type . " which cannot have children in the with tree, but you provided " . join(',', keys %{$with->{$rel_name}{with}}), param => 'with', frames => 3);
        }

    }

    return $with;
}

=begin devnotes

At this point, %args should have
   where    => a Class::ReluctantORM::SQL::Where object, with unreconciled columns and tables
    - OR -
   raw_where => an unparsed SQL string
   raw_where_execargs => execargs or an empty arrayref
  order_by => an OrderBy object or undef
  limit    => an int or undef
  offset   => an int or undef
  with     => version 0.4+ syntax with relationships included

Should return a fully reconciled Class::ReluctantORM::SQL object.

=cut

sub __dq_build_sql {
    my $self = shift;
    my %args = @_;

    my $sql = Class::ReluctantORM::SQL->new('SELECT');
    my $class = ref($self) || $self;
    # Should have a FROM, created when we ran dq_check_args
    $sql->from($args{from});


    # Most output columns will be added when reconcile() is called
    # But we have to be sure that we add any extra columns requested by relationships
    # (Since zero-join relations won't appear in the FROM clause, reconcile won't know about it)
    my @rels = __get_rels_from_with($args{with});
    foreach my $rel (__get_rels_from_with($args{with})) {
        foreach my $col ($rel->additional_output_sql_columns) {
            $sql->add_output($col);
        }
    }

    # Set where clause
    if ($args{raw_where}) {
        $sql->raw_where($args{raw_where});
        $sql->_raw_where_execargs($args{raw_where_execargs});
        if (defined $args{no_re_alias_where}) {
            $sql->_raw_where_pristine($args{no_re_alias_where});
        }
    } else {
        $sql->where($args{where_obj});
    }


    # Set order_by
    $sql->order_by($args{order_by});

    # Set limit and offset
    if (defined $args{limit}) {
        $sql->limit($args{limit});
        if (defined $args{offset}) {
            $sql->offset($args{offset});
        }
    }

    return $sql;
}

sub __get_rels_from_with {
    my $with = shift;
    return () unless $with;

    my @rels;

    foreach my $rel_name (keys %{$with}) {
        next if $rel_name eq '__upper_table';
        push @rels, $with->{$rel_name}->{relationship};
        push @rels, __get_rels_from_with($with->{$rel_name}->{with})
    }
    return @rels;
}

1;

