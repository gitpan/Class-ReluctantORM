#==================================================================#
#                  FD Results Processing Support
#==================================================================#
# These are subroutines
#==================================================================#

package Class::ReluctantORM::FetchDeep::Results;
use strict;
use warnings;
use base 'Exporter';
use Data::Diff;
our @EXPORT;
our @EXPORT_OK;
our $DEBUG ||= 0;

=begin  devnotes

The result merging algorithm is based on representing each row as a tree structure, 
then merging that tree with the existing results.

Example:
Ship->fetch_deep(where => q(gun_count > 12), with => { pirates => {}});

Row output:
ship.ship_id, ship.name, ship.gun_count, pirate.pirate_id, pirate.name, pirate.ship_id
       1       Lollipop      13               1              Red Beard      1
       1       Lollipop      13               2              Wesley         1
       2      Gldn Hind      24               3              Drake          2

Though we see 3 rows, we need to produce 2 objects, the first with two children.
We transform the row into a tree, like so:
$tree = {
         1 => {  # this is a composite of the primary keys of the ship
               name => 'Lollipop',
               gun_count => 13,
               ship_id => 1,
               pirates => { # relationship name
                           1 => { # stringified primary keys of the pirate
                                  pirate_id => 1,
                                  name => 'Red Beard',
                                  ship_id => 1,
                                },
                           },
               },
        };

We process the second row in a similar manner, and the we merge as follows:
$tree = {
         1 => {
               ...
               pirates => {
                           1 => { ... },
                           2 => { ... },
                          },
              },
        };
=cut

push @EXPORT, 'fd_inflate';
push @EXPORT_OK, 'fd_inflate';
sub fd_inflate {
    my ($sql, $with, $run_args) = @_;

    my ($ok, $exception) = $sql->is_inflatable(auto_reconcile => 0, auto_annotate => 0);
    unless ($ok) { die $exception; }

    # Build with if not provided
    unless ($with) { $with = fd_guess_with_clause($sql); }

    # Init hints
    my $hints = fd_make_hints($sql, $with);

    # Init forest
    my $forest = {};
    my @ordering_trace = (); # Logs stringified PKs of top-level objects in order, so we can preserve query order

    # Create callback that merges each row into the forest
    my $callback = sub {
        my $sql = shift;
        my $row = { map { $_->alias => $_->output_value() } $sql->output_columns() };
        my $tree = fd_make_tree_from_row($row, $hints);
        push @ordering_trace, (keys %$tree)[0];
        # Merge each row with the existing results (the 'forest')
        $forest = fd_merge_tree_into_forest($forest, $tree);
    };
    $sql->add_fetchrow_listener($callback);

    # Get driver from base class
    my $base_class = $sql->base_table->class();
    my $driver = $base_class->driver();

    # call run_sql on driver
    $driver->run_sql($sql, $run_args);

    # Convert the forest into normal CRO objects
    if ($DEBUG > 2) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- Final forest:\n" . Dumper($forest); }
    my @results = fd_convert_forest_to_objects($forest, $hints);

    # Sort the results in original query order
    my %sort_rank_by_pk = ();
    for (my $i = 0; $i < @ordering_trace; $i++) {
        $sort_rank_by_pk{$ordering_trace[$i]} = $i;
    }
    @results =
      map { $_->[0] }
        sort { $a->[1] <=> $b->[1] }
          map { [$_, $sort_rank_by_pk{__fd_stringify_key_from_obj($_)} ] }
            @results;

    foreach my $obj (@results) {
        $obj->__run_triggers('after_retrieve');
    }

    if ($DEBUG > 2) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- Final result:\n" . Dumper(\@results); }
    return @results;
}


push @EXPORT_OK, 'fd_guess_with_clause';
sub fd_guess_with_clause {
    my $sql = shift;
    my $base_table = $sql->base_table();

    if ($sql->from && $sql->from->relationships()) {
        return __fd_GWC_recursor($sql->from->root_relation(), $base_table);
    } else {
        my $with = { __upper_table => $base_table };
        return $with;
    }
}

sub __fd_GWC_recursor {
    my ($join, $upper_table) = @_;

    # Find all joins whose local table is the upper table
    my @joins = grep { $_->relationship && $upper_table->is_the_same_table($_->relationship->local_sql_table) } $join->joins();

    # Filter out any joins who ALSO have the base table on the right-hand side (as that indicates it is a self-join, and we'll reach it later)
    @joins = grep { !$_->_find_latest_table($upper_table) } @joins;

    my $with = { __upper_table => $upper_table };

    foreach my $j (@joins) {
        my $next_table = $j->relationship->remote_sql_table();
        if ($next_table) {
            $with->{$j->relationship->name} = __fd_GWC_recursor($j, $next_table);
        } else {
            $with->{$j->relationship->name} = { };
        }
    }

    return $with;

}


# $hints contains cached metadata about the query
# $hints->{table} is the Table on the upper end of the query (base table)
# $hints->{columns_by_alias} is a hash of the base table's Columns, keyed by their output column aliases
# $hints->{key_column_aliases} is a arraryref of the output column aliases of the base table's primary keys
# $hints->{children} is a hashref of Hint structures of the child relations, keyed by relationship name

push @EXPORT_OK, 'fd_make_hints';
sub fd_make_hints {
    my ($sql, $with) = @_;

    my $hints = {};

    $hints->{table} = $with->{__upper_table};

    my $ta = $hints->{table}->alias;
    $hints->{columns_by_alias} = 
      {
       map { $_->alias => $_->expression }              # Construct hash mapping alias to Column
       grep { $_->expression->table->alias eq $ta }     # Filter to be only those Columns referring to the base table
       grep { $_->expression->is_column() } # Filter down to those OutputColumns that are based on columns
       $sql->output_columns()               # List all outputs
      };
    my %key_columns = map { lc($_) => 1 } $hints->{table}->class->primary_key_columns;
    $hints->{key_column_aliases} = 
      [ grep { exists($key_columns{lc($hints->{columns_by_alias}->{$_}->column)}) } keys %{$hints->{columns_by_alias}} ];


    $hints->{children} = {};
    foreach my $rel_name (keys %$with) {
        next if ($rel_name eq '__upper_table');
        my $rel = $hints->{table}->class->relationships($rel_name);

        if ($rel->join_depth == 0) {
            # Do not recurse into same-join relations, like HasLazy
        } else {
            $hints->{children}->{$rel_name} = fd_make_hints($sql, $with->{$rel_name}->{with});
        }


    }
    return $hints;
}


push @EXPORT_OK, 'fd_make_tree_from_sql_row';
sub fd_make_tree_from_row {
    my ($row, $hints) = @_; #

    # Build a hash of the object with column aliases pointing to their values
    my %obj;
    foreach my $col (keys %{$hints->{columns_by_alias}}) {
        if (exists $row->{$col}) {
            $obj{$col} = $row->{$col};
        }
    }

    # Recurse into the relationships
    foreach my $rel (keys %{$hints->{children} || {}}) {
        $obj{$rel} = fd_make_tree_from_row($row, $hints->{children}{$rel});
    }

    my $key = __fd_stringify_key_from_row($row, $hints->{key_column_aliases});
    my $tree = { $key => \%obj };
    return $tree;
}

sub __fd_stringify_key_from_row {
    my ($row, $key_list)  = @_;
    my $str = join '_', map { defined($_) ? $_ : 'NULL' } map { $row->{$_} } sort @$key_list;
    return $str;
}

sub __fd_stringify_key_from_obj {
    my ($obj)  = @_;
    # Careful here - be sure to sort by column name, not by field name
    my @pk_cols = sort $obj->primary_key_columns;
    my %keys_by_col = map { $_ => $obj->get($obj->field_name($_)) } @pk_cols;
    my $str = join '_', map { defined($_) ? $_ : 'NULL' } map { $keys_by_col{$_} } @pk_cols;
    return $str;
}


push @EXPORT_OK, 'fd_merge_tree_into_forest';
sub fd_merge_tree_into_forest {
    my ($forest, $tree) = @_;
    my $diff = Data::Diff->new( $forest, $tree );
    my $combined = $diff->apply();

    #print STDERR "Have combined object: \n" . Dumper($combined);

    return $combined;

}

push @EXPORT_OK, 'fd_convert_forest_to_objects';
sub fd_convert_forest_to_objects {
    my ($forest, $hints) = @_;;
    my $class = $hints->{table}->class();
    my $rels = $class->relationships();
    my %fields_by_col_alias = map { $_ => $class->field_name($hints->{columns_by_alias}{$_}->column) } keys %{$hints->{columns_by_alias}};

    my @objs;
    foreach my $composite_pk_value (keys %$forest) {
        # If the object is a null child (ie, the result of a left outer join
        # for which there was no matching child), the composite_pk_value will be 'NULL'
        # This is an artifact of the tree generator, and should be skipped
        next if $composite_pk_value eq 'NULL';

        my $obj_ghost = $forest->{$composite_pk_value};
        my %new_args;
        foreach my $field_name (keys %$obj_ghost) {
            if (exists $rels->{$field_name}) {
                $new_args{$field_name} = [ fd_convert_forest_to_objects($obj_ghost->{$field_name}, $hints->{children}{$field_name}) ];
            } else {
                $new_args{$fields_by_col_alias{$field_name}} = $obj_ghost->{$field_name};
            }
        }
        my $obj = $class->new(%new_args);
        $obj->_is_inserted(1);
        $obj->_mark_all_clean();
        push @objs, $obj;
    }
    return @objs;
}

1;
