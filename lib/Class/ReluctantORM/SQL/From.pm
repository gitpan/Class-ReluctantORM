package Class::ReluctantORM::SQL::From;

=head1 NAME

Class::ReluctantORM::SQL::From - Represent SQL FROM clauses

=head1 SYNOPSIS

  # Save yourself some typing
  use Class::ReluctantORM::SQL::Aliases;

  # Make a From clause using an existing Relation
  my $table = Table->new(...);
  my $from1 = From->new($table);
  my $join = Join->new(...);
  my $from2 = From->new($join);

  # Examine the From clause
  $rel = $from->root_relation();

  # List all referenced tables
  @tables = $from->tables();

  # List all available (not referenced) columns
  @columns = $from->columns();


=head1 DESCRIPTION

Represent a SQL FROM clause, including join information.

=cut

use strict;
use warnings;

use Data::Dumper;
use Scalar::Util qw(blessed);
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::Utilities qw(check_args);

our $DEBUG ||= 0;

use Class::ReluctantORM::SQL::Aliases;

use Class::ReluctantORM::SQL::Expression::Criterion;
use Class::ReluctantORM::SQL::Column;
use Class::ReluctantORM::SQL::Param;
use Class::ReluctantORM::SQL::Table;
use Class::ReluctantORM::SQL::From::Join;
use Class::ReluctantORM::SQL::From::Relation;


=head1 CONSTRUCTORS

=cut

=head2 $from = From->new($rel);

Creates a new FROM clause using the given Class::ReluctantORM::SQL::From::Relation as the base.

=cut

sub new {
    my $class = shift;
    my $rel = shift;
    unless (blessed($rel) && $rel->isa(Relation)) { Class::ReluctantORM::Exception::Param::WrongType->croak(expected => Relation, value => $rel); }
    if (@_) { Class::ReluctantORM::Exception::Param::Spurious->croak(); }

    my $self =  bless { root => $rel }, $class;
    return $self;
}

=begin devnotes

=head2 $from = From->_new_from_with($with);

Creates a new FROM clause by parsing the given fully populated 'with' structure
(as provided by fetch_deep).

=cut

sub _new_from_with {
    my $class = shift;
    my $tb_class = shift;
    my $with = shift;
    my $counter = 0;

    if ($DEBUG > 2) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - in _new_From_with, have with:\n" . Dumper($with); }

    # Prime the pump with the base table
    my $rel = Table->new($tb_class);
    $rel->alias('t0');
    my $self = $class->new($rel);
    $with->{__upper_table} = $rel;

    if (__is_empty($with)) {
        return $self;
    } else {
        $self->{alias_counter} = 1; # zero was used for base table
        $self->{root} = $self->__build_rel_from_with_recursor($rel, $with);
        return $self;
    }

}

sub __is_empty {
    my $href = shift;
    # Ignore the special __upper_table key
    my @keys = grep { $_ ne '__upper_table' } keys %$href;
    return !@keys;
}

sub __build_rel_from_with_recursor {
    my $self = shift;
    my $lhs = shift;
    my $with = shift || {};

    if ($DEBUG > 2) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - in _brfw_recursor, have with:\n" . Dumper($with); }

    # Base case: with is empty
    if (__is_empty($with)) { return $lhs; }

    # Loop over relationships, boosting as we go
    foreach my $relname (keys %$with) {
        next if ($relname eq '__upper_table');
        my $opts = $with->{$relname};
        my $relationship = $opts->{relationship};

        $opts->{join_type} ||= $relationship->join_type();
        if ($DEBUG > 2) {
            print STDERR __PACKAGE__ . ':' . __LINE__ . " - working on relationship $relname\n";
            print STDERR __PACKAGE__ . ':' . __LINE__ . " - have relclass " .  ref($relationship) . "\n";
            print STDERR __PACKAGE__ . ':' . __LINE__ . " - have rel join depth " .  $relationship->join_depth . "\n";
        }

        if ($relationship->join_depth == 0) {
            # Do nothing - no join
        } elsif ($relationship->join_depth == 1) {
            $lhs = $self->__perform_single_join($lhs, $opts);
        } elsif ($relationship->join_depth == 2) {
            $lhs = $self->__perform_double_join($lhs, $opts);
        }
    }

    return $lhs;

}

sub __perform_single_join {
    my $self = shift;
    my $lhs = shift;
    my $opts = shift;
    my $relationship = $opts->{relationship};

    # Init the new right-hand table
    my $right_table = $relationship->remote_sql_table();
    $right_table->alias('t' . $self->{alias_counter}++);

    my $join_type = __normalize_join_type($opts->{join_type});

    # Find the nearest table that matches the one being linked against
    my $left_table = $lhs->_find_latest_table($relationship->local_sql_table);

    # Build join condition
    my $crit = __normalize_single_join_criterion($left_table, $right_table, $opts);

    # Boost left-hand side to be a join while recursing into the right side
    $opts->{with}->{__upper_table} = $right_table;
    $lhs = Join->new(
                     $join_type, 
                     $lhs,
                     $self->__build_rel_from_with_recursor($right_table, $opts->{with}),
                     $crit,
                    );
    $lhs->relationship($relationship);
    return $lhs;
}

sub __perform_double_join {
    my $self = shift;
    my $lhs = shift;
    my $opts = shift;
    my $relationship = $opts->{relationship};

    # Init the new right-hand table and mapping table
    my $right_table = $relationship->remote_sql_table();
    $right_table->alias('t' . $self->{alias_counter}++);

    my $join_table = $relationship->join_sql_table();
    $join_table->alias('t' . $self->{alias_counter}++);

    my $first_join_type = __normalize_join_type($opts->{join_type} || 'LEFT OUTER');
    my $second_join_type = 'INNER';

    my $second_crit = __make_join_criterion_on_keys(
                                                    [$relationship->join_remote_key_sql_columns],
                                                    $join_table,
                                                    [$relationship->remote_key_sql_columns],
                                                    $right_table,
                                                   );
    $opts->{with}->{__upper_table} = $right_table;
    my $second_join = Join->new(
                                $second_join_type,
                                $join_table,
                                $self->__build_rel_from_with_recursor($right_table, $opts->{with}),
                                $second_crit,
                               );


    # Find the nearest table that matches the one being linked against
    my $left_table = $lhs->_find_latest_table($relationship->local_sql_table);

    # Build first join condition
    my $first_crit  = __normalize_double_join_criterion($left_table, $second_join, $opts);

    # Double-Boost left-hand side to be a join while recursing into the right side
    $lhs = Join->new(
                     $first_join_type,
                     $lhs,
                     $second_join,
                     $first_crit,
                    );
    $lhs->relationship($relationship);
    return $lhs;

}

sub __normalize_join_type {
    my $type = shift;
    $type = uc($type) || 'INNER';
    if ($type =~ /RIGHT/) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(
                                                  error => 'Right joins not permitted',
                                                  param => 'join_type',
                                                  frames => 4,
                                                 );
    }
    if ($type =~ /OUTER/) {
        $type = 'LEFT OUTER';
    } elsif ($type =~ /CROSS/) {
        $type = 'CROSS';
    } elsif ($type =~ /NATURAL/) {
        $type = 'INNER'; # keys auto-detected if unspecified
    } else {
        $type = 'INNER';
    }

    return $type;
}

sub __normalize_single_join_criterion {
    my $left_table = shift;
    my $right_table = shift;
    my $join_opts = shift;
    my $rel = $join_opts->{relationship};

    if ($join_opts->{join_on}) {
        my $driver = $rel->linked_class->driver();
        my $where = $driver->parse_where($join_opts->{join_on});
        return $where->root_criterion;
    } else {
        return __make_join_criterion_on_keys([$rel->local_key_sql_columns], $left_table, [$rel->remote_key_sql_columns], $right_table);
    }
}

sub __normalize_double_join_criterion {
    my $left_table = shift;
    my $lower_rel = shift;
    my $join_opts = shift;
    my $rel = $join_opts->{relationship};

    if ($join_opts->{join_on}) {
        my $driver = $rel->linked_class->driver();
        my $where = $driver->parse_where($join_opts->{join_on});
        return $where->root_criterion;
    } else {
        my $join_table = $lower_rel->_find_latest_table($rel->join_sql_table);
        return __make_join_criterion_on_keys([$rel->local_key_sql_columns], $left_table, [$rel->join_local_key_sql_columns], $join_table);
    }
}

sub __make_join_criterion_on_keys {
    my $left_cols = shift;
    my $left_table = shift;
    my $right_cols = shift;
    my $right_table = shift;

    for my $col (@$left_cols) { $col->table($left_table); }
    for my $col (@$right_cols) { $col->table($right_table); }

    my $first_right = shift @$right_cols;
    my $first_left = shift @$left_cols;

    my $root = Criterion->new('=', $first_left, $first_right);

    foreach my $i (0..(@$left_cols - 1)) {
        $root = Criterion->new('AND', $root, Criterion->new('=', $left_cols->[$i], $right_cols->[$i]));
    }

    return $root;
}


=head1 ACCESSORS

=cut

=head2 @columns = $from->columns();

Returns a list of all available columns.  Will fail if not 
all of the relations know their columns.

=cut

sub columns { return shift->root_relation->columns(); }


=head2 @relationships = $from->relationships();

Returns a list of all known relationships in the FROM clause.

=cut

sub relationships {
    my $self = shift;
    my $rel = $self->root_relation();
    my @raw = $self->__rels_recursor($rel);
    # Remove undefs - hmm's have second joins without relationships
    return grep { defined($_) } @raw;
}

sub __rels_recursor {
    my $self = shift;
    my $rel = shift;
    if ($rel->is_leaf_relation) {
        return ();
    } elsif ($rel->is_join) {
        return ($rel->relationship(), $self->__rels_recursor($rel->left_relation), $self->__rels_recursor($rel->right_relation));
    } else {
        Class::ReluctantORM::Exception::Call::NotImplemented->croak();
    }
}

=head2 @joins = $from->joins()

Returns a list of any Joins present in the From clause.

=cut

sub joins {
    my $from = shift;
    return $from->root_relation->joins();
}

=head2 $str = $from->pretty_print();

Returns a human-readable string representation of the clause.  Not appropriate for use for feeding to a prepare() statement.

=cut

sub pretty_print {
    my $self = shift;
    my %args = @_;
    my $prefix = $args{prefix} || '';
    my $str = $prefix . "FROM\n";
    $str .= $self->root_relation->pretty_print(prefix => $prefix . '  ');
    return $str;
}


=head2 $rel = $from->root_relation();

Reads the relation that forms the root of the FROM tree, as a Class::ReluctantORM::SQL::From::Relation.

=cut

sub root_relation { return shift->{root}; }

=head2 @tables = $from->tables(%opts);

Returns a list of all referenced tables.  If a table is refered to more 
than once (due to self-joins), it will be present more than once, but their
aliases will differ.

Supported options:

=over

=item exclude_subqueries

Optional boolean, default false.  If true, tables mentioned only in subqueries will not be included.

=back


=cut

sub tables {
    my $from = shift;
    my %opts = check_args(args => \@_, optional => [qw(exclude_subqueries)]);
    return $from->root_relation->tables(%opts);
}

=head2 $clone = $join->clone();

Makes a deep copy of the Join object.  All SQL objects are clone()'d, but annotations (such as Relationships) are not.

=cut

sub clone {
    my $self = shift;
    my $class = ref $self;
    my $other = $class->new($self->{root}->clone());
    return $other;

}


=head1 AUTHOR

Clinton Wolfe January 2009

=cut

1;

