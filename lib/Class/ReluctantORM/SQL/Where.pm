package Class::ReluctantORM::SQL::Where;

=head1 NAME

Class::ReluctantORM::SQL::Where - Represent SQL WHERE clauses

=head1 SYNOPSIS

  # Save yourself some typing
  use Class::ReluctantORM::SQL::Aliases;

  # This creates an "always true" where clause
  my $where = Where->new();

  # Build criteria using Criterion objects
  my $crit = Criterion->new(
                            '=',
                            Column->new(
                                        column => $column_name,
                                        table => $sql_table,
                                       ),
                            Param->new(),
                           );

  # You can make a new where clause....
  my $where2 = Where->new($crit);

  # Or add to an existing one
  $where->and($crit);
  $where->or($crit);

  # You can also make a where clause directly from a SQL string
  # by using your Driver
  my $driver = Ship->driver();
  my $parsed_where = $driver->parse_where(q(name LIKE '%Beard' AND leg_count < ?));

  # Interrogate a SQL::Where for info
  my @params  = $where->params();
  my @tables  = $where->tables();
  my @columns = $where->columns();

  # Walk the tree - see Class::ReluctantORM::SQL::Where::Criterion for details
  my $crit = $where->root_criterion;
  while ($crit) {
     ...
  }

  # Attach a where clause to a SQL object
  $sql->where($where);

=head1 DESCRIPTION

Represent a SQL where clause abstractly.

TODO DOCS

=head1 USAGE

Generally, you construct a Where object in one of two ways:

=over

=item parse it from a SQL string

=item build it using SQL::Expression::Criterion objects

=back

=cut

use strict;
use warnings;

use SQL::Statement;  # Extended by 
use Data::Dumper;

use Class::ReluctantORM::Exception;
use Scalar::Util qw(blessed);

our $DEBUG = 0;

use Class::ReluctantORM::Utilities qw(check_args);

use Class::ReluctantORM::SQL::Aliases;

use Class::ReluctantORM::SQL::Expression::Criterion;
use Class::ReluctantORM::SQL::Expression::Literal;
use Class::ReluctantORM::SQL::Expression::FunctionCall;
use Class::ReluctantORM::SQL::Column;
use Class::ReluctantORM::SQL::Param;
use Class::ReluctantORM::SQL::Table;
use Class::ReluctantORM::SQL::Parser;

=head1 CONSTRUCTORS

=cut

=head2 $where = SQL::Where->new();

=head2 $where = SQL::Where->new($crit);

Creates a new Where object.

In the first form, creates an "always true" where clause.  You can then safely add constraints using and() and or().

In the second form, creates a where clause whose root criterion will be $crit, a SQL::Where::Criterion.

=cut

sub new {
    my $class = shift;
    my $root  = shift;

    if ($root) {
        unless (blessed($root) && $root->isa(Criterion())) {
            Class::ReluctantORM::Exception::Param::WrongType->croak(expected => Criterion());
        }
    }

    my $self = bless {}, $class;

    $self->{orig_str} = '';

    $self->{root} = $root || Criterion->new_tautology();

    return $self;
}


=head1 CRITERIA-BUILDING METHODS

=cut

=head2 $where->and($crit);

Adds the given SQL::Where::Criterion, ANDing it against the root-level criterion and 
setting the new root criterion from the resulting operation.

In other words,  given 'a=b', if you then call and() with a criteria equivalent to 'c=d', 
you will get '(a=b) AND (c=d)', and the new root criterion will be the AND operation.  
This may then be repeated with and('e=f'), giving '((a=b) AND (c=d)) AND (e=f)'.

=cut

sub and {
    my $self = shift;
    $self->__and_or('AND', @_);
}

=head2 $where->or($crit);

Adds the given SQL::Where::Criterion, ORing it against the root-level criterion and 
setting the new root criterion from the resulting operation.

See and() for examples.

=cut

sub or {
    my $self = shift;
    $self->__and_or('OR', @_);
}

sub __and_or {
    my $self = shift;
    my $op    = shift;
    if (@_ > 1) { Class::ReluctantORM::Exception::Param::Spurious->croak(frames => 2);  }
    my $crit = shift;
    unless (blessed($crit) && $crit->isa(Criterion())) {
        Class::ReluctantORM::Exception::Param::WrongType->croak(
                                                   param => 'criterion',
                                                   value => $crit,
                                                   expected => Criterion(),
                                                   frames => 2,
                                                  );
    }

    $self->{root} = Criterion->new(
                                   $op,
                                   $self->{root},
                                   $crit,
                                  );

}


=head1 MUTATORS

=cut

=head2 $w->bind_params($val1, $val2,...);

Binds the given values to the parameters in the where clause.

=cut

sub bind_params {
    my $self = shift;
    my @vals = @_;
    my @params = $self->params();
    if (@vals < @params) {
        Class::ReluctantORM::Exception::Param::Missing->croak('The number of values must match the number of parameters in the where clause.');
    } elsif (@vals > @params) {
        Class::ReluctantORM::Exception::Param::Spurious->croak('The number of values must match the number of parameters in the where clause.');
    }
    for my $i (0..(@params - 1)) {
        $params[$i]->bind_value($vals[$i]);
    }
}


=head1 ACCESSORS

=cut

=head2 @columns = $where->columns();

Returns the current list of SQL::Column objects referenced in the Where.

=cut

sub columns {
    my $self = shift;
    my @columns = ();
    my $walker = sub {
        my $leaf = shift;
        if ($leaf->is_column) {
            push @columns, $leaf;
        }
    };
    $self->walk_leaves($walker);
    return @columns;
}


=head2 $table = $where->find_table($name_or_alias);

Checks to see if a given table name or alias has been used in the 
where clause, and if so, returns the corresonding Table object.

=cut

sub find_table {
    my $self = shift;
    my $table_or_alias = shift;
    my @tables = $self->tables();

    my @results = grep { $_->table eq $table_or_alias } @tables;
    if (@results > 1) {
        Class::ReluctantORM::Exception::SQL::AmbiguousReference->croak(
                                                           error => "'$table_or_alias' appears to refer to more than one table.",
                                                           referent => $table_or_alias,
                                                           statement => $self->{orig_str},
                                                          );
    }
    if (@results == 1) {
        return $results[0];
    }

    @results = grep { $_->alias eq $table_or_alias } @tables;
    if (@results > 1) {
        Class::ReluctantORM::Exception::SQL::AmbiguousReference->croak(
                                                           error => "'$table_or_alias' appears to refer to more than one table.",
                                                           referent => $table_or_alias,
                                                           statement => $self->{orig_str},
                                                          );
    }
    if (@results == 1) {
        return $results[0];
    }
    return undef;

}


=head2 @params = $where->params();

Returns the current list of SQL::Param objects embedded in the Where.
DBI placeholders get turned into Params.

=cut

sub params {
    my $where = shift;
    return $where->__params_recursor($where->root_criterion);
}

sub __params_recursor {
    my $where = shift;
    my $expr  = shift;
    if ($expr->is_leaf_expression()) {
        if ($expr->is_param()) {
            return $expr;
        } elsif ($expr->is_subquery()) {
            my $select = $expr->statement();
            return ($select->params());
        } else {
            return ();
        }
    } else {
        return (map { $where->__params_recursor($_) } $expr->child_expressions());
    }
}


=head2 $str = $where->pretty_print();

Returns a human-readable string representation of the clause.  Not appropriate for use for feeding to a prepare() statement.

=cut

sub pretty_print {
    my $self = shift;
    my %args = @_;
    my $prefix = $args{prefix} || '';
    my $str = $prefix . "WHERE\n";
    $str .= $prefix . $self->root_criterion->pretty_print(one_line => 1, prefix => $prefix . '  ') . "\n";
    return $str;
}

=head2 $crit = $where->root_criterion();

Returns the root Criterion of the where clause.

=cut

sub root_criterion {
    my $where = shift;
    if (@_) {
        Class::ReluctantORM::Exception::Call::NotMutator->croak();
    }
    return $where->{root};
}

=head2 @tables = $where->tables(%opts);

Returns the current list of SQL::Table objects referenced by the columns in criteria in the Where, as well as in subqueries.

Supported options:

=over

=item exclude_subqueries

Optional boolean, default false.  If true, tables mentioned only in subqueries will not be included.

=back

=cut

sub tables {
    my $self = shift;
    my %opts = check_args(args => \@_, optional => [qw(exclude_subqueries)]);

    my @tables = ();
    my $walker = sub {
        my $expr = shift;
        if ($expr->is_subquery() && !$opts{exclude_subqueries}) {
            push @tables, ($expr->statement->tables());
        } elsif ($expr->is_column() && defined $expr->table) {
            push @tables, $expr->table();
        }
    };
    $self->walk_leaves($walker);
    return @tables;
}

=head2 $where->walk_leaves($code_ref)

Traverses the Where tree, and executes the coderef on each leaf node.  
The coderef is passed the leaf as the one argument.  The leaf is guarenteed
to be a subclass of Class::ReluctantORM::SQL::Expression.

=cut

sub walk_leaves {
    my $self = shift;
    my $code = shift;
    return $self->__walk_leaves_recursor($self->{root}, $code);
}

sub __walk_leaves_recursor {
    my $self = shift;
    my $node = shift;
    my $code = shift;

    if ($node->is_leaf_expression) {
        $code->($node);
    } else {
        foreach my $child ($node->child_expressions) {
            $self->__walk_leaves_recursor($child, $code);
        }

    }
}

=head2 $clone = $w->clone()

Creates a new Where whose root criterion is a clone of the original's root.

=cut


sub clone {
    my $self = shift;
    my $class = ref $self;
    my $other = $class->new();

    $other->{root} = $self->{root}->clone();
    return $other;

}


1;

