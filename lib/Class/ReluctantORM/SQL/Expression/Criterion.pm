package Class::ReluctantORM::SQL::Expression::Criterion;

=head1 NAME

Class::ReluctantORM::SQL::Expression::Criterion - Represent WHERE and JOIN criteria

=head1 SYNOPSIS

  # Save yourself some typing
  use Class::ReluctantORM::SQL::Aliases;

  # This creates an "always true" criteria
  my $crit = Criterion->new_tautology();

  # This creates '1=1'
  my $crit1 = Criterion->new('=', 1, 1);

  # This creates 'my_column = ?'
  my $crit2 = Criterion->new(
                             '=',
                             Column->new(column =>'my_column',
                             Param->new(),
                            );

  # Wrap $crit2 in NOT ( 'NOT my_column = ?' )
  my $crit3 = Criterion->new('NOT', $crit2);

  # Make '(1=1) AND (NOT (my_column = ?))'
  my $crit4 = Criterion->new('AND', $crit1, $crit3);

  # Make a where clause with this as its root
  my $where = Where->new($crit4);

  # Or AND it to an existing one
  $where2->and($crit4);

  # Dump a Criterion as a string (for diagnostics only - NOT RBMS safe)
  my $str = $crit->pretty_print();  # Verbose
  my $str = $crit->pretty_print(one_line => 1);


=head1 DESCRIPTION

Represents a boolean-return predicate call, as needed in JOIN criteria and WHERE clauses.

This is a specialized subclass of a FunctionCall.

=cut

use strict;
use warnings;

use Data::Dumper;
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::Utilities qw(install_method);
our $DEBUG = 0;

use base 'Class::ReluctantORM::SQL::Expression::FunctionCall';

use Class::ReluctantORM::SQL::Aliases;
use Class::ReluctantORM::SQL::Expression::Literal;
use Class::ReluctantORM::SQL::Expression;




=head1 CONSTRUCTORS

=cut

=head2 $crit = SQL::Expression::Criterion->new();

=head2 $crit = SQL::Expression::Criterion->new($op_name, $exp1, [$exp2, ...]);

=head2 $crit = SQL::Expression::Criterion->new($function, $exp1, [$exp2, ...]);

Creates a new criterion.

In the first form, returns an always true criterion.

In the second and third form, creates a new criterion using the
specified operator and argument(s).  An exception
will be thrown if the number of arguments does not 
match the operator's arity.

$op is a string name of a Function.  Case is ignored.

$expN is either a Class::ReluctantORM::SQL::Expression
subclass, or a plain scalar, or undef.  Scalars and undefs will 
be "autoboxed" into being Class::ReluctantORM::SQL::Expression::Literal
objects, with undefs becoming NULLs.

=cut

sub new {
    my $class = shift;
    my $op = shift;
    my @args = @_;
    unless ($op) {
        return $class->new_tautology();
    }

    # Otherwise use FunctionCall constructor
    $class->SUPER::new($op, @args);

}

# clone() inherited from function call

=head2 $crit = SQL::Expression::Criterion->new_tautology();

Returns an 'always true' criterion.

=cut

sub new_tautology {
    my $class = shift;
    return $class->new('=', 1 , 1);
}

=head1 ACCESSORS

=cut


=head2 @args = $crit->arguments();

Returns the arguments of the criterion.

=cut

# From FunctionCall

=head2 @args = $crit->child_expressions();

Returns the child nodes of this node (same as arguments()).  Required by the Expression interface.

=cut

# From FunctionCall

=head2 $bool = $arg->is_criterion();

All objects of this class return true.  The class adds this method to Expression, making all other subclasses of it return false.

=cut

install_method('Class::ReluctantORM::SQL::Expression', 'is_criterion', sub { return 0; });
sub is_criterion { return 1; }

=head2 $bool = $crit->is_leaf_expression();

Always returns false for this class.  Required by the Expression interface.

=cut

sub is_leaf_expression { return 0; }


=head2 $func = $crit->function();

Returns the Function that represents the operator.

=cut

# From FunctionCall

=head2 $str = $crit->pretty_print();

Renders a human-readable representation of the Criterion.

=cut

# From FunctionCall

sub __pp_verbose {
    my $self = shift;
    my %args = @_;
    my $prefix = $args{prefix} || '';
    my $str = $prefix . "CRITERION '" .  $self->function->name() . "'\n";
    my @args = $self->arguments;
    my $last_arg = pop @args;
    foreach my $arg (@args) {
        $str .= $arg->pretty_print(%args, prefix => $prefix . ' | ');
    }
    $str .= $last_arg->pretty_print(%args, prefix => $prefix . ' ` ');
    return $str;
}

=head2 $bool = $crit->is_equivalent($other_crit);

Returns true if the two criteria are certainly equivalent (does not check table or column aliases).

Returns false otherwise.

=cut

sub is_equivalent {
    my $left = shift;
    my $right = shift;

    unless ($right->is_criterion) { return 0; }
    return $left->Class::ReluctantORM::SQL::Expression::FunctionCall::is_equivalent($right);
}



=head1 AUTHOR

Clinton Wolfe January 2009, January 2010

=cut

1;
