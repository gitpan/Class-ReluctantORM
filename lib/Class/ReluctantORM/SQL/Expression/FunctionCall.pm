package Class::ReluctantORM::SQL::Expression::FunctionCall;

=head1 NAME

Class::ReluctantORM::SQL::Expression::FunctionCall - Represent a function call

=head1 SYNOPSIS

  # Save yourself some typing
  use Class::ReluctantORM::SQL::Aliases;

  # This creates "REPLACE(mycol,'old','new')"
  my $fc0 = FunctionCall->new(
                              'replace',
                              Column->new(column =>'mycol',
                              'old',
                              'new',
                             );

  # Same thing
  my $fc0 = FunctionCall->new(
                              Function->by_name('replace'),
                              Column->new(column =>'mycol',
                              'old',
                              'new',
                             );

  # This creates '1=1'
  my $fc1 = FunctionCall->new('=', 1, 1);

  # This creates 'my_column = ?'
  my $fc2 = FunctionCall->new(
                             '=',
                             Column->new(column =>'my_column',
                             Param->new(),
                            );

  # Wrap $fc2 in NOT ( 'NOT my_column = ?' )
  my $fc3 = FunctionCall->new('NOT', $fc2);

  # Make '(1=1) AND (NOT (my_column = ?))'
  my $fc4 = FunctionCall->new('AND', $fc1, $fc3);

  # Dump a FunctionCall as a string (for diagnostics only - NOT RBMS safe)
  my $str = $fc->pretty_print();  # Verbose
  my $str = $fc->pretty_print(one_line => 1);

=head1 DESCRIPTION

Represents an actual call to a function, operator, or stored procedure.  Contains a single Function, and zero
or more Expressions that are used as arguments to the Function.

FunctionCalls are themselves Expressions, so they can be composed (nested).

=cut

use strict;
use warnings;

use Data::Dumper;
use Scalar::Util qw(blessed);
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::Utilities qw(install_method);
our $DEBUG = 0;

use Class::ReluctantORM::SQL::Aliases;
use Class::ReluctantORM::SQL::Function;
use Class::ReluctantORM::SQL::Expression::Literal;
use Class::ReluctantORM::SQL::Expression;

use base 'Class::ReluctantORM::SQL::Expression';

=head1 CONSTRUCTORS

=cut

=head2 $fc = SQL::Expression::FunctionCall->new($func, $exp1, [$exp2, ...]);

=head2 $fc = SQL::Expression::FunctionCall->new($funcname, $exp1, [$exp2,...]);

Creates a new FunctionCall.  The Function to be called 
must be specified, as well as all input expressions.

In the first form, the Function to be called is provided directly.

In the second form, the Function is specified by name, and a Function->by_name lookup is made on your behalf.

An exception will be thrown if the number of arguments does not 
match the operator's arity.

$expN is either a Class::ReluctantORM::SQL::Expression
subclass, or a plain scalar, or undef.  Scalars and undefs will 
be "autoboxed" into being Class::ReluctantORM::SQL::Expression::Literal
objects, with undefs becoming NULLs.

=cut

sub new {
    my $class = shift;
    my $func = shift;
    my @exps = @_;

    if (!ref($func)) {
        $func = Function->by_name($func);
    } elsif (!$func->isa(Function)) {
        Class::ReluctantORM::Exception::Param::WrongType->croak(
                                                                param => 'function',
                                                                value => $func,
                                                                error => "The first arg to new() must be either the name of a Function or a Function object",
                                                                expected => 'Class::ReluctantORM::SQL::Function',
                                                               );
    }

    my $funcname = $func->name();
    if (defined $func->max_inputs()) {
        unless (@exps <= $func->max_inputs()) {
            Class::ReluctantORM::Exception::Param::Spurious->croak(error => "The '$funcname' operator allows at most " . $func->max_inputs() . " arguments");
        }
    }
    unless (@exps >= $func->min_inputs()) {
        Class::ReluctantORM::Exception::Param::Missing->croak(error => "The '$funcname' operator requires at least " . $func->min_inputs() . " arguments");
    }

    my @boxed_exps = ();
    foreach my $exp (@exps) {
        if (!ref($exp)) {
            push @boxed_exps, Literal->new($exp);
        } elsif (! (blessed($exp) && $exp->isa('Class::ReluctantORM::SQL::Expression'))) {
            Class::ReluctantORM::Exception::Param::WrongType->croak(
                                                       error => "FunctionCall input expressions must be either plain scalars or Expressions",
                                                       param => 'exp',
                                                       value => $exp,
                                                       expected => 'Class::ReluctantORM::SQL::Expression',
                                                      );
        } else {
            push @boxed_exps, $exp;
        }
    }

    my $self = {
                function => $func,
                exps => \@boxed_exps,
               };
    bless $self, $class;

    foreach my $exp (@boxed_exps) { $exp->parent_expression($self);  }

    return $self;
}

=head2 $clone = $fc->clone();

Makes a new FunctionCall.  The arguments of the original are deeply cloned.  The Function itself will be re-used, since each Function type is a singleton.

=cut

sub clone {
    my $self = shift;
    my $class = ref $self;

    my @cloned_args = map { $_->clone(); } $self->arguments();

    my $other = $class->new(
                            $self->function(),
                            @cloned_args,
                           );

    return $other;
}


=head1 ACCESSORS

=cut


=head2 @exps = $fc->arguments();

Returns the input expressions of the function call.

=cut

sub arguments { return @{shift->{exps}}; }

=head2 @exps = $fc->child_expressions();

Returns the child nodes of this node (same as arguments()).  Required by the Expression interface.

=cut

sub child_expressions { return shift->arguments(); }

=head2 $bool = $arg->is_function_call();

All objects of this class return true.  The class adds this method to Expression, making all other subclasses of it return false.

=cut

install_method('Class::ReluctantORM::SQL::Expression', 'is_function_call', sub { return 0; });
sub is_function_call { return 1; }


=head2 $bool = $fc->is_leaf_expression();

Returns true if the number of arguments to the function call are zero.  Required by the Expression interface.

=cut

sub is_leaf_expression { return (shift->arguments() == 0); }


=head2 $func = $fc->function();

Returns the Function being referred to by the FunctionCall.

=cut

sub function { return shift->{function}; }

=head2 $str = $fc->pretty_print();

Renders a human-readable representation of the FunctionCall.

=cut

sub pretty_print {
    my $self = shift;
    my %args = @_;
    if ($args{one_line}) {
        return $self->__pp_brief(%args);
    } else {
        return $self->__pp_verbose(%args);
    }
}

sub __pp_brief {
    my $self = shift;
    my %args = @_;
    my $str = '';
    $str .= '(';
    $str .= join ',',  (
                        ("'" . $self->function->name . "'"),
                        map { $_->pretty_print(%args) } $self->arguments()
                       );
    $str .= ')';
    return $str;
}

sub __pp_verbose {
    my $self = shift;
    my %args = @_;
    my $prefix = $args{prefix} || '';
    my $str = $prefix . "FUNCTION_CALL '" .  $self->function->name . "'\n";
    my @args = $self->arguments;
    my $last_arg = pop @args;
    foreach my $arg (@args) {
        $str .= $arg->pretty_print(%args, prefix => $prefix . ' | ');
    }
    $str .= $last_arg->pretty_print(%args, prefix => $prefix . ' ` ');
    return $str;
}

=begin devdocs

=head2 $bool = $fc->is_equivalent($expr);

Returns true if the two criteria are certainly equivalent (does not check table or column aliases).

Returns false otherwise.

Buggy - has false negatives, but no known false positives.

=cut

sub is_equivalent {
    my $ca = shift;
    my $cb = shift;

    unless ($cb->is_function_call()) { return 0; }
    unless ($ca->function->name eq $cb->function->name) { return 0; }

    if ($ca->function->is_associative()) {
        # TODO - massaging (ie, flatten AND trees?)
    }

    my @args_a = $ca->arguments();
    my @args_b = $cb->arguments();
    unless (@args_a == @args_b) { return 0; }

    if (!$ca->function->is_commutative()) {
        # order must match
        for my $i (0..$#args_a) {
            unless ($args_a[$i]->is_equivalent($args_b[$i])) {
                return 0;
            }
        }
    } else {
        # Ugggggg this is a terrible approach
        my (%unmatched_a, %unmatched_b);
        for my $i (0..$#args_a) {
            $unmatched_a{$i} = $args_a[$i];
        }

      ARG_B:
        foreach my $arg_b (@args_b) {
            foreach my $arg_a_idx (keys %unmatched_a) {
                my $arg_a = $unmatched_a{$arg_a_idx};
                if ($arg_a->is_equivalent($arg_b)) {
                    delete $unmatched_a{$arg_a_idx};
                    next ARG_B;
                }
            }
            # If we get here, no match for arg_b
            return 0;
        }
    }

    return 1;

}




=head1 AUTHOR

Clinton Wolfe January 2010

=cut

1;
