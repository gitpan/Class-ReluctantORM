package Class::ReluctantORM::SQL::Function;
use strict;
use warnings;

=head1 NAME

 Class::ReluctantORM::SQL::Function - Represent a SQL function, aggregator, stored proc, or operator

=head1 SYNOPSIS

 use Class::ReluctantORM::SQL::Aliases;

 # Explicit lookup....
 my $eq = Function->by_name('='); # not case senstive
 my $min_arity = $eq->min_inputs; # 2 for '='


 # Usually used implicitly....
 # This automatically looks up the '=' function by name
 my $crit = Criterion->new('=', $column, $param);
 my $crit2 = Criterion->new($func, $column, $param);

 # Looks up '=' implicitly
 my $fc1 = FunctionCall->new('=', $column, $param);
 my $fc2 = FunctionCall->new($func, $column, $param);

 # Register new custom functions
 Function->register(
  name => 'froobulate',
  min_inputs => 2,
  max_inputs => 43,
  is_associative => 1,
  is_cummutative => 1,
 );

 # Now...
 my $crit = Criterion->new('froobulate', $column, $param);

 # Note: your driver must know how to render froobulate!

 # List functions
 my @funcs = Function->list_all_functions();
 my @funcs = Function->list_default_functions();
 my @funcs = Function->list_aggregate_functions();

=head1 DESCRIPTION

Registry for functions, operators, and stored procedures, so that
they may be represented in an abstract SQL tree.

Each individual function is treated as a singleton by name; so if you request (explicitly or implicitly) the '=' operator two different times, you will get the same object both times.

=head2 Default Function Kit

A fair number of functions are pre-registered, including = < > AND OR NOT.  You can get the complete list by calling Function->list_default_functions().

=head2 A Function is Not a Function Call

A Function object represents _which_ function is being referred to.  To actually call a function in an abstract SQL tree, create a FunctionCall object.

=cut

use base 'Class::Accessor::Fast';
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::Utilities qw(check_args);

our %REGISTRY;

=head1 INSTANCE RETRIEVAL ("CONSTRUCTOR")

=cut

=head2 $f = Function->by_name('name');

Searches for a function with the name given, and returns it.  Throws an exception if no such function has been registered.

The search is case-insensitive.

=cut

sub by_name {
    my $class = shift;
    my $name = shift;
    if (ref($class)) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak('by_name may only be called as a class method');
    }

    unless ($name) {
        Class::ReluctantORM::Exception::Param::Missing->croak(param => 'name');
    }

    $name = uc($name);
    unless (exists $REGISTRY{$name}) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'name', value => $name, error => "No such Function with name '$name'");
    }

    return $REGISTRY{$name};

}

=head2 $bool = Function->is_registered('name');

Returns true if the function name is registered.

=cut

sub is_registered {
    my $class = shift;
    my $name = shift;
    if (ref($class)) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak('by_name may only be called as a class method');
    }

    unless ($name) {
        Class::ReluctantORM::Exception::Param::Missing->croak(param => 'name');
    }

    $name = uc($name);
    return exists($REGISTRY{$name});
}


=head1 OTHER CLASS METHODS

=cut

=head2 $f = Function->register(%options);

Registers a new Function.  After this, you can explicitly or implicitly refer to the function by name.

Options:

=over

=item name

Required string, may be symbols.  Must be unique - no other registered Function may have the same name.

=item min_inputs

Required positive integer.  Minimum number of arguments the function takes.

=item max_inputs

Optional positive integer.  If not provided, Function is assumed to have no limit.

=item is_aggregate

Optional boolean, default false.  If true, marks this function as an aggregrate function.

=item is_associative

Optional boolean, default false.  If true, indicates CRO can re-group multiple invocations of this function.  So, (1+2)+3 = 1+(2+3).

=item is_commutative

Optional boolean, default false.  If true, indicates CRO can re-order arguments of this function.  So, 1+2 = 2+1.

=back

=cut

sub register {
    my $class = shift;
    my %args = check_args(
                          args => \@_,
                          required => [qw(name min_inputs)],
                          optional => [qw(max_inputs is_aggregate is_associative is_commutative)],
                         );

    if (ref($class)) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak('register may only be called as a class method');
    }

    $args{is_aggregate}   ||= 0;
    $args{is_associative} ||= 0;
    $args{is_commutative} ||= 0;

    $args{name} = uc($args{name});
    if (exists $REGISTRY{$args{name}}) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'name', value => $args{name}, error => "A Function with name '$args{name}' already is registered");
    }

    my $func = bless {}, $class;
    foreach my $attr (qw(name min_inputs max_inputs is_aggregate is_associative is_commutative)) {
        $func->set($attr, $args{$attr});
    }
    $func->set('is_default', 0);

    # These are singletons.  Permanently caching them is appropriate, so do not weaken this ref.
    $REGISTRY{$func->name()} = $func;
    return $func;
}

=head2 @funcs = Function->list_all_functions();

Returns an array of all registered functions.

=cut

sub list_all_functions { return values %REGISTRY; }

=head2 @funcs = Function->list_default_functions();

Returns an array of all functions that are provided by default, and guarenteed to be renderable by all drivers.

=cut

sub list_default_functions { return grep { $_->is_defualt() } values %REGISTRY; }

=head2 @funcs = Function->list_aggregate_functions();

Returns an array of all aggregate functions.

=cut

# This gets hammered on, so cache return
our @AGGREGATES;
sub list_aggregate_functions {
    unless (@AGGREGATES) {
        @AGGREGATES = grep { $_->is_aggregate() } values %REGISTRY;
    }
    return @AGGREGATES;
}

=head1 INSTANCE METHODS

=cut

=head2 $str = $f->name()

Returns the name of the Function.  Always available and always unique.

=cut

__PACKAGE__->mk_ro_accessors('name');

=head2 $int = $f->min_inputs()

Returns the minimum number of inputs the function accepts.  Always available.

=cut

__PACKAGE__->mk_ro_accessors('min_inputs');

=head2 $int = $f->max_inputs()

Returns the maximum number of inputs the function accepts.  May be undef, 
in which case the function has no upper limit on the number of inputs.  May also be zero, as for NOW().

=cut

__PACKAGE__->mk_ro_accessors('max_inputs');

=head2 $bool = $f->is_default()

Returns true if the function is part of the default kit, which all drivers are required to be able to render.  Returns false if the Function was registered by you.

=cut

__PACKAGE__->mk_ro_accessors('is_default');

=head2 $bool = $f->is_aggregate()

Returns true if the function is an aggregate function, like COUNT or MAX.  These functions may only be used in output column expressions, and alter the semantics of the query.

=cut

__PACKAGE__->mk_ro_accessors('is_aggregate');

__PACKAGE__->mk_ro_accessors('is_associative');

__PACKAGE__->mk_ro_accessors('is_commutative');

=head2  $same = $func->clone();

Sine each function is a singleton, it doesn't make sense to clone them.  This method thus returns the original, and is provided for consistency with other SQL objects.

=cut

sub clone {
    my $self = shift;
    # Function is a singleton - return the same thing
    return $self;
}

#===================================================#
#                 Default Functions
#===================================================#

our @DEFAULTS = (
                 {name => 'AND',    min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 1 },
                 {name => 'OR',     min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 1 },
                 {name => 'NOT',    min_inputs => 1, max_inputs => 1, is_associative => 0, is_commutative => 0 },
                 {name => '=',      min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 1 },
                 {name => '<>',     min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 1 },
                 {name => '>',      min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 0 },
                 {name => '<',      min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 0 },
                 {name => '>=',     min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 0 },
                 {name => '<=',     min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 0 },
                 {name => 'IS',     min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 1 },
                 {name => 'IS NOT', min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 1 },
                 {name => 'LIKE',   min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 1 },
                 {name => 'ILIKE',  min_inputs => 2, max_inputs => 2, is_associative => 1, is_commutative => 1 },
                 {name => 'EXISTS', min_inputs => 1, max_inputs => 1, is_associative => 0, is_commutative => 0 },
                 {name => 'REPLACE',min_inputs => 3, max_inputs => 3, is_associative => 0, is_commutative => 0 },

                 {name => '+', min_inputs => 2, max_inputs => undef, is_associative => 1, is_commutative => 1 },
                 {name => '-', min_inputs => 2, max_inputs => 2, is_associative => 0, is_commutative => 0 },

                 {name => 'KEY_COMPOSITOR_INSIDE_SUBQUERY', min_inputs => 1, max_inputs => undef},
                 {name => 'KEY_COMPOSITOR_OUTSIDE_SUBQUERY', min_inputs => 1, max_inputs => undef},

                 # Aggregates
                 {name => 'COUNT',  min_inputs => 1, max_inputs => 1, is_aggregate => 1 },
                 {name => 'MAX',    min_inputs => 1, max_inputs => 1, is_aggregate => 1 },
                 {name => 'MIN',    min_inputs => 1, max_inputs => 1, is_aggregate => 1 },
                 {name => 'AVG',    min_inputs => 1, max_inputs => 1, is_aggregate => 1 },
                 {name => 'STDDEV', min_inputs => 1, max_inputs => 1, is_aggregate => 1 },
                 {name => 'SUM',    min_inputs => 1, max_inputs => 1, is_aggregate => 1 },
                );
foreach my $def (@DEFAULTS) {
    my $f = __PACKAGE__->register(%$def);
    $f->set('is_default', 1);
}

1;


