package Class::ReluctantORM::SQL::Expression::Literal;

=head1 NAME

Class::ReluctantORM::SQL::Where::Literal - Represent literals in WHEREs

=head1 SYNOPSIS

  # Save yourself some typing
  use Class::ReluctantORM::SQL::Aliases;

  # Make a literal for some reason.
  my $lit_num       = Literal->new(1);
  my $lit_str       = Literal->new('foo');
  my $lit_empty_str = Literal->new('');
  my $lit_null      = Literal->new(undef);
  my $clone         = $other_lit->clone();

  # These are shortcut constructors
  $null  = Literal->NULL();
  $true  = Literal->TRUE();
  $false = Literal->FALSE();
  $blip  = Literal->EMPTY_STRING();

  # This throws an exception - to force proper use of NULL semantics
  eval { Literal->new();  };

  # Criterion provides auto-boxing
  my $crit = Criterion->new('=', 1, 1);

  # Same thing
  my $crit = Criterion->new('=', Literal->new(1), Literal->new(1));

=head1 DESCRIPTION

It's not likely you'll need to interact directly with this module.  It is used to
simply provide a consistent interface for literal arguments in WHERE clauses.

=head1 DATA TYPE SUPPORT

There is very rudimentary support for data types.  Since perl is very loosely typed, and so is the DBI placeholder system, there's not much sense in building a strongly typed SQL object model.

Its current purpose is to simply distinguish Boolean values from string or numeric values.

Data types are represented by an all-caps string.  Literal will use BOOLEAN, NULL, STRING, and NUMBER by default, but if you pass in other values, it won't complain.

=cut

use strict;
use warnings;

use Data::Dumper;
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::Utilities qw(install_method);
use Scalar::Util qw(looks_like_number);

use base 'Class::ReluctantORM::SQL::Expression';
our $DEBUG = 0;

# heh
our $ONE_TRUE_TRUE  = 'TRUE';
our $ONE_TRUE_FALSE = 'FALSE';
our @FALSINESS = (
                  qr/^FALSE$/i,
                  qr/^F$/i,
                  qr/^-1$/,
                  qr/^#F$/i,
                 );


=head1 PREFAB CONSTRUCTORS

These constructors represent Literals that are common or awkward to specify.  Their value should be obvious.

=over

=item $lit = Literal->FALSE()

=item $lit = Literal->TRUE()

=item $lit = Literal->NULL()

=item $lit = Literal->EMPTY_STRING()

=back

=cut

sub FALSE { return __PACKAGE__->new(0, 'BOOLEAN'); }
sub TRUE  { return __PACKAGE__->new(1, 'BOOLEAN'); }
sub NULL  { return __PACKAGE__->new(undef, 'NULL'); }
sub EMPTY_STRING { return __PACKAGE__->new('', 'STRING'); }

=head1 GENERIC CONSTRUCTOR

=cut

=head2 my $lit = Literal->new($value);

=head2 my $lit = Literal->new($value, $data_type);

Creates a new Literal with the given value.  $value is required.  Pass a literal undef
to get a Literal that represents NULL.

The optional second parameter is an all-caps string representing the data type.  You may send any value here.  If not provided, it will be guessed as one of NULL, STRING, or NUMERIC (using Scalar::Util::looks_like_number()).

=cut

sub new {
    my $class = shift;
    unless (@_) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'value'); }

    my $val = shift;
    my $data_type = shift;
    unless ($data_type) {
        if (!defined($val)) {
            $data_type = 'NULL';
        } elsif (looks_like_number($val)) {
            $data_type = 'NUMBER';
        } else {
            $data_type = 'STRING';
        }
    }
    my $self = bless { value => $val, data_type => uc($data_type) }, $class;

    if ($self->data_type eq 'BOOLEAN') {
        $self->__normalize_boolean_value();
    }
    return $self;
}

sub __normalize_boolean_value {
    my $self = shift;
    my $v = $self->value();
    if ($v) {
        if (grep { $v =~ $_ } @FALSINESS) {
            $self->value($ONE_TRUE_FALSE);
        } else {
            $self->value($ONE_TRUE_TRUE);
        }
    } else {
        # '', 0
        $self->value($ONE_TRUE_FALSE);
    }
}

=head1 ACCESSORS AND MUTATORS

=cut


=head2 @empty = $crit->child_expressions();

Always returns an empty list.  Required by the Argument interface.

=cut

sub child_expressions { return (); }

=head2 $bool = $arg->is_literal();

All objects of this class return true.  The class add this method to Expression, making all other subclasses of it return false.

=cut

install_method('Class::ReluctantORM::SQL::Expression', 'is_literal', sub { return 0; });
sub is_literal { return 1; }


=head2 $bool = $crit->is_leaf_expression();

Always returns true for this class.  Required by the Argument interface.

=cut

sub is_leaf_expression { return 1; }


=head2 $str = $col->pretty_print();

Renders a human-readable representation of the Literal.

=cut

sub pretty_print {
    my $self = shift;
    my %args = @_;
    if ($args{one_line}) {
        my $val = $self->value();
        my $dt  = $self->data_type();
        if ($dt eq 'NULL') {
            $val = 'NULL';
        } elsif ($dt eq 'STRING') {
            $val = "'" . $val . "'";
        }
        return $val . ':' . $dt;
    } else {
        return ($args{prefix} || '' ) . 'LITERAL ' . $self->pretty_print(one_line => 1) . "\n";
    }
}

=head2 $clone = $lit->clone();

Makes a new Literal with the same boxed value as the original.

=cut

sub clone {
    my $self = shift;
    my $class = ref $self;
    return $class->new($self->value(), $self->data_type());
}


=head2 $val = $lit->value();

Returns the enclosed value.  Keep in mind that undef represents NULL.

You may need to check the data type to confirm that you have the right thing.  For example, a Literal->FALSE->value() will return 0.

=cut

__PACKAGE__->mk_accessors('value');

=head2 $str = $lit->data_type();

Returns an all-caps string representing the datatype.

=cut

__PACKAGE__->mk_accessors('data_type');

=head2 $bool = $lit->is_equivalent($expr);

Returns true if $expr is a Literal, with matching data_type and value.

=cut

sub is_equivalent {
    my $left = shift;
    my $right = shift;
    unless ($right->is_literal()) { return 0; }
    unless ($left->data_type() eq $right->data_type()) { return 0; }

    my $dt = $left->data_type();
    if (0) { # formatting
    } elsif ($dt eq 'NULL') {
        return ((!defined($left->value())) && (!defined($right->value())));
    } elsif ($dt eq 'NUMBER') {
        return ($left->value() == $right->value());
    } else {
        # May have some nasty string coercions....
        return ($left->value() eq $right->value());
    }

}


=head1 AUTHOR

Clinton Wolfe January 2009

=cut

1;
