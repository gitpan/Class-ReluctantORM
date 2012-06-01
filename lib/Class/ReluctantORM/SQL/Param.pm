package Class::ReluctantORM::SQL::Param;

=head1 NAME

Class::ReluctantORM::SQL::Param - Represent a placeholder in a SQL statement

=head1 SYNOPSIS

  use Class::ReluctantORM::SQL::Aliases;

  # Make a placeholder
  my $param = Param->new();

  # Set and read a value to the param
  $param->bind_value('foo');
  $param->bind_value(undef);  # This binds NULL
  my $val =  $param->bind_value();

  # Use the param in a Where criterion ('foo = ?')
  my $crit = Criterion->new('=', Column->new(column => 'foo'), $p);


=head1 DESCRIPTION

Represents a placeholder in a SQL statement.

=cut

use strict;
use warnings;

use Data::Dumper;
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::Utilities qw(install_method);
use Scalar::Util qw(looks_like_number);

use base 'Class::ReluctantORM::SQL::Expression';
our $DEBUG = 0;

=head1 CONSTRUCTOR

=cut

=head2 $p = Param->new();

=head2 $p = Param->new($value);

=head2 $p = Param->new(undef);

Makes a new param object.

In the first form, no value is bound.

In the second form, the given value is bound.

In the third form, the NULL value is bound.

=cut

sub new {
    my $class = shift;
    my $self = bless {}, $class;

    if (@_) {
        $self->bind_value(@_);
    }

    return $self;
}



=head1 ACCESSORS and MUTATORS

=cut

=head2 $v = $p->bind_value();

=head2 $p->bind_value($value);

=head2 $p->bind_value(undef);

Reads or sets the value used in Driver parameter binding.

In the first form, the value, if any, is returned.  An undefined value is ambiguous; 
use has_bind_Value to detect a bind value.

In the second form, the bind value is set to the scalar provided.

In the third form, the bind value is set to undef, which means it will be interpreted by the Driver as NULL.

=cut

sub bind_value {
    my $self = shift;
    if (@_) {
        $self->has_bind_value(1);
        my $value = shift;
        unless (!defined($value) || !ref($value)) {
            Class::ReluctantORM::Exception::Param::WrongType->croak(
                                                       error => 'bind_Value must be a either a scalar or undef',
                                                       expected => 'scalar',
                                                       value => $value,
                                                      );
        }
        if (@_) {
            Class::ReluctantORM::Exception::Param::Spurious->croak();
        }
        $self->set('bind_value', $value);
    }
    return $self->get('bind_value');
}


=head2 @empty = $p->child_expressions();

Always returns an empty list.  Required by the Expression interface.

=cut

sub child_expressions { return (); }

=head2 $bool = $p->has_bind_value()

If true a bind value has been set.  Don't
rely on bind_value(), as undef is a valid value.

=cut

__PACKAGE__->mk_accessors(qw(has_bind_value));

=head2 $bool = $arg->is_param();

All objects of this class return true.  The class add this method to its parent class, making all other subclasses of return false.

=cut

install_method('Class::ReluctantORM::SQL::Expression', 'is_param', sub { return 0; });
sub is_param { return 1; }


=head2 $bool = $p->is_leaf_expression();

Always returns true for this class.  Required by the Expression interface.

=cut

sub is_leaf_expression { return 1; }

=head2 $str = $param->pretty_print();

Renders a human-readable representation of the Param.

=cut

sub pretty_print {
    my $self = shift;
    my %args = @_;
    if ($args{one_line}) {
        my $str = '?';
        if ($self->has_bind_value) {
            $str .= '(bind:';
            my $val = $self->bind_value();
            if (!defined($val)) {
                $str .= 'NULL';
            } elsif (looks_like_number($val)) {
                $str .= $val;
            } else {
                $str .= "'" . $val . "'";
            }
            $str .= ')';
        }
        return $str;
    } else {
        return ($args{prefix} || '' ) . 'PARAM ' . $self->pretty_print(one_line => 1) . "\n";
    }
}

=head2 $clone = $p->clone();

Creates a new Param, copying the bound value of the original if it had one.

=cut

sub clone {
    my $self = shift;
    my $class = ref $self;
    if ($self->has_bind_value) {
        return $class->new($self->bind_value());
    } else {
        return $class->new();
    }
}

=head2 $bool = $param->is_equivalent($expr);

Returns true if $expr is a Param, with matching has_bind_value() and value.

=cut

sub is_equivalent {
    my $left = shift;
    my $right = shift;
    unless ($right->is_param()) { return 0; }

    if ($left->has_bind_value()) {
        unless ($right->has_bind_value()) { return 0; }
        my ($lbv, $rbv) = ($left->bind_value(), $right->bind_value());
        return (
                (!defined($lbv) && !defined($rbv)) # both undef
                ||
                ((defined($lbv) && defined($rbv)) && ($lbv == $rbv)) # both defined and equal
               );
    } else {
        return !$right->has_bind_value();
    }
}


=head1 AUTHOR

Clinton Wolfe

=cut

1;
