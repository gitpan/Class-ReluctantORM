package Class::ReluctantORM::SQL::Expression;

=head1 NAME

Class::ReluctantORM::SQL::Expression - Base class for SQL expressions

=head1 DESCRIPTION

Abstract base class for SQL expressions.  Often used as arguments to Functions, as sources for OutputColumns, and in other places.

Useful known subclasses:

=over

=item Class::ReluctantORM::SQL::Column

=item Class::ReluctantORM::SQL::Param

=item Class::ReluctantORM::SQL::Expression::Criterion

=item Class::ReluctantORM::SQL::Expression::Literal

=item Class::ReluctantORM::SQL::Expression::FunctionCall

=item Class::ReluctantORM::SQL::SubQuery

=back

=cut

use strict;
use warnings;

use Data::Dumper;
use Class::ReluctantORM::Exception;
our $DEBUG ||= 0;

use Scalar::Util qw(weaken);

use base 'Class::Accessor::Fast';


=head1 VIRTUAL METHODS

All of these methods are intended to be overridden in subclasses.  Some methods 
provide a default implementation.

=cut

=head2 $bool = $arg->is_leaf_expression();

Indicates if the object is a terminal point on the Expression tree.  Default implementation returns true.

=cut

sub is_leaf_expression { return 1; }

=head2 @args = $arg->child_expressions();

Returns any children of the expression.  Results only defined if is_leaf is false.

=cut

sub child_expressions { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $arg = $arg->parent_expression();

Returns the parent node of the expression.  If undefined, this is the root node.

=cut

sub parent_expression {
    my $self = shift;
    if (@_) {
        my $real = shift;
        my $weak_ref = \$real;
        weaken($weak_ref);
        $self->set('parent_ref', $weak_ref);
    }
    my $ref = $self->get('parent_ref');
    if ($ref) {
        return ${$ref};
    } else {
        return undef;
    }
}

=head2 $str = $arg->pretty_print();

Returns a human-readable string representation of the expression.

=cut

sub pretty_print { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $rel->walk_leaf_expressions($coderef);

Recurses throughout the expression tree, and executes the coderef on each leaf of the expression.

The coderef will be passed the leaf expression as the only parameter.

=cut

sub walk_leaf_expressions {
    my $expr = shift;
    my $coderef = shift;
    if ($expr->is_leaf_expression()) {
        $coderef->($expr);
    } else {
        foreach my $child ($expr->child_expressions()) {
            $child->walk_leaf_expressions($coderef);
        }
    }
}

=begin devdocs

=head2 $bool = $exp->is_equivalent($other);

Returns true if the $other expression is equivalent to this one.

Buggy - returns false negatives.

=cut

sub is_equivalent { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

sub __break_links {
    my $expr = shift;

    # We maintain links both ways - parent to child and child to parent.  Break them.
    foreach my $cexpr (grep { defined($_) } $expr->child_expressions) {
        $cexpr->__break_links();
    }
    $expr->set('parent_ref', undef);
}


=head1 AUTHOR

Clinton Wolfe January 2009

=cut

1;
