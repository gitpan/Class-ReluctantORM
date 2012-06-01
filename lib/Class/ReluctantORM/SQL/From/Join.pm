package Class::ReluctantORM::SQL::From::Join;

=head1 NAME

Class::ReluctantORM::SQL::From::Join - Represent a JOIN in a SQL statement

=head1 SYNOPSIS

  use Class::ReluctantORM::SQL::Aliases;

  # Make three kinds of joins
  my $join1 = Join->new('INNER', $left_rel, $right_rel, $criterion);
  my $join2 = Join->new('LEFT OUTER', $left_rel, $right_rel, $criterion);
  my $join3 = Join->new('CROSS', $left_rel, $right_rel, $criterion);

  # Make a tree of joins - (a INNER JOIN b) INNER JOIN c
  my $join4 = Join->new('INNER', $table_a, $table_b, $criterion);
  my $join5 = Join->new('INNER', $join4, $table_c, $criterion);

  # Use it in a FROM clause
  my $from = From->new($join5);


=head1 DESCRIPTION

Represents a JOIN in a SQL statement.  Inherits from Class::ReluctantORM::SQL::From::Relation .

Each JOIN has two children, a left relation and a right relation.  
In addition, there is a Criterion that represents the join condition, and a type that represents the JOIN type.

RIGHT OUTER joins are not supported.  Transform them into LEFT OUTERs.

NATURAL joins are not supported, because the Criterion must be explicit.

=cut

use strict;
use warnings;

use Class::ReluctantORM::Exception;
use Data::Dumper;
use Class::ReluctantORM::Utilities qw(install_method check_args);
use Scalar::Util qw(blessed);

our $DEBUG ||= 0;

use base 'Class::ReluctantORM::SQL::From::Relation';
use Class::ReluctantORM::SQL::Aliases;
use Class::ReluctantORM::SQL::Column;
use Class::ReluctantORM::SQL::Table;


=head1 CONSTRUCTORS

=cut

=head2 $join = Join->new($type, $left_rel, $right_rel, $crit, [$relationship]);

Creates a new Join.

$type must be one of INNER, LEFT OUTER, or CROSS.

$left_rel and $right_rel are Relation subclasses (this includes 
Tables, Joins, and SubQueries).

$crit is a Criterion specifying the join condition(s).

$relationship is an optional Relationship.  This is used as a hint when resolving ambiguities in the SQL, and is optional.

=cut

sub new {
    my $class = shift;
    if (@_ < 4) { Class::ReluctantORM::Exception::Param::Missing->croak(); }
    if (@_ > 4) { Class::ReluctantORM::Exception::Param::Spurious->croak(); }

    my $self = bless {}, $class;
    $self->type(shift);
    $self->left_relation(shift);
    $self->right_relation(shift);
    $self->criterion(shift);
    $self->relationship(shift);

    return $self;
}

=head2 $clone = $join->clone();

Makes a deep copy of the Join object.  All SQL objects are clone()'d, but annotations (such as Relationships) are not.

=cut

sub clone {
    my $self = shift;
    my $class = ref $self;
    my $other = $class->new(
                            $self->type(),
                            $self->left_relation()->clone(),
                            $self->right_relation()->clone(),
                            $self->criterion()->clone(),
                           );
    $other->relationship($self->relationship());
    return $other;
}

=head1 ACCESSORS AND MUTATORS

=cut

=head2 $join->alias(...);

=head2 $join->has_column(...);

=head2 $join->columns(...);

=head2 $join->tables();

=head2 $join->knows_any_columns(...);

=head2 $join->knows_all_columns(...);

=head2 $join->pretty_print(...);

These methods are inherited from Relation.

=cut


=head2 @rel = $join->child_relations();

Returns a two-element array with the left and right relations.  Required by the Relation interface.

=cut

sub child_relations {
    my $self = shift;
    return ($self->left_relation, $self->right_relation);
}

=head2 $join->criterion($crit);

=head2 $crit = $join->criterion();

Reads or sets the join condition as a Class::ReluctantORM::SQL::Where::Criterion .

=cut

__PACKAGE__->mk_accessors(qw(criterion));

=head2 $bool = $join->is_leaf_relation();

Always returns false for this class.  Required by the Relation interface.

=cut

sub is_leaf_relation { return 0; }

=head2 $bool = $rel->is_join();

All objects of this class return true.  The class adds this method to its parent class, making all other subclasses of return false.

=cut

install_method('Class::ReluctantORM::SQL::From::Relation', 'is_join', sub { return 0; });
sub is_join { return 1; }


=head2 $join->left_relation($rel);

=head2 $rel = $join->left_relation();

Reads or sets the left-hand relation of the join condition a Class::ReluctantORM::SQL::From::Relation .

=cut

sub left_relation {
    my $self = shift;
    return $self->__relation_accessor('left', @_);
}

=head2 $r = $join->relationship();

=head2 $join->relationship($relationship);

Reads or sets auxiliary relationship data, a Class::ReluctantORM::Relationship.

=cut

__PACKAGE__->mk_accessors(qw(relationship));

=head2 $join->right_relation($rel);

=head2 $rel = $join->right_relation();

Reads or sets the right-hand relation of the join condition a Class::ReluctantORM::SQL::From::Relation .

=cut

sub right_relation {
    my $self = shift;
    return $self->__relation_accessor('right', @_);
}
sub __relation_accessor {
    my $self = shift;
    my $side = shift;
    $side .= '_relation';

    if (@_) {
        my $rel = shift;
        unless (blessed($rel) && $rel->isa(Relation)) { Class::ReluctantORM::Exception::Param::WrongType->croak(expected => Relation, frames => 2, value => $rel); }
        $rel->parent_relation($self);
        $self->set($side, $rel);
    }
    return $self->get($side);

}

=head2 $join->type($type);

=head2 $type = $join->type();

Reads or sets the join type - one of INNER, LEFT OUTER, or CROSS.

=cut

our %JOIN_TYPES = map { $_ => 1 } ('INNER', 'LEFT OUTER', 'CROSS');

sub type {
    my $self = shift;
    if (@_) {
        my $type = uc(shift);
        unless (exists $JOIN_TYPES{$type}) {
            Class::ReluctantORM::Exception::Param::BadValue->croak(
                                                      error => 'Type must be one of ' . (join ', ', keys %JOIN_TYPES),
                                                      param => 'type',
                                                      value => $type,
                                                     );
        }
        $self->set('type', $type);
    }
    return $self->get('type');
}

sub knows_all_columns {
    my $self = shift;
    return $self->left_relation->knows_all_columns && $self->right_relation->knows_all_columns;
}

sub knows_any_columns {
    my $self = shift;
    return $self->left_relation->knows_any_columns || $self->right_relation->knows_any_columns;
}

sub tables {
    my $self = shift;
    my %opts = check_args(args => \@_, optional => [qw(exclude_subqueries)]);
    return ($self->left_relation->tables(%opts), $self->right_relation->tables(%opts));
}

sub columns {
    my $self = shift;
    unless ($self->knows_any_columns) { Class::ReluctantORM::Exception::SQL::AmbiguousReference->croak('Cannot call columns when knows_any_columns is false'); }
    return ($self->left_relation->columns, $self->right_relation->columns);
}

sub has_column {
    my $self = shift;

    unless ($self->knows_any_columns) { Class::ReluctantORM::Exception::SQL::AmbiguousReference->croak('Cannot call has_columns when knows_any_columns is false'); }
    my $col_name = shift;

    return $self->left_relation->has_column($col_name) || $self->right_relation->has_column($col_name);

}

sub pretty_print {
    my $self = shift;
    my %args = @_;
    my $prefix = $args{prefix} || '';
    my $str = $prefix . $self->type . ' JOIN ON ' . $self->criterion->pretty_print(one_line => 1) . "\n";
    $str .= $self->left_relation->pretty_print(prefix => $prefix . ' | ');
    $str .= $self->right_relation->pretty_print(prefix => $prefix . ' ` ');
    return $str;
}


sub __break_links {
    my $rel = shift;

    # We maintain links both ways - parent to child and child to parent.  Break them.
    foreach my $crel ($rel->child_relations) {
        $crel->__break_links();
    }
    $rel->set('parent_ref', undef);
    $rel->criterion->__break_links();
}


=head1 AUTHOR

Clinton Wolfe

=cut


1;
