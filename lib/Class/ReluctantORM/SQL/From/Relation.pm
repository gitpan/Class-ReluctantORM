package Class::ReluctantORM::SQL::From::Relation;

=head1 NAME

Class::ReluctantORM::SQL::From::Relation - Base class for SQL relations

=head1 DESCRIPTION

Abstract base class to represent a SQL relation.

Known subclasses:

=over

=item Class::ReluctantORM::SQL::Table

=item Class::ReluctantORM::SQL::From::Join

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

=head2 $rel->alias('my_alias');

=head2 $alias = $rel->alias();

Reads or sets the alias used for this relation in SQL.

=cut

__PACKAGE__->mk_accessors('alias');

=head2 @args = $arg->child_relations();

Returns any children of the object.  Results only defined if is_leaf is false.

=cut

sub child_relations { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 @cols = $rel->columns()

Returns a boolean indicating whether a column is present in this relation.  Only valid if knows_columns() is true.

No default implementation provided.

=cut

sub columns { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $bool = $rel->has_column('col_name')

Returns a boolean indicating whether a column is present in this relation.  Only valid if knows_columns() is true.

No default implementation provided.

=cut

sub has_column { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $bool = $arg->is_leaf_relation();

Indicates if the object is a terminal point on the From tree.  Default implementation returns true.

=cut

sub is_leaf_relation { return 1; }

=head2 $bool = $rel->knows_all_columns()

Returns a boolean indicating whether all output columns are known in advance from this relation.

No default implementation provided.

=cut

sub knows_all_columns { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $bool = $rel->knows_any_columns()

Returns a boolean indicating whether any output columns are known in advance from this relation.

No default implementation provided.

=cut

sub knows_any_columns { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }


=head2 $rel = $rel->parent_relation();

Returns the parent node of the object.  If undefined, this is the root node.

=cut

sub parent_relation {
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
        return;
    }
}

=head2 $str = $rel->pretty_print();

Renders a human-readable version of the relation to a string.

=cut

sub pretty_print { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 @tables = $rel->tables(%opts);

Returns a list of all tables referenced in the relation and its children.

Supported options:

=over

=item exclude_subqueries

Optional boolean, default false.  If true, tables mentioned only in subqueries will not be included.

=back

=cut

sub tables { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $table = $rel->leftmost_table();

Finds the "base" table, the one added earliest.  This will return either a Table or a SubQuery, but never a Join.

=cut

sub leftmost_table {
    my $rel = shift;
    until (!$rel->is_join()) {
        $rel = $rel->left_relation();
    }
    return $rel;
}

=begin devnotes

=head2 $table = $rel->_find_latest_table($seek_table);

Performs a right-branch-first search of the relation tree, looking for a table that matches the schema name and table name of the given argument.  Alias is ignored.

This finds the last table of that name to be added.

=cut

sub _find_latest_table {
    my $rel = shift;
    my $seek = shift;
    if ($rel->__matches_table($seek)) { return $rel; }
    foreach my $kid (reverse $rel->child_relations) {
        my $result = $kid->_find_latest_table($seek);
        if ($result) { return $result; }
    }
    return;
}

=begin devnotes

=head2 $table = $rel->_find_earliest_table($seek_table);

Performs a left-branch-first search of the relation tree, looking for a table that matches the schema name and table name of the given argument.  Alias is ignored.

This finds the first table of that name to be added.

=cut


sub _find_earliest_table {
    my $rel = shift;
    my $seek = shift;
    if ($rel->__matches_table($seek)) { return $rel; }
    foreach my $kid ($rel->child_relations) {
        my $result = $kid->_find_earliest_table($seek);
        if ($result) { return $result; }
    }
    return;
}



sub __matches_table {
    my $table = shift;
    unless ($table->is_table) { return 0; }
    my $seek = shift;
    if (1 # for formatting
        && $seek->schema
        && $table->schema
        && $table->schema eq $seek->schema
       ) {
        return $table->table eq $seek->table;
    }
    return $table->table eq $seek->table;
}

=head2 $rel->walk_leaf_relations($coderef);

Recurses throughout the relation tree, and executes the coderef on each leaf of the relation.

The coderef will be passed the leaf relation as the only parameter.

=cut

sub walk_leaf_relations {
    my $rel = shift;
    my $coderef = shift;
    if ($rel->is_leaf_relation()) {
        $coderef->($rel);
    } else {
        foreach my $child ($rel->child_relations()) {
            $child->walk_leaf_relations($coderef);
        }
    }
}


=head2 @joins = $rel->joins()

Returns a list of any Joins present in the children of this Relation.

=cut

sub joins {
    my ($rel) = @_;
    unless ($rel->is_join()) { return (); }
    return ($rel, map { $_->joins() } $rel->child_relations());
}

sub __break_links {
    my $rel = shift;

    # We maintain links both ways - parent to child and child to parent.  Break them.
    foreach my $crel ($rel->child_relations) {
        $crel->__break_links();
    }
    $rel->set('parent_ref', undef);
}


=head1 AUTHOR

Clinton Wolfe January 2009

=cut

1;
