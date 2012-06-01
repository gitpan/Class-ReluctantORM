package Class::ReluctantORM::SQL::SubQuery;

=head1 NAME

Class::ReluctantORM::SQL::SubQuery - Represent a sub-SELECT in a FROM or WHERE clause

=head1 SYNOPSIS

  use Class::ReluctantORM::SQL::Aliases;

  # Make a SELECT as usual
  my $select = SQL->new('select');
  $select->from(Table->new(table => 'mytable');
  $select->where(Criterion->new('=', 1,1));

  # Make a subquery
  my $subquery = SubQuery->new($select);

  # Use it as an expression
  my $in_crit = Criterion->new(
                               'IN',
                               'needle',
                               $subquery, # haystack
                              );


  # Or use it as a JOIN relation
  # TODO DOCS


=head1 DESCRIPTION

Wrapper around a SELECT statement, that implements both the Expression
interface as well as the Relation interface, allowing it to be used in
a WHERE clause or FROM clause.

=cut

use strict;
use warnings;

use Data::Dumper;
use Scalar::Util qw(blessed);
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::Utilities qw(install_method check_args);

our $DEBUG ||= 0;

use Scalar::Util qw(weaken);

use base 'Class::ReluctantORM::SQL::Expression';
use base 'Class::ReluctantORM::SQL::From::Relation';


=head1 CONSTRUCTOR

=cut

=head2 $sq = SubQuery->new($select);

Creates a new SubQuery containing the SQL object given by $select.  The SQL 
object's operation must be 'SELECT'.

=cut

sub new {
    my $class = shift;
    my $sql = shift;
    my $self = bless {}, $class;
    $self->statement($sql);
    return $self;
}

=head1 ACCESSORS

=cut

=head2 $sql = $sq->statement();

=head2 $sq->statement($sql);

Sets or reads the underlying SQL object.

=cut

sub statement {
    my $self = shift;
    unless (@_) {
        return $self->get('statement');
    }

    my $sql = shift;
    unless (blessed($sql) && $sql->isa('Class::ReluctantORM::SQL')) {
        Class::ReluctantORM::Exception::Param::WrongType->croak
            (
             param => 'sql',
             value => $sql,
             expected => 'Class::ReluctantORM::SQL',
            );
    }
    unless ($sql->operation eq 'SELECT') {
        Class::ReluctantORM::Exception::Param::BadValue->croak
            (
             param => 'sql',
             value => $sql,
             error => "SQL statement's operation() must be SELECT",
             expected => 'SELECT-type SQL statement',
            );
    }
    $self->set('statement', $sql);
}

=head2 $bool = $sq->is_subquery();

All objects of this class return true.  The class adds this method to both Expression and Relation, making all other subclasses of them return false.

=cut

install_method('Class::ReluctantORM::SQL::From::Relation', 'is_subquery', sub { return 0; });
install_method('Class::ReluctantORM::SQL::Expression', 'is_subquery', sub { return 0; });
sub is_subquery { return 1; }

#=======================================================#
#                    From Relation
#=======================================================#


=head2 $sq->alias('my_alias');

=head2 $alias = $sq->alias();

From Relation interface.

Reads or sets the alias used for this relation in FROM clauses.

=cut

__PACKAGE__->mk_accessors('alias');


=head2 $str = $sq->pretty_print();

From both Expression and Relation interfaces.

Renders a human-readable version of the relation to a string.

=cut

sub pretty_print {
    my $self = shift;
    my %args = @_;
    my $prefix = $args{prefix} || '';
    my $str = $prefix . 'SUBQUERY alias:' . ($self->alias() || '(none)') . "\n";
    $prefix .= '  ';
    $str .= $self->statement->pretty_print(%args, prefix => $prefix);
    return $str;
}


=head2 @cols = $sq->columns()

Returns a list of re-aliased columns returned by the subquery.  This presents the externally visible set of columns.

From Relation interface.

=cut

sub columns {
    my $self = shift;
    my $sq_alias = $self->alias();
    my @external_cols;
    foreach my $internal_output_col ($self->statement->output_columns) {
        my $ext_col = Column->new(
                                  table => $sq_alias,
                                  column => $internal_output_col->alias() || $internal_output_col->name(),
                                 );
        push @external_cols, $ext_col;
    }
    return @external_cols;
}

=head2 $bool = $sq->has_column('col_name')

Returns a boolean indicating whether a column is present in the external columns returned.  The name will be the re-aliased name.

From Relation interface.

=cut

sub has_column {
    my $self = shift;
    my $needle = shift;
    return grep { $_->name eq $needle } $self->columns;
}


=head2 $bool = $sq->knows_all_columns()

Returns a boolean indicating whether all output columns are known in advance from this relation.  Always returns true for SubQueries.

From Relation interface.

=cut

sub knows_all_columns { return 1; }

=head2 $bool = $sq->knows_any_columns()

Returns a boolean indicating whether any output columns are known in advance from this relation.  Always returns true.

From Relation interface.

=cut

sub knows_any_columns { return 1; }

=head2 @tables = $sq->tables(%opts);

Returns a list of all tables referenced in the FROM clause of the subquery.

From the Relation interface.

If the exclude_subqueries option is enabled, this returns an empty list.

=cut

sub tables {
    my $self = shift;
    my %opts = check_args(args => \@_, optional => [qw(exclude_subqueries)]);
    if ($opts{exclude_subqueries}) {
        return ();
    } else {
        return $self->statement()->from()->tables(%opts);
    }

}


#=======================================================#
#                    Conflicts
#=======================================================#


=head2 @rels = $sq->child_relations();

Always returns an empty list.  If you want to access the relations in the subquery, use $sq->statement->from->child_relations().

From the Relation interface.

=cut

sub child_relations { return (); }


=head2 $bool = $sq->is_leaf_relation();

Indicates if the object is a terminal point on the From tree.  Always returns true.

From the Relation interface.

=cut

sub is_leaf_relation { return 1; }

=head2 $rel = $sq->parent_relation();

Returns the parent node of the object.  If undefined, this is the root node.

From the Relation interface.

=cut

# Inherited


=head2 $bool = $sq->is_leaf_expression();

Indicates if the object is a terminal point on the Expression tree.  Always returns true.

=cut

sub is_leaf_expression { return 1; }

=head2 @exps = $sq->child_expressions();

Always returns an empty list.

=cut

sub child_expressions { return (); }

=head2 $exp = $sq->parent_expression();

Returns the parent node of the expression.  If undefined, this is the root node.

=cut

# Inherited from Expression

=head2 $clone = $sq->clone()

Creates a new SubQuery whose statement is a clone of the original's statement.

=cut

sub clone {
    my $self = shift;
    my $class = ref $self;
    return $class->new($self->statement->clone());
}


=head1 AUTHOR

Clinton Wolfe January 2010

=cut

1;


