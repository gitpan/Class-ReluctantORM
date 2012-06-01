package Class::ReluctantORM::SQL::Column;

=head1 NAME

Class::ReluctantORM::SQL::Column - Represent a Column in a SQL statement

=head1 SYNOPSIS

  use Class::ReluctantORM::SQL::Aliases;

  # Make a column
  my $col1 = Column->new(); # undetermined name
  my $col2 = Column->new(column => 'my_col'); # undetermined table
  my $col3 = Column->new(column => 'my_col', table => Table->new()); # fully specified

  # Use a column in a Where clause criterion ('foo' = ?)
  my $crit = Criterion->new('=', Column->new(column => 'foo'), Param->new());
  my $where = Where->new($crit);
  $sql->where($where);
  my @cols = $where->columns();

  # Use a column in an OrderBy clause
  my $ob = OrderBy->new();
  $ob->add($col, 'DESC');
  my @cols = $ob->columns;

  # Use the column as an output column
  my $sql = SQL->new(...);
  $sql->add_output($col);


=head1 DESCRIPTION

Represents a database column in a SQL statement.  Used wehere you need to refer to a column, except for SELECT output columns (Which wraps Column in an OutputColumn to allow for an expression).

=cut

use strict;
use warnings;

use Data::Dumper;
use Scalar::Util qw(blessed);
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::Utilities qw(install_method);

use base 'Class::ReluctantORM::SQL::Expression';
our $DEBUG = 0;


=head1 CONSTRUCTORS

=cut

=head2 $col = Column->new();

=head2 $col = Column->new(column => $column_name, [alias => 'col_alias']);

=head2 $col = Column->new(column => $column_name, table => $table, [alias => 'col_alias']);

Makes a new Column object.

In the first form, the column's identity is undetermined.  You must call $col->column() before 
trying to render the SQL.

In the second and third forms, the columns name is provided.  Optionally provide an alias
for the column, which will be used in output and order_by roles.  If the second form is 
used, attempts will be made to disambiguate the column by looking for matching 
tables as the SQL statement is built.

In the third form, a reference to a Table object is provided, fully determining the column's identity.

=cut

sub new {
    my $class = shift;
    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;
    my %expected = map { $_ => 1 } qw(column table alias);
    my @extra = grep { !exists $expected{$_} } keys %args;
    if (@extra) { Class::ReluctantORM::Exception::Param::Spurious->croak(params => \@extra); }
    if ($args{table}  && !$args{column}) { Class::ReluctantORM::Exception::Param::Missing->croak("If table is provided, you must also provide column"); }

    my $self = bless {}, $class;
    $self->table($args{table});
    $self->column($args{column});
    $self->alias($args{alias});

    return $self;
}


=head1 ACCESSORS AND MUTATORS

=cut

=head2 $col_alias_name = $col->alias();

=head2 $col->alias($col_alias_name);

Reads or sets the column alias.

=cut

__PACKAGE__->mk_accessors(qw(alias));

=head2 @empty = $col->child_expressions();

Always returns an empty list.  Required by the Expression interface.

=cut

sub child_expressions { return (); }

=head2 $col_name = $col->column();

=head2 $col->column($col_name);

Reads or sets the case-insensitve column name.

=cut

__PACKAGE__->mk_accessors(qw(column));


=head2 $bool = $arg->is_column();

All objects of this class return true.  The class adds this method to its parent class, making all other subclasses of return false.

=cut

install_method('Class::ReluctantORM::SQL::Expression', 'is_column', sub { return 0; });
sub is_column { return 1; }


=head2 $bool = $col->is_leaf_expression();

Always returns true for this class.  Required by the Expression interface.

=cut

sub is_leaf_expression { return 1; }

=head2 $value = $col->output_value();

=head2 $col->output_value($value);

Reads or sets the output value of the column.  This only makes sense if the
column was used as an output column on a SQL query.  An undef should interpreted as NULL.

=cut

__PACKAGE__->mk_accessors(qw(output_value));

=head2 $str = $col->pretty_print();

Renders a human-readable representation of the Column.

=cut

sub pretty_print {
    my $self = shift;
    my %args = @_;
    if ($args{one_line}) {
        if ($self->alias) { return $self->alias; }
        if ($self->table) {
            my $t = $self->table;
            if ($t->alias) { return $t->alias . '.' . $self->column; }
            if ($t->schema) { return $t->schema . '.' . $t->table . '.' . $self->column; }
            return $t->table . '.' . $self->column;
        }
        return  $self->column;
    } else {
        return ($args{prefix} || '' ) . 'COLUMN ' . $self->pretty_print(one_line => 1) . "\n";
    }
}


=head2 $table = $col->table();

=head2 $col->table($table);

Reads or sets the Table object that the Column belongs to.  On set, the table is checked to 
confirm that the column named is indeed a column of that table.

=cut

sub table {
    my $self = shift;
    if (@_) {
        my $table = shift;
        if ($table) {
            unless (blessed($table) && $table->isa('Class::ReluctantORM::SQL::Table')) {
                Class::ReluctantORM::Exception::Param::WrongType->croak(
                                                           expected => 'Class::ReluctantORM::SQL::Table',
                                                           value => $table,
                                                           param => 'table',
                                                          );
            }

            # Confirm that the table has this as a column
            if ($table->knows_all_columns && $self->column && !$table->has_column($self->column)) {
                Class::ReluctantORM::Exception::Param::BadValue->croak(
                                                                       error => $self->column . " is not a column of table " . $table->table,
                                                                      );
            }
            $self->set('table', $table);
        }
    }
    return $self->get('table');
}

=head2 $clone = $col->clone()

Copies the column, by deeply cloning the table, and then directly copying the alias and column name.

=cut


sub clone {
    my $self = shift;
    my $class = ref $self;

    my $other = $class->new();

    foreach my $simple (qw(column alias)) {
        if ($self->$simple) {
            $other->$simple($self->$simple);
        }
    }

    foreach my $complex (qw(table)) {
        if ($self->$complex) {
            $other->$complex($self->$complex()->clone());
        }
    }

    return $other;
}

=head2 $bool = $param->is_equivalent($expr);

Returns true if $expr is a Column, with matching column name.  Alias is IGNORED.

If both columns have Tables, then the table name (and schema, if present) are compared.  The table aliases are IGNORED.  If only one column has a Table, that difference is IGNORED.

=cut

sub is_equivalent {
    my $left = shift;
    my $right = shift;
    unless ($right->is_column()) { return 0; }

    # If one column name is missing, both must be.
    if (!$left->column || !$right->column) {
        return (!$left->column && !$right->column);
    }
    # Otherwise column names must match, case insensitively.
    unless (uc($left->column()) eq uc($right->column())) { return 0; }

    # Don't check aliases.

    # Table checks.
    # If either table is missing, assume a match.
    if (!$left->table || !$right->table) { return 1; }

    # Both have tables.
    return $left->table->is_the_same_table($right->table());

}


=head1 AUTHOR

Clinton Wolfe

=cut

1;
