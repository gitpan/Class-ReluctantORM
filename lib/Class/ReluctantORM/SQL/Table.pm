package Class::ReluctantORM::SQL::Table;

=head1 NAME

Class::ReluctantORM::SQL::Table - Represent a Table in a SQL statement

=head1 SYNOPSIS

  use Class::ReluctantORM::SQL::Aliases;

  # TODO DOCS - synopsis is way out of date

  my $table = Class::ReluctantORM::SQL::Table->new(table => $table, schema => $schema);
  my $table = Class::ReluctantORM::SQL::Table->new($cro_class);

  # Now use $table in other Class::ReluctantORM::SQL operations

  $string = $driver->render_aliased_table($table);
  $string = $driver->render_table_alias_definition($table);

=head1 DESCRIPTION

Represents a database table in a SQL statement.  Inherits from Class::ReluctantORM::SQL::From::Relation .

=cut

use strict;
use warnings;

use Class::ReluctantORM::Exception;
use Data::Dumper;

use Class::ReluctantORM::Utilities qw(install_method check_args);
our $DEBUG ||= 0;

use base 'Class::ReluctantORM::SQL::From::Relation';

use Class::ReluctantORM::SQL::Aliases;

use Class::ReluctantORM::SQL::Column;


=head1 CONSTRUCTORS

=cut

=head2 $table = Table->new($cro_class);

=head2 $table = Table->new(table => 'table_name');

=head2 $table = Table->new(table => 'table_name', schema => 'schema_name');

Creates a new Table reference.  In the first form, the 
Table will learn its identity from the given Class::ReluctantORM class.  This is the 
preferred approach, as it allows the table to know what columns it has, etc.

In the second and third forms, the table is identified by an explicit table name.

=cut

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    if (@_ == 1) {
        my $table_class = shift;
        #unless ($table_class->isa('Class::ReluctantORM')) {
        #    Class::ReluctantORM::Exception::Param::WrongType->croak(param => 'class', expected => 'Class::ReluctantORM');
        #}
        $self->class($table_class);
    } else {
        my %args = check_args(args => \@_, required => [qw(table)], optional => [qw(schema columns alias)]);
        $self->table($args{table});
        $self->schema($args{schema});
        $self->alias($args{alias});
        if ($args{columns}) {
            unless (ref($args{columns}) eq 'ARRAY') {
                Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak(param => "columns");
            }
        }
        $self->set('manual_columns', $args{columns});
    }
    return $self;
}

=head1 ACCESSORS AND MUTATORS

=cut

=head2 $table->alias(...);

=head2 $table->has_column(...);

=head2 $table->columns(...);

=head2 $table->tables(...);

=head2 $table->knows_any_columns(...);

=head2 $table->knows_all_columns(...);

=head2 $table->pretty_print(...);

These methods are inherited from Relation.

=cut

=head2 @empty = $table->child_relations();

Always returns an empty list.  Required by the Relation interface.

=cut

sub child_relations { return (); }


=head2 $table->class($cro_class);

=head2 $class = $table->class();

Reads or sets the Class::ReluctantORM class that this Table represents.  
If setting, the table name and scheme name will be overwritten.

=cut

sub class {
    my $self = shift;
    if (@_) {
        my $table_class = shift;
        $self->set('class', $table_class);
        $self->table($table_class->table_name);
        $self->schema($table_class->schema_name);
    }
    return $self->get('class');
}

=head2 $bool = $table->is_leaf_relation();

Always returns true for this class.  Required by the Relation interface.

=cut

sub is_leaf_relation { return 1; }

=head2 $bool = $rel->is_table();

All objects of this class return true.  The class add this method to its parent class, making all other subclasses of return false.

=cut

install_method('Class::ReluctantORM::SQL::From::Relation', 'is_table', sub { return 0; });
sub is_table { return 1; }

=head2 $table->schema('schema_name');

=head2 $name = $table->schema();

Reads or sets the schema name.

=cut

__PACKAGE__->mk_accessors(qw(schema));

=head2 $table->table('table_name');

=head2 $name = $table->table();

Reads or sets the table name.

=cut

__PACKAGE__->mk_accessors(qw(table));

sub tables { my @result = (shift); return @result; }

sub knows_all_columns {
    my $self = shift;
    return defined($self->class());
}

sub knows_any_columns {
    my $self = shift;
    return $self->class() || $self->get('manual_columns');
}

sub _copy_manual_columns {
    my $table1 = shift;
    my $table2 = shift;
    my @manuals;
    foreach my $col ($table2->columns) {
        push @manuals, Column->new(table => $table1, column => $col->column);
    }
    $table1->set('manual_columns', \@manuals);
}

sub columns {
    my $self = shift;
    if ($self->class) {
        return map { Column->new(column => $_, table => $self) } $self->class->column_names;
    } elsif ($self->get('manual_columns')) {
        return map { Column->new(column => $_, table => $self) } @{$self->get('manual_columns')};
    } else {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak("Cannot call 'columns' when neither the class of the table nor manual columns are known");
    }
}

=head2 @cols = $t->primary_key_columns()

Returns a list of SQL Column objects that represent the columns that make up the primary key on the table.  You can only call this if $t->knows_all_columns is true; otherwise, you'll get an exception.

=cut

sub primary_key_columns {
    my $self = shift;
    unless ($self->knows_all_columns) { Class::ReluctantORM::Exception::Call::NotPermitted->croak('Cannot call columns when knows_all_columns is false'); }
    return map { Column->new(column => $_, table => $self) } $self->class->primary_key_columns;
}


sub has_column {
    my $self = shift;
    my $col_name = shift;
    my %existing;
    if ($self->class) {
        %existing = map { uc($_) => 1 } $self->class->column_names();
    } elsif ($self->get('manual_columns')) {
        %existing = map { uc($_) => 1 } @{$self->get('manual_columns')};
    } else {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak('Cannot call has_columns when knows_all_columns is false');
    }

    return exists($existing{uc($col_name)});
}

sub pretty_print {
    my $self = shift;
    my %args = @_;
    my $prefix = $args{prefix} || '';
    my $str = $prefix . 'TABLE ';
    $str .= $self->class ? ('(' . $self->class . ') ') : '';
    $str .= $self->schema ? ($self->schema . '.') : '';
    $str .= $self->table;
    $str .= $self->alias ? ( ' AS ' . $self->alias) : '';
    $str .= "\n";
    return $str;
}

=head2 $str = $t->display_name()

Returns a string suitable for display to the user.  Used in exception messages.

=cut

sub display_name {
    my $self = shift;
    my $str = '';
    $str .= $self->schema ? ($self->schema . '.') : '';
    $str .= $self->table;
    $str .= $self->alias ? ( ' AS ' . $self->alias) : '';
    return $str;
}

=head2 $bool = $table1->is_the_same_table($table2, <$check_aliases>);

Returns true if $table1 and $table2 refer to the same schema name and table name.

If $check_aliases is provided and true, the two Tables must be using the same table alias.

=cut

sub is_the_same_table {
    my $table1 = shift;
    my $table2 = shift;
    unless ($table2) { return; }
    my $check_aliases = shift;
    my $aliases_match =
      $check_aliases ? 
        ($table1->alias() && $table2->alias() && ($table1->alias() eq $table2->alias())) :
          1;

    # If we know both schemae, compare them; otherwise assume they match
    my $schemas_match =
      ($table1->schema() && $table2->schema()) ?
        ($table1->schema() eq $table2->schema()) :
          1;

    # Must know both names and they must match, or must know neither
    my $table_names_match =
      ($table1->table() && $table2->table() && ($table1->table() eq $table2->table())) ||
        (!$table1->table() && !$table2->table());

    return $aliases_match && $schemas_match && $table_names_match;

}

=head2 $clone = $t->clone();

Makes a new Table object, copying over the name, alias, schema, and class of the original.

=cut

sub clone {
    my $self = shift;
    my $class = ref $self;

    my $other;
    if ($self->class) {
        $other = $class->new($self->class);
    } else {
        $other = $class->new(table => $self->table);
        if ($self->schema) {
            $other->schema($self->schema());
        }
    }

    if ($self->alias) {
        $other->alias($self->alias);
    }

    return $other;
}


=head1 AUTHOR

Clinton Wolfe

=cut


1;
