package Class::ReluctantORM::SQL;

=head1 NAME

Class::ReluctantORM::SQL - Represent SQL Statements

=head1 SYNOPSIS

  use Class::ReluctantORM::SQL::Aliases;

  # Insert
  my $insert = Class::ReluctantORM::SQL->new('insert');
  $insert->table(Table->new(table => 'table_name'));

  # TODO DOCS

  $sql->table(Table->new($tb_class);
  $sql->add_input($sql_column);
  $sql->add_output($sql_column);

=head1 DESCRIPTION

Represent SQL DML statements (INSERT, SELECT, UPDATE, and DELETE) in an abstract, driver-independent way.  Class::ReluctantORM uses this suite of classes to construct each statement that it executes, then passes it to the Driver for rendering and execution.  Results are then stored in the SQL object, and may be retrieved directly or inflated into Class::ReluctantORM objects.

=head2 Clauses, Relations, and Expressions

The SQL objects are loosely grouped into 4 categories:

=over

=item Statement - Class::ReluctantORM::SQL

Represents a DML SQL statement, its parameters and bindings, and output columns and fetched values.

Provides a location for the clauses, whether as strings or as objects.

=item Clauses - Where, From, OrderBy, Limit

Represents major portions of the statement.  These clauses are independent objects which are built separately, then attached to the SQL statment object.

=item Relations - Table, Join, SubQuery

Represents a table-like entity.  Relations share a common superclass (Class::ReluctantORM::SQL::Relation), know about their columns, and are used in From clauses.

=item Expressions - Literal, FunctionCall, Column, Param

Represents an expression, which may be used in numerous locations.

=back


=head2 Retrieving and Inflating Results

Some SQL statement objects can have OutputColumn objects associated with them (this includes all SELECT statments, and INSERT and UPDATE statements with RETURNING clauses).  As results are retrieved, the values are stored in these OutputColumns.

If the statement is expected to only have one row of results, you can simply do this:

  $driver->run_sql($sql);
  foreach my $oc ($sql->output_columns) {
    # do something with $oc->output_value();
  }

If the statement is expected to return multiple rows, you should register a callback:

  my $handle_fetchrow = sub {
     my $sql = shift;
     foreach my $oc ($sql->output_columns) {
       # do something with $oc->output_value();
     }
  };
  $sql->add_fetchrow_listener($handle_fetchrow);
  $driver->run_sql($sql)

If you are seeking Class::ReluctantORM model objects (like Ships and Pirates), you need to use the inflation facility:

  if ($sql->is_inflatable()) {
     @ships = $sql->inflate();
  } else {
     # Too complex
  }

=head2 Parsing Support

Parsing support is provided by the Driver area.  See Class::ReluctantORM::Driver.

=head2 Non-Parsed SQL Support

If you perform a query with 'parse_sql' false (or set that as a global default, see Class::ReluctantORM - Non-Parsed SQL Support), the SQL object still acts as the data object and provides execution and fetching services.  Instead of populating the where attribute (which is expected to be a Where object), populate the raw_where attribute (which is expected to be a string, the SQL WHERE clause).

You may build your SQL object out of a mix of objects and raw SQL, though this is less likely to work.

Support is eventually planned for there to be a rw_from, raw_ordeR_by, raw_group_by, and raw_statement.  For now, only raw_where is supported.



=begin devdocs

also provide raw_order_by raw_from raw_group_by and raw_statement

=cut

=head2 Annotate and Reconcile

After constructing a SQL object, it will usually need some additional metadata associated with it before being executed.  This metadata can generally be discovered automatically.

The annotate() method is called internally (usually before an inflate()) to associate table references with classes in your model.

The reconcile() method is called internally before the rendering process to ensure that all column and table references are resolvable and unambiguous.

=head2 Auto-Aliasing of SQL Classes

Because the class names tend to get rather long, this module by default 
exports subroutines whose return value is the name of a SQL class.  For example:

  Table() # returns 'Class::ReluctantORM::SQL::Table';

This allows you to do this:

  my $table = Table->new(...);

This functionality is very similar to that provided by the 'aliased' CPAN module, 
but here is provided automatically.

=head2 Limitations

This is not a general purpose SQL abstraction library, but it is close.
Operations that are not supported by Class::ReluctantORM will generally not be well supported by this module.

In particular:

=over

=item DML only

No support for data definition language (CREATE TABLE, etc) is planned.

=item Single-table INSERTs, UPDATEs, and DELETEs

There is no support for UPDATE ... FROM, for example.

=item Aggregate Support is in its infancy

Aggregates are not supported in combination with JOINs.

=back

=cut

use strict;
use warnings;
our $DEBUG ||=2;

use Data::Dumper;
use Scalar::Util qw(blessed);
use Class::ReluctantORM::Utilities qw(check_args);
use Class::ReluctantORM::FetchDeep::Results qw(fd_inflate);

use base 'Class::ReluctantORM::OriginSupport';
use base 'Class::Accessor::Fast';

use Class::ReluctantORM::SQL::Aliases;

use Class::ReluctantORM::SQL::Column;
use Class::ReluctantORM::SQL::Expression::Criterion;
use Class::ReluctantORM::SQL::Expression;
use Class::ReluctantORM::SQL::From;
use Class::ReluctantORM::SQL::Function;
use Class::ReluctantORM::SQL::Expression::FunctionCall;
use Class::ReluctantORM::SQL::From::Join;
use Class::ReluctantORM::SQL::Expression::Literal;
use Class::ReluctantORM::SQL::OrderBy;
use Class::ReluctantORM::SQL::OutputColumn;
use Class::ReluctantORM::SQL::Param;
use Class::ReluctantORM::SQL::From::Relation;
use Class::ReluctantORM::SQL::SubQuery;
use Class::ReluctantORM::SQL::Table;
use Class::ReluctantORM::SQL::Where;


=head1 CONSTRUCTORS

=cut

=head2 $sql = SQL->new('operation');

Creates a new abstract SQL object. Operation must be one of 
INSERT, UPDATE, DELETE, or SELECT.  Case is ignored.

=cut

our %OPERATIONS = map {uc($_) => 1} qw(select update delete insert);

sub new {
    my $class = shift;
    my $op = shift;
    unless ($op) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'operation'); }
    unless (exists $OPERATIONS{uc($op)}) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'operation', value => uc($op));
    }

    my $self = bless {}, $class;
    $self->set('operation', uc($op));
    $self->{outputs} = [];
    $self->{inputs} = [];
    $self->{fetchrow_listeners} = [];
    $self->{reconcile_options} =
      {
       add_output_columns => 1,
       realias_raw_sql => 1,
      };

    $self->__set_unique_alias_prefix();
    $self->table_alias_counter(0);
    $self->column_alias_counter(0);
    $self->capture_origin();

    return $self;
}

# Internal
__PACKAGE__->mk_accessors(qw(unique_alias_prefix));
sub __set_unique_alias_prefix {
    my $self = shift;
    # Derive a unique prefix from the memory address of $self
    # using the last 4 digits of the address
    my ($address) = "$self" =~ /0x.+([a-f0-9]{4})\)$/;
    $self->unique_alias_prefix('_' . $address . '_');
}

=head1 ACCESSORS AND MUTATORS

=cut

=head2 $sql->add_input($col, $param);

Adds an input column to the statement.  Valid only for 
insert and update operations.

Arguments are the SQL::Column that should get the value stored to it, 
and the SQL::Param that will carry the value.

=cut

sub add_input {
    my $self = shift;
    my $col = shift;
    my $param = shift;

    my %permitted = map {uc($_) => 1} qw(update insert);

    unless (exists $permitted{$self->operation}) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak('add_input is only permitted for UPDATE and INSERT operations');
    }
    unless (blessed($col) && $col->isa('Class::ReluctantORM::SQL::Column')) {
        Class::ReluctantORM::Exception::Param::WrongType->croak(param => 'column', expected => 'Class::ReluctantORM::SQL::Column');
    }

    unless ($self->input_subquery) {
        unless (blessed($param) && $param->isa('Class::ReluctantORM::SQL::Param')) {
            Class::ReluctantORM::Exception::Param::WrongType->croak(param => 'param', expected => 'Class::ReluctantORM::SQL::Param');
        }
    }

    push @{$self->{inputs}}, {column => $col, param => $param};

    return 1;

}

=head2 $oc = $sql->add_output($output_column);

=head2 $oc = $sql->add_output($column);

=head2 $oc = $sql->add_output($expression);

Adds an output column to the statement.  Valid only for 
insert, select and update operations.

In the first form, an OutputColumn you have constructed is added to the list directly.

In the second and third forms, the argument is first wrapped in a new OutputColumn object, then added.  Note that a Column is a subclass of Expression, so this is really the same usage.

The (possibly new) OutputColumn is returned.

=cut

sub add_output {
    my $self = shift;
    my $oc = shift;

    if (blessed($oc) && $oc->isa(Expression)) {
        $oc = OutputColumn->new($oc);
    } elsif (!(blessed($oc) && $oc->isa(OutputColumn))) {
        Class::ReluctantORM::Exception::Param::WrongType->croak(param => 'expression', expected => Expression, error => "need an Expression or a OutputColumn object");
    }

    my %permitted = map {uc($_) => 1} qw(update insert select);

    unless (exists $permitted{$self->operation}) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak('add_output is only permitted for SELECT, UPDATE and INSERT operations');
    }
    push @{$self->{outputs}}, $oc;

    return $oc;
}

=head2 $sql->remove_all_outputs();

Removes all output columns from the SQL statement.

=cut

sub remove_all_outputs {
    my $self = shift;
    $self->{outputs} = [];
}

# Internal
__PACKAGE__->mk_accessors(qw(table_alias_counter));

# Internal SQL-to-Driver linkage
__PACKAGE__->mk_accessors(qw(_sth _sql_string _execution_driver));

=head2 $str = $sql->new_table_alias();

Get a table alias that is certainly unique within this SQL statement, and probaby unique accross substatements (and superstatments, if you will).

=cut

sub new_table_alias {
    my $self = shift;
    my $counter = $self->table_alias_counter($self->table_alias_counter() + 1);
    my $pfx = $self->unique_alias_prefix();
    return 'tx' . $pfx . sprintf('%04d', $counter);
}

# Internal
__PACKAGE__->mk_accessors(qw(column_alias_counter));

=head2 $str = $sql->column_table_alias();

Get a column alias that is certainly unique within this SQL statement, and probaby unique accross substatements (and superstatments, if you will).

=cut

sub new_column_alias {
    my $self = shift;
    my $counter = $self->column_alias_counter($self->column_alias_counter() + 1);
    my $pfx = $self->unique_alias_prefix();
    return 'cx' . $pfx . sprintf('%04d', $counter);
}


=head2 @bindings = $sql->get_bind_values();

Returns an array of values bound to the
parameters of the query, in query placeholder order.

This will include input bindings first, followed by where clause bindings.

=cut

sub get_bind_values {
    my $self = shift;
    my @binds = (
                 (map { $_->bind_value } $self->input_params),
                 ($self->raw_where ? map { $_->bind_value } $self->_raw_where_params : ()),
                 ($self->where ? map { $_->bind_value } $self->where->params : ()),
                );
    return @binds;
}

sub params {
    my $self = shift;
    my @params = (
                  $self->input_params,
                  ($self->input_subquery ? $self->input_subquery->params : ()),
                  ($self->raw_where ? $self->_raw_where_params : ()),
                  ($self->where ? $self->where->params : ()),
                 );
    return @params;
}

=head2 $q = $sql->input_subquery();

=head2 $sql->input_subquery($subquery);


Applicable only to INSERT statements.  Sets a SubQuery to use as the source for INSERT ... SELECT statements.

=cut

sub input_subquery {
    my $self = shift;
    if (@_) {
        my $sq = shift;
        unless (blessed($sq) && $sq->isa(SubQuery)) {
            Class::ReluctantORM::Exception::Param::WrongType->croak(param => 'subquery', expected => SubQuery, value => $sq);
        }
        unless ($self->operation() eq 'INSERT') {
            Class::ReluctantORM::Exception::Call::NotPermitted->croak('You may only set an input_subquery on an INSERT statment.  This is a ' . $self->operation . " statement.");
        }
        $self->set('input_subquery', $sq);
    }
    return $self->get('input_subquery');

}

=head2 $sql->set_bind_values($val1, $val2,...);

Binds the given values to the parameters in the where clause.

=cut

sub set_bind_values {
    my $self = shift;
    my @vals = @_;
    my @params = $self->params();
    if (@vals < @params) {
        Class::ReluctantORM::Exception::Param::Missing->croak('The number of values must match the number of parameters in the where clause.');
    } elsif (@vals > @params) {
        Class::ReluctantORM::Exception::Param::Spurious->croak('The number of values must match the number of parameters in the where clause.');
    }
    for my $i (0..(@params - 1)) {
        $params[$i]->bind_value($vals[$i]);
    }
}


=head2 $from = $sql->from();

=head2 $sql->from($sql_FROM_object);

Gets or sets the FROM clause of the query.  The argument is a
Class::ReluctantORM::SQL::From .

=cut

sub from {
    my $self = shift;
    if (@_) {
        my $thing = shift;
        if (!ref($thing)) {
            # Setting raw_from via from() - kinda sloppy
            $self->raw_from($thing);
        } elsif ($thing->isa(From)) {
            # Clear raw_from
            $self->raw_from(undef);
            $self->set('from', $thing);
        } else {
            Class::ReluctantORM::Exception::Param::WrongType->croak
                (
                 param => 'from',
                 expected => From . ' or raw SQL string',
                 value => $thing,
                );
        }
    }
    return $self->get('from');
}

=begin vaporware

=head2 $str = $sql->raw_from();

=head2 $sql->raw_from();

If you choose not to (or are unable to) use the From object to represent your FROM clause, you can use this facility to pass in a raw SQL string that will be used as the from clause.

It will not pass through unmolested - see Class::ReluctantORM::Driver - Raw SQL Mangling .

=cut

sub raw_from {
    my $self = shift;
    if (@_) {
        my $thing = shift();
        if (!defined($thing)) {
            # OK, clearing
            $self->set('raw_from', undef);
        } elsif (!ref($thing)) {
            $self->set('from', undef);
            $self->set('raw_from', $thing);
        } else {
            Class::ReluctantORM::Exception::Param::WrongType->croak
                (
                 param => 'raw_from',
                 expected => 'raw SQL string',
                 value => $thing,
                );
        }
    }
    return $self->get('raw_from');
}



=head2 @pairs = $sql->inputs();

Returns the list of inputs as an array of hashrefs.  Each hashref has keys 'column' and 'param'.

Only valid for INSERT and UPDATE statements.

=cut

sub inputs {
    my $self = shift;
    unless ($self->operation eq 'INSERT' || $self->operation eq 'UPDATE' ) { Class::ReluctantORM::Exception::Call::NotPermitted->croak('May only call inputs() on an INSERT or UPDATE statement.  Use input_params instead.'); }
    return @{$self->{inputs} || []};
}

=head2 @params = $sql->input_params();

Returns the list of input params as an array.

To get where clause params, call $sql->where->params();

=cut

sub input_params {
    my $self = shift;
    if ($self->operation eq 'INSERT' || $self->operation eq 'UPDATE') {
        if ($self->input_subquery) {
            return $self->input_subquery->statement->params();
        } else {
            return map { $_->{param} } $self->inputs;
        }
    } else {
        return ();
    }
}


=head2 $int = $sql->limit();

=head2 $sql->limit($int);

=head2 $sql->limit(undef);

Reads, sets, or clears the LIMIT clause of the statement.

=cut

__PACKAGE__->mk_accessors(qw(limit));

=head2 $int = $sql->offset();

=head2 $sql->offset($int);

=head2 $sql->offset(undef);

Reads, sets, or clears the OFFSET clause of the statement.

=cut

__PACKAGE__->mk_accessors(qw(offset));




=head2 $op = $sql->operation();

Reads the operation (command) of the SQL statement.  Result 
will be one of INSERT, DELETE, SELECT, or UPDATE.

=cut

sub operation {
    my $self = shift;
    if (@_) { Class::ReluctantORM::Exception::Call::NotMutator->croak(); }
    return $self->get('operation');
}

=head2 $where = $sql->order_by();

=head2 $sql->order_by($order);

Sets the optional ORDER BY clause of the query.  The argument is a
Class::ReluctantORM::SQL::OrderBy .

=cut

sub order_by {
    my $self = shift;
    if (@_) {
        $self->set('order_by', shift);
    }
    my $ob = $self->get('order_by');
    unless ($ob) {
        $ob = OrderBy->new();
        $self->set('order_by', $ob);
    }

    return $ob;
}



=head2 @cols = $sql->output_columns();

Returns the list of output columns as OutputColumns.

=cut

sub output_columns {
    my $self = shift;
    return @{$self->{outputs}};
}

=head2 $table = $sql->table();

=head2 $sql->table($table);

Reads or sets the target table for use with INSERT, UPDATE, and DELETE queries.  
It is invalid to call this on a SELECT query (use from() to set a From clause, instead).

=cut

sub table {
    my $self = shift;
    if ($self->operation eq 'SELECT') { Class::ReluctantORM::Exception::Call::NotPermitted->croak('Do not call table() on a SELECT query.  Use tables() to read tables and from() to set a from clause.'); }
    if (@_) {
        my $t = shift;
        unless (blessed($t) && $t->isa(Table)) { Class::ReluctantORM::Exception::Param::WrongType->croak(expected => Table, value => $t); }
        $self->set('table', $t);
    }
    return $self->get('table');
}

=head2 $table = $sql->base_table();

=cut

sub base_table {
    my $sql = shift;
    if (@_) {
        Class::ReluctantORM::Exception::Call::NotMutator->croak();
    }
    if ($sql->operation() eq 'SELECT') {
        return $sql->from()->root_relation()->leftmost_table();
    } else {
        return $sql->table();
    }
}

=head2 @tables = $sql->tables(%opts);

Returns an array of all tables involved in the query, both from the from clause and the where clause.

Supported options:

=over

=item exclude_subqueries

Optional boolean, default false.  If true, tables mentioned only in subqueries will not be included.

=back

=cut

sub tables {
    my $self = shift;
    my %opts = check_args(args => \@_, optional => [qw(exclude_subqueries)]);

    my @from_tables;
    if ($self->operation eq 'SELECT') {
        unless ($self->from) {
            Class::ReluctantORM::Exception::Call::NotPermitted->croak('For SELECT statements, you must set the FROM clause using  from() before calling tables().');
        }
        @from_tables = $self->from ? $self->from->tables(%opts) : ();
    } else {
        @from_tables = $self->table ? ($self->table()) : ();
    }

    my @where_tables = $self->where ? $self->where->tables(%opts) : ();

    # Unique-ify this list using their memory addresses
    my %tables = map {('' . $_ . '') => $_ } (@from_tables, @where_tables);
    return values %tables;
}


=head2 $where = $sql->where();

=head2 $sql->where($sql_where);

Reads or sets the WHERE clause of the query.  The argument is a
Class::ReluctantORM::SQL::Where .

=cut

sub where {
    my $self = shift;
    if (@_) {
        my $thing = shift;
        if (!ref($thing)) {
            # Setting raw_where via where() - kinda sloppy
            $self->raw_where($thing);
        } elsif ($thing->isa(Where)) {
            # Clear raw_where
            $self->raw_where(undef);
            $self->set('where', $thing);
        } else {
            Class::ReluctantORM::Exception::Param::WrongType->croak
                (
                 param => 'where',
                 expected => Where . ' or raw SQL string',
                 value => $thing,
                );
        }
    }
    return $self->get('where');
}

=head2 $str = $sql->raw_where();

=head2 $sql->raw_where();

If you choose not to (or are unable to) use the Where object to represent your WHERE clause, you can use this facility to pass in a raw SQL string that will be used as the where clause.

It will not pass through unmolested - see Class::ReluctantORM::Driver - Raw SQL Mangling .

=cut

sub raw_where {
    my $self = shift;
    if (@_) {
        my $thing = shift();
        if (!defined($thing)) {
            # OK, clearing
            $self->set('raw_where', undef);
        } elsif (!ref($thing)) {
            $self->set('where', undef);
            $self->__find_raw_where_params($thing);
            $self->set('raw_where', $thing);
        } else {
            Class::ReluctantORM::Exception::Param::WrongType->croak
                (
                 param => 'raw_where',
                 expected => 'raw SQL string',
                 value => $thing,
                );
        }
    }
    return $self->get('raw_where');
}

sub _raw_where_execargs {
    my $self = shift;
    if (@_) {
        $self->set('raw_where_execargs', shift);
    }
    return $self->get('raw_where_execargs');
}

sub _raw_where_pristine {
    my $self = shift;
    if (@_) {
        $self->set_reconcile_option('realias_raw_sql', !shift);
    }
}

sub _cooked_where {
    my $self = shift;
    if (@_) {
        $self->set('cooked_where', shift);
    }
    return $self->get('cooked_where');
}

sub __find_raw_where_params {
    my $self = shift;
    my $raw = shift;
    # TODO - check for ?'s in quoted strings more effectively
    while ($raw =~ s{'.*?'}{}g) { }  # Crudely delete all quoted strings from the SQL
    my @params = map { Param->new() } $raw =~ m{(\?)}g;
    $self->_raw_where_params(\@params);
}

sub _raw_where_params {
    my $self = shift;
    if (@_) {
        $self->set('raw_where_params', shift);
    }
    return @{$self->get('raw_where_params') || []};
}



#========================================================#
#                  Inflation Support
#========================================================#

=head1  INFLATION SUPPORT

These methods implement the ability to create CRO model objects from a SQL query object.

=cut

=head2 $bool = $sql->is_inflatable(%make_inflatable_opts);

=head2 ($bool, $exception) = $sql->is_inflatable(%make_inflatable_opts);

Analyzes the SQL statement and tries to determine if it 
can be successfully used to inflate CRO model objects after
execution.  Calls make_inflatable before performing the analysis, passing on any options.

This captures any exception from the analysis, and optionally returns it in the second form.

A false return from is_inflatable indicates that inflate() will certainly fail before executing.

A true return indicates that inflate() will survive at least until execution
(a runtime database error may still occur).

=cut

sub is_inflatable {
    my $sql = shift;
    my %args = check_args(args => \@_, optional => [qw(auto_annotate auto_reconcile add_output_columns)]);
    unless (defined($args{auto_annotate}))      { $args{auto_annotate} = 1;  }
    unless (defined($args{auto_reconcile}))     { $args{auto_reconcile} = 1;  }
    unless (defined($args{add_output_columns})) { $args{add_output_columns} = 1;  }

    eval {
        $sql->make_inflatable(%args);
    };
    if ($@) {
        return wantarray ? (0, $@) : 0;
    }

    # Inflatability checks
    my @checks =
      (
       '__is_inflatable_find_base_class',
       '__is_inflatable_has_output_columns',
       '__is_inflatable_all_non_join_tables_are_in_relationships',
       '__is_inflatable_all_joins_have_relationships',
       '__is_inflatable_essential_output_columns_present_and_reconciled',
      );

    my $inflatable = 1;
    my $exception = undef;
    foreach my $check (@checks) {
        if ($inflatable) {
            my $check_result = 1;
            ($check_result, $exception) = $sql->$check;
            $inflatable &&= $check_result;
        }
    }

    return wantarray ? ($inflatable, $exception) : $inflatable;
}


sub __is_inflatable_find_base_class {
    my $sql = shift;
    my $base_table = $sql->base_table();
    return $base_table->class() ? (1, undef) : (0, Class::ReluctantORM::Exception::SQL::NotInflatable->new(error => 'Base table does not have a class associated with it', sql => $sql));
}

sub __is_inflatable_has_output_columns {
    my $sql = shift;
    return (scalar $sql->output_columns) ? (1, undef) : (0, Class::ReluctantORM::Exception::SQL::NotInflatable->new(error => 'SQL object has no output columns', sql => $sql));
}

sub __is_inflatable_essential_output_columns_present_and_reconciled {
    my $sql = shift;

    my $ok = 1;
    my $check = 1;
    my $exception = undef;

    my %cache = 
      map { __is_inflatable_EOCPAR_column_name($_) => $_}
        $sql->output_columns();

    # Check the base table
    my $base = $sql->base_table();
    ($check, $exception) = __is_inflatable_EOCPAR_columns_present_for_table($base, \%cache);
    $ok &&= $check;

    # Check all relationships
    if ($sql->from()) {
        foreach my $rel ($sql->from->relationships()) {
            last unless $ok;
            my @tables = ($rel->local_sql_table(), $rel->remote_sql_table());

            foreach my $table (@tables) {
                next unless $ok;
                next unless $table;
                # May seem odd, but it's actually OK for a relationship to be
                # present while missing the local or remote table IFF the relationship has a join depth > 1
                # (relied on by HasManyMany->fetch_all())
                next if ($rel->join_depth > 1 && !grep { $_->is_the_same_table($table) } $sql->tables(exclude_subqueries => 1));

                ($check, $exception) = __is_inflatable_EOCPAR_columns_present_for_table($table, \%cache);
                $ok &&= $check;
            }
        }
    }

    return (($ok ? 1 : 0), $exception);
}

sub __is_inflatable_EOCPAR_column_name {
    my $oc = shift;
    if ($oc->expression->is_column()) {
        my $col = $oc->expression();
        if ($col->table) {
            if ($col->table->schema) {
                return $col->table->schema . '.' . $col->table->table . '.' . $col->column;
            } else {
                return '(unknown schema).' . $col->table->table . '.' . $col->column;
            }

        } else {
            return '(unknown table).' . $col->column();
        }
    } else {
        return '(expression)';
    }
}


sub __is_inflatable_EOCPAR_columns_present_for_table {
    my $table = shift;
    my $column_lookup = shift;
    my $sql = shift;

    my $ok = 1;
    my $check = 1;
    my $exception = undef;

    my $class = $table->class();
    foreach my $ec ($class->essential_sql_columns($table)) {
        last unless $ok;
        my $eoc = OutputColumn->new(expression => $ec);
        $check = exists $column_lookup->{__is_inflatable_EOCPAR_column_name($eoc)};
        unless ($check) {
            $exception = Class::ReluctantORM::Exception::SQL::NotInflatable::MissingColumn->new
              (
               table => $table->schema . '.' . $table->table(),
               column => $ec->column,
               sql => $sql,
              );
        }
        $ok &&= $check;
    }
    return ($ok, $exception);
}

sub __is_inflatable_all_non_join_tables_are_in_relationships {
    my $sql = shift;

    my @non_join_tables = 
      grep { ! Class::ReluctantORM->_is_join_table(table_obj => $_) }
        $sql->tables(exclude_subqueries => 1);

    # We're OK if it's just the base table left
    if (@non_join_tables == 1 && $non_join_tables[0]->is_the_same_table($sql->base_table)) {
        return (1, undef);
    }

    unless ($sql->from) {
        # WTF - has multiple tables, but no FROM clause?
        return (0, Class::ReluctantORM::Exception::SQL::NotInflatable->new(sql => $sql, error => "Multiple tables, but no from clause...  confused am I!"));
    }

    my @rels = $sql->from->relationships();
  TABLE:
    foreach my $table (@non_join_tables) {
        foreach my $rel (@rels) {
            foreach my $end (qw(local_sql_table remote_sql_table)) {
                my $rel_table = $rel->$end();
                if ($rel_table && $table->is_the_same_table($rel_table)) {
                    next TABLE;
                }
            }
        }
        # Been through all the relations and didn't a rel for this table
        return (0, Class::ReluctantORM::Exception::SQL::NotInflatable::ExtraTable->new(sql => $sql, error => "A table is neither an intermediate join table, nor does it appear at either end of any relationships", table => $table));
    }
    
    # All tables check out....
    return (1, undef);
}

sub __is_inflatable_all_joins_have_relationships {
    my $sql = shift;
    unless ($sql->from) { return (1, undef); }
    my @joins = $sql->from->joins();
    foreach my $j (@joins) {
        unless ($j->relationship()) {
            return (0, Class::ReluctantORM::Exception::SQL::NotInflatable::VagueJoin->new(sql => $sql, error => "A join does not have a Relationship associated with it", join => $j));
        }
    }

    # A-OK
    return (1, undef);
}

=head2 $sql->make_inflatable(%opts);

Performs various actions to increase the inflatability of the SQL object.  Calls annotate and reconcile.  If any exceptions are thrown, they are passed on.

Compare to is_inflatable, which optionally calls make_inflatable but captures any exception.

Currently supported options:

=over

=item auto_annotate

Optional boolean, default true.  If true, call annotate() before performing the analysis.  If false, you are saying that you have already attached any model metadata.

=item auto_reconcile

Optional boolean, default true.  If true, call reconcile() before performing the analysis.

=item add_output_columns

Optional boolean, default true.  If auto_reconcile is true, output columns will be added to the query to ensure that all essential (non-lazy) columns are present in the query.  If auto_reconcile is false, has no effect.

=back

=cut

sub make_inflatable {
    my $sql = shift;
    my %args = check_args(args => \@_, optional => [qw(auto_annotate auto_reconcile add_output_columns)]);
    unless (defined($args{auto_annotate}))      { $args{auto_annotate} = 1;  }
    unless (defined($args{auto_reconcile}))     { $args{auto_reconcile} = 1;  }
    unless (defined($args{add_output_columns})) { $args{add_output_columns} = 1;  }

    if ($args{auto_annotate}) {
        $sql->annotate();
    }
    if ($args{auto_reconcile}) {
        $sql->reconcile(add_output_columns => $args{add_output_columns});
    }
}

=head2 $sql->annotate();

Scans the SQL tree and attaches Tables and Relationships where they can be determined.

=cut

sub annotate {
    my $sql = shift;

  TABLE:
    foreach my $table ($sql->tables()) {
        if (Class::ReluctantORM->_is_join_table(table_obj => $table)) {
            my $jst = Class::ReluctantORM->_find_sql_table_for_join_table(table_obj => $table);
            $table->_copy_manual_columns($jst);
            $table->schema($jst->schema());
        } elsif (!$table->class()) {
            my $class = Class::ReluctantORM->_find_class_by_table(table_obj => $table);
            # might not be found (alias macro, for example)
            # alias macros will get resolved during reconciliation anyway
            if ($class) {
                $table->class($class);
            }
        }
    }

    # Hunt for relationships in the joins
    if ($sql->from) {
        $sql->__annotate_find_relationships();
    }


    # Anything else?

}

sub __annotate_find_relationships {
    my $sql = shift;
    __annotate_FR_recursor($sql, $sql->from->root_relation());
}

sub __annotate_FR_recursor {
    my $sql = shift;
    my $rel = shift;
    unless ($rel->is_join) { return; }
    my $join = $rel;

    my ($right_rel, $left_rel) = ($join->right_relation(), $join->left_relation());

    # Maybe it's already set?
    if ($join->relationship()) {
        # Just recurse and return
        __annotate_FR_recursor($sql, $left_rel);
        __annotate_FR_recursor($sql, $right_rel);
        return;
    }

    # Find the leftmost table on the each side
    my $left_table  = $left_rel->leftmost_table();
    my $right_table = $right_rel->leftmost_table();
    my @candidates;

    # Look for a relationship in which the local table of the relationship is the left table
    # and the right table is either the remote table or the join table
    if (@candidates == 0) {
        @candidates = Class::ReluctantORM->_find_relationships_by_local_table(table_obj => $left_table);
        @candidates =
          grep {
              ($_->remote_sql_table && $right_table->is_the_same_table($_->remote_sql_table, 0)) ||
                ($_->join_sql_table && $right_table->is_the_same_table($_->join_sql_table, 0))
            }
            @candidates;
    }

    # Look for a relationship in which the local table of the relationship is the right table
    # and the left table is either the remote table or the join table
    if (@candidates == 0) {
        @candidates = Class::ReluctantORM->_find_relationships_by_local_table(table_obj => $right_table);
        @candidates =
          grep {
              ($_->remote_sql_table && $left_table->is_the_same_table($_->remote_sql_table, 0)) ||
                ($_->join_sql_table && $left_table->is_the_same_table($_->join_sql_table, 0))
            }
            @candidates;
    }

    # Try desperate measures?
    if (1) {

        # Look for a relationship in which the remote table of the relationship is the left table
        # and the right table is either the local table or the join table
        if (@candidates == 0) {
            @candidates = Class::ReluctantORM->_find_relationships_by_remote_table(table_obj => $left_table);
            @candidates =
              grep {
                  ($_->remote_sql_table && $right_table->is_the_same_table($_->local_sql_table, 0)) ||
                    ($_->join_sql_table && $right_table->is_the_same_table($_->join_sql_table, 0))
                }
                @candidates;
        }

        # Look for a relationship in which the remote table of the relationship is the right table
        # and the left table is either the local table or the join table
        if (@candidates == 0) {
            @candidates = Class::ReluctantORM->_find_relationships_by_remote_table(table_obj => $right_table);
            @candidates =
              grep {
                  ($_->remote_sql_table && $left_table->is_the_same_table($_->local_sql_table, 0)) ||
                    ($_->join_sql_table && $left_table->is_the_same_table($_->join_sql_table, 0))
                }
                @candidates;
        }
    }


    # The candidate relationships must have a criterion that is equivalent to the one on the join
    @candidates = grep { $_->matches_join_criterion($join->criterion()) } @candidates;
    my %unique_candidates = map { $_->method_name => $_ } @candidates;
    @candidates = values %unique_candidates;

    if (@candidates == 0) {
        Class::ReluctantORM::Exception::SQL::NotInflatable::VagueJoin->croak(join => $join, error => "Could not find any relationships that matched the tables on the ends of this Join", sql => $sql);
    } elsif (@candidates > 1) {
        Class::ReluctantORM::Exception::SQL::NotInflatable::VagueJoin->croak(join => $join, error => "Could not find a unique relationship that matched the tables on the ends of this Join", sql => $sql);
    } else {
        # Yay,  exactly one relationship matched
        $join->relationship($candidates[0]);
    }

    # Recurse
    __annotate_FR_recursor($sql, $left_rel);
    __annotate_FR_recursor($sql, $right_rel);

}

=head2 @objects = $sql->inflate();

Executes the given query, and builds Class::ReluctantORM model objects directly from the results.

This does not call is_inflatable() or make_inflatable() for you.  See those methods to increase your chances of success.

=cut

sub inflate {
    my $sql = shift;
    my @results = fd_inflate($sql); # yipes
    return @results;
}




#========================================================#
#                  Column Disambiguation
#========================================================#

=begin devdocs

=head2 $sql->set_reconcile_option(option => $value);

This might go public one day, but for now it's best left to those who read the source.

You can use this to set reconciliation options.  Read reconcile() to see what they do.

=cut

sub set_reconcile_option {
    my $sql = shift;
    my %opts = @_;
    foreach my $opt (keys %opts) {
        $sql->{reconcile_options}{$opt} = $opts{$opt};
    }
}


=head2 $sql->reconcile();

Prepares the SQL object for rendering.  This includes:

=over

=item ensure output columns are generated

=item disambiguate column references in the WHERE and ORDER BY clauses

=back

There is no harm in calling this method multiple times.  This method will 
throw exceptions if it encounters irreconcilable ambiguities.

=cut

sub reconcile {
    my $sql = shift;
    my %args = check_args(args => \@_, optional => [qw(add_output_columns realias_raw_sql)]);

    my %instance_options = %{$sql->{reconcile_options}};
    my %options = (%instance_options, %args);

    $sql->__reconcile_in_subqueries();

    $sql->__build_reconciliation_cache();
    $sql->__disambiguate_columns_in_from();
    $sql->__set_default_table_aliases();
    $sql->__build_reconciliation_cache();  # Rebuild needed after setting defualt table aliases

    $sql->__resolve_alias_macros();
    $sql->__disambiguate_columns_in_where();

    if ($options{add_output_columns}) {
        $sql->__add_output_columns();
    }
    $sql->__disambiguate_columns_in_output();
    $sql->__disambiguate_columns_in_order_by();

    delete $sql->{_rc};
    return 1;

}


sub __add_output_columns {
    my $sql = shift;

    if ($sql->operation eq 'DELETE') { return; }

    # Add base columns for tables
    foreach my $table ($sql->tables) {
        if ($table->class) {
            foreach my $col ($table->class->essential_sql_columns($table)) {
                $sql->add_output($col);
            }
        }
    }

    # Add extra columns for relations
    if ($sql->from) {
        foreach my $relship ($sql->from->relationships) {
            foreach my $col ($relship->additional_output_sql_columns) {
                $sql->add_output($col);
            }
        }
    }
}

sub __build_reconciliation_cache {
    my $self = shift;
    my @from_tables;
    if ($self->operation eq 'SELECT') {
        @from_tables = $self->from ? $self->from->tables() : ();
    } else {
        @from_tables = $self->table ? ($self->table()) : ();
    }
    my %tables_by_alias    = map { $_->alias => $_ } grep { defined($_->alias) } @from_tables;
    my %tables_by_mem      = map { ('' . $_ . '') => $_ } @from_tables;
    my %tables_by_schema   = map { ($_->schema . '.' .  $_->table) => $_ } grep { defined($_->schema) } @from_tables;
    my %tables_by_name;
    foreach my $table (@from_tables) {
        $tables_by_name{$table->table} ||= [];
        push @{$tables_by_name{$table->table}}, $table;
    }
    my %tables_by_column;
    foreach my $table (grep { $_->knows_any_columns } @from_tables) {
        my @col_names = map { lc($_->column) } $table->columns;
        foreach my $col_name (@col_names) {
            $tables_by_column{$col_name} ||= [];
            push @{$tables_by_column{$col_name}}, $table;
        }
    }
    my %tables_by_relation;
    if ($self->operation eq 'SELECT') { # assumes only selects have FROMs
        foreach my $join ($self->from->joins()) {
            if ($join->relationship()) {
                my $rel = $join->relationship();
                my $relname = $rel->method_name();
                $tables_by_relation{$relname} = {};
                $tables_by_relation{$relname}{parent}  = $join->_find_earliest_table($rel->local_sql_table());
                $tables_by_relation{$relname}{child} = $join->_find_latest_table($rel->remote_sql_table());
                if ($rel->join_depth > 1) {
                    $tables_by_relation{$relname}{join} = $join->_find_latest_table($rel->join_sql_table());
                }
            }
        }
    }


    $self->{_rc} = {
                    by_alias    => \%tables_by_alias,
                    by_mem      => \%tables_by_mem,
                    by_schema   => \%tables_by_schema,
                    by_name     => \%tables_by_name,
                    by_column   => \%tables_by_column,
                    by_relationship => \%tables_by_relation
                   };
}

sub __reconcile_in_subqueries {
    my $sql = shift;

    # Never add output columns to a subquery
    my %opts = (%{$sql->{reconcile_options}}, add_output_columns => 0);

    my $reconciler = sub {
        my $thing = shift;
        if ($thing->is_subquery()) {
            my $st = $thing->statement();
            $st->reconcile(%opts);
        }
    };

    # Look for subqueries in output columns
    foreach my $oc ($sql->output_columns) {
        $oc->expression->walk_leaf_expressions($reconciler);
    }

    # Look for subqueries in from
    if ($sql->from) {
        $sql->from->root_relation->walk_leaf_relations($reconciler);
    } elsif ($sql->table) {
        $sql->table->walk_leaf_relations($reconciler);
    }

    # Look for subqueries in where
    if ($sql->where) {
        $sql->where->root_criterion->walk_leaf_expressions($reconciler);
    }

    # Might have a input subquery (INSERTs only)
    if ($sql->input_subquery) {
        $sql->input_subquery->statement->reconcile(%opts);
    }

}

# See 'Alias Macros' in Class/ReluctantORM/Manual/SQL.pod
sub __resolve_alias_macros {
    my $sql = shift;

    my @cols = (
                ($sql->where ? $sql->where->columns : ()),
                ($sql->order_by ? $sql->order_by->columns : ()),
               );

    foreach my $column (@cols) {
        my $info = $sql->__looks_like_alias_macro($column);
        next unless $info;
        my $table;

        if ($info->{type} eq 'base') {
            $table = $sql->from->root_relation->leftmost_table();
        } else {

            # Find the referred-to relationship
            my @matching_relations = grep { $_->method_name eq $info->{relname} } $sql->from->relationships();
            if (@matching_relations != 1) {
                Class::ReluctantORM::Exception::SQL::AmbiguousReference->croak
                    ("Must have exactly one reference to the relationship '$info->{relname}' to use a alias macro");
            }
            my $relationship = $matching_relations[0];

            # OK, find the JOIN that uses that relationship...
            my ($join) = 
              grep { $_->relationship && $_->relationship->method_name eq $relationship->method_name }
                $sql->from->joins();

            if ($info->{type} eq 'parent') {
                # Hunt down the left-branch of the JOIN, looking for the LINKING table
                my $seek = Table->new($relationship->linking_class());
                $table = $join->_find_earliest_table($seek);
            } elsif ($info->{type} eq 'child') {
                # Hunt down the right-branch of the JOIN, looking for the LINKED table
                my $seek = Table->new($relationship->linked_class());
                $table = $join->_find_latest_table($seek);
            } elsif ($info->{type} eq 'join') {
                my $seek = $relationship->join_sql_table();
                $table = $join->_find_latest_table($seek);
            } else {
                Class::ReluctantORM::Exception::NotImplemented->croak
                    ("Don't know how to handle an alias macro of type '$info->{type}'");
            }
        }

        unless ($table) {
            Class::ReluctantORM::Exception::SQL::TooComplex->croak
                ("Unable to resolve alias macro '" . $column->table->table . "' -- try simplifying?");
        }

        # Finally
        $column->table($table);

    }
}

my @ALIAS_MACRO_PATTERNS = (
                            # Make these case-insensitive, since SQL::Statement will uppercase them
                            qr(MACRO__(base)__)i,
                            qr(MACRO__(parent)__(.+)__)i,
                            qr(MACRO__(child)__(.+)__)i,
                            qr(MACRO__(join)__(.+)__)i,
                           );

sub __looks_like_alias_macro {
    my $sql = shift;
    my $column = shift;
    my $table = $column->table();
    unless ($table) { return undef; }
    my $name = $table->table();  # Don't use alias here - it may have been set by __set_default_table_aliases();
    unless ($name) { return undef; }
    foreach my $pat (@ALIAS_MACRO_PATTERNS) {
        my ($type, $relname) = $name =~ $pat;
        if ($type) {
            $type = lc($type);
            # Find the relationship
            my $lcrelname = '';
            unless ($type eq 'base') {
                if ($sql->from) {
                    ($lcrelname) = grep { lc($relname) eq lc($_) } map { $_->method_name } $sql->from->relationships();
                    unless ($lcrelname) {
                        Class::ReluctantORM::Exception::SQL::AmbiguousReference->croak
                            ("Could not resolve alias macro '$name' - no relationship with name '$relname' (looked case insensitively)");
                    }
                    $relname = $lcrelname;
                }
            }
            return { type => $type, relname => $relname };
        }
    }
    return undef;
}


sub __disambiguate_columns_in_where {
    my $self = shift;
    if ($self->raw_where) {
        $self->__raw_where_bind_params();
        if ($self->{reconcile_options}{realias_raw_sql}) {
            $self->__raw_where_realias(); # sets cooked_where
        } else {
            $self->_cooked_where($self->raw_where());
        }
    } elsif ($self->where) {
        foreach my $col ($self->where->columns) {
            $self->__disambiguate_column($col);
        }
    }
}

sub __raw_where_bind_params {
    my $sql = shift;

    # This is kinda dumb - at this point, we're reconciling, 
    # and further changes to the SQL are not permitted.  So if 
    # anyone called set_bind_params ALREADY, respect that.  But
    # if they didn't, notice that and make a last minute bind.
    my $already_bound = 1;
    for ($sql->_raw_where_params) { $already_bound &&= $_->has_bind_value(); }
    return if $already_bound;

    return unless defined($sql->_raw_where_execargs()); # Uhh, should this be an exception?

    my @ea = @{$sql->_raw_where_execargs() || []};
    foreach my $p ($sql->_raw_where_params) {
        $p->bind_value(shift @ea);
    }
}


# This one is doing string replacements on a SQL string, not working with objects

sub __raw_where_realias {
    my $sql = shift;
    my $raw = $sql->raw_where();
    my $working = $raw;

    # TODO - this whole method should probably be moved into Driver, 
    # or else provide a way to set the driver being used
    my $driver_class = Class::ReluctantORM->default_driver_class();

    if ($sql->operation eq 'SELECT') {
        # Have to work hard - may have multiple source tables, perhaps even same table multiple times
        # At this point, from() should be defined, annotated, and reconciled

        my %rels_by_name = map { $_->method_name => $_  } $sql->from->relationships();

        # process alias macros
        foreach my $amre (@ALIAS_MACRO_PATTERNS) {
            if (my ($type, $relname) = $working =~ $amre) {
                my $alias;
                if ($type eq 'base') {
                    $alias = $sql->base_table->alias();
                } elsif ($type eq 'parent') {
                    $alias = $sql->{_rc}{by_relationship}{$relname}{parent}->alias;
                } elsif ($type eq 'child') {
                    $alias = $sql->{_rc}{by_relationship}{$relname}{child}->alias;
                } elsif ($type eq 'join') {
                    $alias = $sql->{_rc}{by_relationship}{$relname}{join}->alias;
                }
                $working =~ s{$amre}{$alias}ge;
            }
        }

        # Now loop over the known tables in the query
        # and look for anything that might be refering to that table

        my ($oq, $cq, $ns) = ($driver_class->open_quote, $driver_class->close_quote, $driver_class->name_separator);
        foreach my $t ($sql->from->tables()) {
            my $alias = $t->alias() . $ns;

            # "schema_name"."table".
            if ($t->schema) {
                my $re1 = '(' . $oq . $t->schema . $cq . '\\' . $ns . $oq . $t->table . $cq . '\\' . $ns . ')';
                $working =~ s/$re1/$alias/g;
                if ($DEBUG > 2) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - alias sub pass one:\nre:\t$re1\nadjusted where:\t$working\n"; }
            }

            # schema.table.
            if ($t->schema) {
                my $re2 = '(' . $t->schema . '\\' . $ns . $t->table . '\\' . $ns . ')';
                $working =~ s/$re2/$alias/g;
                if ($DEBUG > 2) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - alias sub pass two:\nre:\t$re2\nadjusted where:\t$working\n"; }
            }

            # "table".
            my $re3 = '(' . $oq . $t->table . $cq . '\\' . $ns . ')';
            $working =~ s/$re3/$alias/g;
            if ($DEBUG > 2) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - alias sub pass 3:\nre:\t$re3\nadjusted where:\t$working\n"; }

            # table.
            my $re4 = '(' . $t->table . '\\' . $ns . ')';
            $working =~ s/$re4/$alias/g;
            if ($DEBUG > 2) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - alias sub pass 4:\nre:\t$re4\nadjusted where:\t$working\n"; }

        }
        # OK, at this point, $working is as good as we can practically make it.  It may still have
        # ambiguous table or column references, but if so, the user should use the alias macro facility.

    } else {
        # Assume we don't support FROM (or USING) with UPDATE, INSERT, or DELETE
        # so we only have one source table.  Nothing to do.
    }


    $sql->_cooked_where($working);

}



sub __disambiguate_columns_in_output {
    my $sql = shift;

    # Collect all Columns, even those buried in Expressions
    my @columns;
    my $walker = sub {
        my $expr = shift;
        if ($expr->is_column) {
            push @columns, $expr;
        }
    };
    foreach my $expr (map { $_->expression } $sql->output_columns()) {
        $expr->walk_leaf_expressions($walker);
    }

    # Disambiguate the columns
    foreach my $col (@columns) {
        $sql->__disambiguate_column($col);
    }

    # At this point, each column knows which table it goes with, but it 
    # may not have an alias, and it may be a duplicate.

    my @all_output_columns = $sql->output_columns;
    my @simple_outputs = grep { $_->expression->is_column } @all_output_columns;
    my @expression_outputs = grep { !$_->expression->is_column } @all_output_columns;

    # Even after uniqueification, we need to preserve order.
    my %oc_order;
    for (0..$#all_output_columns) { $oc_order{$all_output_columns[$_]} = $_; }

    # Filter out duplicates among the simple 
    # column outputs, using tablealias.columnname as the key
    my %unique_simple_cols =
      map { ($_->expression->table->alias . '.' . $_->expression->column) => $_ }
        @simple_outputs;

    # We don't try to uniqueify any expression columns
    my @unique_all_cols = (values(%unique_simple_cols), @expression_outputs);

    # OK, put them back in original order
    my @ordered_unique_cols = 
      sort { $oc_order{$a} <=> $oc_order{$b} }
        @unique_all_cols;

    $sql->{outputs} = \@ordered_unique_cols;

    # Now set column aliases
    $sql->__set_default_column_aliases();

}

sub __disambiguate_columns_in_order_by {
    my $self = shift;
    return unless $self->order_by;
    foreach my $col ($self->order_by->columns) {
        $self->__disambiguate_column($col);
    }
}

sub __disambiguate_columns_in_from {
    my $sql = shift;
    return unless $sql->from;

    # Look for criteria and resolve their columns
    my $walker = sub {
        my $expr = shift;
        if ($expr->is_column()) {
            $sql->__disambiguate_column($expr);
        }
    };
    foreach my $join ($sql->from->joins()) {
        $join->criterion->walk_leaf_expressions($walker);
    }


    # This will disambiguate any columns in tables referenced in the from clause
    foreach my $col ($sql->from->columns) {
        $sql->__disambiguate_column($col);
   }
}

sub __disambiguate_columns_in_input {
    my $self = shift;
    foreach my $pair ($self->inputs) {
        $self->__disambiguate_column($pair->{column});
    }
}


sub __disambiguate_column {
    my $self = shift;
    my $col  = shift;
    my $table = $col->table();
    my %cache = %{$self->{_rc}};

    if ($table) {

        my $alias = $table->alias() || 'no alias';
        if ($DEBUG > 2) { print STDERR __PACKAGE__ . ':' . __LINE__ . "Have table " . $table->table . "($alias) for column " . $col->column . "\n";  }

        # If we have a table, look it up by memory address first; do nothing if found (already unambiguous)
        if (exists $cache{by_mem}{'' . $table . ''}) { return; }

        # look up by alias and replace if found
        if ($table->alias && exists($cache{by_alias}{$table->alias})) {
            $col->table($cache{by_alias}{$table->alias});
            return;
        }

        # look up by schema.table and replace if found
        if ($table->schema && exists($cache{by_schema}{$table->schema . '.' . $table->table})) {
            $col->table($cache{by_schema}{$table->schema . '.' . $table->table});
            return;
        }

        # look up by table and replace if found; panic if more than one table with that name
        my @tables_with_that_name = @{$cache{by_name}{$table->table} || []};
        if (@tables_with_that_name == 1) {
            $col->table($tables_with_that_name[0]);
        } elsif (@tables_with_that_name == 0) {
            Class::ReluctantORM::Exception::SQL::AmbiguousReference->croak("The column " . $col->column . " apparently belongs to a table that is not referenced in the query (" . $table->table . ")");
        } else {
            Class::ReluctantORM::Exception::SQL::AmbiguousReference->croak("The column " . $col->column . " could not be unambiguously assigned to a table - candidates: " . (join ',', map { ($_->schema ? ($_->schema . '.') : '') . $_->table } @tables_with_that_name));
        }

    } else {
        # else no table, so look by column
        my @tables_with_that_column = @{$cache{by_column}{lc($col->column)} || []};

        if (@tables_with_that_column == 1) {
            $col->table($tables_with_that_column[0]);
        } elsif (@tables_with_that_column == 0) {
            Class::ReluctantORM::Exception::SQL::AmbiguousReference->croak("The column " . $col->column . " has no table specified, and no table in the query could be found that has that column.");
        } else {
            Class::ReluctantORM::Exception::SQL::AmbiguousReference->croak("The column " . $col->column . " could not be unambiguously assigned to a table by column name - candidates: " . (join ',', map { ($_->schema ? ($_->schema . '.') : '') . $_->table  . ($_->alias ? ' (' . $_->alias . ')' : '') } @tables_with_that_column));
        }
    }
}

#=======================================================#
#                  DIRECT EXECUTION
#=======================================================#

=head1 DIRECT EXECUTION

These low-level methods allow you to use DBI-style prepare/execute/fetch cycles on SQL objects.

Use $driver->prepare($sql) to start this process.

=cut

=head2 $bool = $sql->is_prepared();

Returns true if the SQL object has been prepared using $driver->prepare().

=cut

sub is_prepared {
    my $sql = shift;
    return defined ($sql->_sth());
}

=head2 $sql->execute();

=head2 $sql->execute(@bind_values);

In the first form, executes the statement using the existing values bound to the Params (if any).

In the second form, binds the given values to the parameters in the SQL object, and executes the statement handle.  

is_prepared() must return true for this to work. If anything goes wrong (including database errors) an exception will be thrown.

=cut

__PACKAGE__->mk_accessors(qw(execute_hints));

sub execute {
    my $sql = shift;
    unless ($sql->is_prepared()) {
        Class::ReluctantORM::Exception::SQL::ExecuteWithoutPrepare->croak();
    }

    # If binds were provided, set them
    if (@_) {
        $sql->set_bind_values(@_);
    }

    my %monitor_args = $sql->__monitor_args();
    my $driver = $sql->_execution_driver();

    $driver->_monitor_execute_begin(%monitor_args);
    $driver->_pre_execute_hook($sql);
    $sql->_sth->execute($sql->get_bind_values());
    $driver->_post_execute_hook($sql);
    $driver->_monitor_execute_finish(%monitor_args);

    return;
}

sub __monitor_args {
    my $sql = shift;
    return (
            sql_obj => $sql, 
            sql_str => $sql->_sql_string,
            binds => [ $sql->get_bind_values() ],
            sth => $sql->_sth(),
           );
}

=head2 $sql->fetchrow();

Fetches one row from the statment handle.  The fetched values are bound to the Output Columns of the SQL object - access them using $sql->output_columns.

=cut

sub fetchrow {
    my $sql = shift;

    unless ($sql->is_prepared()) {
        Class::ReluctantORM::Exception::SQL::ExecuteWithoutPrepare->croak();
    }

    my $row = $sql->_sth->fetchrow_hashref();
    $sql->_execution_driver->_monitor_fetch_row($sql->__monitor_args(), row => $row);
    $sql->set_single_row_results($row);

    return $row;
}

=head2 $sql->fetch_all();

Fetches all rows from the statement handle, and calls your callback after fetching each row (see $sql->add_fetchrow_listener()).

=cut

sub fetch_all {
    my $sql = shift;

    unless ($sql->is_prepared()) {
        Class::ReluctantORM::Exception::SQL::ExecuteWithoutPrepare->croak();
    }

    my %monitor_args = $sql->__monitor_args();
    while (my $row = $sql->_sth->fetchrow_hashref()) {
        $sql->_execution_driver->_monitor_fetch_row(%monitor_args, row => $row);
        $sql->set_single_row_results($row);
    }

    return;
}


=head2 $sql->finish();

Releases the statement handle.  is_prepared() must be true for this to work.

=cut

sub finish {
    my $sql = shift;
    unless ($sql->is_prepared()) {
        Class::ReluctantORM::Exception::SQL::FinishWithoutPrepare->croak();
    }

    $sql->_sth->finish();
    $sql->_execution_driver->_monitor_finish($sql->__monitor_args());

    $sql->_sth(undef);
    $sql->_sql_string(undef);
    $sql->_execution_driver(undef);

    return;
}


#=======================================================#
#                  Results Fetching
#=======================================================#

=head1 FETCHING RESULTS

=cut

=head2 $bool = $sql->has_results();

Returns true if the SQL object has been executed and has at least one row of results.

=cut

__PACKAGE__->mk_accessors(qw(has_results));

=head2 $sql->add_fetchrow_listener($coderef);

Adds a coderef that will be called with the SQL object as the only argument immediately after a row is fetched.  You may then obtain results from the $sql->output_columns, calling output_value on each.

=cut

sub add_fetchrow_listener {
    my $self = shift;
    my $coderef = shift;
    unless (ref($coderef) eq 'CODE') {
        Class::ReluctantORM::Exception::Param::WrongType->croak(expected => 'CODEREF', param => 'code');
    }
    push @{$self->{fetchrow_listeners}}, $coderef;
}


=head2 $sql->clear_fetchrow_listeners();

Clears the list of listeners.

=cut

sub clear_fetchrow_listeners {
    my $self = shift;
    $self->{fetchrow_listeners} = [];
}

sub _notify_fetchrow_listeners {
    my $self = shift;
    foreach my $coderef (@{$self->{fetchrow_listeners}}) {
        $coderef->($self);
    }
}

sub set_single_row_results {
    my $sql = shift;
    my $row = shift;
    if ($row) {
        foreach my $col ($sql->output_columns) {
            $col->output_value($row->{$col->alias});
        }
        $sql->has_results(1);
        $sql->_notify_fetchrow_listeners();
    } else {
        $sql->has_results(0);
    }
}


#=================================================================#
#                           MISC METHODS
#=================================================================#


=head1 MISC METHODS

=cut

=head2 $str = $sql->pretty_print();

Returns a human-readable string representation of the query.  Not appropriate for use for feeding to a prepare() statement.

=cut

sub pretty_print {
    my $self = shift;
    my %args = @_;
    my $op = $self->operation;
    my $prefix = $args{prefix} || '';
    my $str = $prefix . "$op\n";
    $prefix .= '  ';
    if ($op ne 'DELETE') {
        $str .= $prefix . "OUTPUT Columns:\n";
        foreach my $oc ($self->output_columns) {
            $str .= $prefix . '  ' . $oc->pretty_print(one_line => 1) . "\n";
        }
    }
    if ($op ne 'SELECT') {
        $str .= $prefix . 'TABLE: ' . $self->table->pretty_print(one_line => 1) . "\n";
    } else {
        $str .= $self->from->pretty_print(prefix => $prefix);
    }

    if (($op eq 'INSERT') || ($op eq 'UPDATE')) {
        $str .= $self->__pretty_print_inputs(prefix => $prefix);
    }
    if ($op eq 'INSERT' && $self->input_subquery()) {
        $str .= $prefix . "INPUT SUBQUERY:\n";
        $str .= $self->input_subquery->statement->pretty_print(prefix => $prefix . '  ');
    }
    if ($op ne 'INSERT') {
        if ($self->_cooked_where) {
            $str .= 'WHERE[cooked] ' . $self->_cooked_where() . "\n";
        } elsif ($self->raw_where) {
            $str .= 'WHERE[raw] ' . $self->raw_where() . "\n";
        } elsif ($self->where) {
            $str .= $self->where->pretty_print(prefix => $prefix);
        }
    }
    if ($self->order_by) {
        $str .= $self->order_by->pretty_print(prefix => $prefix);
    }
    if (defined $self->limit) {
        $str .= $prefix . 'LIMIT ' . $self->limit;
        if (defined $self->offset) {
            $str .= 'OFFSET ' . $self->offset;
        }
    }

    return $str;
}
sub __pretty_print_inputs {
    my $self = shift;
    my %args = @_;
    my $prefix = $args{prefix} || '';
    my $str = $prefix . "INPUTS:\n";
    foreach my $i ($self->inputs) {
        $str .= $prefix . '  ';
        $str .= $i->{column}->pretty_print(one_line => 1);
        if ($i->{param}) {
            $str .= ' = ';
            $str .= $i->{param}->pretty_print(one_line =>1);
            $str .= "\n";
        }
    }
    return $str;
}


=head2 $sql->set_default_output_aliases();

Ensures that each table and output column has 
an alias.  If a table or column already has
an alias, it is left alone.

=cut

sub set_default_output_aliases {
    my $self = shift;

    $self->__set_default_table_aliases();
    $self->__set_default_column_aliases();
}

sub __set_default_column_aliases {
    my $self = shift;

    # Make sure each output column has an alias
    foreach my $oc (grep { !defined($_->alias)} $self->output_columns) {
        my $exp = $oc->expression();
        if ($exp->is_column()) {
            my $col = $oc->expression();
            $oc->alias($col->table->alias() . '_' . $col->column);
        } else {
            # Make something up
            $oc->alias($self->new_column_alias());
        }
    }
}

sub __set_default_table_aliases {
    my $self = shift;
    my $counter = 0;

    my %tables_by_alias = map { $_->alias => $_ } grep { defined($_->alias) } $self->tables;

    # Make sure each table has an alias
    # Be sure to exclude those whose names look like a alias macro!
    foreach my $table (grep {!defined($_->alias)} $self->tables) {
        my $alias = 'ts' . $counter;
        while (exists $tables_by_alias{$alias}) {
            $counter++;
            $alias = 'ts' . $counter;
        }
        $table->alias($alias);
        $tables_by_alias{$alias} = $table;
    }

}


sub clone {
    my $self = shift;
    my $class = ref $self;

    my $other = $class->new($self->operation());

    # Scalars
    if (defined $self->limit) { $other->limit($self->limit()); }
    if (defined $self->offset) { $other->offset($self->offset()); }
    if (defined $self->raw_where) { 
        $other->raw_where($self->raw_where());
        if ($self->_cooked_where) { $other->_cooked_where($self->_cooked_where); }
        if ($self->_raw_where_execargs) { $other->_raw_where_execargs($self->_raw_where_execargs); }
        if ($self->_raw_where_params)   { $other->_raw_where_params([ map { $_->clone() } $self->_raw_where_params ]); }
    }

    # Single Objects
    if ($self->where)    { $other->where(    $self->where->clone()    ); }
    if ($self->get('table'))  { $other->table(    $self->table->clone()    ); }
    if ($self->from)     { $other->from(     $self->from->clone()     ); }
    if ($self->order_by) { $other->order_by( $self->order_by->clone() ); }
    if ($self->input_subquery) { $other->input_subquery($self->input_subquery->clone()); }

    # Lists of things
    foreach my $input (@{$self->{inputs}}) {
        push @{$other->{inputs}},
          {
           column => $input->{column}->clone(),
           param => $input->{param}->clone(),
          };
    }
    foreach my $output ($self->output_columns) {
        $other->add_output($output->clone());
    }

    return $other;

}


sub DESTROY {
    my $sql = shift;
    # Break links between all objects

    if ($sql->from && $sql->from->root_relation)  { $sql->from->root_relation->__break_links();   }
    if ($sql->where && $sql->where->root_criterion) { $sql->where->root_criterion->__break_links(); }
}

1;
