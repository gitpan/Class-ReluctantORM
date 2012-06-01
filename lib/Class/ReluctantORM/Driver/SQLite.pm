package Class::ReluctantORM::Driver::SQLite;

=head1 NAME

Class::ReluctantORM::Driver::SQLite - SQLite driver for CRO

=head1 SYNOPSIS

  # See Class::ReluctantORM::Driver

=head1 DRIVER IDIOSYNCRACIES

Supports any version of sqlite supported by DBD::SQLite.

SQLite is itself fairly idiosyncratic.  Here are how they impact this driver:

=over

=item No RETURNING support on INSERT/UPDATE/DELETE

Since SQLite doesn't support this SQL extension, and DBD::SQLite doesn't support using output binds to read them (a hack the Oracle DBD provides), you can't use RETURNING to get columns from INSERT/UPDATE/DELETE statements you provide in SQL, nor can you add output columns to SQL objects performing those operations.  Worse, if your model uses refresh_on_update columns, they must be fetched by a separate SELECT (handled internally by the Driver, but you need to be aware of the penalty).

=item Column names forced to be lowercase


=back


=cut


    # How to obtain a primary key value with SQLite
    # 1.  If the table has a single-column integer PK, SQLite will effectively auto-increment,
    #     and the value returned by DBI->las_insert_id will be the new value for that column.
    # 2.  If the table has amultiple-column PK, or non-integer, SQLite will return a bigint
    #     rowid as the last_insert_id, which can then be used to SELECT the new primary key values.


use strict;
use warnings;

use DBI::Const::GetInfoType;
our $DEBUG = 0;

use Data::Dumper;
use Data::Diff;

use Scalar::Util qw(looks_like_number);
use base 'Class::ReluctantORM::Driver';
use Class::ReluctantORM::Exception;

use Class::ReluctantORM::SQL::Aliases;

use constant USE_EXPLICIT_TABLE_NAME => 0;
use constant USE_TABLE_ALIAS => 1;
use constant CREATE_TABLE_ALIAS => 2;
use constant CREATE_COLUMN_ALIAS => 3;
use constant USE_BARE_COLUMN => 4;

our %FUNCTION_RENDERERS;

use Class::ReluctantORM::Driver::SQLite::Functions;
use Class::ReluctantORM::FetchDeep::Results qw(fd_inflate);

sub init {
    my $self = shift;
    $self->{open_quote}  = $self->dbh->get_info($GetInfoType{SQL_IDENTIFIER_QUOTE_CHAR});
    $self->{close_quote} = $self->dbh->get_info($GetInfoType{SQL_IDENTIFIER_QUOTE_CHAR});
}

sub supports_namespaces { return 1; }  # Awkwardly, yes

sub aptitude {
    my ($class, $brand, $version) = @_;

    my $score = 0;
    if ($brand eq 'SQLite') { $score += .8; }

    # $version is like 3.7.2
    my ($maj, $min, $rel) = map { $_ + 0 } split /\./, $version;

    if ($maj == 3) {
        $score += .2;
    } elsif ($maj == 2) {
        $score += .1;
    }

    return $score;
}

sub read_fields {
    my $self = shift;
    my $schema = shift;
    my $table = shift;

    my $sth = $self->dbh->column_info(undef, $schema, $table, '%');

    my @column_names;
    while (my $col_info = $sth->fetchrow_hashref()) {
        if ($DEBUG > 1) { print STDERR __PACKAGE__ . ":" . __LINE__ . " - have column profile values:\n" . Dumper($col_info);  }
        push @column_names, $col_info->{COLUMN_NAME};
    }

    my %fieldmap = map { (lc($_) => lc($_)) } @column_names;

    return \%fieldmap;
}

sub find_primary_key_columns {
    my ($driver, $schema, $table) = @_;
    return $driver->dbi_dbh->primary_keys(undef, $schema, $table);
}


sub run_sql {
    my $driver = shift;
    my $sql  = shift;
    my $hints = shift;

    # Figure out if we need to split
    if ($driver->_sl_must_split_sql($sql, $hints)) {
        return $driver->_sl_split_and_run_sql($sql, $hints);
    } else {
        return $driver->_sl_run_single_sql($sql, $hints);
    }
}

sub _sl_must_split_sql {
    my $driver = shift;
    my $sql  = shift;
    my $hints = shift;

    if ($sql->operation eq 'SELECT') {
        return 0; # Can always run a SELECT directly

    } elsif (0 == $sql->output_columns ) {
        return 0; # Can always run one if there are no outputs (may happen with secondary Audit inserts)
    } else {
        # Can run directly if there are no non-pk output columns, AND the base table has a single-column primary key
        my $class = $sql->base_table->class();
        my @pk_columns = $class->primary_key_columns;

        if ($sql->operation eq 'INSERT') {
            # In this case, we have to go get the SQLite rowID, 
            # then use it to read out the primary key columns with a SELECT.
            if (@pk_columns > 1) { return 1; }

            # If we have a single-column PK, we'll read it using DBI's last insert id, 
            # so no need for a SELECT (unless we have non-pk columns in the output)
        }

        # OK, otherwise, we can do it in one query, so long as all output columns in the PK
        # (all PK columns get put on the refresh-on-update list, so if something isn't
        # on that list, it really is a refresh.  PKs themselves are supposed to be immutable in CRO.)
        foreach my $oc ($sql->output_columns()) {
            unless ($oc->expression->is_column && $oc->is_primary_key) {
                # Funky refresh column - must split
                return 1;
            }
        }

        # All clear
        return 0;
    }
}


sub _sl_split_and_run_sql {
    my $driver = shift;
    my $original_statement  = shift;
    my $hints = shift;

    #my $primary_statement   = $original_sql->clone();

    # Start building up the secondary statement
    my $secondary_statement = SQL->new('SELECT');
    my $base_table = $original_statement->base_table()->clone();
    $secondary_statement->from(From->new($base_table));

    # Copy output columns
    foreach my $oc ($original_statement->output_columns) {
        $secondary_statement->add_output($oc->clone());
    }

    # Figure out where clause
    my $where;
    if ($original_statement->operation eq 'INSERT') {
        # We'd only get here if the table has multi-column PKs OR has refresh-on-updates
        $where = Where->new();
        $where->and(
                    Criterion->new(
                                   '=',
                                   Column->new(table => $base_table, column =>  'oid'),
                                   Param->new(),
                                  ),
                   );

    } else {
        $where = $original_statement->where->clone();
    }
    $secondary_statement->where($where);

    # Build primary statement
    my $primary_statement = $original_statement->clone();
    $primary_statement->remove_all_outputs();

    # Run primary
    $driver->_sl_run_single_sql($primary_statement, $hints);

    # Collect params for secondary
    my @binds;
    if ($original_statement->operation eq 'INSERT') {
        # use SQLite OID fetcher
        @binds = (
                  $driver->dbi_dbh->last_insert_id('', $base_table->schema, $base_table->table, ''),
                 );
    } else {
        @binds = $original_statement->where ? map { $_->bind_value } $original_statement->where->params : ();
    }
    $secondary_statement->set_bind_values(@binds);

    # Run secondary
    $driver->_sl_run_single_sql($secondary_statement, $hints);

    # Copy output values from secondary to original
    # Note: since the original never got rendered, it will have 
    # fewer columns than the secondary.  We can't just replace them, 
    # because the caller may have saved references to the OCs.

    my %secondary_outputs = 
      map { ($_->expression->is_column ? $_->expression->column : $_->alias) => $_ }
        $secondary_statement->output_columns();

    # Copy over any values for columns that existed
    foreach my $original_output ($original_statement->output_columns()) {
        my $moniker = $original_output->expression->is_column ? 
          $original_output->expression->column : 
            $original_output->alias();
        $original_output->output_value($secondary_outputs{$moniker}->output_value);
        delete $secondary_outputs{$moniker};
    }

    # Clone any columns that were in secondary but not original
    foreach my $new_secondary (values %secondary_outputs) {
        $original_statement->add_output($new_secondary->clone);
    }

    return 1;
}


sub _sl_run_single_sql {
    my $driver = shift;
    my $sql  = shift;
    my $hints = shift;

    $driver->prepare($sql, $hints);
    my $sth = $sql->_sth();
    my $str = $sql->_sql_string();

    # OK, run the query
    my @binds = $sql->get_bind_values();
    $driver->_monitor_execute_begin(sql_obj => $sql, sql_str => $str, binds => \@binds, sth => $sth);
    $driver->_pre_execute_hook($sql);
    $sth->execute(@binds);
    $driver->_post_execute_hook($sql);
    $driver->_monitor_execute_finish(sql_obj => $sql, sql_str => $str, binds => \@binds, sth => $sth);

    # Fetch the result, if any
    if ($sql->output_columns) {
        while (my $row = $sth->fetchrow_hashref()) {
            $driver->_monitor_fetch_row(sql_obj => $sql, sql_str => $str, binds => \@binds, sth => $sth, row => $row);
            $sql->set_single_row_results($row);
        }
    }
    $sth->finish();
    $driver->_monitor_finish(sql_obj => $sql, sql_str => $str, sth => $sth);

    return 1;
}


sub render {
    my $driver = shift;
    my $sql    = shift;
    my $hints  = shift;

    if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - Have run_sql operation: " . $sql->operation . "\n"; }

    unless ($hints->{already_transformed}) {
        $driver->_monitor_render_begin(sql_obj => $sql);
        $sql->annotate();
        my $should_add_output_columns = $sql->operation eq 'SELECT';
        $sql->reconcile(add_output_columns => $should_add_output_columns);
        $driver->_monitor_render_transform(sql_obj => $sql);
    }

    my %dispatcher = (
                      INSERT => \&__sl_render_insert,
                      SELECT => \&__sl_render_select,
                      DELETE => \&__sl_render_delete,
                      UPDATE => \&__sl_render_update,
                     );
    my $str = $dispatcher{$sql->operation()}->($driver, $sql, $hints);

    $driver->_monitor_render_finish(sql_obj => $sql, sql_str => $str);
    $sql->_sql_string($str);
    $sql->_execution_driver($driver);

    return $str;
}

sub _post_execute_hook {
    my $driver = shift;
    my $sql = shift;
    my $hints = $sql->execute_hints() || {};

    if ($sql->operation eq 'INSERT' && $hints->{set_pk_from_last_insert_id}) {
        my $table = $sql->base_table();
        my ($oc) = $sql->output_columns();  # Should only be 1
        my $last_insert_id = $driver->dbi_dbh()->last_insert_id
          (
           undef,  # sqlite doesn't use catalogs
           $table->schema,
           $table->table,
           $oc->expression->column,
          );

        $oc->output_value($last_insert_id);
    }
}


sub _sl_table_name {
    my $driver = shift;
    my $table = shift;
    my $tn = $driver->_sl_quoted($table->table);
    if ($table->schema) {
        $tn = $driver->_sl_quoted($table->schema) . '.' . $tn;
    }
    return $tn;
}

sub _sl_quoted {
    my $driver = shift;
    my $text = shift;
    return '"' . $text . '"';
}

sub execute_fetch_deep {
    my $driver  = shift;
    my $sql = shift;
    my $with = shift;

    $with->{__upper_table} ||= $sql->base_table();

    # Transform SQL
    $driver->__sl_fd_transform_sql($sql);

    # Return results
    return fd_inflate($sql, $with, {already_transformed => 1});
}

#=============================================================================#
#                        SQL Rendering (SQLite Dialect)
#=============================================================================#

sub __sl_render_update {
    my $driver = shift;
    my $sql = shift;
    my $str = '';

    if ($sql->output_columns()) {
        Class::ReluctantORM::Exception::Param::BadValue->croak
            (
             error => "The SQLite driver does not permit UPDATE statements to have output columns (no RETURNING support)",
             param => 'sql_obj',
             value => $sql,
            );
    }


    $str .= 'UPDATE ';
    $str .= $driver->__sl_render_table_name($sql->table);
    $str .= ' SET ';

    $str .= join ',', map { 
        $driver->__sl_render_column_name($_->{column},USE_BARE_COLUMN,0,0)
          . ' = '
            . '?'
    } $sql->inputs();

    $str .= ' WHERE ';
    $str .= $driver->__sl_render_where_clause($sql->where, USE_EXPLICIT_TABLE_NAME);

    return $str;
}


sub __sl_render_insert {
    my $driver = shift;
    my $sql = shift;
    my $str = '';

    my $set_pk_from_last_insert_id = 0;
    my @outputs = $sql->output_columns();

    if (@outputs == 1 && $outputs[0]->is_primary_key()) {
        $set_pk_from_last_insert_id = 1;
    } elsif (@outputs == 0) {
        $set_pk_from_last_insert_id = 0;
    } else {
        Class::ReluctantORM::Exception::Param::BadValue->croak
            (
             error => "The SQLite driver does not permit INSERT statements to have output columns (no RETURNING support)",
             param => 'sql_obj',
             value => $sql,
            );
    }

    $str .= 'INSERT INTO ';
    $str .= $driver->_sl_table_name($sql->table);
    $str .= ' (';
    $str .= join ',', map { $driver->_sl_quoted($_->{column}->column) } $sql->inputs();
    $str .= ') ';
    if ($sql->input_subquery()) {
        $str .= $driver->__sl_render_select($sql->input_subquery->statement);
    } else {
        $str .= ' VALUES (';
        $str .= join ',', map { '?' } $sql->inputs();
        $str .= ')';
    }

    $sql->execute_hints({ set_pk_from_last_insert_id => $set_pk_from_last_insert_id });

    return $str;
}

sub __sl_render_delete {
    my $driver = shift;
    my $sql = shift;
    my $str = '';

    if ($sql->output_columns()) {
        Class::ReluctantORM::Exception::Param::BadValue->croak
            (
             error => "The SQLite driver does not permit DELETE statements to have output columns (no RETURNING support)",
             param => 'sql_obj',
             value => $sql,
            );
    }


    $str .= 'DELETE FROM ';
    $str .= $driver->__sl_render_table_name($sql->table);

    $str .= ' WHERE ';
    $str .= $driver->__sl_render_where_clause($sql->where, USE_EXPLICIT_TABLE_NAME);

    return $str;
}

sub __sl_render_select {
    my $driver = shift;
    my $sql = shift;
    my $str = "SELECT \n";
    $str .= $driver->__sl_render_output_column_list($sql->output_columns);
    $str .= "\n FROM \n";
    $str .= $driver->__sl_render_from_clause($sql->from);
    $str .= "\n WHERE \n";
    $str .= $driver->__sl_render_where_clause($sql->where, USE_TABLE_ALIAS);
    if ($sql->order_by->columns) {
        $str .= "\n ORDER BY \n";
        $str .= $driver->__sl_render_order_by_clause($sql->order_by);
    }
    if (defined ($sql->limit())) {
        $str .= " LIMIT " . $sql->limit() ."\n";
        if (defined ($sql->offset())) {
            $str .= " OFFSET " . $sql->offset() ."\n";
        }
    }
    return $str;
}


#=================================================================================#
#                           SQL Clause Rendering
#=================================================================================#


sub __sl_render_output_column {
    my $driver = shift;
    my $oc = shift;
    my $use_table_aliases = shift || USE_EXPLICIT_TABLE_NAME;
    my $str = $driver->__sl_render_expression($oc->expression, $use_table_aliases);
    if ($oc->alias) {
        $str .= ' AS ' . $oc->alias;
    }
    return $str;
}

sub __sl_render_output_column_list {
    my $driver = shift;
    my @cols = @_;
    my $str = join ', ',
        map {
            $driver->__sl_render_output_column($_, USE_TABLE_ALIAS);
        } @cols;
    return $str;
}

sub __sl_render_order_by_clause {
    my $driver = shift;
    my $ob = shift;
    my $str = join ', ',
      map {
          $driver->__sl_render_column_name($_->[0], 1, 0, 1)
            . ' '
              . $_->[1]
      } $ob->columns_with_directions;
    return $str;
}

sub __sl_render_from_clause {
    my $driver = shift;
    my $from = shift;
    my $rel = $from->root_relation();
    return $driver->__sl_render_relation($rel);
}

sub __sl_render_where_clause {
    my $driver = shift;
    my $where = shift;
    unless ($where) { return '1=1'; }
    my $use_table_aliases = shift || USE_EXPLICIT_TABLE_NAME;
    my $crit = $where->root_criterion();
    return $driver->__sl_render_criterion($crit, $use_table_aliases);
}



#=================================================================================#
#                           SQL Expression Rendering
#=================================================================================#

sub __sl_render_expression {
    my $driver = shift;
    my $exp = shift;
    my $use_table_aliases = shift || USE_EXPLICIT_TABLE_NAME;

    if ($exp->is_param) {
        return '?';
    } elsif ($exp->is_column) {
        return $driver->__sl_render_column_name($exp, $use_table_aliases, 0, 0);
    } elsif ($exp->is_literal) {
        my $val = $exp->value;
        if (looks_like_number($val)) {
            return $val;
        } else {
            return "'$val'";
        }
    # Criterion case now handled by Function Call
    #} elsif ($exp->is_criterion) {
    #    return $driver->__sl_render_criterion($exp, $use_table_aliases);
    } elsif ($exp->is_function_call) {
        return $driver->__sl_render_function_call($exp, $use_table_aliases);
    } elsif ($exp->is_subquery()) {
        return $driver->__sl_render_subquery_as_expresion($exp);
    } else {
        # Don't know how to handle this
        my $type = ref($exp);
        Class::ReluctantORM::Exception::NotImplemented->croak(__PACKAGE__ . " does not know how to render a $type");
    }
}

sub __sl_render_column_name {
    my $driver = shift;
    my $col = shift;
    my $use_table_alias = shift || USE_EXPLICIT_TABLE_NAME;
    my $use_column_alias = shift || 0;
    my $make_column_alias = shift || 0;

    my $table = $col->table;

    my $name = '';

    if ($use_column_alias && $col->alias) {
        $name = $col->alias;
    } elsif ($table) {
        if ($use_table_alias == USE_TABLE_ALIAS) {
            $name .= $table->alias . '.';
        } elsif ($use_table_alias == USE_EXPLICIT_TABLE_NAME) {
            $name = $driver->__sl_render_table_name($table) . '.';
        } elsif ($use_table_alias == USE_BARE_COLUMN) {
            # Do nothing
        }
        $name .=  $driver->_sl_quoted($col->column);
        if ($make_column_alias && $col->alias) {
            $name .= ' AS ' . $col->alias;
        }
    } elsif ($use_table_alias == USE_BARE_COLUMN) {
        $name .=  $driver->_sl_quoted($col->column);
        if ($make_column_alias && $col->alias) {
            $name .= ' AS ' . $col->alias;
        }
    }

    return $name;
}

sub __sl_render_table_name {
    my $driver = shift;
    my $table = shift;
    my $alias_mode = shift || USE_EXPLICIT_TABLE_NAME;
    my $name = '';

    if (($alias_mode == USE_TABLE_ALIAS) && $table->alias) {
        return $table->alias();
    }

    if ($table->schema) {
        $name .= $driver->_sl_quoted($table->schema) . '.';
    }
    $name .= $driver->_sl_quoted($table->table);
    return $name;
}


sub __sl_render_relation {
    my $driver = shift;
    my $rel = shift;
    my $alias_mode = shift || USE_EXPLICIT_TABLE_NAME;
    my $sql = '';

    if ($rel->is_leaf_relation) {
        if ($rel->is_table) {
            $sql = $driver->__sl_render_table_name($rel, $alias_mode);
        } else {
            # Don't know how to handle this
            Class::ReluctantORM::Exception::Call::NotImplemented->croak(__PACKAGE__ . ' does not know how to render a non-table leaf relation');
        }
    } else {
        if ($rel->is_join) {
            $sql = '(' . $driver->__sl_render_relation($rel->left_relation, $alias_mode);
            $sql .= ' ' . $driver->__sl_render_join_type($rel->type) . ' ';
            $sql .= $driver->__sl_render_relation($rel->right_relation, $alias_mode);

            # Always use table alias in ON criteria - PG requires it
            $sql .= ' ON ' . $driver->__sl_render_criterion($rel->criterion, USE_TABLE_ALIAS) . ')';
        } else {
            Class::ReluctantORM::Exception::Call::NotImplemented->croak(__PACKAGE__ . ' does not know how to render a non-join non-leaf relation');
        }
    }

    if ($rel->alias) {
        $sql .= ' ' . $rel->alias;
    }

    return $sql;
}


sub __sl_render_join_type {
    my $driver = shift;
    my $raw_type = shift;
    return $raw_type . ' JOIN';
}

sub __sl_render_subquery_as_expresion {
    my $driver = shift;
    my $subquery = shift;
    return '(' . $driver->__sl_render_select($subquery->statement()) . ')';
}


# This is currently an alias for function_call
sub __sl_render_criterion { return __sl_render_function_call(@_); }

sub __sl_render_function_call {
    my $driver = shift;
    my $criterion = shift;
    my $use_table_aliases = shift || USE_EXPLICIT_TABLE_NAME;

    # Avoid $_
    my @args;
    foreach my $arg ($criterion->arguments) {
        push @args, $driver->__sl_render_expression($arg, $use_table_aliases);
    }

    my $sql;
    my $func = $criterion->function();
    if (exists $FUNCTION_RENDERERS{$func->name()}) {
        $sql = $FUNCTION_RENDERERS{$func->name()}->(@args);
    } else {
        Class::ReluctantORM::Exception::NotImplemented->croak(__PACKAGE__ . " does not know how to render a function call for function " . $func->name());
    }
    return $sql;
}


sub __sl_fd_transform_sql {
    my $driver  = shift;
    my $sql = shift;

    $driver->_monitor_render_begin(sql_obj => $sql);
    $sql->make_inflatable(auto_reconcile => 1, auto_annotate => 1);

    if ($sql->limit) {
        $driver->__sl_transform_sql_fold_limit_for_deep($sql);
    }

    $driver->_monitor_render_transform(sql_obj => $sql);
}


#=============================================================================#
#                   Fetch Deep SQL Transformation
#=============================================================================#

=begin devnotes

=head2 Transformations on Limits, Offsets, and Ordering In Joins

Given:
Ship->fetch_deep(
                 where => <WHERE1>
                 with => { pirates => {}},
                 limit => <LIMIT1>,
                 order_by => <ORDER1>,
                 offset => <OFFSET1>,
                );

Initial SQL looks like:
SELECT
   FROM
     TABLE (Ship)
     LEFT OUTER JOIN TABLE (Pirate)
  WHERE <WHERE1>
  ORDER BY <ORDER1>
  LIMIT <LIMIT1>
  OFFSET <OFFSET1>

This is wrong - the limit will apply to the ship-pirate join,
when it should apply only to ships.

Transform to:
SELECT
   FROM
     TABLE (Ship)
     LEFT OUTER JOIN TABLE (Pirate)
  WHERE CompositePK(Ship) IN (
         SUBSELECT CompositePK(Ship)
              FROM Ship
            WHERE <WHERE1>
            ORDER BY <ORDER1>
          LIMIT <LIMIT1>
          OFFSET <OFFSET1>
        )
  ORDER BY <ORDER1>

with the additional constraints that:
  - ORDER1 may only refer to Ship
  - WHERE1 may only refer to Ship
  - WHERE1 and ORDER1 must be re-aliasable


=cut

sub __sl_transform_sql_fold_limit_for_deep {
    my $driver = shift;
    my $sql = shift;

    # Determine the base table
    my $base_table = $sql->base_table();
    unless ($base_table->is_table()) {
        Class::ReluctantORM::Exception::NotImplemented->croak(__PACKAGE__ . " doesn't know what to do with a non-table base relation");
    }

    # Check that order clause only refers to base table
    foreach my $table ($sql->order_by->tables()) {
        unless ($base_table->is_the_same_table($table)) {
            Class::ReluctantORM::Exception::SQL::TooComplex->croak(__PACKAGE__ . " can't handle a ORDER BY clause on a fetch_deep that refers to anything other than the base table.");
        }
    }

    # Check that where clause only refers to base table
    foreach my $table ($sql->where->tables()) {
        unless ($base_table->is_the_same_table($table)) {
            Class::ReluctantORM::Exception::SQL::TooComplex->croak(__PACKAGE__ . " can't handle a WHERE clause on a fetch_deep-with-limit that refers to anything other than the base table.");
        }
    }

    # Create new SELECT statement, with re-aliased base references
    my $select = Class::ReluctantORM::SQL->new('select');
    my $alias = $sql->new_table_alias();
    my $table_copy = Table->new($base_table->class());
    $table_copy->alias($alias);
    $select->from(From->new($table_copy));

    # Extract and move where clause
    my $inner_where = $sql->where();
    $sql->where(undef); # Clear outer where
    foreach my $col ($inner_where->columns) {
        # Force columns referenced in the where clause to refer to new, re-aliased copy of table
        $col->table($table_copy);
    }
    $select->where($inner_where);

    # Copy order by clause, re-alias, and attach to select statement
    my $inner_ob = OrderBy->new();
    foreach my $crit ($sql->order_by->columns_with_directions) {
        my ($outer_col, $direction) = @$crit;
        my $inner_col = Column->new(column => $outer_col->column, table => $table_copy);
        $inner_ob->add($inner_col, $direction);
    }
    $select->order_by($inner_ob);

    # Move limit and offset clauses to inner select
    $select->limit($sql->limit());
    $sql->limit(undef);
    $select->offset($sql->offset());
    $sql->offset(undef);

    # Alter SELECT statement to return composite PK
    my $oc = OutputColumn->new(
                               expression => 
                               FunctionCall->new('key_compositor_inside_subquery', $table_copy->primary_key_columns()),
                               alias => '',
                              );
    $select->add_output($oc);


    # Replace top-level WHERE with single criteria, seeking a composite key in the subselect
    my $new_top_where = Where->new(
                                   Criterion->new(
                                                  'in',
                                                  FunctionCall->new('key_compositor_outside_subquery', $base_table->primary_key_columns()),
                                                  SubQuery->new($select),
                                                 )
                                  );
    $sql->where($new_top_where);


}

1;
