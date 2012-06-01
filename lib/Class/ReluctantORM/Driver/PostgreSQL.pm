package Class::ReluctantORM::Driver::PostgreSQL;

=head1 NAME

Class::ReluctantORM::Driver::PostgreSQL - PG driver for TB

=head1 SYNOPSIS

  # See Class::ReluctantORM::Driver


=head1 DRIVER IDIOSYNCRACIES

This driver supports PostgreSQL 8+.  It may work with 7.4, but this has not been tested.

This driver supports parsing.  See Class::ReluctantORM::Driver::PostgreSQL::Parsing for details.

=cut

use strict;
use warnings;

use DBI::Const::GetInfoType;
our $DEBUG = 0;

use Data::Dumper;
use Data::Diff;

use Scalar::Util qw(looks_like_number);
use base 'Class::ReluctantORM::Driver';
use Class::ReluctantORM::Exception;

use Class::ReluctantORM::SchemaCache;
use Class::ReluctantORM::SQL::Aliases;

use constant USE_EXPLICIT_TABLE_NAME => 0;
use constant USE_TABLE_ALIAS => 1;
use constant CREATE_TABLE_ALIAS => 2;
use constant CREATE_COLUMN_ALIAS => 3;
use constant USE_BARE_COLUMN => 4;

our %FUNCTION_RENDERERS;
our %COLUMN_CACHE; # Keyed by schema name, then table name, then column name, then hash of column info

use Class::ReluctantORM::Driver::PostgreSQL::Functions;
use Class::ReluctantORM::Driver::PostgreSQL::Parsing;
use Class::ReluctantORM::FetchDeep::Results qw(fd_inflate);

sub init {
    my $self = shift;
    $self->{open_quote}  = '"'; # $self->cro_dbh->get_info($GetInfoType{SQL_IDENTIFIER_QUOTE_CHAR}); # This is an expensive call for something that never changes
    $self->{close_quote} = $self->{open_quote};
}

sub supports_namespaces { return 1; }

sub aptitude {
    my ($class, $brand, $version) = @_;

    my $score = 0;
    if ($brand eq 'PostgreSQL') { $score += .8; }

    my ($maj, $min, $rel) = map { $_ + 0 } split /\./, $version;

    if ($maj == 7 || $maj == 8) {
        $score += .2;
    } elsif ($maj == 6 || $maj == 9) {
        $score += .1;
    }

    return $score;
}

sub find_primary_key_columns {
    my ($driver, $schema_name, $table_name) = @_;

    my $schema_cache = Class::ReluctantORM::SchemaCache->instance();
    my $pks = $schema_cache->read_primary_keys_for_table($schema_name, $table_name);
    if ($pks) {  return $pks;  }
    $pks = $driver->dbi_dbh->primary_keys(undef, $schema_name, $table_name);
    $schema_cache->store_primary_keys_for_table($schema_name, $table_name, $pks);
    return $pks;
}

sub read_fields {
    my $self = shift;
    my $schema_name = shift;
    my $table_name = shift;


    # We use two-layer caching - first layer is the whole-db cache file
    my $schema_cache = Class::ReluctantORM::SchemaCache->instance();
    my $fieldmap = $schema_cache->read_columns_for_table($schema_name, $table_name);
    if ($fieldmap) {
        return $fieldmap;
    }

    # OK, cache miss on the whole-db file.  Now fetch all columns in the namespace (PG schema).
    # (this lets us run one query per schema, not per table)
    $self->__populate_column_cache_for_schema($schema_name);

    unless (exists $COLUMN_CACHE{$schema_name}{$table_name}) {
        Class::ReluctantORM::Exception::Param::BadValue->croak
            (
             error => "Could not list columns for table '$schema_name.$table_name' - does it exist?",
             param => ' table or schema name',
             value => '$schema_name.$table_name',
            );
    }

    my @column_names;
    foreach my $col_info (values %{$COLUMN_CACHE{$schema_name}{$table_name}}) {
        if ($DEBUG > 1) { print STDERR __PACKAGE__ . ":" . __LINE__ . " - have column profile values:\n" . Dumper($col_info);  }
        $fieldmap->{lc($col_info->{COLUMN_NAME})} = $col_info->{COLUMN_NAME};
    }

    $schema_cache->store_columns_for_table($schema_name, $table_name, $fieldmap);

    return $fieldmap;
}

# Cache on a per-schema basis
sub __populate_column_cache_for_schema {
    my $self = shift;
    my $schema_name = shift;
     unless (exists $COLUMN_CACHE{$schema_name}) {
        if ($DEBUG) { print STDERR __PACKAGE__ . ":" . __LINE__ . " - column cache MISS for schema '$schema_name'";  }
        # Cache is empty - run column_info for all tables in that schema
        my $sth = $self->cro_dbh->column_info(undef, $schema_name, '%', '%');
        while (my $col_info = $sth->fetchrow_hashref()) {
            my $table = $col_info->{TABLE_NAME};
            $COLUMN_CACHE{$schema_name}{$table} ||= {};
            $COLUMN_CACHE{$schema_name}{$table}{lc($col_info->{COLUMN_NAME})} = $col_info;
        }
    }
}

sub purge_column_cache { %COLUMN_CACHE = (); }

# inherit sub run_sql

sub render {
    my $driver = shift;
    my $sql    = shift;
    my $hints  = shift;

    if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - Have run_sql operation: " . $sql->operation . "\n"; }

    unless ($hints->{already_transformed}) {
        $driver->_monitor_render_begin(sql_obj => $sql);
        $sql->annotate();
        $sql->reconcile();
        $driver->_monitor_render_transform(sql_obj => $sql);
    }

    my %dispatcher = (
                      INSERT => \&__pg_render_insert,
                      SELECT => \&__pg_render_select,
                      DELETE => \&__pg_render_delete,
                      UPDATE => \&__pg_render_update,
                     );
    my $str = $dispatcher{$sql->operation()}->($driver, $sql, $hints);

    $driver->_monitor_render_finish(sql_obj => $sql, sql_str => $str);
    $sql->_sql_string($str);
    $sql->_execution_driver($driver);

    return $str;
}




sub _pg_table_name {
    my $driver = shift;
    my $table = shift;
    my $tn = $driver->_pg_quoted($table->table);
    if ($table->schema) {
        $tn = $driver->_pg_quoted($table->schema) . '.' . $tn;
    }
    return $tn;
}

sub _pg_quoted {
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
    $driver->__pg_fd_transform_sql($sql);

    # Return results
    return fd_inflate($sql, $with, {already_transformed => 1});
}

#=============================================================================#
#                           SQL Rendering (Postgres Dialect)
#=============================================================================#

sub __pg_render_insert {
    my $driver = shift;
    my $sql = shift;
    my $str = '';

    $str .= 'INSERT INTO ';
    $str .= $driver->_pg_table_name($sql->table);
    $str .= ' (';
    $str .= join ',', map { $driver->_pg_quoted($_->{column}->column) } $sql->inputs();
    $str .= ') ';
    if ($sql->input_subquery()) {
        $str .= $driver->__pg_render_select($sql->input_subquery->statement);
    } else {
        $str .= ' VALUES (';
        $str .= join ',', map { '?' } $sql->inputs();
        $str .= ')';
    }

    # Build RETURNING clause if needed
    if ($sql->output_columns()) {
        $sql->set_default_output_aliases();
        $str .= ' RETURNING ';
        $str .= join ',', map { $driver->__pg_render_output_column($_, USE_EXPLICIT_TABLE_NAME) } $sql->output_columns();
    }

    return $str;
}

sub __pg_render_update {
    my $driver = shift;
    my $sql = shift;
    my $str = '';

    $str .= 'UPDATE ';
    $str .= $driver->__pg_render_table_name(
                                            $sql->table,
                                            #( ($driver->server_version >= 8.2) ? CREATE_TABLE_ALIAS : USE_EXPLICIT_TABLE_NAME)
                                           );
    $str .= ' SET ';

    $str .= join ',', map { 
        $driver->__pg_render_column_name($_->{column},USE_BARE_COLUMN,0,0)
          . ' = '
            . '?'
    } $sql->inputs();

    $str .= ' WHERE ';
    $str .= $driver->__pg_render_where_clause($sql, USE_EXPLICIT_TABLE_NAME);

    # Build RETURNING clause if needed
    if ($sql->output_columns()) {
        $sql->set_default_output_aliases();
        $str .= ' RETURNING ';
        $str .= join ',', map { $driver->__pg_render_output_column($_, USE_EXPLICIT_TABLE_NAME) } $sql->output_columns();
    }

    return $str;
}

sub __pg_render_delete {
    my $driver = shift;
    my $sql = shift;
    my $str = '';

    $str .= 'DELETE FROM ';
    $str .= $driver->__pg_render_table_name($sql->table);

    $str .= ' WHERE ';
    $str .= $driver->__pg_render_where_clause($sql, USE_EXPLICIT_TABLE_NAME);

    return $str;
}


sub __pg_fd_transform_sql {
    my $driver  = shift;
    my $sql = shift;

    $driver->_monitor_render_begin(sql_obj => $sql);
    $sql->make_inflatable(auto_reconcile => 1, auto_annotate => 1);

    if ($sql->limit) {
        $driver->__pg_transform_sql_fold_limit_for_deep($sql);
    }

    $driver->_monitor_render_transform(sql_obj => $sql);
}

sub __pg_render_select {
    my $driver = shift;
    my $sql = shift;
    my $str = "SELECT \n";
    $str .= $driver->__pg_render_output_column_list($sql->output_columns);
    $str .= "\n FROM \n";
    $str .= $driver->__pg_render_from_clause($sql->from);
    $str .= "\n WHERE \n";
    $str .= $driver->__pg_render_where_clause($sql, USE_TABLE_ALIAS);
    if ($sql->order_by->columns) {
        $str .= "\n ORDER BY \n";
        $str .= $driver->__pg_render_order_by_clause($sql->order_by);
    }
    if (defined ($sql->limit())) {
        $str .= " LIMIT " . $sql->limit() ."\n";
        if (defined ($sql->offset())) {
            $str .= " OFFSET " . $sql->offset() ."\n";
        }
    }
    return $str;
}

sub __pg_render_output_column {
    my $driver = shift;
    my $oc = shift;
    my $use_table_aliases = shift || USE_EXPLICIT_TABLE_NAME;
    my $str = $driver->__pg_render_expression($oc->expression, $use_table_aliases);
    if ($oc->alias) {
        $str .= ' AS ' . $oc->alias;
    }
    return $str;
}


sub __pg_render_column_name {
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
            $name = $driver->__pg_render_table_name($table) . '.';
        } elsif ($use_table_alias == USE_BARE_COLUMN) {
            # Do nothing
        }
        $name .=  $driver->_pg_quoted($col->column);
        if ($make_column_alias && $col->alias) {
            $name .= ' AS ' . $col->alias;
        }
    } elsif ($use_table_alias == USE_BARE_COLUMN) {
        $name .=  $driver->_pg_quoted($col->column);
        if ($make_column_alias && $col->alias) {
            $name .= ' AS ' . $col->alias;
        }
    }

    return $name;
}

sub __pg_render_table_name {
    my $driver = shift;
    my $table = shift;
    my $alias_mode = shift || USE_EXPLICIT_TABLE_NAME;
    my $name = '';

    if (($alias_mode == USE_TABLE_ALIAS) && $table->alias) {
        return $table->alias();
    }

    if ($table->schema) {
        $name .= $driver->_pg_quoted($table->schema) . '.';
    }
    $name .= $driver->_pg_quoted($table->table);
    return $name;
}

sub __pg_render_output_column_list {
    my $driver = shift;
    my @cols = @_;
    my $str = join ', ',
        map {
            $driver->__pg_render_output_column($_, USE_TABLE_ALIAS);
        } @cols;
    return $str;
}

sub __pg_render_order_by_clause {
    my $driver = shift;
    my $ob = shift;
    my $str = join ', ',
      map {
          $driver->__pg_render_column_name($_->[0], 1, 0, 1)
            . ' '
              . $_->[1]
      } $ob->columns_with_directions;
    return $str;
}

sub __pg_render_from_clause {
    my $driver = shift;
    my $from = shift;
    my $rel = $from->root_relation();
    return $driver->__pg_render_relation($rel);
}

sub __pg_render_where_clause {
    my $driver = shift;
    my $sql = shift;
    my $alias_mode = shift || USE_EXPLICIT_TABLE_NAME;
    if ($sql->raw_where) {
        unless ($sql->_cooked_where()) {
            Class::ReluctantORM::Exception::Call::ExpectationFailure->croak
                ('SQL has raw_where but no _cooked_where - did reconcile fail?');
        }
        return $sql->_cooked_where(); # Anything else needed? TODO - apply alias_mode?
    } else {
        my $where = $sql->where();
        unless ($where) { return '1=1'; }
        my $crit = $where->root_criterion();
        return $driver->__pg_render_criterion($crit, $alias_mode);
    }
}

sub __pg_render_relation {
    my $driver = shift;
    my $rel = shift;
    my $alias_mode = shift || USE_EXPLICIT_TABLE_NAME;
    my $sql = '';

    if ($rel->is_leaf_relation) {
        if ($rel->is_table) {
            $sql = $driver->__pg_render_table_name($rel, $alias_mode);
        } else {
            # Don't know how to handle this
            Class::ReluctantORM::Exception::Call::NotImplemented->croak(__PACKAGE__ . ' does not know how to render a non-table leaf relation');
        }
    } else {
        if ($rel->is_join) {
            $sql = '(' . $driver->__pg_render_relation($rel->left_relation, $alias_mode);
            $sql .= ' ' . $driver->__pg_render_join_type($rel->type) . ' ';
            $sql .= $driver->__pg_render_relation($rel->right_relation, $alias_mode);

            # Always use table alias in ON criteria - PG requires it
            $sql .= ' ON ' . $driver->__pg_render_criterion($rel->criterion, USE_TABLE_ALIAS) . ')';
        } else {
            Class::ReluctantORM::Exception::Call::NotImplemented->croak(__PACKAGE__ . ' does not know how to render a non-join non-leaf relation');
        }
    }

    if ($rel->alias) {
        $sql .= ' ' . $rel->alias;
    }

    return $sql;
}


sub __pg_render_join_type {
    my $driver = shift;
    my $raw_type = shift;
    return $raw_type . ' JOIN';
}

sub __pg_render_expression {
    my $driver = shift;
    my $exp = shift;
    my $use_table_aliases = shift || USE_EXPLICIT_TABLE_NAME;

    if ($exp->is_param) {
        return '?';
    } elsif ($exp->is_column) {
        return $driver->__pg_render_column_name($exp, $use_table_aliases, 0, 0);
    } elsif ($exp->is_literal) {
        return $driver->__pg_render_literal($exp);
   # Criterion case now handled by Function Call
    #} elsif ($exp->is_criterion) {
    #    return $driver->__pg_render_criterion($exp, $use_table_aliases);
    } elsif ($exp->is_function_call) {
        return $driver->__pg_render_function_call($exp, $use_table_aliases);
    } elsif ($exp->is_subquery()) {
        return $driver->__pg_render_subquery_as_expresion($exp);
    } else {
        # Don't know how to handle this
        my $type = ref($exp);
        Class::ReluctantORM::Exception::NotImplemented->croak(__PACKAGE__ . " does not know how to render a $type");
    }
}

sub __pg_render_literal {
    my $driver = shift;
    my $literal = shift;

    my $val = $literal->value();
    my $dt = $literal->data_type();

    if (0) {
    } elsif ($dt eq 'NULL') {
        return 'NULL';  # Not quoted
    } elsif ($dt eq 'BOOLEAN') {
        return $literal->is_equivalent(Literal->TRUE) ? 'TRUE' : 'FALSE'; # Not quoted
    } elsif (looks_like_number($val)) {
        return $val;
    } else {
        return "'$val'";
    }
}

sub __pg_render_subquery_as_expresion {
    my $driver = shift;
    my $subquery = shift;
    return '(' . $driver->__pg_render_select($subquery->statement()) . ')';
}


# This is currently an alias for function_call
sub __pg_render_criterion { return __pg_render_function_call(@_); }

sub __pg_render_function_call {
    my $driver = shift;
    my $criterion = shift;
    my $use_table_aliases = shift || USE_EXPLICIT_TABLE_NAME;

    # Avoid $_
    my @args;
    foreach my $arg ($criterion->arguments) {
        push @args, $driver->__pg_render_expression($arg, $use_table_aliases);
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

sub __pg_transform_sql_fold_limit_for_deep {
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
    if (!$sql->raw_where) {
        foreach my $table ($sql->where->tables()) {
            unless ($base_table->is_the_same_table($table)) {
                Class::ReluctantORM::Exception::SQL::TooComplex->croak(__PACKAGE__ . " can't handle a WHERE clause on a fetch_deep-with-limit that refers to anything other than the base table.");
            }
        }
    }

    # Create new SELECT statement, with re-aliased base references
    my $select = Class::ReluctantORM::SQL->new('select');
    my $alias = $sql->new_table_alias();
    my $table_copy = Table->new($base_table->class());
    $table_copy->alias($alias);
    $select->from(From->new($table_copy));

    # Extract and move where clause
    if ($sql->_cooked_where) {
        # Move outer where into inner

        # TODO - may wish we re-aliased things!
        $select->raw_where($sql->raw_where);
        $select->_cooked_where($sql->_cooked_where);
        $select->_raw_where_execargs($sql->_raw_where_execargs);
        $select->_raw_where_params([ $sql->_raw_where_params ]);

        # Clear outer where
        $sql->raw_where(undef);
        $sql->_cooked_where(undef);
        $sql->_raw_where_execargs([]);
        $sql->_raw_where_params([]);

    } else {
        my $inner_where = $sql->where();
        $sql->where(undef); # Clear outer where
        foreach my $col ($inner_where->columns) {
            # Force columns referenced in the where clause to refer to new, re-aliased copy of table
            $col->table($table_copy);
        }
        $select->where($inner_where);
    }

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
