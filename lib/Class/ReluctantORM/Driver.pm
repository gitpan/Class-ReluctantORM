package Class::ReluctantORM::Driver;

=head1 NAME

Class::ReluctantORM::Driver - Abstract interface for RDBMSs

=head1 SYNOPSIS

  # ---- Driver Creation ----
  # Driver instantiation is usually implicit
  MyClass->build_class($dbh => $some_database_handle);
  $driver = MyClass->driver();

  # To do it explicitly...
  # Create a raw database connection (or use a Class::ReluctantORM::DBH subclass)
  my $dbh = DBI->connect(...);

  # Now make a driver from the handle.  The $dbh will
  # be checked for its RDBMS brand and version, and the
  # best-matching Driver subclass will be used.
  my $driver = Class::ReluctantORM::Driver->make_driver($cro_class, $dbh);

  # ---- SQL Parsing ----
  if ($driver->supports_parsing()) {
    $sql_obj = $driver->parse_statement("SELECT foo FROM bar WHERE baz = 'beep'");
    $sql_where = $driver->parse_where("baz = 'beep'");
    $sql_order_by = $driver->parse_order_by("dog DESC");
  }

  # ---- SQL Rendering ----
  $sql_string = $driver->render($sql_obj, $hints);

  # ---- SQL Execution ----
  $driver->run_sql($sql_obj);

  # If you like prepare-execute cycles...
  $driver->prepare($sql_obj);
  if ($sql_obj->is_prepared()) {
      $sql_obj->execute(@bindings);  # Use output columns or callback to fetch results
      $sql_obj->finish();
  }

  # Or just get the DBI dbh and bang on it directly
  my $dbi_dbh = $driver->dbi_dbh();

=cut

=for vaporware

  # (normally in autocommit mode)
  if ($driver->supports_transactions()) {
     $driver->begin_transaction();

     # Do things....

     if ($driver->is_in_transaction()) { ... }

     if (...) {
        $driver->commit_transaction();
     } else {
        $driver->rollback_transaction();
     }
     # Returns to autocommit after a call to either commit or rollback

  }

=cut

=pod

  # ---- DB MetaData and SQL String Utils ----
  $field2column_map = $driver->read_fields($schema, $table);     # May be cached
  @columns = $driver->find_primary_key_columns($schema, $table); # May be cached
  $char = $driver->open_quote();
  $char = $driver->close_quote();
  $char = $driver->name_separator();
  $str = $driver->table_case($name);
  $str = $driver->schema_case($name);
  $str = $driver->column_case($name);

=head1 DESCRIPTION

The Driver facility provides RDBMS-specific dialect support.  In other words, high-level methods (like reading a list of fields in a table) are available via any Driver, while behind the scenes a Driver subclass is speaking a particular dialect to your database handle.

Drivers provide five major groups of functionality: 

=over

=item Database metadata access

List columns, keys, etc.  This area is immature.

=item SQL Generation

Transform a CRO SQL object into a SQL string in the driver's dialect.  See render()

=item SQL Execution

Using the Class::ReluctantORM::DBH, execute SQL strings on the database and retrieve the results.  See prepare()

=item SQL Parsing

Transform a SQL string in the Driver's dialect into a CRO SQL object for later manipulation.  Such support is just out of its infancy - call it toddler-dom.  See supports_parsing(), parse_statement(), and parse_where().

=item Monitor Support

Drivers are the internal attachment point for Monitors, which are used to track database access.

=back

This page documents the Driver superclass, which specifies the API provided by all drivers.  See the individual drivers to learn more about their idiosyncracies (eg Class::ReluctantORM::Driver::PostgreSQL).

Each Class::ReluctantORM subclass has its own Driver (this allows different classes to originate from different databases).  It is expected that you will re-use database handles across multiple drivers, however.

=head1 METHODS TO OVERRIDE WHEN SUBCLASSING

When creating an RDBMS-specific subclass, you will need to override the following methods:

=over

=item aptitude

=item init

=item read_fields

=item find_primary_key_columns

=item run_sql

=item execute_fetch_deep

=item render

=item prepare

=item supports_parsing

=item parse_statement

=item parse_where

=item parse_order_by

=back

=cut

use strict;
use warnings;
use DBI::Const::GetInfoType;
use Scalar::Util qw(blessed weaken);
use Class::ReluctantORM::Utilities qw(conditional_load_subdir);
use Data::Dumper;

use Class::ReluctantORM::Monitor;
use Class::ReluctantORM::SQL::Aliases;

our $DEBUG = 0;

our @DRIVERS;
BEGIN {
    @DRIVERS = conditional_load_subdir(__PACKAGE__);
}


=head1 INSTANCE METHODS

=cut

=head2 $dbh = $driver->cro_dbh();

Returns the Class::ReluctantORM::DBH object that provides low-level connectivity.

=cut

sub cro_dbh {
    my $self = shift;

    if ($self->{db_class}) {
        # Call new() and return whatever we get back - this allows for connection pooling
        return $self->{db_class}->new();
    } else {
        # Bare handle?
        return $self->{dbh};
    }
}

=head2 $dbh = $driver->dbi_dbh();

Returns a DBI database handle.  If the  Class::ReluctantORM::DBH object is not based on DBI, this will be undef.

=cut

sub dbi_dbh {
    my $self = shift;
    return $self->cro_dbh()->dbi_dbh();
}

#  awful old name
sub dbh { return $_[0]->cro_dbh(); }

# TODO DOCS
sub open_quote     { return '"'; }
sub close_quote    { return '"'; }
sub name_separator { return '.'; }

=head2 $bool = $driver->supports_namespaces()

Returns true if the driver supports namespaces (schemae - containers within a database for tables).  If so, you can use name_separator to construct fully qualified table names.

Default implementation returns false.

=cut

sub supports_namespaces { return 0; }



#==================================================================#
#                          Protected Methods
#==================================================================#

sub _install_dbh_error_trap {
    my $inv = shift;
    my $dbh = shift;
    $dbh->set_handle_error(Class::ReluctantORM::Exception->make_db_error_handler());
}

sub _new {
    my $class = shift;
    my ($tb_class, $dbh, $dbclass, $brand, $version) = @_;
    my $self = bless {
                      tb_class => $tb_class,
                      dbh   => $dbh,
                      db_class => $dbclass,
                      brand => $brand,
                      version => $version,
                      monitors => [],
                     }, $class;
    if ($dbh) { weaken($self->{dbh}); }
    $self->init();
    return $self;
}

#==================================================================#
#                         DRIVER SETUP
#==================================================================#


=head1 DRIVER SETUP METHODS

=cut

=head2 $driver = Class::ReluctantORM::Driver->make_driver($cro_class, $dbhandle, $dbclass);

Searches for the best available driver for the given database handle.  The Class::ReluctantORM subclass name is passed for advisory purposes to the underlying driver subclass.

=cut

my %DBMS_INFO_CACHE;

sub make_driver {
    my $class = shift;
    my $cro_class = shift;
    my $dbh = shift;
    my $dbclass = shift;

    $dbh = Class::ReluctantORM::DBH->_boost_to_cro_dbh($dbh);
    $class->_install_dbh_error_trap($dbh);
    my $brand = $DBMS_INFO_CACHE{$dbh->dbi_dbh}{SQL_DBMS_NAME}  ||= $dbh->get_info($GetInfoType{SQL_DBMS_NAME}); # Expensive call, which will not change for the specific dbh
    my $version = $DBMS_INFO_CACHE{$dbh->dbi_dbh}{SQL_DBMS_VER} ||= $dbh->get_info($GetInfoType{SQL_DBMS_VER}); # Expensive call, which will not change for the specific dbh

    my @scores = sort { $b->[1] <=> $a->[1] } map { [ $_, $_->aptitude($brand, $version) ] } @DRIVERS;

    if ($DEBUG > 1) {
        print STDERR __PACKAGE__ . ':' . __LINE__ . " - have make_driver scores for $brand, $version:\n" . Dumper(\@scores);
    }

    if ($scores[0][1] < 0.5) {
        warn("For database $brand $version, no suitable Class::ReluctantORM::Driver could be found. Using $scores[0][0] (score: $scores[0][1])");
    }

    my $impl_class = $scores[0][0];
    my $self = $impl_class->_new($cro_class, $dbh, $dbclass, $brand, $version);

    return $self;
}

=head2 $n = $class->aptitude($brand, $version);

Should return a number between 0 and 1 indicating how well the 
driver can handle the given type of database server.  Scores less than 0.5 
are considered ill-equipped.  The highest-scoring driver will be used.

Default implementation returns 0.


=cut

sub aptitude { return 0; }

=head2 $driver->init();

Called with no args just after driver construction.  The dbh is 
available at this point.

Default implementation does nothing.

=cut

sub init {}


#==================================================================#
#                         DB METADATA
#==================================================================#

=head1 DATABASE METADATA METHODS

=cut

=head2 $version_string = $driver->server_brand();

Returns the vendor-specific brand name (cached from Driver creation, the result of a DBI get_info SQL_DBMS_NAME call).

=cut

sub server_brand {
    my $driver = shift;
    return $driver->{brand};
}

=head2 $version_string = $driver->server_version();

Returns the vendor-specific version string (cached from Driver creation, the result of a DBI get_info SQL_DBMS_VER call).

=cut

sub server_version {
    my $driver = shift;
    return $driver->{version};
}



=head2 $field2column_map = $driver->read_fields($schema_name, $table_name);

Scans the associated table for columns, and returns a hash mapping
lowercased field named to database case column names.

This data may be cached.  See the Class::ReluctantORM global option schema_cache_policy, and the Class::ReluctantORM::SchemaCache .

Default implementation throws a PureVirtual exception.  You must override this method.

=cut

sub read_fields { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 @cols = $driver->find_primary_key_columns();

Returns a list of columns used in the table's primary key.  This is 
usually a one-item list.

This data may be cached.  See the Class::ReluctantORM global option schema_cache_policy, and Class::ReluctantORM::SchemaCache .

Default implementation throws a PureVirtual exception.  You must override this method.

=cut

sub find_primary_key_columns { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $str = $driver->schema_case($str);

=head2 $str = $driver->table_case($str);

=head2 $str = $driver->column_case($str);

Adjusts the given string to be in the case expected by the driver for the given object type.

Default implementation is to lowercase everything.

=cut

sub schema_case { return lc($_[1]); }
sub table_case  { return lc($_[1]); }
sub column_case { return lc($_[1]); }

#==================================================================#
#                         SQL EXECUTION
#==================================================================#

=head1 SQL EXECUTION SUPPORT

These methods provide the rendering and execution capabilities of the driver.

=head2 Execution Hinting

Most of the execution support methods accept an optional hashref that provides hints for the driver.  Currently supported hints:

=over

=item already_transformed

Boolean, default false.  If true, indicates that the given SQL object has already been through its transformation phase, and should not be trnasformed again.

=back

Specific drivers may extend this list.

=cut



=head2 $driver->run_sql($sql, $hints);

Executes the given SQL object.  The SQL object then contains
the results; you can also use the $sql->add_fetchrow_listener() method to add a hook.

The 'hints' arg is optional, and is a hashref as specified in EXECUTION HINTING.

A reasonable default implementation is provided.

=cut

sub run_sql {
    my $driver = shift;
    my $sql  = shift;
    my $hints = shift;

    $driver->prepare($sql, $hints);
    my $sth = $sql->_sth();
    my $str = $sql->_sql_string();

    # OK, run the query
    my @binds = $sql->get_bind_values();
    $driver->_monitor_execute_begin(sql_obj => $sql, sql_str => $str, binds => \@binds, sth => $sth);
    eval {
        $sth = $driver->cro_dbh->execute($sth, @binds);
    };
    if ($@) {
        # Rethrow so we have CRO tracing
        Class::ReluctantORM::SchemaCache->instance->notify_sql_error($@);
        Class::ReluctantORM::Exception::SQL::ExecutionError->croak(error => $@);
    }
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


=head2 @results = $driver->execute_fetch_deep($sql_obj);

Renders, executes, and parses the results of a "fetch deep" style SQL query.  Internally, this may be the same as run_sql; but some drivers may need to perform extra transformations for fetch deep (to allow use of LIMIT clauses, for example).

=cut

sub execute_fetch_deep { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $str = $driver->render($sql_obj, $hints);

Takes a Class::ReluctantORM::SQL object, and renders it down to a SQL string.

The 'hints' arg is optional, and is a hashref as specified in EXECUTION HINTING.

=cut

sub render { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $sth = $driver->prepare($sql_obj, $hints);

Renders the SQL object to a SQL string, then passes it through the underlying DBI dbh, and returns the resulting statement handle.  You can then use either $sql_obj->execute() or if you prefer a lower-level approach, you can operate directly on the statement handle.

The 'hints' arg is optional, and is a hashref as specified in EXECUTION HINTING.

A reasonable default implementation is provided.

=cut

sub prepare {
    my $driver = shift;
    my $sql = shift;
    my $hints = shift;

    $driver->render($sql);
    my $sth = $driver->cro_dbh->prepare($sql->_sql_string());
    $sql->_sth($sth);

    return $sth;
}

sub _pre_execute_hook { }
sub _post_execute_hook { }

#==================================================================#
#                         TRANSACTION SUPPORT
#==================================================================#

# TODO - transaction support

#==================================================================#
#                         PARSING SUPPORT
#==================================================================#

=head2 $bool = $driver->supports_parsing();

Returns true if you can call parse_statement() or parse_where() and expect it to possibly work.

Default implementation returns false.

=cut

sub supports_parsing { return 0; }

=head2 $sql_obj = $driver->parse_statement($sql_string, $options);

EXERIMENTAL

Examines the $sql_string, and builds a Class::ReluctantORM::SQL object that semantically represents the statement.  Syntax details will not be preserved - you can't round-trip this.

If a problem occurs, an exception will be thrown, either Class::ReluctantORM::Exception::SQL::ParseError (your fault for sending garbage) or Class::ReluctantORM::Exception::SQL::TooComplex (our fault for having a weaksauce parser).

$options is an optional hashref that contains parsing options or hints for the parser.  It may be dialect specific - see your driver subclass for details.

=cut

sub parse_statement { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $where_obj = $driver->parse_where($where_clause_string, $options);

EXERIMENTAL

Examines the $where_clause_string, and builds a Class::ReluctantORM::SQL::Where object that semantically represents the WHERE clause.  You should not include the word WHERE.

Other details as for parse_statement.

=cut

sub parse_where { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $order_by_obj = $driver->parse_order_by($order_clause_string, $options);

EXERIMENTAL

Examines the $order_clause_string, and builds a Class::ReluctantORM::SQL::OrderBy object that semantically represents the clause.  You should not include the words ORDER BY.

Other details as for parse_statement.

A default implementation is provided.  It's stupid - it only allows column names and sort directions, no expressions.

=cut

sub parse_order_by {
    my $driver = shift;
    my $sql = shift || '';

    # Permit (but ignore) any ORDER BY prefix
    $sql =~ s/^ORDER BY\s+//i;

    # Split on commas
    my @stanzas = split /\s*,\s*/, $sql;
    my $ob = OrderBy->new();
    my %permitted = map { $_ => 1 } qw(asc ASC desc DESC);
    foreach my $stanza (@stanzas) {
        $stanza =~ s/^\s+//; # strip leading spaces
        $stanza =~ s/\s+$//; # strip trailing spaces
        my ($col_name, $dir) = split /\s+/, $stanza;
        if ($dir && !exists $permitted{$dir}) {
            Class::ReluctantORM::Exception::SQL::ParseError->croak(
                                                                   error => "Could not parse '$stanza' in order by clause",
                                                                   sql => $sql,
                                                                  );
        }
        my $col = $driver->parse_column($col_name);
        $ob->add($col, $dir);
    }
    return $ob;
}


=begin devdocs

=head2 $col = driver->parse_column($str);

Parses a string token (a column name) into a Column.  This is used for parsing things like "schema"."table"."column" .

=cut

sub parse_column {
    my $driver = shift;
    my $str = shift;
    my ($oq, $cq, $ns) =
      ( $driver->open_quote, $driver->close_quote, $driver->name_separator);

    my @parts = split '\\' . $ns, $str;
    for (@parts) {
        s{^$oq}{};
        s{$cq$}{};
    }

    my ($colname, $table, $schema) = reverse @parts;
    my $col = Column->new(
                          table => ($table ? Table->new(table => $table, schema => $schema) : undef),
                          column => $colname,
                         );
    return $col;

}



#==================================================================#
#                         MONITOR SUPPORT
#==================================================================#

=head1 MONITOR SUPPORT

These methods allow the Driver to integrate with the Monitor system.

=cut

=head2 $driver->install_monitor($mon);

Adds a monitor to the driver.  $mon should be a Class::ReluctantORM::Monitor.

Note that monitors may be added on a system-wide basis by calling Class::ReluctantORM::install_global_monitor().

=cut

sub install_monitor {
    my $driver = shift;
    my $mon = shift;
    unless ($mon) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'monitor'); }
    unless (blessed($mon) && $mon->isa('Class::ReluctantORM::Monitor')) {
        Class::ReluctantORM::Exception::Param::WrongType->croak(param => 'monitor', expected => 'Class::ReluctantORM::Monitor', value => $mon);
    }

    push @{$driver->{monitors}}, $mon;
    return 1;
}

=head2 $driver->monitors();

Returns the list of monitors for this driver.  The list includes monitors specific to this driver, as well as global monitors.

=cut

sub monitors {
    my $driver = shift;
    return (
            @{$driver->{monitors}},
            Class::ReluctantORM->global_monitors(),
           );
}

=head2 $driver->driver_monitors();

Returns the list of monitors for this driver, excluding global monitors.

=cut

sub driver_monitors {
    my $driver = shift;
    return @{$driver->{monitors}},
}

=head2 $driver->remove_driver_monitors();

Removes all monitors on this driver.  Global monitors are not affected.

=cut

sub remove_driver_monitors {
    my $driver = shift;
    $driver->{monitors} = [];
}

=head1 MONITORING EVENT METHODS

Driver implementations should call these methods at the appropriate time as they work.

All methods take named parameters.  Each method lists its required arguments.  The arguments are  as follows:

=over

=item sql_obj

The Class::ReluctantORM::SQL object being rendered.

=item sql_str

The rendered SQL string, ready for a prepare().  This will be in the driver's dialect.

=item sth

The DBI statement handle.

=item binds

An arrayref of arguments to DBI execute().

=item row

A hashref of data returned by a single row, as returned by $sth->fetchrow_hashref

=back

=cut

=head2 $d->_monitor_render_begin(sql_obj => $so);

Notifies the monitoring system that the driver has begun work to render the given SQL object.

Arguments: sql_obj, original, untouched Class::ReluctantORM::SQL object.

=cut

sub _monitor_render_begin { __notify_monitors(@_, event => 'render_begin'); }

=head2 $d->_monitor_render_transform(sql_obj => $so);

Notifies the monitoring system that the driver has finished transforming the SQL object.

Arguments: sql_obj, the post-transformation Class::ReluctantORM::SQL object.

=cut

sub _monitor_render_transform { __notify_monitors(@_, event => 'render_transform'); }

=head2 $d->_monitor_render_finish(sql_obj => $so, sql_str => $ss);

Notifies the monitoring system that the driver has finished rendering the SQL object.

=cut

sub _monitor_render_finish { __notify_monitors(@_, event => 'render_finish'); }

=head2 $d->_monitor_execute_begin(sql_obj => $so, sql_str => $ss, sth =>$sth, binds => \@binds);

Notifies the monitoring system that the driver is about to perform a DBI execute.

=cut

sub _monitor_execute_begin { __notify_monitors(@_, event => 'execute_begin'); }

=head2 $d->_monitor_execute_finish(sql_obj => $so, sql_str => $ss, sth =>$sth, binds => \@binds);

Notifies the monitoring system that the driver has returned from performing a DBI execute.

=cut

sub _monitor_execute_finish { __notify_monitors(@_, event => 'execute_finish'); }

=head2 $d->_monitor_fetch_row(sql_obj => $so, sql_str => $ss, sth =>$sth, binds => \@binds, row => \%row);

Notifies the monitoring system that the driver has returned from performing a DBI fetchrow.

=cut

sub _monitor_fetch_row { __notify_monitors(@_, event => 'fetch_row'); }

=head2 $d->_monitor_finish(sql_obj => $so, sql_str => $ss, sth => $sth);

Notifies the monitoring system that the driver has finished the query.

=cut

sub _monitor_finish { __notify_monitors(@_, event => 'finish'); }

sub __notify_monitors {
    my $driver = shift;
    my %args = @_;
    $args{driver} = $driver;
    my $event = $args{event};
    delete $args{event};
    $event = 'notify_' . $event;

    foreach my $monitor ($driver->monitors) {
        $monitor->$event(%args);
    }

    return 1;
}





1;
