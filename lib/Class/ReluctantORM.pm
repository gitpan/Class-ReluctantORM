package Class::ReluctantORM;

use strict;
use warnings;

=head1 NAME

Class::ReluctantORM - An ORM emphasizing prefetching

=head1 SYNOPSIS

  package Pirate;
  use base 'Class::ReluctantORM';

  Pirate->build_class(
    primary_key => 'pirate_id',  # May be an arrayref for multi-col PKs
    table => 'pirates',
    schema => 'high_seas',
    db_class => 'Some::DB::Class',
    deletable => 0,
  );
  Pirate->has_one(Ship);

  # Elsewhere...
  package main;

  # Fetch on primary key
  my $p = Pirate->fetch(123);

  # Fetch on any field (dies on no results)
  my @peeps = Pirate->fetch_by_name('Dread Pirate Roberts');

  # Same, with no dying
  my @peeps = Pirate->search_by_name('Dread Pirate Roberts');

  # Make a pirate in memory
  $matey = Pirate->new(name => 'Wesley');

  $matey->insert(); # Save to DB
  $matey->name('Dread Pirate Roberts'); # Modify in memory
  if ($matey->is_dirty) {
     # Yes, we have unsaved changes
     $matey->update(); # Commit to DB
  }

  # Try to access a related object that hasn't been fetched
  my $ship;
  eval { $ship = $matey->ship(); };
  if ($@) {
     # Splat - Class::ReluctantORM throws exceptions if you access
     # an unfetched relation
  }

  # Fetch a pirate and his related ship
  # See Class::ReluctantORM::Manual::Relationships
  my $matey = Pirate->fetch_by_name_with_ship('Wesley');

  # Or more flexibly
  my $matey = Pirate->fetch_deep(
      name => 'Wesley',
      with => { ship => {} },
  );

  # Works
  $ship = $matey->ship();

  # Lots more....

=head1 DESCRIPTION

Class::ReluctantORM, or CRO, is an ORM that uses exceptions to detect some coding practices that may lead to scalability problems while providing enhanced transparency into database accesses.

=head2 What is an ORM?

An ORM is an Object-Relational Mapping system.  It treats tables in a database as classes, and rows in those tables as objects.  Foreign key relationships among tables become aggregation (has-a) relationships among objects.

Well-known ORMs include Perl's DBI::Class and Rose::DB, Ruby's ActiveRecord, and Java's 

=head2 Why use an ORM?

=over

=item Stay in the OOP mindset

Thinking OOPishly and thinking RDBMSishly are quite different.  By treating database rows as real objects, you stay in the OOP mindset.  Some programmers will see a productivity gain from this.

=item Reduce SQL usage to the hard cases

Simple things are extremely easy, and require no SQL.  Harder problems still require SQL, but you can isolate them more easily.

=item Schema changes are much easier

Many schema changes are detected automatically (column additions result in new methods, for example). You also have a Perl layer in which you can intercept changes at the class level, if needed.

=item Possible RDBMS independence

If you rely on the ORM to generate queries, it will speak a dialect specific to the database being used.  You may be able to change databases later without major code changes.

=item Reduce code duplication

Many classes need the functionality of CRUD (create, retreive, update, delete).  On WET (non-DRY) projects, many modules implement that functionality, in many places.

=item Reduce inconsistency

Likewise, there is no reason why 4 different modules
should name their search methods 4 different things.

=back


=head2 Why NOT use an ORM?

=over

=item Opaque SQL generation

The magic that goes into turning a method call into a database query can be difficult to unravel.

=item Hiding queries behind methods hides costs

It is easy to accidentally hammer a database by, for example, calling a single-row-fetching method in a loop.

=item Difficult to rely on the ORM to generate efficient SQL

Optimizing SQL usually means making vendor or dataset specific tweaks.  ORMs may make that difficult or impossible, and the stuff that they generate will usually be fairly generic.

=back

=head2 Why use Class::ReluctantORM?

=over

=item It encourages you to combine fetches

Because it is easy to detect exactly when a related, but unfetched, object is accessed (an exception is thrown), it is easy to determine exactly which fetches can be combined, and to keep those fetches trimmed down.  See L<Class::ReluctantORM::Manual::Prefetching>

=item Querying methods are named consistently

Developers will generally be able to tell if a method will hit the database.

=item A sophisticated, extensible query generation monitoring system

You can easily create monitors to watch database activity - whether you are interested in the SQL being generated, the values returned, the data volume, or the wall time.  And it is easy to write your own.  See L<Class::ReluctantORM::Manual::Monitors>

=item It has a abstract SQL model

CRO uses an abstract SQL model using real objects to represent pieces of a SQL statement.  This allows more flexibility than some other approaches.  See L<Class::ReluctantORM::SQL>.

=item Perl-side triggers

Run code before or after saves, retrieves, deletes, etc.  Add and remove multiple triggers on each event.  See L</"TRIGGER SUPPORT">

=item Mutator Filters

Apply arbitrary transformations to data upon being read or written to the object. See L<Class::ReluctantORM::Filter>.

=back

=head2 Why NOT use Class::ReluctantORM?

=over

=item It has a tiny developer base.

You might consider DBI::Class if you are looking for the go-to, widely-used ORM with excellent plugins and commericial support, or Rose::DB if you like the scrappy underdog approach.

=item It is immature.

There are some missing parts, though it is in production on our sites.  But it may not support your favorite RDBMS, and there are pieces that are unpretty.  It also doesn't have support that you might expect it to (like Moose integration, for example).

=item You might not like it.

The basic idea is that it will throw an exception if you do something stupid (well, that it can detect as stupid, anyway).  The idea is that you then, thoughtfully and at implementation time (not deployment time), do something less stupid.  You might not care for that approach - it's a little paternalistic.  Also, its advantages are fewer in a production environment (presumably you already have all of your fetches tuned at that point).

=back

=head1 DOCUMENTATION ROADMAP

=head2 The Manual

L<Class::ReluctantORM::Manual> Start here for a narrative introduction to CRO.

=head2 Alternate Base Classes

Most CRO model classes will inherit directly from Class::ReluctantORM.  These laternate base classes offer additional functionality for special circumstances.

=over

=item L<Class::ReluctantORM::Static> - Base class for "type tables"

=item L<Class::ReluctantORM::Audited> - Base class that audits database changes to a second, audit-log table

=item L<Class::ReluctantORM::SubClassByRow> - Base class for instance-singleton classes, allowing behavior inheritance

=back

=head2 Major Core Subsystems

=over

=item L<Class::ReluctantORM::Driver> - RDBMS support

=item L<Class::ReluctantORM::SQL> - SQL abstraction system

=item L<Class::ReluctantORM::Relationship> - Relationships between classes

=item L<Class::ReluctantORM::Monitor> - Database activity monitoring

=item L<Class::ReluctantORM::Filter> - Transform data on read/write to the object.

=item L<Class::ReluctantORM::Registry> - Cache fetched objects by their PKs

=back


=head1 DOCUMENTATION FOR THIS MODULE ITSELF

The remainder of this file is documentation for the Class::ReluctantORM module itself.

=over

=item L</"CRO-GLOBAL METHODS"> - methods that affect all CRO objects or classes

=item L</"MODEL CLASS CONFIGURATION"> - How to configure your class

=item L</"CLASS METADATA METHODS"> - Information about your class.

=item L</"CONSTRUCTORS"> - various ways of creating an object

=item L</"PRIMARY KEYS"> - methods related to primary keys

=item L</"CRUD"> - create, update, and delete.  Retrieve is covered under L</"CONSTRUCTORS">.

=item L</"DIRTINESS"> - detect changes to in-memory data

=item L</"FIELD ACCESSORS"> - reading and writing the attributes of your objects

=item L</"FILTER SUPPORT"> - methods related to Filters

=item L</"RELATIONSHIP SUPPORT"> - connect this class to other classes

=item L</"MONITORING SUPPORT"> - install and remove monitors from CRO objects.

=item L</"TRIGGER SUPPORT"> - install and remove triggers

=back

=cut


use Carp;
use Scalar::Util qw(refaddr);

use Data::Dumper;

use base 'Class::ReluctantORM::Base';
use base 'Class::ReluctantORM::OriginSupport';


use Class::ReluctantORM::Utilities qw(check_args install_method install_method_on_first_use install_method_generator conditional_load nz deprecated);
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::DBH;
use Class::ReluctantORM::Driver;
use Class::ReluctantORM::Relationship;
use Class::ReluctantORM::SQL::Aliases;
use Class::ReluctantORM::SQL;
use Class::ReluctantORM::FetchDeep;
use Class::ReluctantORM::FilterSupport;
use Class::ReluctantORM::Collection;
use Class::ReluctantORM::Registry;
use Class::ReluctantORM::Registry::None;

our $VERSION = "0.52_0";

our $DEBUG = 0;
our $SOFT_TODO_MESSAGES = 0;
our $DEBUG_SQL = 0; # Set to true to print all SQL to STDERR

our %PENDING_RELATIONS = (); # Delayed loading mechanism

our %METHODS_TO_BUILD_ON_FIRST_USE = ();
our %METHOD_GENERATORS = ();
our @GLOBAL_MONITORS = ();
our %CLASS_METADATA = ();
our %REGISTRY_BY_CLASS;
our %GLOBAL_OPTIONS;
BEGIN {
    $GLOBAL_OPTIONS{parse_where}      = 1;
    $GLOBAL_OPTIONS{parse_where_hard} = 1;
    $GLOBAL_OPTIONS{populate_inverse_relationships} = 1;
    $GLOBAL_OPTIONS{schema_cache_policy} = 'None';
    $GLOBAL_OPTIONS{schema_cache_file} = undef; # No sane default
}

=head1 CRO-GLOBAL METHODS

=head2 $setting = Class::ReluctantORM->get_global_option('option');

=head2 Class::ReluctantORM->set_global_option('option', 'value');

Reads or sets a global option.  Global options take effect immediately, and affect all CRO classes and objects.

Some options may be set on a per-class basis - see set_class_option.

The option name provided must be on the following list:

=over

=item parse_where

Boolean, default true.  If true, try to convert SQL strings passed as the value of the 'where' option to search(), fetch_deep(), delete_where() and update() into Class::ReluctantORM::SQL::Where objects.  (If the parsing attempt fails, see parse_where_hard for behavior.) If false, do not even attempt to parse; all strings are treated as raw_where (but SQl::Where objects you have constructed are handled normally).

You can also control this on a per-query basis using the parse_where option to fetch_deep() and others.

=item parse_where_hard

Boolean, default true.  If true, when a Where parsing attempt fails, throw an exception.  If false, instead use the SQL string as a raw_where clause, and continue.

=back

=item populate_inverse_relationships

Boolean, default true.  Relationships may have an inverse (for example, if a Ship has-many Pirates, the Pirate has-one Ship).  So when fetching a Ship and its Pirates, we can optionally set each Pirate to have its Ship already populated, as well.

=item schema_cache_policy

String enum.  Controls behvior of schema scanning (column listings) at startup.

=over

=item NONE (default) Perform no schema caching.  Columns will be listed on each table referenced in a build_class call; the scan will happen at process start (usually compile phase).

=item SIMPLE If a cache file exists, read it and use it for all column info.  If no cache file exists, perform the scan, then write the cache file.  If the database schema changes, you'll need to manually delete the cache file to regenerate it.

=item CLEAR_ON_ERROR Like SIMPLE, but will delete the cache file if a database error (of any kind, may not be related to schema changes) occurs.  Provides a bit of auto-recovery if your process is restartable.

=back

=item schema_cache_file

String absolute path to a writable file, where schema data will be cached.  Ignored if schema_cache_policy is NONE.  The file will be in JSON format.  No default provided.

=back

=cut

sub get_global_option {
    my $inv = shift;
    my $opt = shift;
    unless (exists $GLOBAL_OPTIONS{$opt}) {
        Class::ReluctantORM::Exception::Param::BadValue->croak
            (
             param => 'option_name',
             value => $opt,
             expected => 'one of ' . join(',', sort keys %GLOBAL_OPTIONS),
            );
    }
    return $GLOBAL_OPTIONS{$opt};
}

sub set_global_option {
    my $inv = shift;
    my $opt = shift;
    my $val = shift;
    unless (exists $GLOBAL_OPTIONS{$opt}) {
        Class::ReluctantORM::Exception::Param::BadValue->croak
            (
             param => 'option_name',
             value => $opt,
             expected => 'one of ' . join(',', sort keys %GLOBAL_OPTIONS),
            );
    }
    my $subname = '__' . $opt . '_setter';
    if ($inv->can($subname)) {
        $inv->$subname($val);
    } else {
        $GLOBAL_OPTIONS{$opt} = $val;
    }
}

sub __schema_cache_policy_setter {
    my $inv = shift;
    my $val = shift;
    my @policies = Class::ReluctantORM::SchemaCache->policy_names;
    unless ($val =~ (join('|', @policies))) {
        Class::ReluctantORM::Exception::Param::BadValue->croak
            (
             param => 'schema_cache_policy',
             value => $val,
             expected => 'one of ' . (join(', ', @policies)),
            );
    }
    $GLOBAL_OPTIONS{schema_cache_policy} = $val;
}

=head2 @class_names = Class::ReluctantORM->list_all_classes();

Lists all classes that are CRO derivates, and have had build_class called.

=cut

sub list_all_classes {
    return keys %CLASS_METADATA;
}

=head2 $driver_class = Class::ReluctantORM->default_driver_class();

Returns the class name of the Driver used by the most CRO subclasses.

=cut

sub default_driver_class {
    my $cro = shift;
    my %votes_by_driver = ();
    foreach my $class ($cro->list_all_classes) {
        $votes_by_driver{ref($class->driver())}++;
    }
    my @winners =
      map { $_->[0] }
        sort { $b->[1] <=> $a->[1] }
          map { [ $_, $votes_by_driver{$_} ] } keys %votes_by_driver;
    return $winners[0];
}

=head2 $bool = Class::ReluctantORM->is_class_available($cro_class);

Returns a boolean indicating whether the given CRO class has been loaded yet.

Note: If passed the special value 'SCALAR', always returns true.

=cut

sub is_class_available {
    my $class = shift;
    my $cro_class = shift;
    return exists($CLASS_METADATA{$cro_class}) || ($cro_class eq 'SCALAR');
}


=head1 MODEL CLASS CONFIGURATION

=head2 $class->build_class(%args);

Sets up the class.  Arguments:

=over

=item dbh

The database handle used to talk to the database.  This may be either a DBI handle or a Class::ReluctantORM::DBH subclass instance.  You must provide either this arg or the db_class arg.

=item db_class

A class that knows how to connect to the database when its new() method is called with no arguments.  The instance must be a Class::ReluctantORM::DBH subclass.

=item schema

Schema name in the database.

=item table

Table name in the database.

=item primary_key

Required.  Must either be auto-populated, or you must explicitly provide value(s) when you do an insert.
New in v0.4, this may either be a string (for single-column keys) or 
an arrayref of strings (for multi-column keys).

=item fields (optional, array ref or hashref)

If not provided, the $db_class->table_info 
will be be called to determine the field list.

You may also decouple field names from column names by passing a
hashref instead of an array ref.  The hashref should 
map class field names to table column names.

=item ro_fields (optional)

Unsettable fields. Default: all fields updatable.

=item volatile_fields (optional)

Optional arrayref of strings.  Read-write accessors will be created for these fields, allowing you to store volatile information.  This data will not be loaded or saved to the database, and the fields will not be listed by field_names() etc.

=item insertable (optional)

Default true. If present and false, insert() will throw an exception.

=item updatable (optional)

Default true. If present and false, update() will throw an exception.

=item deletable (optional)

Default true. If present and false, delete() will throw an exception.

=item refresh_on_update (optional)

Optional list of fields that should be refreshed after performing an UPDATE or INSERT
(perhaps because they were updated by a database trigger).

=item registry (optional)

Name of a Class::ReluctantORM::Registry subclass to use as the Registry for this class.  If not
provided, defaults to Class::ReluctantORM::Registry->default_registry_class() .  See Class::ReluctantORM::Registry for details.

=back

=cut

# Move this out so that subclasses can use it
sub __build_class_arg_spec {
    return (
            one_of => [
                       [qw(db_class dbh)],
                      ],
            mutex => [
                      [qw(lazy_fields non_lazy_fields)],
                     ],
            optional => [qw(fields ro_fields volatile_fields insertable deletable updatable refresh_fields registry)],
            required => [qw(primary_key schema table)],
           );
}

sub build_class {
    my $class = shift;
    my %args = check_args(
                          args => \@_,
                          $class->__build_class_arg_spec(),
                         );

    if ($DEBUG > 1) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - In CRO build_class:\nClass: $class\nArgs:" . Dumper(\%args); }

    if (defined $CLASS_METADATA{$class}) { Class::ReluctantORM::Exception::Call::NotPermitted->croak("It appears that $class has already been initialized.  You cannot call build_class twice."); }

    # Record class metadata
    my %metadata = ();
    $CLASS_METADATA{$class} = \%metadata;
    for my $flag (qw(updatable deletable insertable)) { $metadata{$flag} = defined($args{$flag}) ? $args{$flag} : 1; }

    $class->__build_class_init_driver(\%metadata, \%args);
    $class->__build_class_setup_fields(\%metadata, \%args);
    $class->__build_class_setup_refresh_list(\%metadata, \%args);

    # OK, call super to setup field list and accessors
    $class->SUPER::build_class(%args, fields => [ keys %{$metadata{fieldmap}} ]);

    # Setup fetchers and searchers
    $class->__build_class_setup_fetchers($class->field_names());
    $class->__build_class_setup_aggregators($class->field_names());

    $class->__build_class_setup_registry($args{registry});

    # Setup Relationships
    $metadata{relations} = {};

    # Setup lazy/non-lazy
    my @lazy_fields;
    if ($args{lazy_fields}) {
        @lazy_fields = @{$args{lazy_fields}};
    } elsif ($args{non_lazy_fields}) {
        my %non_lazy = map { $_ => 1 } (@{$args{non_lazy_fields}}, $class->primary_key_fields);
        @lazy_fields = grep { ! exists $non_lazy{$_} } $class->field_names();
    }
    if ($DEBUG > 1) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - In CRO build_class, have lazy fields:" . Dumper(\@lazy_fields); }
    foreach my $field (@lazy_fields) {
        $class->has_lazy($field);
    }

    # Setup all other relationships
    Class::ReluctantORM::Relationship->notify_class_available($class);
}

sub __build_class_init_driver {
    my ($class, $metadata, $args) = @_;

    for my $f (qw(table schema primary_key)) {
        $metadata->{$f} = $args->{$f};
    }

    # Repack primary key as an array if it's not already
    $metadata->{primary_key} = ref($metadata->{primary_key}) eq 'ARRAY' ? $metadata->{primary_key} : [ $metadata->{primary_key} ];

    # Make sure we have a dbh
    my ($dbh, $dbc);
    if ($args->{db_class}) {
        $dbc = $args->{db_class};
        conditional_load($dbc);
        Class::ReluctantORM::DBH->_quack_check($dbc);
        $dbh = $dbc->new();
    } else {
        $dbh = $args->{dbh};
    }

    $metadata->{driver} = Class::ReluctantORM::Driver->make_driver($class, $dbh, $dbc);
}


sub __build_class_setup_fields {
    my ($class, $metadata, $args) = @_;

    my $dbc = $args->{db_class};

    # Get field-column map
    my $fields = $args->{fields};
    if ($fields) {
        unless (ref($fields) eq 'ARRAY' || ref($fields) eq 'HASH') { Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak(param => 'fields'); }
        # Turn arrays into hashes
        if (ref($fields) eq 'ARRAY') { $fields = { map { $_ => $_ } @$fields }; }
        unless (%$fields) { Class::ReluctantORM::Exception::Param::Empty->croak(param => 'fields'); }
    } else {
        # Load fields from table info
        unless ($dbc->can('column_info')) {
            Class::ReluctantORM::Exception::Param->croak(message => "If you are going to omit fields, db_class must support column_info method.",  param => 'db_class');
        }
        $fields = $metadata->{driver}->read_fields($metadata->{schema}, $metadata->{table});

        # Confirm we got something
        unless (keys %{$fields}) {
            Class::ReluctantORM::Exception::Param->croak(message => 'Empty column list for schema ' . $metadata->{schema} . ', table ' . $metadata->{table} . ' - does table exist?',
                                            param => 'table',
                                            value => $metadata->{table},
                                                        )

        }
    }
    $metadata->{fieldmap} = $fields;

    # Make sure each primary key is in the field list
    foreach my $pk (@{$metadata->{primary_key}}) {
        unless (exists $fields->{$pk}) {
            Class::ReluctantORM::Exception::Param->croak(message => 'Primary key(s) not found in column list for class ' . $class,
                                            param => 'primary_key',
                                            value => $pk,
                                           );
        }
    }

    # Setup volatiles
    if ($args->{volatile_fields}) {
        foreach my $vf (@{$args->{volatile_fields}}) {
            $class->add_volatile_field($vf);
        }
    }
}

sub __build_class_setup_refresh_list {
   my ($class, $metadata, $args) = @_;

   my $refreshes = $args->{refresh_on_update} || [];
   if ($refreshes && ref($refreshes) ne 'ARRAY') {
       Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak(param => 'refresh_on_update');
   }

   # Make sure each primary keys are all on the list
   foreach my $pk (@{$metadata->{primary_key}}) {
       unless (grep {$_ eq $pk} @{$refreshes}) {
           push @{$refreshes}, $pk;
       }
   }

   my $fields = $metadata->{fieldmap};

   # Make sure they're all on the field list
   foreach my $rf (@{$refreshes}) {
       unless (exists $fields->{$rf}) {
           Class::ReluctantORM::Exception::Param->croak(message => "refresh on update fields must be present in field list",  param => 'refresh_on_update');
       }
   }
   $metadata->{refresh_on_update} = $refreshes;

}

sub __build_class_setup_fetchers {
    my $class = shift;
    my @fields = @_;
    foreach my $field (@fields) {
        foreach my $type ('search', 'fetch') {
            my $name = $type . '_by_' . $field;
            # install_method_on_first_use( ... ); # inlined
            $Class::ReluctantORM::METHODS_TO_BUILD_ON_FIRST_USE{$class}{$name}
              = sub { $class->_make_fetcher(
                                            $field, 
                                            ($type eq 'fetch'),
                                            undef,
                                           ) };
        }
    }
}

sub __build_class_setup_aggregators {
    my $class = shift;
    my @fields = @_;

    install_method_generator
      (
       $class,
       sub {
           my ($class, $proposed_method_name) = @_;
           my %aggregators_by_name = map { lc($_->name) => $_ } Function->list_aggregate_functions();
           my $regex = '^(' . join('|', keys %aggregators_by_name) . ')_of_(' . join('|', @fields) . ')$';
           my ($aggregator_name, $field_name) = $proposed_method_name =~ $regex;
           if ($aggregator_name) {
               return $class->_make_aggregator(
                                               $field_name,
                                               $aggregators_by_name{$aggregator_name},
                                              );
           }

           # No patterns left - decline
           return undef;
       });
}

sub __build_class_setup_registry {
    my $class = shift;
    my $registry_class = shift || Class::ReluctantORM::Registry->default_registry_class();
    unless ($registry_class->isa('Class::ReluctantORM::Registry')) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'registry', error => 'Registry class must inherit from Class::ReluctantORM::Registry', value => $registry_class);
    }
    my $registry = $registry_class->new($class);
    $REGISTRY_BY_CLASS{$class} = $registry;
}

=head2 MyClass->add_volatile_field('field_name')

Creates a volatile accessor/mutator method (getter/setter) with the given name.  The field is volatile in the sense that its value is never saved to the database.  Setting a volatile field does not affect dirtiness.

=cut

sub add_volatile_field {
    my $class = shift;
    my $vf = shift;
    my $sub = sub {
        my $self = shift;
        if (@_) { $self->set($vf, shift); }
        return $self->get($vf);
    };
    install_method($class, $vf, $sub);
}

=head1 CLASS METADATA METHODS

=head2 $reg = CroClass->registry();

Returns the Registry associated with this CRO class, which provides an object caching mechanism.  See Class::ReluctantORM::Registry.

=cut

sub registry {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    return $REGISTRY_BY_CLASS{$class};
}


sub __metadata {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;;

    my $hash = $CLASS_METADATA{$class};
    unless (defined $hash) {
        Class::ReluctantORM::Exception::Call::ExpectationFailure->croak
            (
             error => "$class appears to be unitialized.  Must call build_class before calling __metadata().",
            );
    }
    return $hash;
}

sub __alias_metadata {
    my $cro = shift;
    my $target_class = shift;
    my $alias = shift;
    $CLASS_METADATA{$alias} = $CLASS_METADATA{$target_class};
    $REGISTRY_BY_CLASS{$alias} = $REGISTRY_BY_CLASS{$target_class};
}

=begin devdocs

=head2 $CroClass->_change_registry($reg_obj);

=head2 $CroClass->_change_registry($reg_class);

Changes the Registry object used to cache objects for this class.  You can pass a constructed Registry subclass, or the class name (in which case we will call new() on it).

The existing registry is purged before switching.

=end devdocs

=cut

sub _change_registry {
    my $cro_inv = shift;
    if (ref($cro_inv)) {
        Class::ReluctantORM::Exception::Call::NotPermitted::ClassMethodOnly->croak(method => '_change_registry');
    }
    my $cro_class = $cro_inv;

    my $reg_arg = shift;
    unless ($reg_arg->isa('Class::ReluctantORM::Registry')) {
        Class::ReluctantORM::Exception::Param::WrongType->croak(param => 'registry', value => $reg_arg, error => 'registry must inherit from Class::ReluctantORM::Registry.');
    }

    # OK, purge existing reg
    $cro_class->registry->purge_all();

    my $reg;
    unless (ref($reg_arg)) {
        $reg = $reg_arg->new($cro_class);
    }

    $REGISTRY_BY_CLASS{$cro_class} = $reg;
}

=head2 $driver = $class->driver();

Returns the Class::ReluctantORM::Driver object that provides backend-specific functionality.

=cut

sub driver {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    return $class->__metadata()->{driver};
}

=head2 $tablename = $class->table_name();

Returns the name of the table for this class, in the case expected by the database.

=cut

sub table_name {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    my $name =  $class->__metadata()->{table};
    return $class->driver->table_case($name);
}

=head2 $schemaname = $class->schema_name();

Returns the name of the schema for this class.

=cut

sub schema_name {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    my $name =  $class->__metadata()->{schema};
    unless ($name) { return ''; }
    return $class->driver->schema_case($name);
}

=head2 $str = $class->full_table_name();

Returns a quoted, dotted version of the name, using the quote character and name spearator that the database expects.

Postgres example: "foo_schema"."bar_table"

=cut

sub full_table_name {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    my $d = $class->driver();
    return ($class->schema_name ? 
            $d->open_quote() . $class->schema_name . $d->close_quote . $d->name_separator : '')
      . $d->open_quote() . $class->table_name . $d->close_quote();
}

=head2 $colname = $class->column_name($field_name, $field_name2, ..);

Returns the database column underlying the given field.

If more than one field is given, returns a list or arrayref,
depending on context.

=cut

sub column_name {
    my $inv = shift;
    my $class = ref($inv) || $inv;

    my $driver = $class->driver;
    my @cols;
    foreach my $fieldname (@_) {
        push @cols, $driver->column_case($class->__metadata()->{fieldmap}{$fieldname});
    }
    return wantarray ? @cols : ((@_ > 1) ? \@cols : $cols[0]);
}

=head2 $fieldname = $class->field_name($column_name, $column_name2,...);

Returns the object field that represents the given database column.

If more than one column is given, returns a list or arrayref,
depending on context.

=cut

sub field_name {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    my @colnames = @_;
    my %invmap = reverse %{$class->__metadata()->{fieldmap}};
    my @fields = @invmap{@colnames};
    return wantarray ? @fields : ((@_ > 1) ? \@fields : $fields[0]);
}

=head2 $fieldname = $class->first_primary_key_field();

Returns the name of the first primary key field for this class.

This is probably a bad idea - you may want to use primary_key_fields instead.

=cut

sub first_primary_key_field {
    my @pks = shift->primary_key_fields();
    return $pks[0];
}

=head2 @pks = $class->primary_key_fields();

Returns the names of the primary key fields for this class.  Returns an 
array ref in scalar context.

=cut

sub primary_key_fields {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    my $pks = $class->__metadata()->{primary_key};
    return wantarray ? @$pks : $pks;
}

=head2 $bool = $o->is_field_primary_key('fieldname');

Returns true if the named field is a primary key.

=cut

sub is_field_primary_key {
    my $self = shift;
    my $fieldname = shift;
    return grep { $_ eq $fieldname } $self->primary_key_fields();
}

=head2 $fieldname = $class->first_primary_key_column();

Returns the name of the first primary key column for this class, in database column case.

This is probably a bad idea - you may want to use primary_key_columns instead.

=cut


sub first_primary_key_column {
    my @pks = shift->primary_key_columns();
    return $pks[0];
}

=head2 @pks = $class->primary_key_columns();

Returns the name of the primary key columns for this class, in database column case. Returns an
array ref in scalar context.

=cut

sub primary_key_columns {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    my @pks = $class->column_name(@{$class->__metadata()->{primary_key}});
    return wantarray ? @pks : \@pks;
}

=head2 $int = $class->primary_key_column_count();

Returns the number of primary key columns for the class.

=cut

sub primary_key_column_count {
    my $self = shift;
    my @cols = $self->primary_key_columns();
    return scalar(@cols);
}

=head2 @cols = $class->column_names();

Returns the list of database columns, in the same order as field_names.

=cut

sub column_names {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    return @{$class->__metadata()->{columns} ||= [ map { $class->column_name($_) } $class->field_names ]};
}

=head2 @columns = $class->audit_columns();

Returns a list of any columns that are expected to be automatically populated as auditing data.  If the class is not being audited, this list is empty.  See L<Class::ReluctantORM::Audited>.

=cut

sub audit_columns { return (); }

# Takes a list of field or column names and returns a list
# of things that are definitely columns
sub __to_column_name {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    my @candidates = @_;
    my @results;
    my %columns = map { $_ => 1 } $class->column_names();
    my %columns_by_fields  = %{$class->__metadata()->{fieldmap}};

    foreach my $c (@candidates) {
        if (exists $columns{$c}) {
            push @results, $c;
        } elsif (exists $columns_by_fields{$c}) {
            push @results, $columns_by_fields{$c};
        } else {
            Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'column or field name', value => $c, frames => 2);
        }
    }

    return @results;
}


=head2 @fields = $class->fields();

=head2 @fields = $class->field_names();

Returns a list of the fields in the class.

=cut

# field_names() inherited from Class

sub fields { return shift->field_names(); }

=head2 @fields = $class->field_names_including_relations()

Returns a merged list of both the direct fields as well fields defined via Relationships.

=cut

sub field_names_including_relations {
    my $inv = shift;
    return ($inv->field_names(), $inv->relationship_names());
}

=head2 @fields = $class->refresh_fields();

=head2 @cols = $class->refresh_columns();

Returns a list of the fields or columns that should be refreshed
on each update or insert.

=cut

sub refresh_fields {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    return @{$class->__metadata()->{refresh_on_update}};
}
sub refresh_columns {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    return $class->column_name($class->refresh_fields());
}


=head2 @fields = $class->essential_fields();

Returns a list of the fields that are always fetched when an object of
this type is fetched from the database.  Normally this is the same as
fields(), but some Relationships (HasLazy, for example) will modify this.

=cut

sub essential_fields {
    my $inv = shift;
    my $class = ref($inv) || $inv;

    # If a field appears on the relations list, remove it from the
    # essentials list.
    my %rels_by_name = %{$class->relationships()};
    my @essentials = grep { not(exists($rels_by_name{$_})) } $class->fields();
    return @essentials;
}

=head2 @fields = $class->essential_sql_columns($table);

Returns a list of SQL::Column objects  that are always fetched when an object of
this type is fetched from the database.  Normally this is the same as
sql_columns(), but some Relationships (HasLazy, for example) will modify this.

Optionally, pass in a SQL::Table reference to specify the Table instance to link each column to.

=cut

sub essential_sql_columns {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    my $table = shift || Class::ReluctantORM::SQL::Table->new($class);

    my @col_names = $class->column_name($class->essential_fields);
    #print STDERR "Hvae essential columns for table " . $table->table . ":\n" . Dumper(\@col_names);
    my @cols = map {
        Class::ReluctantORM::SQL::Column->new(
                                                     table => $table,
                                                     column => $_
                                                    );
    } @col_names;
    return @cols;

}


=head2 $bool = $class->is_static();

Returns true if the class is "static" - usually implemented via Class::ReluctantORM::Static.  Such classes fetch all rows on the first fetch, and tehn cache thier results for the life of the process.

=cut

sub is_static { return 0; }

=head2 $bool = $class->updatable();

Returns true if this class permits update() to be called.

=cut

sub updatable {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    return $class->__metadata()->{updatable};
}

=head2 $bool = $class->deletable();

Returns true if this class permits delete() to be called.

=cut

sub deletable {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    return $class->__metadata()->{deletable};
}

=head2 $bool = $class->insertable();

Returns true if this class permits insert() to be called.

=cut

sub insertable {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    return $class->__metadata()->{insertable};
}



#==============================================================#
#                           Constructors
#==============================================================#

=head1 CONSTRUCTORS

There are three classes of constructors:

=over

=item memory only

These constructors, new() and clone(), only create an
object in memory.  Use insert() to commit them to the database.

=item database fetch

These constructors, fetch() and search(), take an existing
database row and turn it into an object in memory.

=item memory and database

The create() constructor creates a new row in the database and 
returns the new object.

=back

Fetch and Search differ in their handling of empty result
sets: fetch methods throw an exception if nothing is found,
while search methods simply return undef or an empty list.


=cut

=head2 $o = $class->new(field1 => $value1, ...);

Creates a new object in memory only (no database contact).

=cut

sub new {
    my $class = shift;

    # Allow passing hash or hashref
    my $hash_ref = {};
    if (@_ == 1) {
        $hash_ref = shift;
        unless (ref($hash_ref) eq 'HASH') { Class::ReluctantORM::Exception::Param::ExpectedHashRef->croak(); }
    } elsif (@_ % 2) {
        Class::ReluctantORM::Exception::Param::ExpectedHash->croak();
    } else {
        $hash_ref = { @_ };
    }

    if ($DEBUG > 1) { print STDERR __PACKAGE__ . ":" . __LINE__ . " - have new params:\n" . Dumper($hash_ref);  }

    my @allowable_args = ($class->field_names(), $class->relationship_names());
    foreach my $arg (keys %$hash_ref) {
        unless (grep {$arg eq $_} @allowable_args) {
            Class::ReluctantORM::Exception::Param::Spurious->croak(param => $arg);
        }
    }

    my $self = $class->SUPER::new($hash_ref);
    $self->{_dirty_fields} = {};
    $self->{_is_inserted} = 0;

    # Check registry for a hit
    my $existing;
    if ($self->has_all_primary_keys_defined()) {
        my $existing = $class->registry->fetch($self->id());
        if ($existing) {
            $self = $existing; # will cause a registry purge
        }
    }

    # Force store of this object in registry (either it is new or it was just purged)
    $class->registry->store($self);

    unless ($existing) {
        # Set fields dirty - have to do this manually here since SUPER::new calls set(), 
        # not the actual mutator.
        foreach my $f ($class->field_names) {
            if (exists $hash_ref->{$f}) {
                if ($DEBUG > 1) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - in new, marking dirty field: $f\n"; }
                $self->_mark_field_dirty($f);
            }
        }
        if ($DEBUG > 1) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - after new, have dirty fields :" . Dumper([$self->dirty_fields]); }
    }

    # Look for relations and perform implicit setup
    foreach my $rel ($self->relationships) {
        my $rel_field = $rel->method_name();
        next unless exists $hash_ref->{$rel_field};
        if ($existing && $existing->is_fetched($rel_field)) {
            $rel->merge_children($self, $hash_ref->{$rel_field});
        } else {
            $rel->_handle_implicit_new($self, $hash_ref);
        }
    }

    $self->capture_origin();

    return $self;
}

=head2 $o = $class->create(field1 => $value1, ...);

Creates a new object in memory, and creates a matching row in the database.

=cut

sub create {
    my $class = shift;
    my $self = $class->new(@_);
    $self->insert();
    foreach my $rel ($self->relationships) {
        $rel->_handle_implicit_create($self, { @_ });
    }
    return $self;
}


=head2 $o = $class->fetch(123);

=head2 $o = $class->fetch(key1_name => $key1_val, key2_name => key2_val...);

Retrieves the object from the database whose primary
key matches the given argument(s).

In the first form, valid only for classes with a single-column 
primary key, the one primary value must be provided.

In the second form, you may specify values for multi-column
primary keys.  Any PK columns not specified will be interpreted
as null.  You may specify either field names or column names; they
will be interpreted first as column names, and if that fails,
will be treated as field names.

If no such object exists, an Class::ReluctantORM::Exception::Data::NotFound is thrown.
For a gentler approach, use the search() family.

=cut

sub fetch {
    my $class = shift;
    my %pk;

    # Check args
    if (!@_) {
        Class::ReluctantORM::Exception::Param::Missing->croak(param => 'primary key value');
    } elsif (@_ == 1) {
        unless ($class->primary_key_column_count == 1) { Class::ReluctantORM::Exception::Data::NeedMoreKeys->croak(); }
        $pk{$class->first_primary_key_column} = shift;
    } elsif (@_ % 2) {
        Class::ReluctantORM::Exception::Param::ExpectedHash->croak();
    } else {
        my %args = @_;
        my @cols = keys %args;
        @pk{$class->__to_column_name(@cols)} = @args{@cols};
    }

    # Build Where clause
    my $where = Where->new();
    my $table = Table->new($class);
    foreach my $colname (keys %pk) {
        my $col = Column->new(
                              table => $table,
                              column => $colname,
                             );
        my $prm = Param->new();
        $prm->bind_value($pk{$colname});

        $where->and(Criterion->new('=', $col, $prm));
    }

    return $class->fetch_deep(where => $where, with => {});

}

=head2 @objects = $class->fetch_all([order => 'order_clause']);

Fetches all rows from the table, optionally ordered by the given order clause.

For pagination support, see search().

=cut

sub fetch_all {
    my $class = shift;
    return $class->search(where => Where->new(), @_);
}

=head2 @objects = $class->fetch_deep( FIELD_NAME => $value, %common_options);

=head2 @objects = $class->fetch_deep( where => $where_obj, %common_options);

=head2 @objects = $class->fetch_deep( where => $sql_string, execargs => \@binds, parse_where => 0, %common_options);

Performs a query with broad and/or deep prefetching.  The three forms offer different ways of specifying search criteria.

In the first form, provide exactly one field name with value.  The search operator will be an '='.

In the second form, provide a Class::ReluctantORM::SQL::Where object.  It may contain Params, which must have their bind values already set.

In the third form, provide a SQL string in a dialect that your Driver will understand.  You may use '?' to represent a bind placeholder, and provide the bind values in the execargs argument.  Depending on the values of the global options 'parse_where' and 'parse_where_hard', CRO may attempt to use the Driver to parse the SQL string into a Where object (which has certain advantages internally, especially for object inflation).  If this fails, a ParseError exception will be thrown. You may disable this behavior with parse_where.  Even if parse_where is false, the SQL string will still be mangled - we need to perform table-realiasing.  Table alias macros are supported.



=head2 @objects = $class->fetch_deep( where => $clause, execargs => [], with => { subfield => {}}, hint => '', limit => 5, offset => 6, order_by => '', parse_where => 0 );

Common options:

=over

=item limit

Optional integer.  order_by is required if you use this (otherwise your results are nondeterministic). Limits the number of top-level objects.  Due to JOINs, more rows may be actually returned.  Better drivers can do this in SQL, but some drivers may be obliged to implement this in Perl.  Some drivers may place restrictions on the WHERE clause if you use limit (like only permitting a where to reference the main table).

=item offset

Option integer, onlly permitted if limit is provided.  Skip this many records.

=item order_by

Optional sort instructions.  Provide either a Class::ReluctantORM::SQL::OrderBy, or a SQL string.  You may only reference columns from the primary table.  Some drivers may be obliged to implement this in Perl.

=item hint

Optional driver hints.  See your driver documentation.

=item with

Prefetching instructions.  See below and Class::ReluctantORM::Manual::Prefetching .

=back

To specify the prefetch tree, provide the 'with' parameter as a hashref.  Name each subfield by 
method_name, using an empty hashref to denote a leaf.  For example, if you are calling 
Pirate->fetch_deep, and you want the pirate's ship and parrot to be
prefetched, use with => {parrot => {}, ship => {}}.  To get the ship's home port as well, use
with => {parrot => {}, ship => { home_port => {}}} .


It is an error to pass unrecognized parameters to this method.

In list context, all results are returned as a list.  In scalar context, only the first top-level 
result is returned.  If the query results in empty results, an exception is thrown.  See search_deep 
for an exceptionless alternative.

=cut

# Implemented in Class::ReluctantORM::FetchDeep

=head2 $object = $class->search($id);

=head2 @objects = $class->search(where => $clause, execargs => [execargs], order_by => $clause, limit => 5, offset => 3);

In the first form, acts as a non-fatal fetch().  You may only use this form if your class has a single-column primary key.

In the second form, full-fledged search facility.
ou 
In either form, returns all results as a list in array context, or first result in scalar context.  In the case of no results, returns an empty list in list context or undef in scalar context.

The where clause is the only required option.  Use column names, not field names (though they are usually the same).  Do not include the word 'WHERE'.  You may use placeholders ('?'), so long as you include the execargs argument as well, which should be an arrayref of your arguments.

Supports pagination.

=cut

sub search {
    my $class = shift;
    if (@_ == 1) {
        unless (@{[$class->primary_key_columns]} == 1) {
            Class::ReluctantORM::Exception::Call::NotPermitted->croak('You may only use the single-argument form of search() with classes that have single-column primary keys.');
        }
        my $pkc = ($class->primary_key_columns)[0];
        my $prm = Param->new();
        $prm->bind_value($_[0]);
        @_ = (
              where => Where->new(Criterion->new('=', Column->new(column => $pkc), $prm)),
             );
    }
    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;

    if (exists $args{with}) {
        Class::ReluctantORM::Exception::Param::Spurious->croak(value => $args{with}, param => 'with', error => 'search() does not take a "with" parameter.  Did you mean search_deep()?');
    }

    $args{with} = {};
    return $class->search_deep(%args);
}

=head2 $o = $class->search_by_FIELD($value);

=head2 @objects = $class->search_by_FIELD($value);

Similar to fetch_by_FIELD, but returns undef or an empty list 
when no results are available, rather than throwing an exception.

=cut

# Created during call to build_class


=head2 @objects = $class->search_deep( FIELD_NAME => $value, with => { subfield => {}}, hint => '', limit => 5, offset => 6, order_by => '' );

=head2 @objects = $class->search_deep( where => $clause, execargs => [], with => { subfield => {}}, hint => '', limit => 5, offset => 6, order_by => '' );

Operates identically to fetch_deep, but does not
throw an exception if no results are found.

=cut

# Implemented in Class::ReluctantORM::FetchDeep

=head2 $pirate->fetch_deep_overlay(with => \%with);

=head2 Pirate->fetch_deep_overlay(with => \%with, objects => \@pirates);

Given an existing, already-fetched object, performs an afterthought fetch - returning to the database to fetch additional related objects.

Other methods allow you to do this on a per-relation-basis (ie, $pirate->fetch_ship()) or to fetch deeply, starting with one relation ($ship->pirates->fetch_deep(with => {booties => {}})) .  This method, however, acts on the parent object, allowing you to fetch accross multiple relations in one query.

In the first form, one query is performed to "re-fetch" a copy of the object, then the original is merged with the copy.

In the second form, multiple objects may be re-fetched with one query.

While merging, the fresh copy from the database wins all conflicts.  Additionally, if you re-fetch over a relation you have modified, the changes are lost.  Finally, there is nothing stopping you from fetching a "shallower" tree than you originally fetched.

=cut

#==============================================================#
#                       Primary Keys
#==============================================================#


=head1 PRIMARY KEYS

=cut

=head2 $key = $o->id();

=head2 $key_href = $o->id();

=head2 @keys = $o->id();

Returns the primary key value(s) for this object.  If $o->is_inserted()
is false, this will return undef.

In the first form, (scalar context), if the class has only one 
primary key column, the primary key value is returned.  If the object has not been inserted, undef is returned.

In the second form (scalar context), if the class a multi-column primary key, a hashref is returned with the primary keys listed by their field names.  If the object has not been inserted, undef is returned.

In the third form, (list context), the primary key values are returned 
as a list, guarenteed to be in proper PK definition order.  If the 
object has not been inserted, an empty list is returned (NOT a 
list of undefs, which could be confused with an all-NULL primary key)

Use $class->primary_key_fields to get the names of the primary key fields.

=cut

=head2 $key = $o->primary_key();

=head2 $key_href = $o->primary_key();

=head2 @keys = $o->primary_key();

=head2 $key = $o->primary_keys();

=head2 $key_href = $o->primary_keys();

=head2 @keys = $o->primary_keys();

primary_key() and primary_keys() are aliases for id().

=cut

sub primary_key  { return shift->id(); }
sub primary_keys { return shift->id(); }

sub id {
    my $self = shift;
    my @pk_fields = $self->primary_key_fields();
    if (@pk_fields == 1) {
        my $method = $pk_fields[0];
        return wantarray ? ($self->$method()) : $self->$method;
    } else {
        if (wantarray) {
            unless ($self->is_inserted()) { return (); }
            return map { $self->$_ } @pk_fields;
        } else {
            unless ($self->is_inserted()) { return undef; }
            return { map { $_ => $self->$_ } @pk_fields };
        }
    }
}

=head2 $bool = $obj->has_all_primary_keys_defined();

Returns true if all primary key columns have a defined value.

If this is true, we can reliably identify this object in a unique way.

=cut

sub has_all_primary_keys_defined {
    my $self = shift;
    foreach my $pkf ($self->primary_key_fields()) {
        unless (defined($self->raw_field_value($pkf))) {
            return 0;
        }
    }
    return 1;
}

#==============================================================#
#                         CRUD
#==============================================================#

=head1 CRUD

=head2 $o->insert();

Commits a newly created object into the database.

If the class was built with 'refresh_on_update' fields, these fields are fetched, 
using a single query for the insert and the fetch.  The primary key is always fetched.

If the object already has been inserted, dies.

=cut

sub insert {
    my $self = shift;

    # Must allow insert
    unless ($self->insertable) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak(message => 'This class is configured to not permit inserts.  See Class::ReluctantORM->build_class().');
    }

    # Prevent obvious double inserts
    if ($self->is_inserted()) {
        Class::ReluctantORM::Exception::Data::AlreadyInserted->croak(primary_key => Dumper((scalar $self->primary_key)));
    }

    $self->__run_triggers('before_insert');

    $self->_check_for_cascade_on_upsert();

    # Build SQL
    my $sql = Class::ReluctantORM::SQL->new('insert');
    my $table = Class::ReluctantORM::SQL::Table->new($self);
    $sql->table($table);

    # Build input columns
    foreach my $f ($self->dirty_fields()) {
        my $col = Class::ReluctantORM::SQL::Column->new(
                                                               column => $self->column_name($f),
                                                               table => $table,
                                                              );
        my $param = Class::ReluctantORM::SQL::Param->new();
        if ($DEBUG > 2) {
            my ($colname, $val) = ($col->column, $self->raw_field_value($f));
            $val = defined($val) ? $val : 'NULL';
            print STDERR __PACKAGE__ . ':' . __LINE__ . "- in insert, binding $colname to $val\n";
        }
        $param->bind_value($self->raw_field_value($f));
        $sql->add_input($col, $param);
    }

    # Build output columns
    $self->__add_refresh_output_columns_to_sql($sql, $table);

    # Run SQL
    # Use run_sql, not prepare/execute - this allows the driver
    # to split the query (SQLite needs this, for example)
    $self->driver->run_sql($sql);
    $self->_refresh_from_sql($sql);

    # Clear dirty flags
    $self->_mark_all_clean();
    $self->{_is_inserted} = 1;

    # Alert relations of new primary key
    foreach my $rel ($self->relationships) {
        $rel->_notify_key_change_on_linking_object($self);
    }

    # (re) store in registry - registries should refuse to
    # store an object with any nulls in the primary keys, so
    # this should be a new entry
    $self->registry->store($self);

    $self->__run_triggers('after_insert');

    return 1;
}

sub _refresh_from_sql {
    my $self = shift;
    my $sql = shift;

    $self->__run_triggers('before_refresh');

    foreach my $oc ($sql->output_columns) {
        if ($oc->expression->is_column()) {
            my $field = $self->field_name($oc->expression->column);
            $self->raw_field_value($field, $oc->output_value);
        }
    }

    $self->__run_triggers('after_refresh');
}

sub __add_refresh_output_columns_to_sql {
    my $self = shift;
    my $sql = shift;
    my $table = shift;

    my %is_pk = map { $_ => 1 } $self->primary_key_columns();

    foreach my $c ($self->refresh_columns) {
        my $col = Class::ReluctantORM::SQL::Column->new(
                                                        column => $c,
                                                        table => $table,
                                                       );
        my $oc = OutputColumn->new(expression => $col, is_primary_key => $is_pk{$c});
        $sql->add_output($oc);
    }
}


=head2 $o->update();

Commits any changes to an object to the database, and clears the dirty flag.

If the class was built with 'refresh_on_update' fields, these fields are fetched, 
using a single query for the update and the fetch.

If the class was built with the updatable flag false, this always dies.

If the object is not dirty, does nothing.

If the object already not been inserted, dies.

=cut

sub update {
    my $self = shift;

    # Must allow update
    unless ($self->updatable) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak(message => 'This class is configured to not permit updates.  See Class::ReluctantORM->build_class().');
    }

    # Must be already inserted
    unless ($self->is_inserted()) {
        Class::ReluctantORM::Exception::Data::UpdateWithoutInsert->croak();
    }

    $self->_check_for_cascade_on_upsert();

    # Must be dirty
    unless ($self->is_dirty) { return; }

    $self->__run_triggers('before_update');

    # Build SQL
    my $sql = Class::ReluctantORM::SQL->new('update');
    my $table = Class::ReluctantORM::SQL::Table->new($self);
    $sql->table($table);

    # Build input columns
    foreach my $f ($self->dirty_fields()) {
        my $p = Param->new();
        $p->bind_value($self->raw_field_value($f));
        my $col = Column->new(
                              column => $self->column_name($f),
                              table => $table,
                             );
        $sql->add_input($col, $p);
    }

    # Build Where Clause
    $sql->where($self->__make_pk_where_clause($table));

    # Build output columns
    $self->__add_refresh_output_columns_to_sql($sql, $table);

    # Run SQL
    $self->driver->run_sql($sql);
    $self->_refresh_from_sql($sql);

    # Clear firty flags
    $self->_mark_all_clean();

    $self->__run_triggers('after_update');
    return 1;
}


# Ensure that if the object has any fetched relation with local keys,
# that the related items are already saved
sub _check_for_cascade_on_upsert {
    my $self = shift;
  RELATION:
    foreach my $rel ($self->relationships()) {
        next RELATION unless ($rel->local_key_fields()); # Skip it if it has no local key fields (eg, has_many)
        my $field = $rel->method_name();
        next RELATION unless ($self->is_relation_fetched($field)); # Skip it unless we've tried to put something there
        my $related = $self->$field();
        next RELATION if (ref($related) && $related->isa('Class::ReluctantORM::Collection')); # Ignore collections
        next RELATION unless (ref($related) && $related->isa('Class::ReluctantORM')); # SKip it unless it is something that can be inserted
        unless ($related->is_inserted()) {
            Class::ReluctantORM::Exception::Data::UnsupportedCascade->croak
                ("Cannot update or insert, because related object in '$field' has not been saved first");
        }
    }
}

sub __make_pk_where_clause {
    my $self = shift;
    my $table = shift;

    # Build WHERE
    my $where = Class::ReluctantORM::SQL::Where->new();
    foreach my $f ($self->primary_key_fields()) {
        my $p = Param->new();
        $p->bind_value($self->$f);
        $where->and(Criterion->new(
                                   '=',
                                   Column->new(
                                               column => $self->column_name($f),
                                               table => $table,
                                              ),
                                   $p,
                                  )
                   );
    }
    return $where;
}


=head2 $o->save();

Convenience method.  Calls either insert() or update(), 
depending on is_inserted.  Does nothing if the object was not dirty.

=cut

sub save {
    my $self = shift;
    unless ($self->is_dirty()) { return; }

    $self->__run_triggers('before_save');

    if ($self->is_inserted()) {
        $self->update();
    } else {
        $self->insert();
    }

    $self->__run_triggers('after_save');
}


=head2 $o->delete();

Deletes the corresponding row from the database.

If the class was built with the deletable flag false, this always dies.

If the object has not been inserted, dies.

=cut

sub delete {
    my $self = shift;

    # Must allow delete
    unless ($self->deletable) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak(message => 'This class is configured to not permit deletes.  See Class::ReluctantORM->build_class().');
    }

    # Must be already inserted
    unless ($self->is_inserted()) { Class::ReluctantORM::Exception::Data::DeleteWithoutInsert->croak(); }

    $self->__run_triggers('before_delete');

    my $class = ref($self);
    $class->delete_where(where => $self->__make_pk_where_clause(Table->new($class)));

    # Clear the primary key
    foreach my $pk ($self->primary_key_fields) {
        $self->set($pk, undef);
    }

    # Clear the dirty field trackers
    $self->_mark_all_clean();

    # Not in the Db anymore
    $self->{_is_inserted} = 0;

    $self->__run_triggers('after_delete');

    return 1;
}

=head2 $class->delete_where(where => '...', execargs => [ ... ]);

Delete arbitrary rows from the database.  Does not affect objects already fetched.

If the class was built with the deletable flag false, this always dies.

'where' may be a SQL string, or a Class::ReluctantORM::SQL::Where object.  
If where is a sql string and contains '?' characters, you must also provide the execargs option with bindings.

=cut

sub delete_where {
    my $class = shift;
    $class = ref($class) || $class;
    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;
    unless (exists $args{where}) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'where'); }

    # Must allow delete
    unless ($class->deletable) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak(message => 'This class is configured to not permit deletes.  See Class::ReluctantORM->build_class().');
    }

    # Build SQL
    my $sql = Class::ReluctantORM::SQL->new('delete');
    my $table = Class::ReluctantORM::SQL::Table->new($class);
    $sql->table($table);

    my $where;
    if (UNIVERSAL::isa($args{where}, Where)) {
        $where = $args{where};
    } else {
        $where = $class->driver->parse_where($args{where});
        $where->bind_params(@{$args{execargs} || []});
    }
    $sql->where($where);

    # Run SQL
    $class->driver->run_sql($sql);

    return 1;

}

DESTROY {
    my $self = shift;
    #print "# CRO DESTROY called\n";
    if ($self && $self->registry) {
        $self->registry->purge($self);
    }
}

#==============================================================#
#                       Dirty Facility
#==============================================================#

=head1 DIRTINESS

"Dirtiness" refers to whether the data in-memory has been modified since being read from the database.  If so, we know we need to save that data, and call it "dirty".

=head2 $bool = $o->is_dirty();

Returns true if the object has been modified since it was
thawed from the database, or if it has never been inserted at all.

=cut

sub is_dirty {
    my $self = shift;
    my $dirty_fields = scalar $self->dirty_fields();
    return $dirty_fields || !$self->is_inserted();
}

sub _mark_field_dirty {
    my $self = shift;
    my $field = shift;
    $self->{_dirty_fields}{$field} = 1;
}

sub _mark_field_clean {
    my $self = shift;
    my $field = shift;
    $self->{_dirty_fields}{$field} = 0;
}

sub _mark_all_clean {
    my $self = shift;
    $self->{_dirty_fields} = {};
}

=head2 $bool = $o->is_field_dirty('field_name');

Checks an individual field for dirtiness.

=cut

sub is_field_dirty {
    my $self = shift;
    my $field = shift;
    return $self->{_dirty_fields}{$field} || 0;
}

=head2 @fields = $o->dirty_fields();

=head2 @cols = $o->dirty_columns();

Returns a list of fields or columns that are due for
an update.  Fields get added to this list whenever you call a mutator.

=cut

sub dirty_fields {
    my $self = shift;
    return grep { $self->{_dirty_fields}{$_} } keys %{$self->{_dirty_fields}};
}

sub dirty_columns { return $_[0]->column_name($_[0]->dirty_fields); }


=head2 $bool = $o->is_inserted();

Returns true if the object originated from the database, or has 
been inserted into the database since its creation.

=cut

sub is_inserted { return shift->{_is_inserted}; }
sub _is_inserted {
    my $self = shift;
    if (@_) {
        $self->{_is_inserted} = shift;
    }
    return $self->{_is_inserted};
}

#=========================================================#
#               Code Generation and
#                AUTOLOAD Facility
#=========================================================#

=head1 FIELD ACCESSORS

These methods correspond to data attributes (member variables) on the OO side, and table columns on the relational side.

At startup, as each model class calls build_class, CRO will list the columns on your table and create a method for each column.

Two caveats are in order if you are in a long-running process like mod_perl.  First, this column detection only happens once, at compile time, 
so adding a column while running is safe, but to see the column in your 
datamodel, you'll need to restart.  Secondly, since the running code 
expects the columns to always be there, renaming or deleting columns 
may be a breaking change (of course, if you're using those accessors or 
mutators, that's a breaking change anyway).  The concern is that the 
problem will not be detected until the code hits the database.

Primitive aggregate functionality is provided, but unless your needs are simple, you will be a sad little panda.

=head2 $value = $obj->foo_column()

=head2 $obj->foo_column($foo_value)

To read the value, just call the method with no arguments.  The value will be passed through any Filters, then returned.

To set the value, call the method with the new value.  CRO will pass the new value through any Filters, then update the object with the value.  The data is not saved to the database until you call save() or update().

To set a column to NULL, pass undef as the value.

=head2 $number = $class->count_of_foo(where => $where, execargs => \@args)

=head2 $number = $class->avg_of_foo(where => $where, execargs => \@arg)

=head2  .. etc ..

For each column, methods are created on first use that have the name <AGGREGATE>_of_<COLUMN>.  The list of aggregate functions is determined by your Driver; but you do get a handful by default - see L<Class::ReluctantORM::SQL::Function>.

You may optionally provide a where clause, with optional execargs, as for the search() methods.

=cut

=begin devdocs

=head2 $coderef = $class->_make_fetcher($field, $fatal, $rel);

Builds a coderef that forms the body of the
fetch_by_FIELD, fetch_with_REL, and fetch_by_FIELD_with_REL auto-generated methods.

$field is the name of the field to search on.

$fatal is whether a "miss" search should throw a NotFound exception.

$rel is the name of the relationship to deep-fetch.  If undef, 
no relations will be fetched.

=end devdocs

=cut

sub _make_fetcher {
    my ($class, $field, $fatal, $rel_name) = @_;
    my $code = sub {
        my $class2 = shift;
        my $value = shift;
        if (defined($field) && !defined($value)) { Class::ReluctantORM::Exception::Param::Missing->croak(param => $field . ' value'); }

        #my $table = Table->new($class);
        my $table = Table->new(table => 'MACRO__base__');

        my %deep_args;

        my $where;
        if (ref($field) eq 'ARRAY') {
            # Searching on multiple fields (ie, multiple keys)

            # Better hope $value is an array ref too...
            unless (ref($value) eq 'ARRAY') {
                Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak(param => 'value');
            }
            unless (@$value == @$field) {
                Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'value', error => 'Must be an array ref with ' . (scalar @$field) . ' elements, to match ' . join(',', @$field));
            }

            my $root_crit;
            foreach my $i (0..(scalar @$field -1)) {
                my $crit = Criterion->new(
                                          '=',
                                          Column->new(table => $table, column => $class->column_name($field->[$i])),
                                          Param->new($value->[$i]),
                                         );
                $root_crit = $root_crit ? Criterion->new('AND', $root_crit, $crit) : $crit;
            }
            $where = Where->new($root_crit);

        } elsif ($field) {
            $where = Where->new
              (Criterion->new(
                              '=',
                              Column->new(table => $table, column => $class->column_name($field)),
                              Param->new($value),
                             ));
        } else {
            $where = Where->new(); # always true
        }
        $deep_args{where} = $where;

        if (!wantarray) {
            $deep_args{limit} = 1;
            # We're required to provide an order by if we send an limit, so order by base table PK
            my $ob = Class::ReluctantORM::SQL::OrderBy->new();
            foreach my $pk_col ($class->primary_key_columns) {
                $ob->add(Column->new(table => $table, column => $pk_col));
            }
            $deep_args{order_by} = $ob;
        }

        if ($rel_name) {
            $deep_args{with} = { $rel_name => {} };
        }
        my @results = $class2->search_deep(%deep_args);
        unless (@results) {
            if ($fatal) { Class::ReluctantORM::Exception::Data::NotFound->croak(criteria => $value); }
            return wantarray ? () : undef;
        }

        return wantarray ? @results : $results[0];
    };
    return $code;
}

=begin devdocs

=head2 make_accessor

Override this (defined by Class::Accessor) so that we track dirty status
And catch foreign key changes on has_ones

=end devdocs

=cut

sub make_accessor {
    my ($class, $field) = @_;

    # Build a closure around $field.
    return sub {
        my $self = shift;

        if(@_) {
            my $new_val = shift;
            $new_val = $self->__apply_field_write_filters($field, $new_val);
            if (nz($self->get($field),'UNDEF') ne nz($new_val, 'UNDEF')) {
                $self->_mark_field_dirty($field);

                # If the field is the local foreign key field of a relation,
                # clear the fetched flag.
                foreach my $relation_name (keys %{$class->__metadata()->{relations}}) {
                    my $rel = $class->__metadata()->{relations}{$relation_name};
                    if (grep { $_ eq $field } $rel->local_key_fields()) {
                        $rel->_mark_unpopulated_in_object($self);
                    }
                }

                return $self->set($field, $new_val);
            }
            else {
                my $raw_value = $self->get($field);
                my $cooked_value = $self->__apply_field_read_filters($field, $raw_value);
                return $cooked_value;
            }
        }
        else {
            my $raw_value = $self->get($field);
            my $cooked_value = $self->__apply_field_read_filters($field, $raw_value);
            return $cooked_value;
        }
    };
}

sub _make_aggregator {
    my $class = shift;
    my $field = shift;
    my $aggrfunc = shift;

    my $column = $class->column_name($field);
    return sub {
        my $class2 = shift;
        my %args = check_args(args => \@_, optional => [qw(where execargs)]);

        my $where = $args{where};
        if (!$where) {
            $where = Where->new();
        } elsif (UNIVERSAL::isa($where, Where)) {
            $where->bind_params(@{$args{execargs} || []});
        } else {
            my $driver = $class->driver();
            $where = $driver->parse_where($args{where});
            $where->bind_params(@{$args{execargs} || [] });
        }

        my $table = Table->new($class);
        my $fc = FunctionCall->new($aggrfunc,
                                   Column->new(table => $table,
                                               column => $column));
        my $oc = OutputColumn->new(expression => $fc, alias => 'aggr_result');

        my $sql = SQL->new('SELECT');
        $sql->where($where);
        $sql->from(From->new($table));
        $sql->add_output($oc);
        $sql->set_reconcile_option(add_output_columns => 0);


        my $driver = $class2->driver();
        $driver->run_sql($sql);

        my $result = $oc->output_value();
        return $result;

    };

}

sub AUTOLOAD {
    # Mainly, we're here to auto-generate methods as requested by Class::ReluctantORM::Utilities::install_method_on_first_use() and install_method_generator
    our $AUTOLOAD;
    my ($class, $method_name) = $AUTOLOAD =~ /(.+)::([^:]+)$/;

    my $method_body_coderef;

    my $method_maker = $METHODS_TO_BUILD_ON_FIRST_USE{$class}{$method_name};
    if ($method_maker) {
        $method_body_coderef = $method_maker->();
    } else {
        foreach my $generator (@{$METHOD_GENERATORS{$class} || []}) {
            last if $method_body_coderef = $generator->($class, $method_name);
        }
    }
    unless ($method_body_coderef) {
        Class::ReluctantORM::Exception::Call::NoSuchMethod->croak("No such method $AUTOLOAD");
    }

    install_method($class, $method_name, $method_body_coderef);
    goto &$method_body_coderef;
}


#=========================================================#
#                      Filter Support                     #
#=========================================================#

=head1 FILTER SUPPORT

These methods provide support for transforming the value of a field when it is being read from an object, or being written to the object.

One common use of this is to escape all HTML entities, for example.

=cut

# Default implementations - in case Class::ReluctantORM::FilterSupport is disabled
BEGIN {
    unless (__PACKAGE__->can('__apply_field_read_filters')) {
        eval 'sub __apply_field_read_filters { return $_[2]; }';
    }
    unless (__PACKAGE__->can('__apply_field_write_filters')) {
        eval 'sub __apply_field_write_filters { return $_[2]; }';
    }
}

=begin devdocs

=head2 $obj->attach_filter()

Bad method name, add an alias.

=end devdocs

=cut

=head2 $obj->append_filter($filter)

See L<Class::ReluctantORM::Filter>.

=head2 $class->attach_class_filter($filter)

See L<Class::ReluctantORM::Filter>.

=head2 $obj->set_filters(...)

See L<Class::ReluctantORM::Filter>.

=head2 $obj->clear_filters()

See L<Class::ReluctantORM::Filter>.

=head2 $obj->remove_filter(...)

See L<Class::ReluctantORM::Filter>.

=head2 @filters = $obj->read_filters_on_field(...)

See L<Class::ReluctantORM::Filter>.

=head2 @filters = $obj->write_filters_on_field(...)

See L<Class::ReluctantORM::Filter>.

=cut

=head2 $val = $obj->raw_field_value('field');

=head2 $obj->raw_field_value('field', $newval);

Gets or sets the raw, internal value of a field.  This method bypasses the filtering mechanism.

=cut

sub raw_field_value {
    my $self = shift;
    my $field = shift;

    if (my $rel = $self->relationships($field)) {
        return $rel->_raw_mutator($self, @_);
    } else {
        if (@_) {
            my $new_value = shift;
            $self->set($field, $new_value);
            $self->_mark_field_dirty($field);
        }
        return $self->get($field);
    }
}

#=========================================================#
#                      Origin Support                     #
#=========================================================#
# Default implementation in case OriginSupport is not loaded
BEGIN {
    unless (__PACKAGE__->can('capture_origin')) {
        eval 'sub capture_origin { }';
    }
    unless (__PACKAGE__->can('is_origin_tracking_enabled')) {
        eval 'sub is_origin_tracking_enabled { 0; }';
    }
}


#=========================================================#
#                 Relationship Facility
#=========================================================#

=head1 RELATIONSHIP SUPPORT

=head2 $rel = $class->relationships('field');

=head2 $rel_by_name_href = $class->relationships();

=head2 @rels = $class->relationships();

Accesses information about the relationships this class has with other Class::ReluctantORM classes.

In the first form, returns a Class::ReluctantORM::Relationship object (or a subclass thereof), for the given field.  For example, you might say:

  $rel = Pirate->relationships('ship');

In the second form (scalar context), returns a hashref of all relationships the class participates in, keyed by field name.

In the third form (list context), returns an array of all relationships the class participates in.

=cut

sub relationships {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    my $field = shift;
    my $hash = $class->__metadata()->{relations}  || {} ;
    if ($field) { return $hash->{$field}; }
    return wantarray ? (values %$hash) : $hash;
}

=head2 @relnames = $class->relationship_names();

Returns the names of all relationships on the class.  These are the method names used to access the related object or collection.

=cut

sub relationship_names { return keys %{shift->relationships}; }

=head2 $bool = $o->is_relation_fetched('relname');

=head2 $bool = $o->is_field_fetched('fieldname');

Returns true or false, depending on whether the named field or relation has been fetched.
If true, you may call the accessor without rish of a FetchRequired exception.

=cut

=begin devdocs

=head2 $bool = $o->is_fetched('relname');

Deprecated alias

=end devdocs

=cut

sub is_fetched {
    my $self = shift;
    my $fieldname = shift;

    my $rel = $self->relationships($fieldname);
    if ($rel) {
        return $rel->is_populated_in_object($self);
    } else {
        return 1;
    }
}


sub is_relation_fetched { return $_[0]->is_fetched($_[1]); }
sub is_field_fetched { return $_[0]->is_fetched($_[1]); }


=begin devdocs

=head2 $class->register_relationship($rel);

Attaches a relationship to this class without modifying the relationship.  Should only be used by people implementing their own relationships.

=end devdocs

=cut

sub register_relationship {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    my $rel = shift;
    my $name = $rel->method_name();
    $class->__metadata()->{relations}->{$name} = $rel;
}

=head2 $class->clone_relationship($rel);

Copies a relationship to this class, so that this class is the linking class on the new relationship.  The linked class remains the same.

For this to work, this class must have the same foreign keys that the orginal linking class used.

This is useful when you are using table-based inheritance (for example, as under PostgreSQL) and you want your inheriting class to have the same relationships as the parent.  Then you can just do:

  foreach my $rel (Parent->relationships) {
     Child->clone_relationship($rel);
  }

=cut

sub clone_relationship {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    my $rel = shift;
    unless ($rel->isa('Class::ReluctantORM::Relationship')) {
        Class::ReluctantORM::Exception::Param::WrongType->croak
            (
             error => 'clone_relationship takes a real Relationship object',
             expected => 'Class::ReluctantORM::Relationship',
             param => 'relationship',
             value => $rel,
            );
    }

    my @original_args = @{$rel->_original_args_arrayref() || []};
    my $method = $rel->_setup_method_name();
    $class->$method(@original_args);
}


#=========================================================#
#                 Abstract SQL Support
#=========================================================#

our $ENABLE_JOIN_CACHE;

{
    no warnings qw(void); # Test scripts that do use_ok('Class::ReluctantORM') will trigger a 'Too late for CHECK block' warning
    CHECK {
        # It is imperative that we enable the cache only after all relationships are defined
        $ENABLE_JOIN_CACHE = 1;
    }
}

our %JOIN_TABLE_CACHE = (by_schema => {}, by_table => {}, cache_initted => 0);

sub __build_join_table_cache {
    return if $JOIN_TABLE_CACHE{cache_initted};
    foreach my $cro_class (keys %CLASS_METADATA) {
        foreach my $rel ($cro_class->relationships) {
            my $jst = $rel->join_sql_table();
            if ($jst) {
                $JOIN_TABLE_CACHE{by_schema}{$jst->schema()}{$jst->table()} = $rel;
                $JOIN_TABLE_CACHE{by_table}{$jst->table()} = $rel;
            }
        }
    }
    if ($ENABLE_JOIN_CACHE) {
        $JOIN_TABLE_CACHE{cache_initted} = 1;
    }
}

sub _is_join_table {
    my $class = shift;
    my %args = check_args(args => \@_, one_of => [[qw(table_obj table_name)]], optional => [qw(schema_name)]);

    my $table_name = $args{table_obj}  ? $args{table_obj}->table()  : $args{table_name};
    my $schema_name = $args{table_obj} ? $args{table_obj}->schema() : $args{schema_name};

    $class->__build_join_table_cache();

    # If it's a class table, it's not a join table
    if ($class->_find_class_by_table(%args)) {
        return 0;
    }

    my $result;
    if ($schema_name) {
        $result = $JOIN_TABLE_CACHE{by_schema}{$schema_name}{$table_name};
    } else {
        $result = $JOIN_TABLE_CACHE{by_table}{$table_name};
    }

    return $result ? 1 : undef;
}

sub _find_sql_table_for_join_table {
    my $class = shift;
    my %args = check_args(args => \@_, one_of => [[qw(table_obj table_name)]], optional => [qw(schema_name)]);

    my $table_name = $args{table_obj}  ? $args{table_obj}->table()  : $args{table_name};
    my $schema_name = $args{table_obj} ? $args{table_obj}->schema() : $args{schema_name};

    $class->__build_join_table_cache();

    my $rel;
    if ($schema_name) {
        $rel = $JOIN_TABLE_CACHE{by_schema}{$schema_name}{$table_name};
    } else {
        $rel = $JOIN_TABLE_CACHE{by_table}{$table_name};
    }

    unless ($rel) { return undef; }
    my $sql_table = $rel->join_sql_table();  # This copy of the table has manual-set columns
    return $sql_table;
}

sub _find_class_by_table {
    my $class = shift;
    my %args = check_args(args => \@_, one_of => [[qw(table_obj table_name)]], optional => [qw(schema_name)]);

    my $table_name = $args{table_obj}  ? $args{table_obj}->table()  : $args{table_name};
    my $schema_name = $args{table_obj} ? $args{table_obj}->schema() : $args{schema_name};

    foreach my $cro_class (keys %CLASS_METADATA) {
        my $cc_table  = $CLASS_METADATA{$cro_class}{table};
        my $cc_schema = $CLASS_METADATA{$cro_class}{schema};
        if (($table_name eq $cc_table) && (!$schema_name || ($schema_name eq $cc_schema))) {
            return $cro_class;
        }
    }
    return undef;
}

our %RELATIONSHIP_CACHE = (
                           by_local  => { by_schema => {}, by_table => {}},
                           by_remote => { by_schema => {}, by_table => {}},
                           by_join   => { by_schema => {}, by_table => {}},
                           initted   => 0,
                          );

sub __init_relationship_cache {
    return if $RELATIONSHIP_CACHE{initted};
    foreach my $cro_class (keys %CLASS_METADATA) {
        foreach my $rel ($cro_class->relationships()) {
            my $lt = $rel->local_sql_table();
            if ($lt) {
                $RELATIONSHIP_CACHE{by_local}{by_schema}{$lt->schema}{$lt->table} ||= [];
                push @{$RELATIONSHIP_CACHE{by_local}{by_schema}{$lt->schema}{$lt->table}}, $rel;
                $RELATIONSHIP_CACHE{by_local}{by_table}{$lt->table} ||= [];
                push @{$RELATIONSHIP_CACHE{by_local}{by_table}{$lt->table}}, $rel;
            }

            my $jt = $rel->join_sql_table();
            if ($jt) {
                $RELATIONSHIP_CACHE{by_join}{by_schema}{$jt->schema}{$jt->table} ||= [];
                push @{$RELATIONSHIP_CACHE{by_join}{by_schema}{$jt->schema}{$jt->table}}, $rel;
                $RELATIONSHIP_CACHE{by_join}{by_table}{$jt->table} ||= [];
                push @{$RELATIONSHIP_CACHE{by_join}{by_table}{$jt->table}}, $rel;
            }

            my $rt = $rel->remote_sql_table();
            if ($rt) {
                $RELATIONSHIP_CACHE{by_remote}{by_schema}{$rt->schema}{$rt->table} ||= [];
                push @{$RELATIONSHIP_CACHE{by_remote}{by_schema}{$rt->schema}{$rt->table}}, $rel;
                $RELATIONSHIP_CACHE{by_remote}{by_table}{$rt->table} ||= [];
                push @{$RELATIONSHIP_CACHE{by_remote}{by_table}{$rt->table}}, $rel;
            }
        }
    }

    $RELATIONSHIP_CACHE{initted} = 1;
}

sub _find_relationships_by_local_table {
    my $class = shift;
    my %args = check_args(args => \@_, one_of => [[qw(table_obj table_name)]], optional => [qw(schema_name)]);

    my $table_name = $args{table_obj}  ? $args{table_obj}->table()  : $args{table_name};
    my $schema_name = $args{table_obj} ? $args{table_obj}->schema() : $args{schema_name};

    __init_relationship_cache();

    if ($schema_name) {
        return @{$RELATIONSHIP_CACHE{by_local}{by_schema}{$schema_name}{$table_name} || []};
    } else {
        return @{$RELATIONSHIP_CACHE{by_local}{by_table}{$table_name} || []};
    }
}

sub _find_relationships_by_remote_table {
    my $class = shift;
    my %args = check_args(args => \@_, one_of => [[qw(table_obj table_name)]], optional => [qw(schema_name)]);

    my $table_name = $args{table_obj}  ? $args{table_obj}->table()  : $args{table_name};
    my $schema_name = $args{table_obj} ? $args{table_obj}->schema() : $args{schema_name};

    __init_relationship_cache();

    if ($schema_name) {
        return @{$RELATIONSHIP_CACHE{by_remote}{by_schema}{$schema_name}{$table_name} || []};
    } else {
        return @{$RELATIONSHIP_CACHE{by_remote}{by_table}{$table_name} || []};
    }
}

sub _find_relationships_by_join_table {
    my $class = shift;
    my %args = check_args(args => \@_, one_of => [[qw(table_obj table_name)]], optional => [qw(schema_name)]);

    my $table_name = $args{table_obj}  ? $args{table_obj}->table()  : $args{table_name};
    my $schema_name = $args{table_obj} ? $args{table_obj}->schema() : $args{schema_name};

    __init_relationship_cache();

    if ($schema_name) {
        return @{$RELATIONSHIP_CACHE{by_join}{by_schema}{$schema_name}{$table_name} || []};
    } else {
        return @{$RELATIONSHIP_CACHE{by_join}{by_table}{$table_name} || []};
    }
}


#=========================================================#
#                 Monitoring Facility
#=========================================================#

=head1 MONITORING SUPPORT

=head2 Class::ReluctantORM->install_global_monitor($mon);

Installs a monitor that will be used on all Class::ReluctantORM queries.

$mon should be a Class::ReluctantORM::Monitor.

=cut

sub install_global_monitor {
    my $self = shift;
    my $mon = shift;

    unless ($mon) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'monitor'); }
    unless (UNIVERSAL::isa($mon, 'Class::ReluctantORM::Monitor')) {
        Class::ReluctantORM::Exception::Param::WrongType->croak(param => 'monitor', expected => 'Class::ReluctantORM::Monitor', value => $mon);
    }

    push @GLOBAL_MONITORS, $mon;
    return 1;
}

=head2 @mons = Class::ReluctantORM->global_monitors();

Returns a list of globally applicable monitors.

=cut

sub global_monitors { return @GLOBAL_MONITORS; }

=head2 Class::ReluctantORM->remove_global_monitors();

Removes all globally applicable monitors.

=cut

sub remove_global_monitors { @GLOBAL_MONITORS = (); }

=head2 Class::ReluctantORM->remove_global_monitor($mon);

Removes one global monitor.

=cut

sub remove_global_monitor {
    my $class = shift;
    my $monitor = shift;
    @GLOBAL_MONITORS = grep { refaddr($_) != refaddr($monitor) } @GLOBAL_MONITORS;
}

=head2 MyClass->install_class_monitor($mon);

Installs a monitor that will only monitor this specific subclass.  The monitor is actually attached to the driver of this class.

=cut

sub install_class_monitor {
    my $class = shift;
    my $mon = shift;
    $class->driver->install_monitor($mon);
}

=head2 @mons = MyClass->class_monitors();

Lists all monitors specific to this class.

=cut

sub class_monitors { shift->driver->driver_monitors(); }

=head2 MyClass->remove_class_monitors();

Removes all class-specific monitors.

=cut

sub remove_class_monitors { shift->driver->remove_driver_monitors(); }







#==============================================================#
#                       Trigger Support
#==============================================================#


=head1 TRIGGER SUPPORT

Class::ReluctantORM supports Perl-side triggers.  (You are also free to implement db-side triggers, of course.)

A trigger is a coderef that will be called before or after certain events.  The args will be the CRO object, followed by the name of the trigger event.

Triggers are assigned at the class level.  You can assign multiple triggers to the event by making repeated calls to add_trigger.  They will be called in the order they were added.

The following events are currently supported:

=over

=item after_retrieve

=item before_insert, after_insert

=item before_update, after_update

=item before_delete, after_delete

=item before_save,   after_save

=item before_refresh, after_refresh

=back

Before/after save is a little unusual - it is called within save(), and either the insert or update triggers will be called as well.  The order is:

=over

=item 1

before_save

=item 2

before_insert OR before_update

=item 3

after_insert OR after_update

=item 4

after_save

=back

=cut

our %TRIGGER_EVENTS = 
  map { $_ => 1 }
  qw(
        after_retrieve
        before_refresh after_refresh
        before_update after_update
        before_insert after_insert
        before_save   after_save
        before_delete after_delete
    );

=head2 MyClass->add_trigger('event', $coderef);

Arranges for $coderef to be called whenever 'event' occurs.  $coderef will be passed the CRO object and the event name as the two arguments.

=cut

sub add_trigger {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    unless (@_ > 1) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'event, coderef'); }
    if (@_ > 2) { Class::ReluctantORM::Exception::Param::Spurious->croak(); }
    my ($event, $coderef) = @_;
    unless (exists $TRIGGER_EVENTS{$event}) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'event', value => $event, error => 'Must be one of ' . join(',', keys %TRIGGER_EVENTS));
    }
    unless (ref($coderef) eq 'CODE') {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'coderef', value => $coderef, expected => 'CODE reference');
    }

    my $meta = $class->__metadata();
    $meta->{triggers} ||= {};
    $meta->{triggers}{$event} ||= [];
    push @{$meta->{triggers}{$event}}, $coderef;

}

=head2 remove_trigger('event', $codref);

Removes the given trigger from the event.

=cut

sub remove_trigger {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    unless (@_ > 1) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'event, coderef'); }
    if (@_ > 2) { Class::ReluctantORM::Exception::Param::Spurious->croak(); }
    my ($event, $coderef) = @_;
    unless (exists $TRIGGER_EVENTS{$event}) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'event', value => $event, error => 'Must be one of ' . join(',', keys %TRIGGER_EVENTS));
    }
    unless (ref($coderef) eq 'CODE') {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'coderef', value => $coderef, expected => 'CODE reference');
    }

    my $meta = $class->__metadata();
    $meta->{triggers} ||= {};
    $meta->{triggers}{$event} ||= [];
    $meta->{triggers}{$event} =
      [ grep { $_ ne $coderef } @{$meta->{triggers}{$event}} ];
}

=head2 MyClass->remove_all_triggers();

=head2 MyClass->remove_all_triggers('event');

In the first form, removes all triggers from all events.

In the second form, removes all triggers from the given event.

=cut

sub remove_all_triggers {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    if (@_ > 1) { Class::ReluctantORM::Exception::Param::Spurious->croak(); }
    my ($event) = @_;

    my $meta = $class->__metadata();
    $meta->{triggers} ||= {};

    if ($event) {
        unless (exists $TRIGGER_EVENTS{$event}) {
            Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'event', value => $event, error => 'Must be one of ' . join(',', keys %TRIGGER_EVENTS));
        }
        $meta->{triggers}{$event} = [];
    } else {
        $meta->{triggers} = {};
    }
}

=head2 @trigs = MyClass->list_triggers('event');

Lists all triggers from the given event, in the order they will be applied.

=cut

sub list_triggers {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    if (@_ < 1) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'event'); }
    if (@_ > 1) { Class::ReluctantORM::Exception::Param::Spurious->croak(); }
    my ($event) = @_;

    unless (exists $TRIGGER_EVENTS{$event}) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'event', value => $event, error => 'Must be one of ' . join(',', keys %TRIGGER_EVENTS));
    }
    my $meta = $class->__metadata();
    $meta->{triggers} ||= {};
    $meta->{triggers}{$event} ||= [];
    return @{$meta->{triggers}{$event}};
}

sub __run_triggers {
    my $self = shift;
    my $event = shift;

    unless (exists $TRIGGER_EVENTS{$event}) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'event', value => $event, error => 'Must be one of ' . join(',', keys %TRIGGER_EVENTS));
    }

    # LEGACY In TableBacked, triggers were defined by inheritance.
    # Check for a trigger defined in such a way.
    my $method = '_' . $event . '_trigger';
    if ($self->can($method)) {
        deprecated("Using inheritance to define a $event trigger - use add_trigger() instead");
        $self->$method();
    }

    my $class = ref($self);
    foreach my $trig (@{$class->__metadata()->{triggers}{$event} || []}) {
        $trig->($self, $event);
    }
}

=head1 AUTHOR

Clinton Wolfe (clwolfe@cpan.org) 2008-2012

With extensive real-world usage from the fine folks at OmniTI (www.omniti.com).

=cut

=head1 COPYRIGHT

Copyright OmniTI 2012. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut

=head1 BUGS

Let's track them in RT, shall we.  https://rt.cpan.org/Dist/Browse.html?Name=Class-ReluctantORM

=cut



1;

