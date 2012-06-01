package Class::ReluctantORM::Audited;
use strict;
use warnings;
use base 'Class::ReluctantORM';
use Class::ReluctantORM::SQL::Aliases;

our $CLASS_METADATA = \%Class::ReluctantORM::CLASS_METADATA;

our $DEBUG ||= 0;

=head1 NAME

Class::ReluctantORM::Audited - CRO clases with Audit Logging

=head1 SYNOPSIS

  package ImportantThing;
  use strict;
  use warnings;
  use base 'Class::ReluctantORM::Audited';

  __PACKAGE__->build_class(
       # Normal Class::ReluctantORM->build_class options
       table => 'important_things',
       schema => 'stuff',
       primary_key => 'thing_id',
       ...

       # Additional audit table options
       audit_table_name => 'important_changes',
       audit_schema_name => 'audit',
       audit_primary_key => 'change_id', # Or array ref
       audit_columns => [qw(audit_user_id audit_ip_address)],
       audit_insert => 0, # also delete, update
       audit_seamless_mode => 1, # hijack old method names, make purists cry
  );

  # Two ways to provide audit metadata:
  $thing->audited_update($user_id, $ip_address);

  # or magically, if you define
  # ImportantThing::get_audit_metadata_audit_user_id and
  # ImportantThing::get_audit_metadata_audit_ip_address
  $thing->audited_update();

  # To make update() explode, use audit_seamless_mode => 0 (default)

  # this still works because of audit_seamless_mode => 1
  # but you must have the get_audit_data methods
  $thing->update();


=head1 DESCRIPTION

Many times it is neccesary to log changes to tables.  This log, called an
audit log, typically retains the previous state of the row, along with the
action that was performed, and any additional metadata deemed needed by 
the business (such as who made the change).  This class exists to make such
work easier.

This is a Class::ReluctantORM subclass.  All its methods are
available here.  save(), update(), insert(), create(), and delete() are
re-defined in this class.

=head1 CREATING AN AUDIT TABLE

This class expects there to be a separate table for logging changes made
against each orginal table.  It may be in any schema, and have any name. No 
CRO triggers, DB triggers or stored procedures are used.

To create your audit table, create a new table with the same column
and same datatypes as the original table, with the following modifications:

=over

=item Do not make your original primary key(s) unique.  You may wish to add an additional, artificial primary key to the log table; you can identify that column(s) using the audit_primary_key option.

=item Drop any other unique constraints.

=item Add a column, of type text, named "audit_action".  This module will store the name of the action that occurred in this column.

=item If you are auditing INSERTS, all original columns must be made nullable in the audit table.

=item It is your decision as to whether or not to enforce foreign key constraints; most users choose not to.

=item Add any audit metadata columns, for example, the IP address of the user makeing the change.  If the audit metadata column has a default, you need not list it in the audit_columns option to build_class.  Note that your audit column names don't have to start with 'audit_', but is a nice convention.  CRO obtains the list of audit columns from build_class.


=back

=head1 PROVIDING AUDIT METADATA

The audit metadata must be provided "out of band" - it is not part of the actual update/insert/delete, but must be provided for auditing.  To support this, two approaches are available.

=head2 Passing Audit Metadata as Arguments to audited_update, etc

You may provide the metadata directly when calling audited_update, 
audited_delete, audited_insert, or audited_save.  (audited_create is
handled specially).  You must provide all values, and you must provide
them in the order specified in build_class.

   $thing->audited_update($changing_user_id, $ip_address);

For audited_create, you may provide the audit metadata as additional
columns.  Your audit column names must be distinct from your original
column names for this to work.

   ImportantThing->audited_create(
       name => 'whatever',
       other_field => $stuff,
       ...
       audit_user_id => $changing_user_id,
       audit_ip_address => $ip_address,
   );

The downside of this approach is that your calling code must provide these values every time it performs an audited operation, which becomes tiresome.

=head2 Providing Audit Metadata Generators

Alternatively, you can provide instance methods in your subclass that will obtain each piece of metadata when performing an audited action.  The methods should have names that begin with 'get_audit_metadata_FIELDNAME':

  sub get_audit_metadata_audit_user_id {
     my $self = shift;
     # magically determine $changing_user_id from globals or $self
     return $changing_user_id;
  }

With that (and any other generators in place) you can now simply call:

  $thing->audited_update();


=head1 SEAMLESS MODE

You may be wondering what happens now when someone calls, for example, $thing->update().  You have your choice of poisons.

=head2 Seamless Mode Off (Default)

Calling the plain version of any audited action will throw an exception.  You are required
to call the audited_whatever version.

This is more more "pure", in terms of software design. By changing the preconditions for calling a method, we have violated a contract, and we should let the user know it's no longer safe to call it the old way.

The downside is that you now have to retrofit all old code to call audited_whatever.  You'll also need to arrange to have the audit metadata available at each of these invocations.

=head2 Seamless Mode On

Calling the plain version of any audited action will attempt to obtain the audit metadata, then hand off to the audited version.

This means you can do this:

  $thing->update($changing_user_id, $ip_address);

  # Or if youve defined metatdata generator methods, simply
  $thing->update();

That's a lot of magic going on there.  This may lead to unexpected suprises (especially if a metatdata generator fails), but the upside is that you might not have to change any code.

=head1 METHODS

=head2 YourSubClass->build_class(%audit_opts, %cro_opts)

Sets up your class for auditing.  Accepts all options that the stock 
Class::ReluctantORM build_class accepts (in any order).  In addition, the
following auditing options are supported:

=over

=item audit_schema_name

Required.  Name of the schema in the database for the audit table.

=item audit_table_name

Required.  Name of audit table in the database.  For details on how this
table should be constructed, see CREATING AN AUDIT TABLE.

=item audit_primary_key

Optional.  String, or arrayref of strings, identifying an optional primary key on the log table itself.  This allows you to call last_audit_primary_key_value() on your original object, and thus identify individual log entries.

=item audit_columns

Optional arrayref of strings.  These are extra columns (not present in
the original table) that you would also like to include in the log (for
example, IP address).  For details on how to pass these values, see 
PROVIDING AUDIT COLUMN VALUES.

=item audit_inserts, audit_updates, audit_deletes

Optional booleans, default true.  You may set any of these to 0 to disable
auditing on that action.  Disabling all of them disables auditing.

=item audit_seamless_mode

Boolean, default false.  If you provide a true value, seamless mode is enabled.
See SEAMLESS MODE above.

=back

=cut

sub build_class {
    my $class = shift;

    if (@_ % 2) { OmniTI::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;

    # Separate out audit-specifc options, and check them
    my @expected_options = qw(
                                 audit_table_name
                                 audit_schema_name
                                 audit_primary_key

                                 audit_updates
                                 audit_inserts
                                 audit_deletes

                                 audit_columns
                                 audit_seamless_mode
                            );

    my %class_audit_options;
    @class_audit_options{@expected_options} = @args{@expected_options};
    for (@expected_options) { delete $args{$_}; }

    for my $required (qw(audit_table_name audit_schema_name)) {
        unless (defined $class_audit_options{$required}) {
            OmniTI::Exception::Param::Missing->croak(param => $required);
        }
    }
    my $cols = $class_audit_options{audit_columns};
    if ($cols) {
        unless (ref($cols) eq 'ARRAY') {
            OmniTI::Exception::Param::ExpectedArrayRef->croak(param => 'audit_columns');
        }
    }

    # Boost PK to an arrayref
    my $pk = $class_audit_options{audit_primary_key};
    $pk = defined($pk) ? (ref($pk) ? $pk : [ $pk ]) : [];
    $class_audit_options{audit_primary_key} = $pk;

    # Do a normal build_class, for standard table-backed setup
    $class->SUPER::build_class(%args);

    # Setup auditing features
    my $md = $CLASS_METADATA->{$class};
    $md->{audit} = {
                    primary_key => $class_audit_options{audit_primary_key},
                    table_name => $class_audit_options{audit_table_name},
                    schema_name => $class_audit_options{audit_schema_name},
                    columns =>  $class_audit_options{audit_columns} || [],
                    seamless_mode  => $class_audit_options{audit_seamless_mode} || 0,
                   };

    # Which actions to audit?
    foreach my $action (qw(updates deletes inserts)) {
        my $opt = $class_audit_options{'audit_' . $action};
        my $val = defined($opt) ? $opt : 1;
        $md->{audit}->{actions}->{$action} = $val;
    }

    # Scan for audit column data providers, and cache them
    foreach my $column ($class->audit_columns) {
        my $fetcher_name = 'get_audit_metadata_' . $column;
        my $ref = $class->can($fetcher_name);
        if ($ref) {
            $md->{audit}->{fetchers}->{$column} = $ref;
        }
    }

}

=head2 $bool = YourSubClass->is_audited();

Always returns true.  Plain Class::ReluctantORM classes always return false.

=cut

sub is_audited { return 1; }

=head2 @col_names = YourSubClass->audit_columns();

Returns an array of column names that are treated as audit metadata columns.  
This array is obtained directly from build_class().

=cut

sub audit_columns {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    return @{$CLASS_METADATA->{$class}->{audit}->{columns}};
}

=head2 @col_names = YourSubClass->audit_primary_key_columns()

If your audit table has a primary key (for change tracking, etc), this will return the column names that make up the primary key.

Returns an empty list if you did not pass a value for audit_primary_key to build_class.

=cut

sub audit_primary_key_columns {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv; 
    return @{$CLASS_METADATA->{$class}->{audit}->{primary_key}};
}

=head2 $bool = YourSubClass->audit_seamless_mode();

Returns true if seamless mode is enabled for this class.

=cut

sub audit_seamless_mode {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    return $CLASS_METADATA->{$class}->{audit}->{seamless_mode};
}


=head2 $bool = YourSubClass->audit_inserts();

Returns true if inserts (and creates) are being audited.

=cut

sub audit_inserts {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    return $CLASS_METADATA->{$class}->{audit}->{actions}->{inserts};
}

=head2 $bool = YourSubClass->audit_updates();

Returns true if updates are being audited.

=cut

sub audit_updates {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    return $CLASS_METADATA->{$class}->{audit}->{actions}->{updates};
}

=head2 $bool = YourSubClass->audit_deletes();

Returns true if deletes are being audited.

=cut

sub audit_deletes {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    return $CLASS_METADATA->{$class}->{audit}->{actions}->{deletes};
}

=head2 $str = YourSubClass->audit_schema_name();

Returns the name of the schema that the audit table lives in.

=cut

sub audit_schema_name {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    return $CLASS_METADATA->{$class}->{audit}->{schema_name};
}

=head2 $str = YourSubClass->audit_table_name();

Returns the name of the audit table in the database.

=cut

sub audit_table_name {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    return $CLASS_METADATA->{$class}->{audit}->{table_name};
}



#=========================================================#
#                  Audit Metdata Handling
#=========================================================#

sub __get_audit_metadata {
    my $self = shift;
    my $class = ref($self);
    my $column_name = shift;

    # Check cache first
    my $adc = $self->get('audit_data_cache') || {};
    if (exists $adc->{$column_name}) {
        return $adc->{$column_name};
    }

    # Check for a coderef to fetch it
    my $code = $CLASS_METADATA->{$class}->{audit}->{fetchers}->{$column_name};
    unless ($code) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak(<<EOT);
Don't know how to get audit column data for column '$column_name'. 
See Class::ReluctantORM::Audited, section PROVIDING AUDIT 
COLUMN VALUES for details.
EOT
    }

    return $code->($self);
}

sub __set_audit_col_vals {
    my $self = shift;
    my @audit_col_vals = @_;
    if (@audit_col_vals) {
        # OK, they provided audit column values directly.  No problem.
        my @cols = $self->audit_columns;
        unless (@cols == @audit_col_vals) {
            my $need_count = @cols;
            my $got_count = @audit_col_vals;
            my $exp = $need_count < $got_count ? 'Class::ReluctantORM::Exception::Param::Spurious' :'Class::ReluctantORM::Exception::Param::Missing';
            $exp->croak("Expected $need_count args to an audited action, but got $got_count.  See Class::ReluctantORM::Audited, section PROVIDING AUDIT COLUMN VALUES for details.");
        }
        my %acv;
        @acv{@cols} = @audit_col_vals;
        $self->set('audit_data_cache', \%acv);
    } else {
        # Setup the audit values first, in case there is a problem.
        my @audit_values = map { $self->__get_audit_metadata($_) } $self->audit_columns;
        my %avc;
        @avc{$self->audit_columns} = @audit_values;
        $self->set('audit_data_cache', \%avc);
    }
}

#=========================================================#
#               Audit Log PK Support
#=========================================================#

=head2 $val = $obj->last_audit_primary_key_value()

=head2 $array_ref = $obj->last_audit_primary_key_value()

=head2 @array = $obj->last_audit_primary_key_value()

If your audit table has a primary key, this will return the primary value of the last log entry.

If your audit table has a single-column primary key, the one value will be returned.

If your audit table has a multi-column primary key, an arrayref of the values, in the same order you used in build_class, will be returned.

In list context, an array is alwas returned.

If no audited action has occurred, it will return undef or an empty list.

=cut

sub last_audit_primary_key_value {
    my $self = shift;

    my $nook = $self->get('_last_audit_primary_key_value_arrayref');
    unless ($nook) { return wantarray ? () : undef; }
    if ($self->audit_primary_key_columns > 1) {
        return wantarray ? @$nook : $nook;
    } else {
        return wantarray ? @$nook : $nook->[0];
    }
}


#=========================================================#
#                Plain CRUD Method Overrides
#=========================================================#



sub update {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    unless ($inv->audit_updates) {  return $inv->SUPER::update(); }

    if ($inv->audit_seamless_mode) {
        return $inv->audited_update(@_);
    } else {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak("$class is an update-audited class.  Please call audited_update instead, or enable seamless mode.");
    }
}

sub insert {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    unless ($inv->audit_inserts) {  return $inv->SUPER::insert(); }

    if ($inv->audit_seamless_mode) {
        return $inv->audited_insert(@_);
    } else {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak("$class is an insert-audited class.  Please call audited_insert instead, or enable seamless mode.");
    }
}

sub delete {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    unless ($inv->audit_deletes) {  return $inv->SUPER::delete(); }

    if ($inv->audit_seamless_mode) {
        return $inv->audited_delete(@_);
    } else {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak("$class is an delete-audited class.  Please call audited_delete instead, or enable seamless mode.");
    }
}

sub save {
    my $self = shift;
    my $class = ref($self);

    if (!$self->audit_updates && $self->is_inserted()) {
        # go ahead with the update
        if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- Audit::save handing off to SUPER::update\n"; }
        return $self->SUPER::update();
    }
    if (!$self->audit_inserts && !$self->is_inserted()) {
        # go ahead with the insert
        if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- Audit::save handing off to SUPER::insert\n"; }
        return $self->SUPER::insert();
    }

    unless ($self->audit_seamless_mode) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak("$class is an save-audited class.  Please call audited_save instead, or enable seamless mode.");
    }

    if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- Audit::save handing off to Audited::audited_save\n"; }
    $self->audited_save(@_);

}

sub create {
    my $inv = shift;
    my $class = ref($inv) ? ref($inv) : $inv;
    unless ($inv->audit_inserts) {  return $inv->SUPER::create(@_); }

    if ($inv->audit_seamless_mode) {
        return $inv->audited_create(@_);
    } else {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak("$class is an insert-audited class.  Please call audited_create instead, or enable seamless mode.");
    }
}

#=========================================================#
#                  Auditing CRUD Methods
#=========================================================#

=head1 AUDITING CRUD METHODS

For each of these methods, the arguments are optional if you have defined metadata generator methods.

=cut

sub __audited_action {
    my $self = shift;
    my $action = shift;
    my @audit_col_vals = @_;

    my $class = ref($self) ? ref($self) : $self;

    $self->__set_audit_col_vals(@_);

    my $audit_table = Table->new(
                                 schema => $self->audit_schema_name,
                                 table  => $self->audit_table_name,
                                );
    my $insert = SQL->new('INSERT');
    $insert->table($audit_table);

    if ($action eq 'INSERT') {
        foreach my $col ($self->audit_columns) {
            $insert->add_input(
                               Column->new(column => $col, table => $audit_table),
                               Param->new($self->__get_audit_metadata($col)),
                              );
        }
        foreach my $col ($self->primary_key_columns) {
            $insert->add_input(
                               Column->new(column => $col, table => $audit_table),
                               Param->new($self->raw_field_value($self->field_name($col))),
                              );
        }
        $insert->add_input(
                           Column->new(column => 'audit_action', table => $audit_table),
                           Param->new('INSERT'),
                          );
    } else {
        # INSERT ... SELECT to copy the row from the original table
        my $select = SQL->new('SELECT');
        my $subquery = SubQuery->new($select);
        $insert->input_subquery($subquery);
        my $src_table = Table->new($class);
        $select->from(From->new($src_table));
        foreach my $col ($self->column_names()) {
            $insert->add_input(Column->new(column => $col, table => $audit_table));
            $select->add_output(Column->new(column => $col, table => $src_table));
        }

        $insert->add_input(Column->new(column => 'audit_action', table => $audit_table));
        $select->add_output(Literal->new($action));

        foreach my $col ($self->audit_columns) {
            $insert->add_input(Column->new(column => $col, table => $audit_table));
            #$select->add_output(Param->new($self->__get_audit_metadata($col)));
            $select->add_output(Literal->new($self->__get_audit_metadata($col)));
        }

        # Build WHERE clause of subquery
        my $root_crit;
        foreach my $pkf ($self->primary_key_fields) {
            my $pkc = $self->column_name($pkf);
            my $crit = Criterion->new(
                                      '=',
                                      Column->new(column => $pkc, table => $src_table),
                                      Param->new($self->raw_field_value($pkf)),
                                     );
            $root_crit = $root_crit ? Criterion->new('AND', $crit, $root_crit) : $crit;
        }
        $select->where(Where->new($root_crit));
    }

    # Add output columns to the audit log insert, to fetch last log ID
    if ($self->audit_primary_key_columns) {
        foreach my $apkc ($self->audit_primary_key_columns) {
            $insert->add_output(Column->new(column => $apkc, table => $audit_table));
        }
    }

    $self->driver->run_sql($insert);

    # Read audit PK if needed
    if ($self->audit_primary_key_columns) {
        my %apk_values;
        foreach my $oc ($insert->output_columns) {
            $apk_values{$oc->expression->column} = $oc->output_value();
        }

        # Set in secret nook
        $self->set('_last_audit_primary_key_value_arrayref',
                   [ @apk_values{$self->audit_primary_key_columns} ],
                  );
    }

}

=head2 $obj->audited_update(@metadata);

Copies the existing row in the database to the audit table along with the metadata, then performs the update on the original table.

=cut

sub audited_update {
    my $self = shift;

    # Must allow update
    unless ($self->updatable) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak(message => 'This class is configured to not permit updates.  See Class::ReluctantORM->build_class().');
    }

    # Must be already inserted
    unless ($self->is_inserted()) {
        Class::ReluctantORM::Exception::Data::UpdateWithoutInsert->croak();
    }

    # Clear cache
    $self->set('audit_data_cache', {});

    $self->__audited_action('UPDATE', @_);
    $self->SUPER::update();
}

=head2 $obj->audited_insert(@metadata);

Performs the insert on the original table, then copies the new primary key and the metadata to the audit table (all other columns are left NULL, as this is a new row and the audit table only records past states).

=cut

sub audited_insert {
    my $self = shift;

    # Do this early to catch any problems
    # because we're doing the INSERT before the audit
    # in order to get the new PK
    $self->__set_audit_col_vals(@_);
    $self->SUPER::insert();
    $self->__audited_action('INSERT', @_);
}

=head2 $obj->audited_delete(@metadata);

Copies the existing data from the original table along with the metadata to the audit table, then performs the delete on the original table.

=cut

sub audited_delete {
    my $self = shift;

    # Must allow delete
    unless ($self->deletable) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak(message => 'This class is configured to not permit deletes.  See Class::ReluctantORM->build_class().');
    }

    # Must be already inserted
    unless ($self->is_inserted()) {
        Class::ReluctantORM::Exception::Data::DeleteWithoutInsert->croak();
    }

    # Clear cache
    $self->set('audit_data_cache', {});

    $self->__audited_action('DELETE', @_);
    $self->SUPER::delete();
}

=head2 $obj->audited_save(@metadata);

Performs either an audited insert or an audited_update, depending
on whether the object has been saved to the database.

NOTE: This does NOT call the before_save_trigger nor the 
after_save_trigger, due to a sequencing conflict.  We're looking for
ways around that, but keep in mind the insert and update triggers
will be called normally.

=cut

sub audited_save {
    my $self = shift;
    unless ($self->is_dirty()) { return; }


    if ($self->is_inserted()) {
        if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- Audit::audited_save handing off to Audited::audited_update\n"; }
        $self->audited_update(@_);
    } else {
        if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- Audit::audited_save handing off to Audited::audited_insert\n"; }
        $self->audited_insert(@_);
    }

}

=head2 $obj = YourSubClass->audited_create(%yourfields, %audit_fields);

Like Class::ReluctantORM's create(), creates a new object in memory and
immediately commits it to the database.  Here, you may also specify
the audit metadata as well, which will get separated out.

=cut

sub audited_create {
    my $class = shift;

    # Next para borrowed from Class::ReluctantORM::new()
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

    # Thresh out the audit values (if any were provided)
    my @cols = $class->audit_columns();
    my @vals = ();
    my $one_was_missing = 0;
    foreach my $col (@cols) {
        if (!exists($hash_ref->{$col})) {
            $one_was_missing = 1;
            last;
        }
        push @vals, $hash_ref->{$col};
        delete $hash_ref->{$col};
    }
    if ($one_was_missing) { @vals = (); }

    # Ok, make the object and auditfully write it to the DB
    my $self = $class->new(%{$hash_ref});
    $self->audited_insert(@vals);
    return $self;

}

=head1 BUGS AND LIMITATIONS

=over

=item Does not call the save() triggers.

=item Audited inserts fill your table with mostly NULL rows.

=back

=head1 AUTHOR

  Clinton Wolfe clinton@omniti.com

=cut


1;
