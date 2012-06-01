package Class::ReluctantORM::Relationship;

=head1 NAME

Class::ReluctantORM::Relationship - Represent links between classes

=head1 SYNOPSIS

  # Add relationships to a Class::ReluctantORM Class
  Pirate->has_one(...); # See Class::ReluctantORM::Relationship::HasOne
  Pirate->has_many(...); # See Class::ReluctantORM::Relationship::HasMany
  Pirate->has_many_many(...); # See Class::ReluctantORM::Relationship::HasManyMany
  Pirate->has_lazy(...); # See Class::ReluctantORM::Relationship::HasLazy

  # Get relationships from a defined Class::ReluctantORM Class
  $rel = Pirate->relationships('method_name');
  $rels_by_name_href = Pirate->relationships();
  @all_rels     = Pirate->relationships();

  # Get information from a relationship
  $str = $rel->type();                 # 'has_one'
  $str = $rel->linked_class();         # 'Ship'
  $str = $rel->linking_class();        # 'Pirate'
  $str = $rel->method_name();          # 'ship'
  $str = $rel->name();                 # 'ship' (alias for method_name)
  $int = $rel->lower_multiplicity()    # 0 for optionals, 1 for required
  $int = $rel->upper_multiplicity()    # 1 for one/lazy, undef for many/many-many

  # If Ship->has_many(Pirate), you'll get the opposite relation here
  # There's no requirement that relationships be invertable, so this is often undef
  $invrel = $rel->inverse_relationship();

  @fields = $rel->local_key_fields();  # fields in Pirate that link to Ship
  @fields = $rel->remote_key_fields(); # array of fields in Ship that link to Pirate
  $int = $rel->join_depth();           # 0, 1, or 2

  # SQL Support
  $tbl = $rel->local_sql_table();
  $tbl = $rel->remote_sql_table();
  $tbl = $rel->join_sql_table();
  @cols = $rel->local_key_sql_columns();
  @cols = $rel->remote_key_sql_columns();
  @cols = $rel->join_local_key_sql_columns();
  @cols = $rel->join_remote_key_sql_columns();
  @cols = $rel->additional_output_sql_columns();

=head1 DESCRIPTION

Represents a relationship between two Class::ReluctantORM classes.

TB Classes have instances of Relationships as class data.  An instance of a 
Relationship does not contain data pertaining to a particular TB object;
for that, see Collection.

=head1 INITIALIZATION

=cut


use strict;
use warnings;

use Data::Dumper;
use Class::ReluctantORM::Utilities qw(conditional_load_subdir install_method install_method_generator);
use base 'Class::Accessor';
use Class::ReluctantORM::SQL::Aliases;

our $DEBUG = 0;

our %PENDING_CODE_FOR_CLASSES;
our $OCTR_IS_LOADING;
our @REL_CLASSES;



BEGIN {
    unless ($OCTR_IS_LOADING) {
        $OCTR_IS_LOADING = 1;
        @REL_CLASSES = conditional_load_subdir(__PACKAGE__);
    }
}

foreach my $class (@REL_CLASSES) {
    if ($DEBUG > 1) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- calling _initialize on $class\n"; }
    $class->_initialize();
}

=head2 $rel_class->_initialize();

The relationship class should do any one-time setup, like registering
methods with Class::ReluctantORM.  Note that this is per-relationship-class
initialization, not per relationship initialization.

The default implementation does nothing.

=cut

sub _initialize { }


# This called from new to initialize relations.  The parent will not have PKs at this time.
sub _handle_implicit_new { Class::ReluctantORM::Exception::Call::PureVirtual->croak('_handle_implicit_new'); }

# This called from create to initialize relations, after the new() and insert() call.  The parent will have PKs at this time.
sub _handle_implicit_create { Class::ReluctantORM::Exception::Call::PureVirtual->croak('_handle_implicit_create'); }

# This called from insert() when a primary key changes on the linking object
sub _notify_key_change_on_linking_object { Class::ReluctantORM::Exception::Call::PureVirtual->croak('_notify_key_change_on_linking_object'); }

=head1 DELAYED LOADING FACILITY

Because Class::ReluctantORM classes are naturally interdependent, it's unlikely that a
relationship will always be able to complete its setup, because the remote end
may not be loaded yet.  The Relationship base class provides a facility for
the delayed execution of setup code.

=cut

=head2 Class::ReluctantORM::Relationship->notify_class_available($tb_class);

Notifies the delayed-loading subsystem that a Class::ReluctantORM class has become available.
At this point, any relationships that were waiting on this class will finish their setup.

You should not override this method.

=cut

sub notify_class_available {
    my $class = shift;
    my $tb_class = shift;

    if ($DEBUG > 1) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - got notification that $tb_class is available\n";  }

    # If there is anything waiting on this class, execute it.
    foreach my $code (@{$PENDING_CODE_FOR_CLASSES{$tb_class} || []}) {
        $code->();
    }
}

=head2 $rel_class->delay_until_class_is_available($tb_class, $coderef);

Registers a coderef to be executed later when the given Class::ReluctantORM
class is loaded.

If the requested class has already been loaded, the code is executed immediately.

=cut

sub delay_until_class_is_available {
    my $class = shift;
    my $tb_class = shift;
    my $coderef = shift;
    if ($DEBUG > 1) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - delay_until_class_available considering code at $coderef\n";  }
    if (Class::ReluctantORM->is_class_available($tb_class)) {
        if ($DEBUG > 1) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - delay_until_class_available doing immediate execution for class $tb_class\n";  }
        $coderef->();
    } else {
        if ($DEBUG > 1) { print STDERR __PACKAGE__ . ':' . __LINE__ . " - delay_until_class_available doing delayed execution for class $tb_class\n";  }
        $PENDING_CODE_FOR_CLASSES{$tb_class} ||= [];
        push @{$PENDING_CODE_FOR_CLASSES{$tb_class}}, $coderef;
    }
}


sub new {
    my $relclass = shift;
    return bless {}, $relclass;
}

=head1 ATTRIBUTES OF RELATIONSHIPS

=cut

=head2 $str = $rel->type();

Returns the type of the relationship - 'has_one', 'has_many', etc.

=cut

sub type { Class::ReluctantORM::Exception::Call::PureVirtual->croak('type'); }

=for devdocs

=head2 $method_name = RelationshipClass->_setup_method_name();

Returns the name of a method you can call to set up a relationship.  Default implementation is to just return the string returned by type().

=cut

sub _setup_method_name { return $_[0]->type(); }

=for devdocs

=head2 $hashref = $rel->_original_args_hashref;

Returns a hashref of (possibly scrubbed) arguments passed to the setup method to initiate the relationship.  You should set this value whenever a new relationship is created.  This is used by CRO->clone_relationship().

=cut

__PACKAGE__->mk_accessors(qw(_original_args_arrayref));

=head2 $str = $rel->method_name();

=head2 $str = $rel->name();

The method that this relationship will add to the linking class (eg, $pirate->ship()).  As this is unique on the class, this is
also used as the name of the relationship.

=cut

__PACKAGE__->mk_accessors(qw(method_name));
sub name { return shift->method_name(); } # Alias

=head2 $str = $rel->linking_class();

The class that initiated the relationship.  This is the "parent" class.

=cut


=head2 $str = $rel->linked_class();

The string name of the class on the far end of the connection.  The "child" class.  For HasLazy, 
this may not be a Class::ReluctantORM subclass; it may even just be SCALAR.

=cut

__PACKAGE__->mk_accessors(qw(linked_class));

=head2 $str = $rel->linking_class();

The class that initiated the relationship.  This is the "parent" class.

=cut

__PACKAGE__->mk_accessors(qw(linking_class));

=head2 $int = $rel->join_depth();

Count of how many joins are required by this relationship in a SQL query.  May range from 0 (lazy) to 2 (has_many_many).

=cut

__PACKAGE__->mk_accessors(qw(join_depth));

=head2 $int = $rel->lower_multiplicity()

Returns the lower bound on the multiplicty of the remote end relationship (ie, the "0" in "one to 0 or 1", or the "1" in "one to 1 or more").

=cut

sub lower_multiplicity { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $int = $rel->upper_multiplicity()

Returns the upper bound on the multiplicty of the remote end of the relationship (ie, the "1" in "one to 0 or 1", or the "more" in "one to 1 or more").

undef is used to represent "no limit".

=cut

sub upper_multiplicity { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $invrel = $rel->inverse_relationship();

Returns the inverse Relationship, if any.  If you have defined Pirate->has_one(Ship) and Ship->has_many(Pirate), 
then you can use this method to obtain the inverse relationship.  Note that you must manually make relationships bidirectional.

Inverse relationships are available by default whenever you have defined exactly one set of bidirectional relations between two classes.
If you have multiple relations between classes, you can use the 'inverse' option to the relationship setup method to specifiy one.

See Class::ReluctantORM::Manual::Relationships for more details.

=cut

__PACKAGE__->mk_accessors(qw(inverse_relationship));

=head2 @fields = $rel->local_key_fields();

Returns an array of the field names used on the linking (local) end of the relationship.

=cut

sub local_key_fields {
    my $rel = shift;
    if (@_) {
        $rel->set('local_key_fields', [@_]);
    }
    my $fields = $rel->get('local_key_fields');
    return @{$fields || []};
}

=head2 @column_names = $rel->local_key_columns();

Returns an array of the column names used on the linking (local) end of the relationship.

=cut


sub local_key_columns {
    my $rel = shift;
    my $class = $rel->linking_class();
    return map { $class->column_name($_) } $rel->local_key_fields();
}

=head2 @column_names = $rel->rmeote_key_columns();

Returns an array of the column names used on the remote (linked) end of the relationship.

=cut

sub remote_key_columns {
    my $rel = shift;
    my $class = $rel->linked_class();
    return map { $class->column_name($_) } $rel->remote_key_fields();
}

=head2 @fields = $rel->remote_key_fields();

Returns an array of the field names used on the remote (linked) end of the relationship.

=cut

sub remote_key_fields {
    my $rel = shift;
    if (@_) {
        $rel->set('remote_key_fields', [@_]);
    }
    my $fields = $rel->get('remote_key_fields');
    return @{$fields || []};
}

=head1 SQL SUPPORT

These functions provide support for the abstract SQL query representation system.

=cut

=head2 $int = $rel->join_depth();

Returns the number of join steps needed to perform a fetch deep inbolving this 
relationship.  Will return 0 (for Lazy relationships), 1 (for has_one and has_many)
or 2 (for has_many_many).

=cut

# sub defined by mk_accessor

=head2 $tbl = $rel->local_sql_table();

Returns a Class::ReluctantORM::SQL::Table object representing the local table.

This is always available.

=cut

sub local_sql_table {
    my $rel = shift;
    return Table->new($rel->linking_class);
}

=head2 $tbl = $rel->remote_sql_table();

Returns a Class::ReluctantORM::SQL::Table object representing the table of the linked class.

This is only available if join_depth is 1 or greater.

=cut

sub remote_sql_table {
    my $rel = shift;
    my $class = $rel->linked_class();
    unless ($class) { return undef; }
    return Table->new($class);
}

=head2 $tbl = $rel->join_sql_table();

Returns a Class::ReluctantORM::SQL::Table object representing the join table.

This is only available if join_depth is 2.

The default implementation always returns undef.

=cut

sub join_sql_table { return undef; }

=head2 $type = $rel->join_type();

Returns the join type for the (first) join required by this relationship, if any.

Default returns 'NONE'.

=cut

sub join_type { return 'NONE'; }

=head2 @cols = $rel->local_key_sql_columns();

Returns a list of Class::ReluctantORM::SQL::Columns involved in the relationship on the local table.

Always available.

=cut

sub local_key_sql_columns {
    my $rel = shift;
    my $table = $rel->local_sql_table();
    my @cols = map {
        Column->new(
                    column => $_,
                    table => $table,
                   );
    } $rel->linking_class->column_name($rel->local_key_fields());
    return @cols;
}

=head2 @cols = $rel->remote_key_sql_columns();

Returns a list of Class::ReluctantORM::SQL::Columns involved in the relationship on the remote table.

Available if join_depth is greater than 1.  If join depth is 2, refers to the farthest columns.

If not available, returns an empty list.

=cut

sub remote_key_sql_columns {
    my $rel = shift;
    my $table = $rel->remote_sql_table();
    unless ($table) { return (); }
    my @cols = map {
        Column->new(
                    column => $_,
                    table => $table,
                   );
    } $rel->linked_class->column_name($rel->remote_key_fields());
    return @cols;
}

=head2 @cols = $rel->join_local_key_sql_columns();

Returns a list of Class::ReluctantORM::SQL::Columns involved in the relationship 
on the join table for the linking class.

Available if join depth is 2.

If not available, returns an empty list.

The default implementation returns an empty list.

=cut
sub join_local_key_columns { return (); }
sub join_local_key_sql_columns { return (); }

=head2 @cols = $rel->join_remote_key_sql_columns();

Returns a list of Class::ReluctantORM::SQL::Columns involved in the relationship 
on the join table for the linked class.

Available if join depth is 2.

If not available, returns an empty list.

The default implementation returns an empty list.

=cut

sub join_remote_key_columns { return (); }
sub join_remote_key_sql_columns { return (); }

=head2 @cols = $rel->additional_output_sql_columns();

Returns a list of columns that should also be selected when fetching items that make up this relationship.

Default implementation is to return nothing.

=cut

sub additional_output_sql_columns { return (); }

=head2 $int = $rel->matches_join_criterion($crit);

Given a SQL Criterion, returns an integer indicating which, if any, of the join levels the criterion could be used to represent.  This is used to support SQL annotation.

The return value will be between 0 and $rel->join_depth(), inclusive.  If there is no match, the return value will be 0.

=cut

sub matches_join_criterion {
    my $rel = shift;
    my $crit = shift;

    for my $level (1..($rel->join_depth())) {
        my $rel_crit = $rel->default_sql_join_criteria($level);
        if ($rel_crit->is_equivalent($crit)) {
            return $level;
        }
    }

    return 0;

}

=begin devdocs

This isn't publicly documented yet, because it might be a bad idea.

=head2 $crit = $rel->default_sql_join_criteria($level);

Returns a Criterion that could be used to represent the Join criteria for the relationship.  $level must be an integer less than or equal to $rel->join_depth().

We can't use this for fetch_deep processing, because it doesn't take into account extra join options/criteria.

=cut

sub default_sql_join_criteria {
    my $rel = shift;
    my $level = shift;
    if ($level > $rel->join_depth()) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'level', value => $level, error => "Max value is " . $rel->join_depth());
    }

    my (@left_cols, @right_cols);

    if (0) { # for formatting
    } elsif ($rel->join_depth == 1 && $level == 1) {
        # Local directly to remote
        @left_cols = $rel->local_key_sql_columns();
        @right_cols = $rel->remote_key_sql_columns();

    } elsif ($rel->join_depth == 2 && $level == 1) {
        # Local to join
        @left_cols = $rel->local_key_sql_columns();
        @right_cols = $rel->join_local_key_sql_columns();

    } elsif ($rel->join_depth == 2 && $level == 2) {
        # Join to remote
        @left_cols = $rel->join_remote_key_sql_columns();
        @right_cols = $rel->remote_key_sql_columns();

    } else {
        Class::ReluctantORM::Exception::NotImplemented->croak("Don't know how to handle relationships with more than 2 join levels");
    }

    # Build criteria pair-wise
    my $crit;
    for my $i (0..$#left_cols) {
        my $new_crit = Criterion->new('=', $left_cols[$i], $right_cols[$i]);

        if ($crit) {
            $crit = Criterion->new('AND', $crit, $new_crit);
        } else {
            $crit = $new_crit;
        }
    }
    return $crit;
}



=head1 OTHER RELATIONSHIP METHODS

=cut

=head2 $bool =  $rel->is_populated_in_object($cro_obj);

Returns true if the relationship is "populated" (fetched) in the given ReluctantORM object.

=cut

sub is_populated_in_object { Class::ReluctantORM::Exception::Call::PureVirtual->croak('is_populated_in_object'); }

=begin devdocs

=head2 $bool =  $rel->_mark_unpopulated_in_object($cro_obj);

Should be called when the Class::ReluctantORM object needs to mark the relationship unfetched (for example, a local key has changed).

=cut

sub _mark_unpopulated_in_object { Class::ReluctantORM::Exception::Call::PureVirtual->croak('_mark_unpopulated_in_object'); }

=begin devdocs

=head2 $rel->_merge_children($cro_obj, $children_ref);

Called when the children specified in $children_ref (an array ref) should be merged into the existing collection for this relationship in $cro_obj.  This can happen in Class::ReluctantORM::new when an object has already been fetched with this relationship and has been found in the Registry, but the call to new specifies child objects as well.

=cut

sub _merge_children { Class::ReluctantORM::Exception::Call::PureVirtual->croak('_merge_children'); }

=begin devdocs

=head2 $rawval =  $rel->_raw_mutator($cro_obj);

=head2 $newval =  $rel->_raw_mutator($cro_obj, $new_value);

Performs a "raw" (non-filtered) access or write to the underlying collection of the CRO object.

=cut

sub _raw_mutator { Class::ReluctantORM::Exception::Call::PureVirtual->croak('raw_mutator'); }



sub _install_search_by_with_methods {
    my $rel = shift;
    my $class = $rel->linking_class();
    my $rel_name = $rel->name();

    install_method_generator
      (
       $class,
       sub {
           my ($class, $proposed_method_name) = @_;

           # Look for search_with_pirates pattern
           my ($fetch_mode, $found_rel_name) = $proposed_method_name =~ /^(search|fetch)_with_($rel_name)$/;
           if ($fetch_mode) {
               my $make_fatal = $fetch_mode eq 'fetch';
               my $base_key_spec
                 = ($class->primary_key_column_count == 1) ?
                   ($class->primary_key_columns())[0] :
                     [ $class->primary_key_columns() ];
               return $class->_make_fetcher($base_key_spec, $make_fatal, $rel_name)
           }

           # Look for search_by_name_with_pirates pattern
           my $regex = '^(search|fetch)_by_(' . join('|', $class->fields) . ')_with_(' . $rel_name . ')$';
           my ($fetch_mode2, $field_name, $found_rel_name2) = $proposed_method_name =~ $regex;
           if ($fetch_mode2) {
               my $make_fatal = $fetch_mode2 eq 'fetch';
               return $class->_make_fetcher($field_name, $make_fatal, $rel_name)
           }

           # No patterns left - decline
           return undef;
       }
      );
}


=head1 AUTHOR

Clinton Wolfe

=cut

1;
