package Class::ReluctantORM::Collection;
use strict;

=head1 NAME

Class::ReluctantORM::Collection - Represent a multirelational attribute

=head1 SYNOPSIS

  # See Class::ReluctantORM
  package Ship;
  Ship->build_class(...);
  Ship->has_many('Pirate');

  package main;
  my $ship = Ship->fetch_by_name('Lollipop');
  my $coll = $ship->pirates();

  # $coll hasn't been populated yet...
  @pirates = $coll->all_items(); # Throws 'FetchRequired' exception
  @pirates = $coll->fetch_all(); # Remembers results
  @pirates = $coll->all_items(); # no exception now

  # If you get ship differently, you can pre-populate the collection
  $ship = Ship->fetch_by_name_with_pirates('Lollipop');
  $coll = $ship->pirates();
  @pirates = $coll->all_items(); # no exception now
  @pirates = $ship->pirates->all(); # Same thing

  # Or try this:
  @search = $coll->search(where => 'where_clause'); 
  # Never remembers results or affects populated status

  # Here's counting:
  my $count = $coll->count(); # Throws 'FetchNeeded' exception unless populated
  my $count = $coll->fetch_count(); # Remembers count, but does not set populated flag

  # Add or delete individual items
  $coll->add($pirate);
  $coll->delete($pirate);

  # This tries to do a delete
  $coll->delete_all();
  $coll->delete_where(where => 'where clause', execargs => []);

  # This could be useful....
  if ($coll->is_populated()) { ... }
  $coll->depopulate();


=head1 DESCRIPTION

A simple container class for one-to-many and many-to-many relationships.

=cut

#=====================================================#
#             Public Virtual Methods
#=====================================================#

=head2 @items = $c->all_items();

=head2 @items = $c->all();

If the collection is already populated, returns an array of the items.

If the collection is not already populated, throws a FetchRequired
exception.

Aliased as all().

=cut

sub all_items { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }
sub all { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $o = $c->first();

Like all_items(), but only returns the first one.

WARNING: Collections are generally unordered, so the identity of the object returned is unreliable.  Use this method when you want _any_ object from the collection.

=cut

sub first {
    my $self = shift;
    if ($self->is_populated) {
        return $self->{_children}->[0];
    } else {
        Class::ReluctantORM::Exception::Data::FetchRequired->croak(called => 'first', call_instead => 'fetch_all', fetch_locations => [ $self->all_origin_traces ]);
    }
}

sub linking_object { return shift->{linking_object}; }

=head2 @items = $c->fetch_all();

Fetchs all the items represented by the collection from
the database and sets the populated flag to true.  Count is 
now also available.

=cut

sub fetch_all { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 @items = $c->fetch_deep(with => {...});

Fetchs all the items represented by the collection, along with any JOINs specified.
Sets the populated flag to true.  Count is now also available.

If no results are aobtained, this does NOT die.

=cut

sub fetch_deep { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }


=head2 $count = $c->count();

If the collection has been populated or fetch_count has been called, 
returns the integer count of items.

Otherwise, throws a FetchRequired exception.

=cut

sub count { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $count = $c->fetch_count();

If the collection has been populated, returns the existing count.

Otherwise, performs a SQL COUNT.  The result is stored for later 
calls to count().

=cut

sub fetch_count { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }


=head2 @items = $c->search(where => 'where clause', limit => '', order => '');

Performs a search on the child table, for record associated with 
the master record and also matching the given where clause 
fragment.  Results are never cached and do not 
affect the populated status.

Returns an empty list when there are no results.  In scalar acontext, returns
first result, or undef if no results.

=cut

sub search { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $bool = $c->is_present($object)

Returns a value indicating the presence of a candidate object among the collection.  The collection must be populated.

Actually returns the count of objects with the same primary key from the collection, so you can use this method to detect duplicates.

=cut

sub is_present {
    my ($self, $object) = @_;
    unless ($self->is_populated) {
        Class::ReluctantORM::Exception::Data::FetchRequired->croak(called => 'is_present', call_instead => 'fetch_all', fetch_locations => [ $self->linking_object->all_origin_traces ]);
    }
    $self->_check_correct_child_class($object);
    my $id = $object->id();
    return scalar grep {$_->id eq $id } @{$self->{_children}};
}

=head2 $c->add($object, [$ignore_dupe_errors)

Associates the given object (which must already exist in the database) with the collection.  If the collection is populated, the object is added to the list of objects in the collection, and the count is increased by one.  If the collection is not yet fetched, the collection will still not be populated after the add (because collections are always either completely fetched or completely unfetched).

Database changes, which happen regardless of populated status, depend on relationship type. For one-to-many relationships, this sets the foreign key in the child object to the primary key of the parent object. For many-to-many relations, this inserts a new row in the join table with the primary keys of both the left and right classes.

Adding a duplicate object is not an error, at least according to this module.  Your database may think otherwise.  If so, you may pass a boolean second parameter, which will then trap and ignore database errors that appear to be uniqueness constraint violations.

Query count: 1

=cut

sub add { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $c->delete($object);

Searches for the given object in the collection and deletes it if found.  The collection must be populated.  For one-to-many relationships, the child object is deleted outright.  For many-to-many relationships, all join table rows matching the two keys are deleted.

Note that for one-to-many relationships, the deletion of the child record may cause database errors if there are objects that depend on the child object (ie, grandchild objects).  You can use constraint actions, such as ON DELETE CASCADE or ON DELETE SET NULL to prevent such errors.

If the object is not found among the collection, no action is taken, and no exception is thrown.  If the object is found, the collection object is updated the new child list and count.

Query count: 1

=cut

sub delete { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $c->delete_all();

For one-to-many, deletes all child records associated with the master record.

For many-to-many, disassociates the child record from the master record (ie, it deletes rows from the join table).

=cut

sub delete_all { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $c->delete_where('where clause');

=head2 $c->delete_where(where => 'where clause', execargs => [1,2,3]);

For one-to-many, deletes all child records associated with the master record and 
matching the given where clause fragment.

For many-to-many, disassociates the child records from the master record 
where the clause matches.

=cut

sub delete_where { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $bool = is_populated();

Returns true if fetch_all has been called, or if the collection 
started life populated.

=cut

sub is_populated { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $c->depopulate();

Clears the populated flag, and flushes any cached results.

=cut

sub depopulate { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }


=head2 $result = $c->sum_of_FIELD();

=head2 $result = $c->max_of_FIELD();

=head2 $result = $c->min_of_FIELD();

=head2 $result = $c->count_of_FIELD();

Aggregate functions, like in Class::ReluctantORM.  You may also provide a where and execargs argument.  Note that your where clause will be modified to enforce the parent-child relationship.

=cut

sub AUTOLOAD {

    my $inv = shift;
    my $class = ref($inv) || $inv;
    my $self = ref($inv) ? $inv : undef;
    our $AUTOLOAD;

    # Never autoload DESTROY
    return if ($AUTOLOAD =~ /::DESTROY$/);

    # Strip classname from method
    my $method = $AUTOLOAD;
    my $re = $class . '::';
    $method =~ s/^$re//;

    #...........
    #  Agregrate autoloaded methods  (max_of_total)
    #...........
    my $re3 = '^(' . join('|', map { lc($_->name) } Class::ReluctantORM::SQL::Function->list_aggregate_functions() ) . ')';
    $re3 .= '_of_';
    my @field_names = $self->rel->linked_class->field_names;
    $re3 .= '(' . join('|', @field_names) . ')$';
    if ($method =~ /$re3/) {
        my ($agg_type, $field) = ($1, $2);
        return $self->__setup_aggregate_autoload($AUTOLOAD, $method, \@_, $agg_type, $field);
    }

    # Otherwise fail
    Class::ReluctantORM::Exception::Call::NoSuchMethod->croak("Could not find method $method in package $class");

}

#=====================================================#
#             Protected Virtual Methods
#=====================================================#

sub _new { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head1 AUTHOR

 Clinton Wolfe, with inspiration from Rob Speed, Chris Schammel, and Dave Hubbard.

=cut

1;
