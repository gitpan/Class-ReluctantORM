package Class::ReluctantORM::Filter;
use strict;
use warnings;

use base 'Class::Accessor';

=head1 NAME

Class::ReluctantORM::Filter - Apply a transformation when reading or writing to a Tablebacked object\'s fields.

=head1 SYNOPSIS

 # In SomeTBClass.pm

 # Build class as usual
 SomeTBClass->build_class(...);

 # Attach a class-wide filter
 SomeTBClass->attach_class_filter(
                            class => 'SomeFilterClass',
                            fields => [ qw(defaults to all) ],
                           );
 # Can stack filters.
 SomeTBClass->attach_class_filter( ... );


 # In application code...

 # Can also attach to one object, instead of class-wide
 # Any of these actions on an object will cause it to copy
 # in the class-level filter list and maintain a separate copy on its own
 my $thing = TBThing->fetch(23);
 $thing->append_filter(
                       class => 'SomeFilterClass',
                       fields => [ qw(defaults to all) ],
                      );
 $thing->append_filter( ... ); # May stack object filters, too
 $thing->set_filters(classes => [], fields => []);
 $thing->remove_filter(class => 'NoGoodFilter', fields => []);
 $thing->clear_filters(fields => []);

 # Can also attach at fetch time
 my @thangs = TBThang->fetch_deep(
                                  mood => 'happy',
                                  with => { other_stuff => {}},
                                  append_filter => {
                                                    class => 'SomeFilterClass',
                                                    fields => [ ... ],
                                                   },
                                  # Or set_filters, remove_filter, or clear_filters
                                 );

 # Using filters

 # If a filter is attached, accessors now read a 
 # raw value from the DB, then pass them through the filter(s) before returning the value.
 my $filtered_value = $thing->field();

 # But you can still get at the raw value if you want 
 # (and you know it's being filtered)
 my $raw = $thing->raw_field_value($field);

 # Filter-on-write
 $thing->field($value_that_will_get_transformed);
 $thing->raw_field_value($field, $value);

=head1 DESCRIPTION

This is the base class for a Filter.  It provides an identity (no-op) transformation on all fields.

=head1 FILTERS AND DIRTINESS

Reading a field value through a filter does not change the dirty state of the object, because you are not altering the internal value of the field in the object - only the value that is being returned to you.

Writing a field value through a filter does make the object dirty for that field, as you ARE affecting the internal value for the field.

=head1 STACKING FILTERS

Filters may be stacked by calling attach_class_filter more than once on a class, or calling append_filter on an object. Given:

  TBSubClass->attach_class_filter(class => 'PaintItBlue');
  TBSubClass->attach_class_filter(class => 'SmearWithGlue');

A value being READ will be transformed as follows:

=over

=item 1

The raw value will be plucked form the object.

=item 2

The value will be painted blue.

=item 3

The value will be smeared with glue.

=item 4

The blue, gluey value will be returned to the caller.

=back

A value being WRITTEN will be transformed in reverse order:

=over

=item 1

The value provided by the caller will be smeared with glue.

=item 2

The value will be painted blue.

=item 3

The gluey, blue object will be stored in the object as the internal value.  Upon saving, the gluey, blue value will go in the database.

=back

Note that the implementation of SmearWithGlue may actually strip off the layer of glue when writing.  Read the documentation of your chosen filter to determine its behavior on reading vs writing; some only act in one direction, while others act in both directions.

Stacking is per-field.

=head1 CLASS vs OBJECT FILTERS

A filter attached at the class level (by calling SomeTBSubClass->attach_class_filter()) will be attached to every instance of the object, no matter how it is fetched or created.  This can be useful for security applications, or when you want to implement a logging facility.

A filter attached to an object is only attached to that object.  Other objects of the same class will not have the filters.

If an object already has class filter(s) and you append, remove, set or clear an object filter, the object makes a copy of all the class filters, then maintains its own private list of filters.  THIS MEANS THE OBJECT WILL NOT BE NOTIFIED OF NEW CLASS FILTERS.  This generally isn't a problem, since class filters are generally applied early.

=head1 CRO METHODS TO MANIPULATE FILTER LISTS

=head2 $class->attach_class_filter(class => FilterClass, fields => \@fields)

Attaches a FilterClass filter to all current and future instances of the CRO class $class.  Does not affect any per-object filters that may be in place.

If the FilterClass has not been loaded, loads it.

The 'fields' parameter is optional, and defaults to a list of all non-PK fields on the class.  You can use this to limit which fields the filter applies to.

=head2 $obj->append_filter(class => FilterClass, fields => \@fields)

Like attach_class_filter, but operates on a per-object basis.  Any class filters are copied to the specific object first.

=head2 $obj->set_filters(classes => \@filters, fields => \@fields)

Sets the object's filter list to be the given set of filters (in the given order) on the given fields.  Here, fields defaults to an empty list - so you can use this to clear filters.

=head2 $obj->clear_filters(fields => \@fields)

Clears filters on the object.  Field list is optional and defaults to all fields, including PKs.

=head2 $obj->remove_filter(class => FilterClass, fields => \@fields)

Removes all filters of the named class on the named fields.  Fields is optional and defaults to all fields.

=head2 @filters = $obj->read_filters_on_field('field')

Returns the list of filters in the order used when reading a field.  If the object has object filters, that list is returned; otherwise, the class list is returned.

=head2 @filters = $obj->write_filters_on_field('field')

Returns the reverse of read_filters_on_field.

=cut

=head1 IMPLEMENTING A FILTER

Unless you are writing a filter, you can stop reading this.

Filters are Perl classes.  They are expected to have two methods, apply_read_filter and apply_write_filter.  If you inherit from Class::ReluctantORM::Filter, you will get default do-nothing implementations of these methods.

As currently used, your filter will not be actually instantiated; this may change in the future.

=head2 $new_value = YourFilter->apply_read_filter($raw_value, $object, $field);

This method should perform whatever transformations are needed to transform $raw_value into $new_value, and return it.  $object and $field are provided in case you want them, though most filters will not need them.  $object is the Class::ReluctantORM object, and $field is the name of the field being transformed.

Note that $raw_value may be undef or an object.

It is possible, but extremely rude, to affect the state of $object.  You should just return the new value; remember, your filter may be chained.

=cut

# Do-nothing version - just return the raw value unaltered
sub apply_read_filter { return $_[1]; };


=head2 $new_value = YourFilter->apply_write_filter($raw_value, $object, $field);

This method should perform whatever transformations are needed to transform $raw_value into $new_value, and return it.  $object and $field are provided in case you want them, though most filters will not need them.  $object is the Class::ReluctantORM object, and $field is the name of the field being transformed.

Note that $raw_value may be undef or an object.

It is possible, but extremely rude, to affect the state of $object.  Do not save the new value in the object (that is handled for you).    You should just return the new value; remember, your filter may be chained.

=cut

# Do-nothing version - just return the raw value unaltered
sub apply_write_filter { return $_[1]; };


1;
