package Class::ReluctantORM::SubClassByRow;

use strict;
use base 'Class::ReluctantORM';
our $DEBUG = 0;
use Data::Dumper;

use Class::ReluctantORM::SQL::Aliases;
use NEXT;
use Class::ReluctantORM::Utilities qw(conditional_load);

our %INITTED_SUBCLASSES = ();

=head1 NAME

Class::ReluctantORM::SubClassByRow - ORM base class for behavior inheritance

=head1 SYNOPSIS

  package ShipType;
  use base 'Class::ReluctantORM::SubClassByRow';
  __PACKAGE__->build_class(
    schema => 'highseas',
    table => 'ship_types',
    primary_key => 'ship_type_id',
    subclass_column => 'subclass_name',
    subclass_preload => 1,
  );

  Ship->has_one('ShipType');

  package ShipType::Galleon;
  sub set_sail { ... }

  package ShipType::Rowboat;
  sub set_sail { ... }

  package main;
  my $ship = Ship->fetch_deep(with => { ship_type => {} }, where => ...);
  my $st = $ship->ship_type();
  $st->isa('ShipType::Galleon');  # true if the ship_type->subclass_name eq Galleon
  $st->isa('ShipType');           # true
  $st->set_sail();                # polymorphic

=head1 DESCRIPTION

CRO usually treats rows as instances, and tables as classes.  But what if what you are storing classes in the tables?  This base module allows you to treat a table's rows as both classes and instances (an instance of a metaclass, basically).  This is most useful for "type tables" in which you want the behavior of an object to vary with its type.  In otherwords, this can provide for behavioral inheritance and polymorphism (by implementing methods in your specific subclasses).

TODO - compatible with CRO::Static?

=head1 INSTANTIATION BEHAVIOR

Whenever a ShipType object is fetched from the database, we examine the column named by the subclass_column to determine which subclass it should be (if the value of the column contains colons, it is used as-is; if no colons are present, the base class is prefixed (ie, 'Dread::Naught' => Dread::Naught, 'Galleon' => 'ShipType::Galleon'.  That subclass is loaded (if it hasn't been loaded already), we confirm that the subclass ISA ShipType (if not, we push ShipType to the front of its @ISA), an optional class initter is run (postload_init) and then the object is created and blessed as the specified subclass.

SubClassByRow also makes arrangements so that metadata requests on a subclass will be handled by the superclass.

=cut


=head1 CLASS CONFIGURATION

=head2 YourBaseClass->build_class(%options);

This class method sets up your classes.  In addition to the options permitted to Class::ReluctantORM->build_class, you may also use:

=over

=item subclass_column

Required string.  Column to examine when instantiating objects to determine class of new object.

=item subclass_preload

Optional boolean, default false.  If true, we'll try to load any modules under the directory where the superclass lives (ie, we'll try to load anything under ShipType/*.pm, ShipType/*/*.pm, etc; anything that then claims to be a ShipType will be registered).  If false, class loading will occur on an ad hoc basis as rows are returned and new subclass_column values are seen.

=back

=cut

sub build_class {
    my $class = shift;
    my %args = @_;

    my $subclass_column = $args{subclass_column} || 'subclass_column';
    delete $args{subclass_column};

    my $subclass_preload = $args{subclass_preload} || 'subclass_preload';
    delete $args{subclass_preload};

    $class->NEXT::DISTINCT::build_class(%args);

    unless (grep { $_ eq $subclass_column } $class->field_names()) {
        Class::ReluctantORM::Exception::Param::BadValue->croak
            (
             param => 'subclass_column',
             value => $subclass_column,
             error => "subclass_column must be a real column of " . $class->table_name,
            );
    }

    my $meta = $class->__metadata();
    $meta->{subclass_by_row}{column} = $subclass_column;
    $meta->{subclass_by_row}{base}   = $class;
}

=head2 my $thing = YourClass->new(%attributes);

Overrides the default CRO constructor.   Acts as a factory method - based on the value of the sublass_column attribute, determines the actual class to bless the object into.  Loads the class if neccesary.

=cut

sub new {
    my $super_class = shift;
    my $calling_package = (caller())[0];
    #print STDERR "Have class $class and calling package $calling_package\n";
    unless ($calling_package =~ /Class::ReluctantORM::/) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak('May only call SubClassByRow::new() from within Class::ReluctantORM');
    }

    my $obj = $super_class->NEXT::DISTINCT::new(@_);
    my $meta = $super_class->__metadata();

    # OK, should have a subclass_column attribute
    my $subclass = $obj->raw_field_value($meta->{subclass_by_row}{column});
    unless ($subclass) {
        Class::ReluctantORM::Exception::Param::Missing->croak
            (
             param => $meta->{subclass_by_row}{column},
             error => "Cannot instantiate a SubClassByRow object without a value for the subclass_column " . $meta->{subclass_by_row}{column} . " - row ID " . $obj->id
            );
    }

    if ($subclass !~ /::/) {
        $subclass = $super_class . '::' . $subclass;
    }

    unless ($INITTED_SUBCLASSES{$subclass}) {
        conditional_load($subclass);
        unless ($subclass->isa($super_class)) {
            Class::ReluctantORM::Exception::Param::BadValue->croak
                (
                 param => $meta->{subclass_by_row}{column},
                 error => "Cannot coerce a $super_class object into a $subclass object - $subclass is not ISA $super_class (row ID " . $obj->id . ')',
                 value => $subclass,
                );
        }
        Class::ReluctantORM->__alias_metadata($super_class, $subclass);
        $INITTED_SUBCLASSES{$subclass} = 1; # Do this before postload init, or else it will recurse if it contains a fetch
        $subclass->_postload_init();
    }

    bless $obj, $subclass;

    return $obj;
}

=head2 $subclass->_postload_init();

Optionally, you can define this method.  It will be called the first time the subclass is loaded, but is gaurenteed to be called after the class's metadata is established.  Default implementation is a no-op.

=cut

sub _postload_init { }

=head1 AUTHOR

Clinton Wolfe

=cut

1;
