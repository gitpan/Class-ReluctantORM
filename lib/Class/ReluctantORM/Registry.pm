package Class::ReluctantORM::Registry;
use strict;
use warnings;

=head1 NAME

  Class::ReluctantORM::Registry - Store CRO instances uniquely

=head1 SYNOPSIS


  # Setup a CRO class to use a Hash-based registry
  package MyCRO;
  use base 'Class::ReluctantORM';
  MyCRO->build_class(
                     ...
                     registry => 'Class::ReluctantORM::Registry::Hash',
                    );

  # Use the default registry class
  OtherCRO->build_class( ... );  # omit registry param

  # Change the default registry class
  Class::ReluctantORM::Registry->default_registry_class
     ('Class::ReluctantORM::Registry::Custom');

  # Disable registries
  Class::ReluctantORM::Registry->default_registry_class
     ('Class::ReluctantORM::Registry::None');

  # Work with a class's registry
  my $reg = MyCRO->registry();
  print "Have " . $reg->count() . " objects cached\n";

  $reg->walk(sub {
     my $obj = shift;
     ...
  });

  my $obj = $reg->fetch($id);
  $reg->store($obj);
  $reg->purge($id);
  $reg->purge_all();

=head1 DESCRIPTION

The Registry facility provides a way to ensure that objects loaded from the database are unique in memory.  Upon initial fetch from the database, each CRO object is stored in its class's Registry.  Subsequent fetches will result in the first object being returned.

This class provides a generalized interface for Registries.  Specific subclasses provide specific implementations, each of which has strengths and weaknesses.  A do-nothing Registry is provided as well, Registry::None . 

Each CRO class may choose its registry implementation by naming a Registry subclass using the 'registry' parameter to build_class.  If the parameter is not provided, the class named by Class::ReluctantORM::Registry->default_registry_class() will be used.  You may change this default by passing a value.

=head1 NOTES ON IMPLEMENTING REGISTRIES

You may also write your own Registry classes.  Simply subclass from this class, and implement the methods listed under METHODS FOR SUBCLASSES TO IMPLEMENT.

You are free to extend the Registry API (for example, with expirations or size or count limits).

You must use weak references to track objects.  Otherwise this will leak memory badly.  See Scalar::Util::weaken .

=head1 METHODS PROVIDED BY THIS SUPERCLASS

=cut

#our $DEFAULT_REGISTRY_CLASS = 'Class::ReluctantORM::Registry::None';
our $DEFAULT_REGISTRY_CLASS = 'Class::ReluctantORM::Registry::Hash';
use base 'Class::Accessor';
use Class::ReluctantORM::Utilities qw(conditional_load_subdir);
use Class::ReluctantORM::Exception;

BEGIN {
    conditional_load_subdir(__PACKAGE__);
};


=head2 $reg = RegClass->new($target_class);

A basic constructor is provided.  It sets the target class and blesses a hashref.

=cut

sub new {
    my $reg_class = shift;
    my $tgt_class = shift;
    my $self = bless {}, $reg_class;
    $self->set('target_class', $tgt_class);
    return $self;
}

=head2 $reg_class = Class::ReluctantORM::Registry->default_registry_class();

=head2 Class::ReluctantORM::Registry->default_registry_class($new_default);

Reads or sets the defualt Registry class used when no 'registry' param is passed to build_class().

When setting, the class passed must be a subclass of Class::ReluctantORM::Registry.

=cut

sub default_registry_class {
    my $inv = shift; # ignore
    if (@_) {
        # setting
        my $new_default = shift;
        unless ($new_default->isa('Class::ReluctantORM::Registry')) {
            Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'default_registry', error => 'Registry class must inherit from Class::ReluctantORM::Registry', value => $new_default);
        }
        $DEFAULT_REGISTRY_CLASS = $new_default;
    }
    return $DEFAULT_REGISTRY_CLASS;
}


=head2 $class = $registry->target_class();

Returns the CRO class for which this Registry is caching objects.

=cut

__PACKAGE__->mk_ro_accessors('target_class');


=head1 METHODS FOR SUBCLASSES TO IMPLEMENT

=cut

=head2 $obj = $registry->fetch($id);

Looks for an object previously stored with composite ID $id, and if found, returns it.

If not found, returns undef.

=cut

sub fetch {  Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }


=head2 $registry->store($obj);

Stores an object in the registry, using its id() as the key.  If the object already existed, it is replaced.

=cut

sub store {  Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }


=head2 $registry->purge($id);

Removes an object from the registry.  This doesn't invalidate any other references to the object, but subsequent fetch()s will return undef until an objecvt with the same ID is stored again.

=cut

sub purge {  Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $registry->purge_all();

Removes all objects from the registry.

=cut

sub purge_all {  Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }


=head2 $int = $registry->count();

Returns an integer count of the number of objects currently tracked by the registry.

=cut

sub count {  Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }


=head2 $registry->walk($coderef);

Fetches each stored object and calls the provided coderef, sending the object as the only argument.

=cut

sub walk {  Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head1 AUTHOR

 Clinton Wolfe clwolfe@cpan.org January 2010

=cut

1;
