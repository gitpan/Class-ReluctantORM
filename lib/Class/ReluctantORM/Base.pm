=head1 NAME

  Class::ReluctantORM::Base - Class building helper

=head1 SYNOPSIS

  package Person;
  use base 'Class::ReluctantORM::Base';

  __PACKAGE__->build_class(
    fields => [ qw(person_id name birthdate) ],
    ro_fields => [ qw(person_id) ],

    # See Class::ReluctantORM
  );

  # Now you have...
  package main;
  my $p = Person->new(name => 'SuchAndSuch', birthdate => '2008-01-12');
  print $p->name . " was born on " . $p->birthdate() . "\n";
  $p->name('WhatsTheirFace');

  $p->person_id(123); # Kaboom, person_id is readonly

=head1 DESCRIPTION

Provides class-building facilities for Class::ReluctantORM, including
defining accessors and mutators.

=head1 SEE ALSO

Class::ReluctantORM, which leverages this class heavily.

=head1 PUBLIC CLASS METHODS

=cut

package Class::ReluctantORM::Base;
use strict;
use Carp;
use Class::ReluctantORM::Exception;
use base 'Class::Accessor';

our $DEBUG = 0;

=head2 $class->build_class(%args);

Sets up $class to have the accessors and mutators given.

Extra parameters are ignored.

=over 4

=item fields

An array ref of field names, which will be used to create accessors, 
and if not listed in ro_fields, also mutators.

=item ro_fields

An array ref of field names, which will be restricted to be read-only.

=back

=cut

sub build_class {
    my $class = shift;

    my $meta = $class->__metadata();

    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;

    my $fields = $args{fields};
    unless ($fields) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'fields'); }
    unless (ref($fields) eq 'ARRAY') { Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak(param => 'fields'); }
    unless (@$fields) { Class::ReluctantORM::Exception::Param::Empty->croak(param => 'fields'); }

    my %mutability_by_field = map { $_ => 1 } @$fields;
    unless ( keys (%mutability_by_field) == @$fields) { Class::ReluctantORM::Exception::Param::Duplicate->croak(param => 'fields', value => join ' ', @$fields); }

    foreach my $f (@{$args{ro_fields} || []}) {
        unless (exists $mutability_by_field{$f}) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'ro_fields', value => $f); }
        $mutability_by_field{$f} = 0;
    }

    # Create accessors
    $class->mk_accessors(grep {$mutability_by_field{$_} == 1} keys %mutability_by_field);
    $class->mk_ro_accessors(grep {$mutability_by_field{$_} == 0} keys %mutability_by_field);

    $meta->{fields} = $fields;
}

# Allow passing a single hashref or a hash
sub new {
    my $class = shift;

    my $hash_ref = {};
    if (@_ == 1) {
        $hash_ref = shift;
        unless (ref($hash_ref) eq 'HASH') { Class::ReluctantORM::Exception::Param::ExpectedHashRef->croak(); }
    } elsif (@_ % 2) {
        Class::ReluctantORM::Exception::Param::ExpectedHash->croak();
    } else {
        $hash_ref = { @_ };
    }

    return $class->SUPER::new($hash_ref);

}

# Override this so that we throw Class::ReluctantORM::Exceptions
sub make_ro_accessor {
    my($class, $field) = @_;

    return sub {
        my $self = shift;

        if (@_) {
            Class::ReluctantORM::Exception::Call::NotMutator->croak(attribute => $field);
        } else {
            return $self->get($field);
        }
    };
}


=head2 @field_names = $class->field_names();

Returns a list of the field names for the given class.

=cut

# Used to be protected
sub field_names { return shift->_field_names(); }

sub _field_names {
    my $inv = shift;
    my $class = ref($inv) || $inv;
    return @{$class->__metadata()->{fields} || []};
}

=head1 AUTHOR

  Clinton Wolfe

=cut

1;
