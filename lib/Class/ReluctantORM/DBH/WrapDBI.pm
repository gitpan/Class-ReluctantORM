package Class::ReluctantORM::DBH::WrapDBI;

use warnings;
use strict;
use Scalar::Util qw(blessed);

use base 'Class::ReluctantORM::DBH';
use base 'Class::Accessor';

=head1 NAME 

Class::ReluctantORM::DBH::WrapDBI - Wrap a DBI database handle

=head1 DESCRIPTION

Thin wrapper around a DBI dbh to match Class::ReluctantORM::DBH's interface.

=cut

__PACKAGE__->mk_accessors(qw(dbi_dbh));

sub new {
    my $class = shift;
    my $dbi_dbh = shift;

    # Check class?
    my $self = bless {}, $class;
    $self->dbi_dbh($dbi_dbh);

    return $self;
}

sub get_info {
    my $self = shift;
    return $self->dbi_dbh->get_info(@_);
}

sub set_handle_error {
    my $self = shift;
    my $coderef = shift;
    $self->dbi_dbh->{HandleError} = $coderef;
}

sub prepare {
    my $self = shift;
    return $self->dbi_dbh->prepare(@_);
}

sub column_info {
    my $self = shift;
    return $self->dbi_dbh->column_info(@_);
}

1;
