package Class::ReluctantORM::Monitor::Timer;

=head1 NAME

Class::ReluctantORM::Monitor::Timer - Track running time of queries

=head1 SYNOPSIS

  use aliased 'Class::ReluctantORM::Monitor::Timer';
  my $mon = Timer->new(highwater_count => N, fatal_threshold => X);
  Class::ReluctantORM->install_global_monitor($mon);
  Pirate->install_class_monitor($mon);

  # Do a query....
  Pirate->fetch(...);

  # Read from the monitor - decimal seconds
  my $count = $mon->last_measured_value();

=head1 DESCRIPTION

A monitor that watches the amount of wall time used to execute a query.

This is a Measuring Monitor.

=cut

use strict;
use warnings;
use base 'Class::ReluctantORM::Monitor::Measuring';

use Time::HiRes qw();

sub permitted_events  { return qw(execute_finish); }
sub default_events    { return qw(execute_finish); }
sub measurement_label { return 'Wall Time for query execute()'; }
sub take_measurement {
    my ($mon, %event) = @_;
    return Time::HiRes::time() - $mon->{_start};
}

sub notify_execute_begin    {
    my $self = shift;
    $self->reset();
    $self->{_start} = Time::HiRes::time();
}

1;
