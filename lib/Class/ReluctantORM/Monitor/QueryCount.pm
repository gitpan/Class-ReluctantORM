package Class::ReluctantORM::Monitor::QueryCount;

=head1 NAME

Class::ReluctantORM::Monitor::QueryCount - Ongoing count of queries

=head1 SYNOPSIS

  use aliased 'Class::ReluctantORM::Monitor::QueryCount';
  my $mon = QueryCount->new();
  Class::ReluctantORM->install_global_monitor($mon);
  Pirate->install_class_monitor($mon);

  # Do a query.... logging and highwater scorekeeping happens
  Pirate->fetch(...);

  # Read from the monitor - should increase by 1 or more with each DB operation
  my $count = $mon->last_measured_value();

  # Reset counter to 0
  $mon->reset();


=head1 DESCRIPTION

Simply counts the number of times the database is queried.  This value may not increase (due to caching by a Registry or Static, a query may not be needed to perform a fetch).  Also, some drivers may have to perform two queries to perform an operation (Eg, insert-and-return-values).

This is a Measuring Monitor.  Note that highwater tracking is supported, but rather meaningless.

=cut

use strict;
use warnings;
use base 'Class::ReluctantORM::Monitor::Measuring';

sub permitted_events  { return qw(execute_begin); }
sub default_events    { return qw(execute_begin); }
sub measurement_label { return 'Number of Queries'; }
sub take_measurement {
    my ($mon, %event) = @_;
    return $mon->last_measured_value + 1;
}


1;
