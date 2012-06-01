package Class::ReluctantORM::Monitor::RowCount;

=head1 NAME

Class::ReluctantORM::Monitor::RowCount - Count rows returned

=head1 SYNOPSIS

  use aliased 'Class::ReluctantORM::Monitor::RowCount';
  my $mon = RowCount->new(highwater_count => N, fatal_threshold => X);
  Class::ReluctantORM->install_global_monitor($mon);
  Pirate->install_class_monitor($mon);

  # Do a query....
  Pirate->fetch(...);

  # Read from the monitor
  my $row_count = $mon->last_measured_value();

  # Reset counter to 0 if desired - the counter gets reset at 
  # the beginning of every query anyway, but you might have need
  # to reset it in a fetchrow callback
  $mon->reset();

=head1 DESCRIPTION

Tracks the number of rows returned by a query,  The counter is reset at the beginning of execution of a query, and it is incremented as each row is fetched.

Note that number of rows does not match the number of objects returned by a fetch.  Joins can make the row count significantly higher by many factors.

This is a Measuring Monitor.

=cut

use strict;
use warnings;
use base 'Class::ReluctantORM::Monitor::Measuring';

sub permitted_events  { return qw(finish); }
sub default_events    { return qw(finish); }
sub measurement_label { return 'Row Count'; }
sub take_measurement {
    my ($mon, %event) = @_;
    # Nothing to do - actual work performed in fetchrow
    return $mon->last_measured_value();
}

sub notify_execute_begin    {
    my ($mon, %event) = @_;

    # Reset on execute
    $mon->reset();
}
sub notify_fetch_row        {
    my ($mon, %event) = @_;
    $mon->last_measured_value($mon->last_measured_value() + 1);
}

1;
