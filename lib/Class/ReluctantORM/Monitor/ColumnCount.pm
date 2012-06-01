package Class::ReluctantORM::Monitor::ColumnCount;

=head1 NAME

Class::ReluctantORM::Monitor::ColumnCount

=head1 SYNOPSIS

  use aliased 'Class::ReluctantORM::Monitor::ColumnCount';
  my $mon = ColumnCounter->new(highwater_count => N, fatal_threshold => X);
  Class::ReluctantORM->install_global_monitor($mon);
  Pirate->install_class_monitor($mon);

  # Do a query.... logging and highwater scorekeeping happens
  Pirate->fetch(...);

  # Read from the monitor
  my $count = $mon->last_measured_value();

=head1 DESCRIPTION

Tracks the number of OutputColumns in the last query executed.

This is a Measuring Monitor.

=cut

use strict;
use warnings;
use base 'Class::ReluctantORM::Monitor::Measuring';

sub permitted_events  { return qw(execute_begin); }
sub default_events    { return qw(execute_begin); }
sub measurement_label { return 'Column Count'; }
sub take_measurement {
    my ($mon, %event) = @_;
    return scalar($event{sql_obj}->output_columns);
}

1;
