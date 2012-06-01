package Class::ReluctantORM::Monitor::RowSize;

=head1 NAME

Class::ReluctantORM::Monitor::RowSize - Track size in bytes of each row

=head1 SYNOPSIS

  use aliased 'Class::ReluctantORM::Monitor::RowSize';
  my $mon = RowSize->new(highwater_count => N, fatal_threshold => X);
  Class::ReluctantORM->install_global_monitor($mon);
  Pirate->install_class_monitor($mon);

  # Do a query....
  Pirate->fetch(...);

  # Read from the monitor
  my $count = $mon->last_measured_value();

  # Reset is not useful here

=head1 DESCRIPTION

Tracks the size of each row returned by a query.  The counter is set to the size, in bytes, of the data contained in each row.  Each row individually could be a high-water mark.

This is a Measuring Monitor.

=cut

use strict;
use warnings;
use base 'Class::ReluctantORM::Monitor::Measuring';
use Class::ReluctantORM::Utilities qw(row_size);

sub permitted_events  { return qw(fetch_row); }
sub default_events    { return qw(fetch_row); }
sub measurement_label { return 'Data size in bytes of this row';  }
sub take_measurement {
    my ($mon, %event) = @_;
    return row_size($event{row});
}

1;
