package Class::ReluctantORM::Monitor::QuerySize;

=head1 NAME

Class::ReluctantORM::Monitor::QuerySize - Running total of data size

=head1 SYNOPSIS

  use aliased 'Class::ReluctantORM::Monitor::QuerySize';
  my $mon = QuerySize->new(highwater_count => N, fatal_threshold => X);
  Class::ReluctantORM->install_global_monitor($mon);
  Pirate->install_class_monitor($mon);

  # Do a query....
  Pirate->fetch(...);

  # Read from the monitor
  my $bytes = $mon->last_measured_value();

  # Reset counter to 0 if desired - the counter gets reset at 
  # the beginning of every query anyway, but you might have need
  # to reset it in a fetchrow callback
  $mon->reset();


=head1 DESCRIPTION

Keeps a running total of the number of bytes returned in all rows of the query.

This is a Measuring Monitor.

=cut

use strict;
use warnings;
use base 'Class::ReluctantORM::Monitor::Measuring';

use Class::ReluctantORM::Utilities qw(row_size);

sub permitted_events  { return qw(finish); }
sub default_events    { return qw(finish); }
sub measurement_label { return 'Bytes Returned by Query'; }
sub take_measurement {
    my ($mon, %event) = @_;
    # Nothing to do - actual measurement was taken during fetchrow
    return $mon->last_measured_value();
}

sub notify_execute_begin    {
    my $self = shift;
    $self->reset();
}

sub notify_fetch_row        {
    my $self = shift;
    my %args = @_;
    my $sql = $args{sql_obj};
    my $row = $args{row};
    $self->last_measured_value($self->last_measured_value() + row_size($row));
}

1;
