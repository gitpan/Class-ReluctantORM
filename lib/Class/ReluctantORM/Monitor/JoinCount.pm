package Class::ReluctantORM::Monitor::JoinCount;

=head1 NAME

Class::ReluctantORM::Monitor::JoinCount - Track JOINs in queries

=head1 SYNOPSIS

  use aliased 'Class::ReluctantORM::Monitor::JoinCount';
  my $mon = JoinCounter->new(highwater_count => N, fatal_threshold => X);
  Class::ReluctantORM->install_global_monitor($mon);
  Pirate->install_class_monitor($mon);

  # Do a query.... logging and highwater scorekeeping happens
  Pirate->fetch(...);

  # Read from the monitor
  my $count = $mon->last_measured_value();

=head1 DESCRIPTION

Tracks the number of JOINs in the FROM clause in the last query that was attempted to be executed.

Note that number of joins does not match number of relationships in the 'with' clause.  Some relationship, such as HasLazy, contribute 0 JOINs, while others contribute more than one (HasManyMany).

This is a Measuring Monitor.

=cut

use strict;
use warnings;
use base 'Class::ReluctantORM::Monitor::Measuring';

sub permitted_events  { return qw(execute_begin); }
sub default_events    { return qw(execute_begin); }
sub measurement_label { return 'Join Count'; }
sub take_measurement {
    my ($mon, %event) = @_;

    my $sql = $event{sql_obj};

    if ($sql->from()) {
        my @joins = $sql->from->joins();
        return scalar(@joins);
    } else {
        return 0;
    }
}

1;
