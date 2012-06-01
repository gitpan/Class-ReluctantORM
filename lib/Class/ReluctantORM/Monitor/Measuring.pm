package Class::ReluctantORM::Monitor::Measuring;

=head1 NAME

Class::ReluctantORM::Monitor::Measuring - Monitor with Metric support

=head1 SYNOPSIS

  #####
  #  Using a Measuring Monitor
  #####

  # Interrogate the monitor
  print "Last query had " . $mon->last_measured_value() . " foo units\n";

  # Worst offenders overall
  foreach my $info ($mon->highwater_marks()) {
     print "Rank: " . $info->{rank} . "\n";
     print "Foo Count: " . $info->{measured_value} . "\n";
     print "Query Object: " . $info->{sql}->pretty_print() . "\n";

     # next three depend on Origin Tracking being enabled
     print "Origin File: "    . $info->{origin}->{file} . "\n";
     print "Origin Line: "    . $info->{origin}->{line} . "\n";
     print "Origin Package: " . $info->{origin}->{package} . "\n";
  }

  # Can also log, etc - does everything a regular Monitor can do

  #####
  # Creating a new Measuring Monitor
  #####

  package FooCount;
  use base 'Class::ReluctantORM::Monitor::Measuring';

  sub measurement_label { return 'Foo Count (Instantaneous)'; }
  sub default_events   { return @list_of_when; }
  sub permitted_events { return @list_of_when; }

  # Gets called whenever a measurement needs to be taken
  sub take_measurement {
    my %event_info = @_; # sql_obj, binds, etc
    return $foo_count;
  }

=head1 DESCRIPTION

The Monitor facility allows you to peek inside the Class::ReluctantORM
SQL render, execute, and fetch process, and see what is going 
on.  Several monitors are included with Class::ReluctantORM, and it is easy
to write your own.

The Measuring Monitors have special support to obtain, track, and act on a measured value.

See Class::ReluctantORM::Monitor for info about using MOnitors in general.  This file only documents the measuring extensions.

=cut

use strict;
use warnings;

use base 'Class::ReluctantORM::Monitor';
use Class::ReluctantORM::Utilities qw(check_args nz);
use Data::Dumper;

our $DEBUG = 0;


=head1 CONSTRUCTORS

=head2 $mon = SomeMonitor->new(...);

See Class::ReluctantORM::Monitor::new().

=cut

our @WHENS = @Class::ReluctantORM::Monitor::WHENS;
our @WHATS = @Class::ReluctantORM::Monitor::WHATS;

sub _monitor_check_args_spec {
    my $monitor_spec = Class::ReluctantORM::Monitor->_monitor_base_check_args_spec();
    push @{$monitor_spec->{optional}}, qw(
                                             highwater_count
                                             log_threshold
                                             fatal_threshold
                                        );
    return $monitor_spec;
}

__PACKAGE__->mk_accessors(qw(
                                highwater_marks_ref
                                last_measured_value
                                log_threshold
                                fatal_threshold
                                highwater_count
                           ));

sub new {
    my $class = shift;
    my %args =
      check_args(
                 %{$class->_monitor_check_args_spec()},
                 args     => \@_,
                );
    my $self = $class->_new(%args);

    # Init Measuring-specifics
    $args{highwater_count} ||= 5;
    $self->highwater_count($args{highwater_count});
    $self->fatal_threshold($args{fatal_threshold});
    $self->log_threshold($args{log_threshold});

    # Init measure to 0
    $self->last_measured_value(0);
    $self->highwater_marks_ref([]);

    # Check WHENS (was defaulted to all by Monitor->_new)
    if (!defined($args{when})) {
        $self->when({ map { $_ => 1 } $self->default_events() });
    } else {
        foreach my $when (keys %{$self->when}) {
            unless (grep { $_ eq $when } $self->permitted_events) {
                Class::ReluctantORM::Exception::Param::BadValue->croak
                    (
                     param => 'when',
                     value => $when,
                     error => "Monitor $class cannot be used on the $when event.  Instead, use one of: " . join(',', $self->permitted_events),
                    );
            }
        }
    }

    return $self;
}

=head1 MEASURING API

These methods should be overridden to implement your monitor's behavior.

=head2  $str = $mon->measurement_label();

Returns a string to be included in the log to label the measured value.  Default is "$monitor_class Observation".

=cut

sub measurement_label {
    my $monitor = shift;
    my $class = ref($monitor) ? ref($monitor) : $monitor;
    return "$class Observation";
}

=head2 @whens = $mon->permitted_events();

Returns a list of events for which it is permitted to take a measurement for this monitor.  If you instantiate a monitor, and request an event not on this list, an exception will be thrown.

Default is all events permitted.

=cut

sub permitted_events { return @WHENS; }

=head2  @whens = $mon->default_events();

Returns a list of events at which measurmeents will automatically be taken, if you do not override this with the 'when' option to new().

Default is all events.

=cut

sub default_events { return @WHENS; }

=head2  $number = $mon->take_measurement(%event_info);

Pure virtual - your Monitor subclass must implement this method.

Called when the monitor needs to take a measurement.  The arguments will be a hash of the event arguments (See Class::ReluctantORM::Monitor - Monitor Interface Methods section), with an additional 'event' key whose value is the name of the event.

=cut

sub take_measurement { Class::ReluctantORM::Exception::Call::PureVirtual->croak('take_measurement'); }

=head1 HIGHWATER TRACKING

Measuring-style Monitors may also support Highwater Tracking.  As the Monitor makes observations, it maintains a list of the N worst unique observations.  N is determined by the value of the highwater_count option passed to the monitor constructor.  
Observations are considered the same if they have the same count and same origin.

=head2  @observations = $mon->highwater_marks()

Returns an array of hashes describing the N unique observations whose measured_value was the largest.

Each hashref has the following keys:

=over

=item rank

Current rank in the highwater scoreboard, with 1 the worst.

=item measured_value

The observed value.

=item sql

The SQL object being executed at the time.

=item origin

Present only if Origin Tracking is enabled (see Class::ReluctantORM->enable_origin_tracking()).  If present, is a hash containing keys file, line, and package, indicating the location of the last stack frame outside of Class::ReluctantORM (usually "your" code).

=back

=cut

sub highwater_marks {
    my $self = shift;
    return @{$self->highwater_marks_ref() || []};
}

sub _record_highwater {
    my ($mon, $sql) = @_;
    return unless $mon->highwater_count();
    my @marks = $mon->highwater_marks;
    my $new_entry = {
                     rank => 0,
                     measured_value => $mon->last_measured_value(),
                     sql => $sql,
                     origin => (scalar $sql->last_origin_frame())
                    };
    my %uniq =
      map { $mon->__hash_highwater_entry($_) => $_ }
        (@marks, $new_entry);

    @marks = sort { $b->{measured_value} <=> $a->{measured_value} } values %uniq;
    @marks = grep { defined $_ } @marks[0..($mon->highwater_count() -1)];
    for (0..$#marks) {
        $marks[$_]->{rank} = $_ + 1;
    }

    $mon->highwater_marks_ref(\@marks);
}

sub __hash_highwater_entry {
    my $mon = shift;
    my $entry = shift;
    my $key = ref($mon) eq 'Class::ReluctantORM::Monitor::QueryCount' ? '' : $entry->{measured_value} . '_';
    if ($entry->{origin}) {
        $key .= nz($entry->{origin}->{file},    'unk') . '_';
        $key .= nz($entry->{origin}->{line},    'unk') . '_';
        $key .= nz($entry->{origin}->{package}, 'unk') . '_';
    } else {
        # ewwww
        $key .= $entry->{sql}->pretty_print();
    }
    return $key;
}

=head2 $bool = $mon->supports_measuring();

Returns true if the Monitor supports counting something (a metric).  This implementation returns true.

=cut

sub supports_measuring     { return 1; }

=head1 MONITOR INFORMATION INTERFACE METHODS

These methods provide information about the monitor.

=cut

=head2 $number = $mon->last_measured_value();

Returns the value of the last observation.

=cut

=head2 $mon->reset();

For measuring monitors, resets the last measured value to zero.

=cut

sub reset {
    my $self = shift;
    $self->last_measured_value(0);
}


sub notify_render_begin     { __measuring_event(@_, event => 'render_begin');     }
sub notify_render_transform { __measuring_event(@_, event => 'render_transform'); }
sub notify_render_finish    { __measuring_event(@_, event => 'render_finish');    }
sub notify_execute_begin    { __measuring_event(@_, event => 'execute_begin');    }
sub notify_execute_finish   { __measuring_event(@_, event => 'execute_finish')    }
sub notify_fetch_row        { __measuring_event(@_, event => 'fetch_row');        }
sub notify_finish           { __measuring_event(@_, event => 'finish');           }

sub __measuring_event {
    my $mon = shift;
    my %event_args = @_;
    my $event = $event_args{event};

    unless ($mon->when->{$event}) { return; }

    # Take a measurement
    $mon->last_measured_value($mon->take_measurement(%event_args));
    $mon->_record_highwater($event_args{sql_obj});

    # Log if needed
    if (!($mon->log_threshold) || ($mon->last_measured_value >= $mon->log_threshold)) {
        $mon->_log_stuff(
                          %event_args,
                          log_extra => {
                                        label => $mon->measurement_label(),
                                        value => $mon->last_measured_value(),
                                       },
                         );
    }

    # Die if needed
    if ($mon->fatal_threshold && $mon->last_measured_value >= $mon->fatal_threshold) {
        Class::ReluctantORM::Exception::SQL::AbortedByMonitor->croak
            (
             monitor  => $mon,
             limit    => $mon->fatal_threshold,
             observed => $mon->last_measured_value,
             sql      => $event_args{sql_obj},
             query_location => [ $event_args{sql_obj}->all_origin_traces() ],
            );
    }

}




=head1 AUTHOR

Clinton Wolfe January 2011


=cut


1;
