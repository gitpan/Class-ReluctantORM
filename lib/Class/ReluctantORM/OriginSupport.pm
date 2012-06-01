package Class::ReluctantORM::OriginSupport;

=head1 NAME

OriginSupport - Add support for Origin tracking to various CRO objects

=head1 SYNOPSIS

  package YourApp;

  Class::ReluctantORM->enable_origin_tracking(1);

  my $ship = Ship->fetch(23);   # Line 56
  my $origin = $ship->origin_frame();
  say $origin->{package}; # YourApp
  say $origin->{line};    # 56
  say $origin->{file};    # whereever this is

  @frames = $ship->origin_frame();
  @frames = $ship->origin_frames();  # Plural alias provided

  use Class::ReluctantORM::SQL;
  Class::ReluctantORM::SQL->enable_origin_tracking(1);

  my $sql = SQL->new( ... );
  $origin = $sql->origin_frame();

=head1 DESCRIPTION

The problem with CRO's Reluctance feature is that when a FetchRequired exception is thrown, you have to go hunting for the location where the original fetch occured (which will be a different location than where the exception was thrown from).  This mix-in superclass adds support for tracking the "origin" of each CRO object.

The origin stack frame is the first stack frame that is from a package that is not from the Class::ReluctantORM tree.

SQL objects also support Origins as a debugging feature.

Origin support adds to memory usage, but has no benefit in production (as you should in theory hit all FetchRequired exceptions in development).  For this reason, origin tracking is disabled by default.

=head1 KNOWN USERS

Currently these CRO modules use OriginSupport:

=over 4

=item Class::ReluctantORM

This means all your model objects will support Origins.

=item Class::ReluctantORM::SQL

This means each SQL statement will be traceable back to its point of origin.

=back

Origin support is enable/disabled on an individual basis for these classes.

=cut

use strict;
use warnings;
use Class::ReluctantORM::Utilities qw(last_non_cro_stack_frame);

my $SVN_VERSION = 0;
$SVN_VERSION = $1 if(q$LastChangedRevision: 27$ =~ /(\d+)/);
our $VERSION = "0.4.${SVN_VERSION}";

our %ENABLED_FOR_CLASS = ();

=head2 Class::ReluctantORM->enable_origin_tracking($bool);

Based on the value of $bool, enables or disables origin tracking for all model objects.

Disabled by default.

=cut

sub enable_origin_tracking {
    my $class = shift;
    my $flag = shift;
    $ENABLED_FOR_CLASS{$class} = $flag;
}

=head2 $bool = $obj->is_origin_tracking_enabled();

=head2 $bool = SomeClass->is_origin_tracking_enabled();

Returns a flag indicating whether origins are being tracked for this class.  May be called as an instance method, but the check is performed at the class level.

=cut

sub is_origin_tracking_enabled {
    my $inv = shift;
    my $enabled = grep { $inv->isa($_) } keys %ENABLED_FOR_CLASS;
    return $enabled;
}

=head2 $frame = $obj->last_origin_frame();

Returns the last non-CRO frame from the last origin trace.  A frame is a hashref representing info about the stack frame.

=cut

sub last_origin_frame {
    my $obj = shift;
    unless ($obj->is_origin_tracking_enabled) { return undef; }
    my @traces = @{$obj->get('_origin_traces') || []};
    my $last_trace = $traces[-1] || [];
    my $last_frame_of_last_trace = $last_trace->[0];
    return $last_frame_of_last_trace;
}

=head2 @frames = $obj->last_origin_trace();

Returns all frames from the last origin trace.  A frame is a hashref representing info about the stack frame.

=cut

sub last_origin_trace {
    my $obj = shift;
    unless ($obj->is_origin_tracking_enabled) { return (); }
    my @traces = @{$obj->get('_origin_traces') || []};
    my $last_trace = $traces[-1] || [];
    return @{$last_trace};
}

=head2 @array_of_arrays = $obj->all_origin_traces();

Returns an array of all traces.  Each trace is an array of stack frames.

An object may have mor ethan one trace, because afterthought fetching causes multiple query origins.

=cut

sub all_origin_traces {
    my $obj = shift;
    unless ($obj->is_origin_tracking_enabled) { return (); }
    my @traces = @{$obj->get('_origin_traces') || []};
    return @traces;
}

=head2 $obj->capture_origin();

Called internally by CRO objects, this method records an origin point.

=cut

sub capture_origin {
    my $obj = shift;
    unless ($obj->is_origin_tracking_enabled) { return; }

    my @trace = last_non_cro_stack_frame();
    my @all_traces = @{$obj->get('_origin_traces') || []};
    push @all_traces, \@trace;
    $obj->set('_origin_traces', \@all_traces);

    return $trace[0];
}

1;
