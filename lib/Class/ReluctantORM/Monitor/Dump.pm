package Class::ReluctantORM::Monitor::Dump;

=head1 NAME

Class::ReluctantORM::Monitor::Dump - Dump SQL structures

=head1 SYNOPSIS

  use aliased 'Class::ReluctantORM::Monitor::Dump' => 'Monitor::Dump';

  # Create a new monitor - dumps everything, all the time, to STDERR
  my $mon = Monitor::Dump->new();

  # List of things you can dump
  my $mon = Monitor::Dump->new(what => qw(sql_object statement binds row));

  # When you can dump them
  my $mon = Monitor::Dump->new(when => qw(render_begin render_transform ...));

=head1 DESCRIPTION

Dumps structures to the log.

This is a basic (non-Measuring) Monitor.

=cut

use strict;
use warnings;
use base 'Class::ReluctantORM::Monitor';

=head1 CONSTRUCTOR

=cut

=head2 $mon = Monitor::Dump->new(%monitor_args);

Creates a Dump monitor.

Identical to the usual defaults for a Monitor (all events, all data to the log) EXCEPT that the 'log' option is given a default of 'STDERR'.

=cut

sub new {
    my $class = shift;
    my %args = @_;
    $args{log} ||= 'STDERR';
    my $self = $class->SUPER::_new(%args);
    return $self;
}

sub notify_render_begin     { $_[0]->_log_stuff(@_[1..$#_], event => 'render_begin'); }
sub notify_render_transform { $_[0]->_log_stuff(@_[1..$#_], event => 'render_transform'); }
sub notify_render_finish    { $_[0]->_log_stuff(@_[1..$#_], event => 'render_finish'); }
sub notify_execute_begin    { $_[0]->_log_stuff(@_[1..$#_], event => 'execute_begin'); }
sub notify_execute_finish   { $_[0]->_log_stuff(@_[1..$#_], event => 'execute_finish'); }
sub notify_fetch_row        { $_[0]->_log_stuff(@_[1..$#_], event => 'fetch_row'); }
#sub notify_finish           { __log_stuff(@_, event => 'finish'); }


1;
