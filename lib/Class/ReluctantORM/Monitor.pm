package Class::ReluctantORM::Monitor;

=head1 NAME

Class::ReluctantORM::Monitor - Monitor CRO Driver activity

=head1 SYNOPSIS

  use aliased 'Class::ReluctantORM::Monitor::ColumnCount';
  # You can also make your own

  # Create a new monitor (args vary with monitor)
  my $mon = ColumnCount->new(log => $io, log_prefix => 'yipes', ...);

  # Install globally
  Class::ReluctantORM->install_global_monitor($mon);

  # Install only for the Ship class's driver
  Model::Ship->driver->install_monitor($mon);

  # Turn on Origin Tracking to find out where the query is being generated
  Class::ReluctantORM->enable_origin_tracking(1);

  # Make queries, etc...
  # Things get logged to $io

=head1 DESCRIPTION

The Monitor facility allows you to peek inside the Class::ReluctantORM
SQL render, execute, and fetch process, and see what is going 
on.  Several monitors are included with Class::ReluctantORM, and it is easy
to write your own.

Monitors may be global or class-specific.  Global monitors are
installed by calling Class::ReluctantORM->install_global_monitor($mon),
and will affect all CRO interactions.  Class-specific monitors
are installed onto the class's driver, using TheClass->driver->install_monitor($mon), and will
only monitor queries originating on that class.

Monitors are grouped into two broad categories: general monitors, which can do anything, and measuring monitors, which have special facilities for measuring, tracking, and acting on a value that they measure.

Several Monitors are included with Class::ReluctantORM (all have Class::ReluctantORM::Monitor as a prefix):

=over

=item Dump

Dumps the query structures to the log.

=item QueryCount

Counts the number of statements executed.  A Measuring monitor.

=item ColumnCount

Counts the number of columns returned by a query.  A Measuring monitor.

=item JoinCount

Counts the number of JOINs in the query.  A Measuring monitor.

=item QuerySize

Monitors the total size, in bytes, of the data returned by a query.  A Measuring monitor.

=item RowCount

Monitors the number of rows returned by the query.  A Measuring monitor.

=item RowSize

Monitors the size, in bytes, of each individual row.  A Measuring monitor.

=item Timer

Tracks execution time of each query.  A Measuring monitor.

=back

=head1 CONTROLLING WHAT TO OUTPUT

These are the possible values for the 'what' option to new(), which controls what data gets logged.

=over

=item sql_object - the abstract Class::ReluctantORM::SQL object, via Data::Dumper

=item sql_object_pretty - the abstract Class::ReluctantORM::SQL object, pretty-printed

=item statement - the rendered SQL statement as a string

=item binds - the list of bind arguments, given to execute()

=item row - the structure returned by fetchrow_hashref

=item origin - the line, file, and package where the query originated

=back

=cut

use strict;
use warnings;
use Class::ReluctantORM::Utilities qw(conditional_load_subdir check_args nz);
use Data::Dumper;

our $DEBUG = 0;

use base 'Class::Accessor';
use IO::Handle;

our @MONITOR_CLASSES;
BEGIN {
    @MONITOR_CLASSES = conditional_load_subdir(__PACKAGE__);
}


=head1 CONSTRUCTORS

=head2 $mon = SomeMonitor->new(...);

Creates a new monitor.  Monitors may extend the list of supported options, but all support:

=over

=item log - an IO::Handle or the string 'STDOUT' or 'STDERR'

Append any log messages to this handle.  If not present, logging is disabled.

=item log_prefix - optional string

Prefix to be used in log messages.  Can be used to distinguish this monitor from others.

=item trace_limit - optional integer

If you use the 'origin' option to 'what', use this to specify how many frames to go back from the origin of the query.  Default: no limit.

=item what - optional arrayref of strings, or the string 'all'.

When logging, indicates what values to log.  Different monitors have different defaults for this.  See CONTROLLING WHAT TO OUTPUT for more info.

=item when - optional arrayref of strings, or the string 'all'.

Indicates which events to pay attention to.  Some monitors may constrain this value because they must listen at certain events.  See CONTROLLING WHEN TO OUTPUT for more info.

=back

Measuring monitors have additional options:

=over

=item log_threshold - optional number

If the measured value is less than this, no log entry is made.  Default: always log.

=item fatal_threshold - optional number

Reflects a hard limit.  If the measured value exceeds the limit, an exception is thrown. Default: no exceptions.

=item highwater_count - integer

If present, enables a "scoreboard" effect.  This many records will be kept (for example, the top 5 queries by column count).  See Class::ReluctantORM::Monitor::Measure - highwater_marks().  Default: remember 5 records.

=back

=cut

our @WHENS = qw(render_begin render_transform render_finish execute_begin execute_finish fetch_row finish);
our @WHATS = qw(sql_object statement binds row sql_object_pretty origin);

sub _monitor_base_check_args_spec {
    return {
            optional => [qw(
                               log
                               log_prefix
                               what
                               when
                               trace_limit
                          )],
           };
}

sub _monitor_check_args_spec { return $_[0]->_monitor_base_check_args_spec(); }


__PACKAGE__->mk_accessors(qw(
                                log
                                log_prefix
                                what
                                when
                                trace_limit
                           ));

sub _new {
    my $class = shift;
    my %args =
      check_args(
                 %{$class->_monitor_check_args_spec()},
                 args     => \@_,
                );

    my $self = bless {}, $class;
    if ($args{log}) {
        if (ref($args{log}) && $args{log}->isa('IO::Handle')) {
            $self->log($args{log});
        } elsif ($args{log} eq 'STDOUT') {
            $self->log(IO::Handle->new_from_fd(fileno(STDOUT), 'w'));
        } elsif ($args{log} eq 'STDERR') {
            $self->log(IO::Handle->new_from_fd(fileno(STDERR), 'w'));
        } else {
            Class::ReluctantORM::Exception::Param::WrongType->croak
                (
                 param => 'log',
                 expected => 'IO::Handle, or the string STDERR or STDOUT',
                 value => $args{log}
                );
        }
        delete $args{log};
    }

    $self->log_prefix($args{log_prefix});
    $self->trace_limit($args{trace_limit});

    if (!($args{when}) || ($args{when} eq 'all')) {
        $self->when( { map { $_ => 1 } @WHENS });
    } elsif ($args{when}) {
        $self->when( { map { $_ => 1 } @{$args{when}} } );
    }


    if (!($args{what}) || ($args{what} eq 'all')) {
        $self->what( { map { $_ => 1 } grep { $_ ne 'sql_object' } @WHATS } );
    } elsif ($args{what}) {
        $self->what( { map { $_ => 1 } @{$args{what}} } );
    }

    return $self;
}


=head2 $bool = $mon->supports_measuring();

Returns true if the Monitor supports measuring something (a metric).  Default implementation returns false.

=cut

sub supports_measuring     { return 0; }


=head1 MONITOR EVENT INTERFACE METHODS

These methods are called whenever a Driver event occurs.

The default implementation is a no-op.

All methods take named parameters.  Each method lists its required arguments.  The arguments are as follows:

=over

=item driver

The Driver that is performing the work.

=item sql_obj

The Class::ReluctantORM::SQL object being rendered.

=item sql_str

The rendered SQL string, ready for a prepare().  This will be in the driver's dialect.

=item sth

The DBI statement handle.

=item binds

An arrayref of arguments to DBI execute().

=item row

A hashref of data returned by a single row, as returned by $sth->fetchrow_hashref

=back

=cut

=head2 $d->notify_render_begin(sql_obj => $so);

Notifies the monitoring system that the driver has begun work to render the given SQL object.

Arguments: sql_obj, original, untouched Class::ReluctantORM::SQL object.

=cut

sub notify_render_begin { }

=head2 $d->notify_render_transform(sql_obj => $so);

Notifies the monitoring system that the driver has finished transforming the SQL object.

Arguments: sql_obj, the post-transformation Class::ReluctantORM::SQL object.

=cut

sub notify_render_transform { }

=head2 $d->notify_render_finish(sql_obj => $so, sql_str => $ss);

Notifies the monitoring system that the driver has finished rendering the SQL object.

=cut

sub notify_render_finish { }

=head2 $d->notify_execute_begin(sql_obj => $so, sql_str => $ss, sth =>$sth, binds => \@binds);

Notifies the monitoring system that the driver is about to perform a DBI execute.

=cut

sub notify_execute_begin { }

=head2 $d->notify_execute_finish(sql_obj => $so, sql_str => $ss, sth =>$sth, binds => \@binds);

Notifies the monitoring system that the driver has returned from performing a DBI execute.

=cut

sub notify_execute_finish { }

=head2 $d->notify_fetch_row(sql_obj => $so, sql_str => $ss, sth =>$sth, binds => \@binds, row => \%row);

Notifies the monitoring system that the driver has returned from performing a DBI fetchrow.

=cut

sub notify_fetch_row { }

=head2 $d->notify_finish(sql_obj => $so, sql_str => $ss, sth => $sth);

Notifies the monitoring system that the driver has finished the query.

=cut

sub notify_finish { }


sub _log_stuff {
    my $mon = shift;
    my %args = @_;
    return unless $mon->log;

    my $msg = '';

    if ($args{sql_obj} && exists($mon->what->{sql_object})) {
        $msg .= $mon->_indent(2, "---SQL Object Dump:---");
        $msg .= $mon->_indent(4, Dumper($args{sql_obj}));
    }
    if ($args{sql_obj} && exists($mon->what->{sql_object_pretty})) {
        $msg .= $mon->_indent(2, "---SQL Object pretty print:---");
        $msg .= $mon->_indent(4, $args{sql_obj}->pretty_print());
    }
    if ($args{sql_str} && exists($mon->what->{statement})) {
        $msg .= $mon->_indent(2, "---SQL Statement:---");
        $msg .= $mon->_indent(4, $args{sql_str});
    }
    if ($args{binds} && exists($mon->what->{binds})) {
        $msg .= $mon->_indent(2, "---Bind values:---");
        $msg .= $mon->_indent(4, Data::Dumper->Dump([$args{binds}], ['*binds']));
    }
    if ($args{row} && exists($mon->what->{row})) {
        $msg .= $mon->_indent(2, "---Row values:---");
        $msg .= $mon->_indent(4, Dumper($args{row}));
    }
    if ($args{sql_obj} && exists($mon->what->{origin}) && $args{sql_obj}->last_origin_frame()) {
        $msg .= $mon->_indent(2, "---Query Origin:---");
        my @trace = $args{sql_obj}->last_origin_trace(); # Don't need all - SQL objects can have at most one
        my $frames_printed = 0;
        foreach my $frame (@trace) {
            $msg .= $mon->_indent(4, $mon->render_origin_frame($frame));
            $frames_printed++;
            if ($mon->trace_limit() &&  $frames_printed >= $mon->trace_limit) {
                last;
            }
        }
    }

    if ($args{log_extra}) {
        if ($args{log_extra}{one_line}) {
            $msg .= $mon->_indent(2, "---" . $args{log_extra}{label} . ":" . $args{log_extra}{value});
        } else {
            $msg .= $mon->_indent(2, "---" . $args{log_extra}{label} . ":---");
            $msg .= $mon->_indent(4, $args{log_extra}{value});
        }
    }

    return unless ($msg);

    $msg = $mon->_log_prefix($args{event}) . "\n" . $msg;
    $mon->log->print($msg);
}

=begin devnotes

=head2 $str = $mon->render_origin_frame()

Compress the origin frame to a string in a pretty way.

=cut

sub render_origin_frame {
    my $mon = shift;
    my $frame = shift;

    # TODO - TB2CRO - OmniTI-ism
    # Special hook for Mungo support
    if ($frame->{package} =~ /Mungo::FilePage/) {
        my $file = $main::Response->{Mungo}->demangle_name($frame->{package} . '::__content');
        $file =~ s{^Mungo::FilePage\(}{};
        $file =~ s{\)$}{};
        return "file: " . $file . " line (approx): " . $frame->{line};
    }

    return "file: " . $frame->{file} . " line: " . $frame->{line};
}


=begin devnotes

Returns a prefix string for use in monitor logging.

=cut

sub _log_prefix {
    my $self = shift;
    my $event = shift;
    my $str = '[' . localtime() . ']';
    if ($self->log_prefix) {
        $str .= '[' . $self->log_prefix . ']';
    }
    $str .= sprintf('[pid%05d]', $$);
    $str .= '[' . $event . ']';
    return $str . ' ';
}

sub _indent {
    my $monitor = shift;
    my $spaces = shift;
    my $str = shift;
    my $indent = ' ' x $spaces;
    return (join "\n", map { $_ ? ($indent . $_) : $_ } split /\n/, $str) . "\n";
}

=head1 AUTHOR

Clinton Wolfe January 2009 - January 2011


=cut


1;
