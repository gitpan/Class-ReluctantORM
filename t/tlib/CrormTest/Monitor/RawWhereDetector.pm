package CrormTest::Monitor::RawWhereDetector;

# Monitor used to detect whether a query used a parsed or raw WHERE clause.
# Like a Counter.
# Has three counts - an no_where count, a raw_where_count, and an object_where_count.
# Calling reset() sets them all back to 0.

use strict;
use warnings;
use Data::Dumper;

our $DEBUG = 0;

use base 'Class::ReluctantORM::Monitor';

sub new {
    my $class = shift;
    my $self = $class->SUPER::_new(@_);
    $self->reset();
    return $self;
}

__PACKAGE__->mk_accessors(qw(no_where_count raw_where_count object_where_count orig_sql));

sub reset {
    my $self = shift;
    $self->no_where_count(0);
    $self->raw_where_count(0);
    $self->object_where_count(0);
    $self->orig_sql(undef);
    return $self;
}

sub notify_render_begin     { }
sub notify_render_transform { }
sub notify_render_finish    { }
sub notify_execute_begin    {
    my $self = shift;
    my %args = @_;
    my $sql = $args{sql_obj};
    if ($sql->raw_where) {
        $self->raw_where_count( $self->raw_where_count() + 1 );
    } elsif ($sql->where) {
        $self->object_where_count( $self->object_where_count() + 1 );
    } else {
        $self->no_where_count( $self->no_where_count() + 1 );
    }

    # Remember SQL generated, but only if it wasn't an INSERT (crudely filters out auditing)
    unless ($sql->operation eq 'INSERT') {
        $self->orig_sql($sql);
    }

}
sub notify_execute_finish   { }
sub notify_fetch_row        { }
sub notify_finish           { }

1;
