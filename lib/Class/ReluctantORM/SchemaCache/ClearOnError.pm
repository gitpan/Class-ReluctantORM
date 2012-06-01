package Class::ReluctantORM::SchemaCache::ClearOnError;
use strict;
use warnings;
use base 'Class::ReluctantORM::SchemaCache::Simple';

our $DEBUG = 0;

sub notify_sql_error {
    my $self = shift;
    my $exception = shift;
    $self->clear();
}

1;
