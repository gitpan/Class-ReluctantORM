package Class::ReluctantORM::SchemaCache::Simple;
use strict;
use warnings;
use base 'Class::ReluctantORM::SchemaCache';

our $DEBUG = 0;

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    $self->read_cache_file();
    return $self;
}



1;
