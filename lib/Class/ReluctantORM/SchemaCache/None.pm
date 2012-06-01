package Class::ReluctantORM::SchemaCache::None;
use strict;
use warnings;

use base 'Class::ReluctantORM::SchemaCache';

our $DEBUG = 0;

sub new {
    my $class = shift;
    return bless {}, $class;
}

# Always misses
sub read_columns_for_table { return undef; }

# No-op
sub store_columns_for_table { return; }
sub read_cache_file { return; }
sub write_cache_file { return; }


1;
