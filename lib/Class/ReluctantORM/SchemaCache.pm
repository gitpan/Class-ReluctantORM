package Class::ReluctantORM::SchemaCache;
use strict;
use warnings;
our $SCHEMA_CACHE;
our $DEBUG = 0;

use Carp;
use Class::ReluctantORM::Utilities qw(conditional_load_subdir read_file write_file json_encode json_decode);

our @POLICY_CLASSES;
BEGIN {
    @POLICY_CLASSES = conditional_load_subdir(__PACKAGE__);
}

=head2 @policy_names = Class::ReluctantORM::SchemaCache->policy_names()

Returns a list of available caching policies.

=cut

sub policy_names {
    return map { s/Class::ReluctantORM::SchemaCache:://; $_ } @POLICY_CLASSES;
}

=head2 $cache = Class::ReluctantORM::SchemaCache->instance();

Returns a Class::ReluctantORM::SchemaCache object, implementing the policy specified by the Class::ReluctantORM global option schema_cache_policy.  This is a singleton object.

=cut


sub instance {
    if ($SCHEMA_CACHE) { return $SCHEMA_CACHE; }
    my $class = 'Class::ReluctantORM::SchemaCache::' . Class::ReluctantORM->get_global_option('schema_cache_policy');
    return $SCHEMA_CACHE = $class->new();
}

=head2 $hashref = $cache->read_columns_for_table($namespace, $table_name);

Looks in the cache, and returns a hashref mapping lowercased column names to database-cased column names.  If there is no hit, undef is returned.  Pass an empty string if the database does not support namespaces.

=cut

sub read_columns_for_table {
    my $self = shift;
    my ($namespace, $table_name) = @_;
    return $self->{__databag}{$namespace || '(none)'}{$table_name}{cols};
}

=head2 $cache->store_columns_for_table($namespace, $table_name, $hashref);

Stores to the cache a hashref mapping lowercased column names to database-cased column names.  The cache file is immediately updated.  Pass an empty string if the database does not support namespaces.

=cut

sub store_columns_for_table {
    my $self = shift;
    my ($namespace, $table_name, $data) = @_;
    $self->{__databag}{$namespace || '(none)'}{$table_name}{cols} = $data;
    $self->write_cache_file();
}

=head2 $arrayref = $cache->read_primary_keys_for_table($namespace, $table_name);

Looks in the cache, and returns an arrayref listing the lowercased column names of the primary key columns, if any, in the order reported by the database.  If there is no hit, undef is returned.  Pass an empty string if the database does not support namespaces.

=cut

sub read_primary_keys_for_table {
    my $self = shift;
    my ($namespace, $table_name) = @_;
    return $self->{__databag}{$namespace || '(none)'}{$table_name}{pk};
}

=head2 $cache->store_primary_keys_for_table($namespace, $table_name, $arrayref);

Stores to the cache an arrayref listing lowercased column names of the primary keys.  The cache file is immediately updated.  Pass an empty string if the database does not support namespaces.

=cut

sub store_primary_keys_for_table {
    my $self = shift;
    my ($namespace, $table_name, $data) = @_;
    $self->{__databag}{$namespace || '(none)'}{$table_name}{pk} = $data;
    $self->write_cache_file();
}

=head2 $cache->clear();

Clears the cache by deleting the cache file.

=cut

sub clear {
    my $cache = shift;
    my $filename = Class::ReluctantORM->get_global_option('schema_cache_file');
    if ($filename && -e $filename) {
        unless (unlink ($filename)) {
            carp ("Could not delete $filename to clear schema cache");
        }
    }
}

sub databag {
    my $self = shift;
    return $self->{__databag};
}

sub read_cache_file {
    my $self = shift;
    my $filename = Class::ReluctantORM->get_global_option('schema_cache_file');

    unless ($filename && -e $filename) {
        # No cache file; treat all as misses
        $self->{__databag} = {};
        return;
    }

    my $raw = read_file($filename);
    $self->{__databag} = json_decode($raw);
}

sub write_cache_file {
    my $self = shift;
    my $filename = Class::ReluctantORM->get_global_option('schema_cache_file');
    return unless $filename;

    my $raw = json_encode($self->{__databag});

    write_file($filename, $raw);

}

=head2 $cache->notify_sql_error($@)

Informs the cache that an error has occurred.  It may or may not be schema-cache related.

Default does nothing.

=cut

sub notify_sql_error { }


1;
