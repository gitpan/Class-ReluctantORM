#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's Create Retreieve Update Delete Functionality
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

use aliased 'Class::ReluctantORM::Monitor::QueryCount';
my $query_counter = QueryCount->new();
Class::ReluctantORM->install_global_monitor($query_counter);

my (@expected, @seen);
my ($id, $ship, $pirate, $booty, $rank);

# Confirm Rank is static
ok(CrormTest::Model::Rank->is_static(), 'Rank should be static');
$test_count++;
ok(not(CrormTest::Model::Ship->is_static()), 'Ship should not be static');
$test_count++;

# References for poking into Static's guts
# WHITEBOX
my $CLASS = 'CrormTest::Model::Rank';
my $OBJECTS = \%Class::ReluctantORM::Static::OBJECTS;
my $INDEXES = \%Class::ReluctantORM::Static::INDEXES;

# Confirm nothing is fetched yet
ok(not(exists $OBJECTS->{$CLASS}), 'static cache should be empty before a fetch');
$test_count++;

# Do a fetch
$query_counter->reset();
$rank = $CLASS->fetch(2);
ok($rank, 'fetch should work the first time'); $test_count++;
is($query_counter->last_measured_value(), 1, "first fetch on a static class should be one query"); $test_count++;

# Confirm fetch populated the cache
ok(exists $OBJECTS->{$CLASS}, 'static cache should be populated after any fetch');
$test_count++;

@seen = @{$OBJECTS->{$CLASS}};
@seen = sort map { $_->name } @seen;
@expected = sort ('Cabin Boy', 'Captain', 'Able Seaman');
is_deeply(\@seen, \@expected, 'Cache should contain all rows');
$test_count++;


$query_counter->reset();
$rank = $CLASS->fetch(2);
is($query_counter->last_measured_value(), 0, "second fetch on a static class should be zero queries"); $test_count++;

$query_counter->reset();
$rank = $CLASS->fetch_by_name('Captain');
is($query_counter->last_measured_value(), 0, "fetch_by_name after fetch on a static class should be zero queries"); $test_count++;

done_testing($test_count);

