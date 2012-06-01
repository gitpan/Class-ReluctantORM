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

my (@expected, @seen);
my ($id, $ship, $pirate, $booty, $rank);

#....
# Fetch all
#....
lives_ok { @seen = CrormTest::Model::Ship->fetch_all(); } 'fetch all on an empty table should not throw an exception'; $test_count++;
is_deeply(\@seen, [], 'fetch_all on an empty table returns empty results'); $test_count++;

my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;

# Make some ships
CrormTest::Model::Ship->create(
                               name => 'Hispanola',
                               ship_type_id => $frigate_type_id,
                               waterline => 80,
                               gun_count => 12,
                              );
CrormTest::Model::Ship->create(
                               name => 'Revenge',
                               ship_type_id => $frigate_type_id,
                               waterline => 80,
                               gun_count => 24,
                              );
CrormTest::Model::Ship->create(
                               name => 'Black Pearl',
                               ship_type_id => $frigate_type_id,
                               waterline => 110,
                               gun_count => 36,
                              );
@seen = CrormTest::Model::Ship->fetch_all();
is(scalar(@seen), 3, 'fetch_all returns the right number of rows'); $test_count++;


# Search on name
throws_ok { @seen = CrormTest::Model::Ship->fetch_by_name('My Little Pony'); } 'Class::ReluctantORM::Exception::Data::NotFound', 'fetch miss should throw an exception'; $test_count++;
lives_ok { @seen = CrormTest::Model::Ship->search_by_name('My Little Pony'); } 'search miss should not throw an exception'; $test_count++;

lives_ok { @seen = CrormTest::Model::Ship->fetch_by_name('Hispanola'); } 'fetch hit should not throw an exception'; $test_count++;
is(scalar(@seen), 1, 'fetch on field returns one item'); $test_count++;

# Search with where clause
lives_ok {
    @seen = CrormTest::Model::Ship->search(
                                           where => "name LIKE '%n%'",
                                          );
} 'where clause search does not throw an exception'; $test_count++;
is(scalar(@seen), 2, 'two ships with "n" in their name'); $test_count++;

# Search with where clause and exerargs
lives_ok {
    @seen = CrormTest::Model::Ship->search(
                                           where => "gun_count > ?",
                                           execargs => [ 20 ]
                                          );
} 'where/exec clause search does not throw an exception'; $test_count++;
is(scalar(@seen), 2, 'two ships more than 20 guns'); $test_count++;


#....
# Order By, Limit, and Offset
#....

# fetch_all with order by clause
#lives_ok {
@seen = CrormTest::Model::Ship->fetch_all(
                                          order_by => "gun_count"
                                         );
#} 'fetch_all with order_by works';
is(scalar(@seen), 3, 'fetch_all with order_by found 3 things'); $test_count++;
@expected = qw(12 24 36);
@seen = map { $_->gun_count } @seen;
is_deeply(\@seen, \@expected, 'Got gun_counts in the right order'); $test_count++;

# fetch_all with order by clause, descending
lives_ok {
    @seen = CrormTest::Model::Ship->fetch_all(
                                              order_by => "gun_count DESC"
                                             );
} 'fetch_all with order_by works'; $test_count++;
is(scalar(@seen), 3, 'fetch_all with order_by found 3 things'); $test_count++;
@expected = reverse qw(12 24 36);
@seen = map { $_->gun_count } @seen;
is_deeply(\@seen, \@expected, 'Got gun_counts in the right order'); $test_count++;

# fetch_all with compound order by clause
lives_ok {
    @seen = CrormTest::Model::Ship->fetch_all(
                                              order_by => "waterline, gun_count DESC"
                                             );
} 'fetch_all with compound order_by works'; $test_count++;
is(scalar(@seen), 3, 'fetch_all with compound order_by found 3 things'); $test_count++;
@expected = ('Revenge', 'Hispanola', 'Black Pearl' );
@seen = map { $_->name } @seen;
is_deeply(\@seen, \@expected, 'Got ships in the right order on a compound order_by'); $test_count++;


# order by should be required when limit is provided
throws_ok {
    CrormTest::Model::Ship->fetch_all(limit => 2);
} 'Class::ReluctantORM::Exception::Param::Missing', 'order_by should be required if limit is provided'; $test_count++;

# limit should be required when offset is provided
throws_ok {
    CrormTest::Model::Ship->fetch_all(offset => 2);
} 'Class::ReluctantORM::Exception::Param::Missing', 'limit should be required if offset is provided'; $test_count++;
#}

# Note: these are SINGLE-TABLE limit/offset tests.  See each relationship and deep test files for multi-table limits.

# Order By with Limit
@seen = ();
lives_ok {
    @seen = CrormTest::Model::Ship->search(
                                           where => '1=1',
                                           order_by => 'gun_count',
                                           limit => 2,
                                          );
} 'search with order_by and limit works'; $test_count++;

is(scalar(@seen), 2, 'search with order_by and limit found right number of things'); $test_count++;
@expected = ('Hispanola', 'Revenge');
@seen = map { $_->name } @seen;
is_deeply(\@seen, \@expected, 'Got ships in the right order on a order_by/limit'); $test_count++;

# Zero Offset
@seen = ();
lives_ok {
    @seen = CrormTest::Model::Ship->search(
                                           where => '1=1',
                                           order_by => 'gun_count',
                                           limit => 2,
                                           offset => 0,
                                          );
} 'search with order_by, limit and 0 offset works'; $test_count++;
is(scalar(@seen), 2, 'search with order_by, limit, and 0 offset found right number of things'); $test_count++;
@expected = ('Hispanola', 'Revenge');
@seen = map { $_->name } @seen;
is_deeply(\@seen, \@expected, 'Got ships in the right order on a order_by/limit/0 offset'); $test_count++;

# One Offset
@seen = ();
lives_ok {
    @seen = CrormTest::Model::Ship->search(
                                           where => '1=1',
                                           order_by => 'gun_count',
                                           limit => 2,
                                           offset => 1,
                                          );
} 'search with order_by, limit and 1 offset works'; $test_count++;
is(scalar(@seen), 2, 'search with order_by, limit, and 1 offset found right number of things'); $test_count++;
@expected = ('Revenge', 'Black Pearl');
@seen = map { $_->name } @seen;
is_deeply(\@seen, \@expected, 'Got ships in the right order on a order_by/limit/1 offset'); $test_count++;

# 2 Offset (limit underrun)
@seen = ();
lives_ok {
    @seen = CrormTest::Model::Ship->search(
                                           where => '1=1',
                                           order_by => 'gun_count',
                                           limit => 2,
                                           offset => 2,
                                          );
} 'search with order_by, limit and 2 offset works'; $test_count++;
is(scalar(@seen), 1, 'search with order_by, limit, and 2 offset found right number of things'); $test_count++;
@expected = ('Black Pearl');
@seen = map { $_->name } @seen;
is_deeply(\@seen, \@expected, 'Got ships in the right order on a order_by/limit/2 offset'); $test_count++;

done_testing($test_count);
