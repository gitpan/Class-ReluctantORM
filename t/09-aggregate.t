#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's aggregate functions

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

my (@expected, @seen);
my ($id, $ship, $result);

my @functions = qw(max min count avg stddev sum);

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

lives_ok {
    $result = CrormTest::Model::Ship->sum_of_gun_count();
} 'sum with no where should work';
$test_count++;
is($result, 72, 'sum should be correct'); $test_count++;

lives_ok {
    $result = CrormTest::Model::Ship->count_of_ship_id();
} 'count with no where should work'; $test_count++;
is($result, 3, 'count should be correct'); $test_count++;

lives_ok {
    $result = CrormTest::Model::Ship->avg_of_gun_count(where => 'waterline < 100');
} 'avg with where should work'; $test_count++;
is(int($result), 18, 'avg should be correct'); $test_count++;

done_testing($test_count);
