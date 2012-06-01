#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's column detector
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;


#....
#  Check detected pirate columns
#....
my @expected = sort qw(pirate_id name leg_count ship_id rank_id captain_id diary);
my @seen = sort CrormTest::Model::Pirate->column_names();
is_deeply(\@seen, \@expected, "check Pirate column names");
$test_count++;

# Fields should match too
@expected = sort qw(pirate_id name leg_count ship_id rank_id captain_id diary);
@seen = sort CrormTest::Model::Pirate->field_names();
is_deeply(\@seen, \@expected, "check Pirate field names");
$test_count++;

#....
#  Check capitalized columns
#....
# FIXTURE - Ship table should use all-caps column names

# column names should always return lower case, even if they were defined in upper case
@expected = sort map { lc($_) } qw(SHIP_ID WATERLINE NAME GUN_COUNT CAPTAIN_PIRATE_ID SHIP_TYPE_ID);
@seen = sort CrormTest::Model::Ship->column_names();
is_deeply(\@seen, \@expected, "check Ship column names, capitalization check");
$test_count++;

@expected = map { lc($_) } @expected;
@seen = sort CrormTest::Model::Ship->field_names();
is_deeply(\@seen, \@expected, "check Ship field names, capitalization check");
$test_count++;

#....
#  Check mapped fields
#....
# FIXTURE - Booty class should have a 'location' column but map it as a 'place' field
@expected = sort qw(booty_id cash_value location secret_map);
@seen = sort CrormTest::Model::Booty->column_names();
is_deeply(\@seen, \@expected, "check Booty column names, mapping check");
$test_count++;

@expected = sort qw(booty_id cash_value place secret_map);
@seen = sort CrormTest::Model::Booty->field_names();
is_deeply(\@seen, \@expected, "check Booty field names, mapping check");
$test_count++;

#....
# Field Method Generation
#....
foreach my $method (qw(id pirate_id name leg_count ship_id rank_id)) {
    can_ok('CrormTest::Model::Pirate', $method);
    $test_count++;
}


done_testing($test_count);

