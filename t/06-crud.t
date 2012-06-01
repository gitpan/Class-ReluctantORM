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

my %TEST_THIS = (
                 NEW    => 1,
                 INSERT => 1,
                 FETCH  => 1,
                 CREATE => 1,
                 UPDATE => 1,
                 DELETE => 1,
                );

my ($new_ship, $new_pirate, $new_rank);

#....
# New()
#....
if ($TEST_THIS{NEW}) {
    # Empty constructor
    my (@expected, @seen);
    my ($id, $ship, $pirate, $booty, $rank);

    $ship = CrormTest::Model::Ship->new();
    ok($ship, "new() returned something"); $test_count++;
    isa_ok($ship, 'CrormTest::Model::Ship', 'new() returned a Ship'); $test_count++;
    isa_ok($ship, 'Class::ReluctantORM', 'new() returned a Class::ReluctantORM'); $test_count++;

    # Shouldn't have hit the database yet.
    ok($ship->is_dirty(), "New objects should be dirty"); $test_count++;
    ok(not($ship->id), "New objectes should not have an ID yet"); $test_count++;
    is($fixture->count_rows('caribbean.ships'),0, "new() creates no rows in the database"); $test_count++;

    # Set attributes
    $ship->name("Revenge");
    $ship->waterline(80);
    $ship->gun_count(32);
    $ship->ship_type_id(ShipType->fetch_by_name('Frigate')->id);
    ok($ship->is_dirty(), "New objects should be dirty after setting fields"); $test_count++;
    throws_ok { $ship->update(); } 'Class::ReluctantORM::Exception::Data::UpdateWithoutInsert', 'update should throw an exception if not inserted yet';
    $test_count++;

    $new_ship = $ship;
}

#....
#  Insert()
#....
if ($TEST_THIS{INSERT}) {
    my (@expected, @seen);
    my ($id, $ship, $pirate, $booty, $rank);

    $ship = $new_ship;
    lives_ok {
        $ship->insert();  
    } "insert should not throw an exception"; $test_count++;

    if ($@) {
        print STDERR $@;
    }
    ok(!$ship->is_dirty(), "Should not be dirty immediately after insert()"); $test_count++;
    ok($ship->id(), "Should have an ID after an insert"); $test_count++;
    is($fixture->count_rows('caribbean.ships'),1, "insert() created 1 row in the database");$test_count++;
    throws_ok { $ship->insert(); } 'Class::ReluctantORM::Exception::Data::AlreadyInserted', "double inserts throw exceptions"; $test_count++;
}

#....
# Fetch
#....
if ($TEST_THIS{FETCH}) {
    my (@expected, @seen);
    my ($id, $ship, $pirate, $booty, $rank);

    $ship = $new_ship;
    $id = $ship->id;
    undef $ship;

    throws_ok {
        $ship = CrormTest::Model::Ship->fetch(990099);
    } 'Class::ReluctantORM::Exception::Data::NotFound', "fetch misses throw exceptions"; $test_count++;
    lives_ok { $ship = CrormTest::Model::Ship->search(990099); } "search misses don't throw exceptions";  $test_count++;
    lives_ok {
        $ship = CrormTest::Model::Ship->search($id)
    } "search hits don't throw exceptions"; $test_count++;
    ok(defined($ship), "search returned an object"); $test_count++;
    is($ship->name, "Revenge", "fetched ship's name is correct"); $test_count++;
    ok(!$ship->is_dirty(), "Should not be dirty immediately after insert()"); $test_count++;

    lives_ok { $rank = CrormTest::Model::Rank->fetch_by_name('Cabin Boy'); } 'Fetch by name works on Ranks'; $test_count++;
    $new_rank = $rank;
}

#....
# Create
#....
if ($TEST_THIS{CREATE}) {
    my (@expected, @seen);
    my ($id, $ship, $pirate, $booty, $rank);

    $ship = $new_ship;
    lives_ok {
        $pirate = CrormTest::Model::Pirate->create(
                                                   name => "Wesley",
                                                   ship => $ship,
                                                  );
    } "create does not throw an exception"; $test_count++;
    is($fixture->count_rows('caribbean.pirates'),1, "create() created 1 row in the database"); $test_count++;
    ok(!$pirate->is_dirty(), "Should not be dirty immediately after insert()"); $test_count++;
    ok($pirate->id(), "should have an ID after an insert");$test_count++;

    # Database default fetching
    is($pirate->leg_count, 2, "refreshed pirate leg count should match database default"); $test_count++;
    is($pirate->ship_id, $ship->id, "implicit foreign key should be set on create"); $test_count++;

    $new_pirate = $pirate;
}

#....
# Update
#....
if ($TEST_THIS{UPDATE}) {
    my (@expected, @seen);
    my ($id, $ship, $pirate, $booty, $rank);

    $pirate = $new_pirate;
    $rank   = $new_rank;

    # Wesley started out as a cabin boy aboard _Revenge_.
    # NOTE: don't use rank() - that would test relations.  Instead use rank_id.
    $pirate->rank_id($rank->id);
    is($pirate->rank_id, $rank->id, "mutator should work"); $test_count++;
    ok($pirate->is_dirty(), "Should be dirty immediately after mutator call"); $test_count++;
    lives_ok {
        $pirate->update(); 
    } 'update should not throw an exception'; $test_count++;

    # Later Wesley was promoted to captain, and he changed his name.
    $rank = CrormTest::Model::Rank->fetch_by_name('Captain');
    $pirate->rank_id($rank->id);
    $pirate->name('Dread Pirate Roberts');

    # Confirm that the dirty fields list is sensible
    @expected = sort qw(rank_id name);
    @seen = sort $pirate->dirty_fields();
    is_deeply(\@seen, \@expected, 'dirty field list is sensible'); $test_count++;

    # Use save() to do the update this time
    lives_ok { $pirate->save(); } 'save() does not throw an exception when an update is needed'; $test_count++;
    throws_ok { $pirate->insert(); } 'Class::ReluctantORM::Exception::Data::AlreadyInserted', 'insert should complain if the object has already been inserted'; $test_count++;
}

#....
# Delete
#....
if ($TEST_THIS{DELETE}) {
    my (@expected, @seen);
    my ($id, $ship, $pirate, $booty, $rank);

    $ship = $new_ship;
    $pirate = $new_pirate;
    $rank   = $new_rank;

    # Try to delete the ship.  Should fail due to DB constraints.
    throws_ok { $ship->delete(); } 'Class::ReluctantORM::Exception::SQL::ExecutionError', 'deleting a referred-to ship should result in an exception.'; $test_count++;

    # Try to delete the rank.  Should fail due to Class::ReluctantORM constraints.
    throws_ok { $rank->delete(); } 'Class::ReluctantORM::Exception::Call::NotPermitted', 'deleting a undeletable item should result in an exception.'; $test_count++;

    # OK, delete Wesley.
    lives_ok { 
        $pirate->delete();
    } 'deleting a pirate should work.'; # aaaaaaasssss yoooooouuuu wiiiiiiiiiiishhhh.......
    $test_count++;
}

done_testing($test_count);

