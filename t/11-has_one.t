#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's has_one relationship support

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;

use aliased 'Class::ReluctantORM::Monitor::QueryCount';
use aliased 'Class::ReluctantORM::Monitor::Dump' => 'Monitor::Dump';

my $DEBUG = 0;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

my $query_counter = QueryCount->new();
Class::ReluctantORM->install_global_monitor($query_counter);

my (@expected, @seen);
my ($id, $ship, $ship2, $pirate, $captain, $rank);

# A Pirate has_one ship
# A Pirate has_one Pirate (Captain)

my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;

#....
# Class method generation
#....

# These seem contradictory.
# So ship should not appear on the real fields list.....
@seen = CrormTest::Model::Pirate->field_names();
is(scalar (grep { $_ eq 'ship' } @seen), 0, "has_one fields should not appear on field list");


# but ship should be a has_one field????
ok(CrormTest::Model::Pirate->is_field_has_one('ship'), "ship should be detected as a has_one field");


# There should be a ship method
can_ok('CrormTest::Model::Pirate', qw(ship_id ship fetch_ship));


# There should be captain methods as well
can_ok('CrormTest::Model::Pirate', qw(captain_id captain fetch_captain));


# There should be rank methods as well
can_ok('CrormTest::Model::Pirate', qw(rank_id rank fetch_rank));


#....
#  Basic explicit creation
#....

# Create a Ship
$ship = CrormTest::Model::Ship->create(
                                       name => 'Revenge',
                                       ship_type_id => $frigate_type_id,
                                       waterline => 80,
                                       gun_count => 24,
                                      );
CrormTest::Model::Ship->create(
                               name => 'Golden Hind',
                               ship_type_id => $frigate_type_id,
                               waterline => 75,
                               gun_count => 22,
                              );

# Create a pirate using a ship_id
$query_counter->reset();
lives_ok {
    $pirate = CrormTest::Model::Pirate->create(
                                               name => 'Puce Beard',
                                               ship_id => $ship->id,
                                              );
} 'creating a child object with a parent ID should work'; 
my $auditted_split_count = $fixture->auditted_split_count();
is($query_counter->last_measured_value(), $auditted_split_count, "creating a child object with a parent ID should be $auditted_split_count queries (audited pirate)"); 

#....
#  Fetch tests on a newly created child
#....
$query_counter->reset();
throws_ok {
    $ship2 = $pirate->ship();
} 'Class::ReluctantORM::Exception::Data::FetchRequired', 'accessing a parent object without fetching is an exception on a newly created child'; 
is($query_counter->last_measured_value(), 0, "accessing a parent object without fetching should be 0 queries"); 

undef $ship2;
$query_counter->reset();

lives_ok {
    $ship2 = $pirate->fetch_ship();
} 'accessing a parent object with fetching is not an exception on a newly created child'; 

ok(defined($ship2), 'fetch_ship returned something'); 
is($query_counter->last_measured_value(), 1, "fetch_ship should be 1 query"); 
ok($pirate->is_fetched('ship'), "relation should be marked fetched after explicit fetch"); 


lives_ok {
    $ship2 = $pirate->ship();
} 'accessing a parent object without fetching after a fetch is not an exception on a newly created child'; 


#....
#  Fetch tests on an existing child
#....

$id = $pirate->id();
undef $pirate;
undef $ship;
undef $ship2;
CrormTest::Model::Pirate->registry->purge_all();
$pirate = CrormTest::Model::Pirate->fetch($id);


$query_counter->reset();
throws_ok {
    $ship2 = $pirate->ship();
} 'Class::ReluctantORM::Exception::Data::FetchRequired', 'accessing a parent object without fetching is an exception on an existing child'; 
is($query_counter->last_measured_value(), 0, "accessing a parent object without fetching is zero queries"); 

$query_counter->reset();
lives_ok {
    $ship2 = $pirate->fetch_ship();
} 'accessing a parent object with fetching is not an exception on an existing child'; 
ok(defined($ship2), 'fetch_ship returned something'); 
ok($pirate->is_fetched('ship'), "relation should be marked fetched after explicit fetch"); 
is($query_counter->last_measured_value(), 1, "afterthought fetch should be 1 query"); 

$query_counter->reset();
lives_ok {
    $ship2 = $pirate->ship();
} 'accessing a parent object without fetching after a fetch is not an exception on an existing child'; 
is($query_counter->last_measured_value(), 0, "fetch after afterthought fetch should be 0 queries"); 

#....
#  Fetch with
#....
$id = $pirate->id();
undef $pirate;
undef $ship;
undef $ship2;

lives_ok {
    $pirate = CrormTest::Model::Pirate->fetch_with_ship($id);
} 'fetch_with_FOO works'; 
ok(defined($pirate), 'fetch_with_FOO returned something'); 
ok($pirate->is_fetched('ship'), "relation should be marked fetched after fetch_with_FOO"); 
can_ok('CrormTest::Model::Pirate', qw(fetch_with_ship));

lives_ok {
    $ship = $pirate->ship();
} 'accessing ship without fetch after fetch_with_ship is not an exception'; 
ok(defined($ship), 'accessing ship returned something'); 

# run fetch_ship and ensure query count does change
# fetch_FOO should perform a refetch
$query_counter->reset();
lives_ok {
    $ship = $pirate->fetch_ship();
} 'afterthought fetch as refresh should live'; 
ok(defined($ship), 'fetch_ship returned something'); 
ok($pirate->is_fetched('ship'), "relation should be marked fetched after explicit fetch"); 
is($query_counter->last_measured_value(), 1, "afterthought fetch as refresh should be 1 query"); 


#....
# Implicit link on create
#....
($ship) = CrormTest::Model::Ship->search_by_name('Revenge');
($rank) = CrormTest::Model::Rank->fetch_by_name('Captain');

lives_ok {
    $captain = CrormTest::Model::Pirate->create(
                                                name => 'Dread Pirate Roberts',
                                                rank => $rank,
                                                ship => $ship,
                                               );
} 'create with implicit objects should work'; 
ok(defined($captain), 'create with implicit objects should return something'); 
ok($pirate->is_fetched('ship'), "relation should be marked fetched after create with implicit objects"); 
lives_ok {
    $ship2 = $captain->ship();
} 'create with implicit objects should allow immediate access to related objects'; 
ok(defined($ship2), 'create with implicit objects should have a result for the related object'); 
is($ship2->id, $ship->id, 'create with implicit objects should return correct object'); 

#....
# Implicit link on create with new referent
#....
$ship = CrormTest::Model::Ship->new(
                                    name => 'Black Pearl',
                                    waterline => 80,
                                    gun_count => 24,
                                   );
# We might want a new exception for this
throws_ok {
    $pirate = CrormTest::Model::Pirate->create(
                                               name => 'Jolly Roger',
                                               rank => $rank,
                                               ship => $ship,
                                              );
} 'Class::ReluctantORM::Exception::Data::UnsupportedCascade', 'implicit relation create with an unsaved related object is an exception'; 

#....
# Mutators and Updates
#....
$id = $pirate->id();
undef $pirate;
undef $ship;
undef $ship2;

$pirate = CrormTest::Model::Pirate->fetch_with_ship($id);
($ship2) = CrormTest::Model::Ship->search_by_name('Golden Hind');
($ship) = CrormTest::Model::Ship->search_by_name('Revenge');

lives_ok {
    $pirate->ship($ship2);
} 'mutator by relation method works'; 
is($pirate->ship_id, $ship2->id, 'mutator by relation method changes foreign key column'); 
ok($pirate->is_dirty(), 'mutator by relation method sets dirty flag'); 
lives_ok {
    $pirate->update();
} 'update after mutator by relation method works'; 
lives_ok {
    $pirate->ship();
} 'access to related object after mutator by relation method works';  

# Now do the same using the underlying ship_id field
lives_ok {
    $pirate->ship_id($ship->id);
} 'mutator on foreign key field works';  
is($pirate->ship_id, $ship->id, 'mutator by foreign key gives right value');  
ok($pirate->is_dirty, 'mutator by foreign key sets dirty flag'); 


ok(!$pirate->is_fetched('ship'), "relation should be cleared after key change"); 
throws_ok {
    $pirate->ship();
} 'Class::ReluctantORM::Exception::Data::FetchRequired', 'mutator by foreign key clears related object, throwing an exception on access'; 


lives_ok {
    $pirate->fetch_ship();
} 'mutator by foreign key clears related object, but refetching it works'; 

ok($pirate->is_dirty, 'mutator by foreign key sets dirty flag, even after refetching related object'); 
lives_ok {
    $pirate->update();
} 'update after mutator by foreign key works'; 


#....
# Self-referential
#....
lives_ok {
    $pirate->captain($captain);
    $pirate->update();
} 'can set self-referential field'; 
my $cappy = $pirate->fetch_captain();
is(ref($pirate), ref($pirate->captain()), 'The self-referential relation should have the same class as the parent'); 

# O captain, my captain?
lives_ok {
    $captain->captain($captain);
    $captain->update();
} 'can set recursive self-referential field'; 

lives_ok {
    $cappy = $captain->captain->captain->captain->captain->captain->captain();
} 'can deeply access recursive relations'; 

done_testing();
