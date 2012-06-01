#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's has_many relationship support
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;

use Class::ReluctantORM::Utilities qw(nz);

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

use aliased 'Class::ReluctantORM::Monitor::QueryCount';
my $query_counter = QueryCount->new();
Class::ReluctantORM->install_global_monitor($query_counter);

my $all = 1;
my %TEST_THIS = (
                 INIT     => 1,
                 FIELDS_AND_METHODS => $all,
                 FETCH_EMPTY        => $all,
                 FETCH_NON_EMPTY    => $all,
                 SET_ON_CHILD       => $all,
                 FETCH_WITH         => $all,
                 FETCH_DEEP         => $all,
                 LIST_CONTEXT       => $all,
                 IMPLICIT_CREATION  => $all,
                 CASCADE            => $all,
                 ATTACH             => $all,
                 REMOVE             => $all,
                 ADD                => $all,
                 IS_PRESENT         => $all,
                 DELETE             => $all,
                 DELETE_WHERE       => $all,
                );
my $DEBUG_REGS_SIZE = 0;

my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;
if ($TEST_THIS{INIT}) {
    Ship->create(name => 'Black Pearl',  waterline => 80, gun_count => 24, ship_type_id => $frigate_type_id, );
    my $ship = Ship->create( name => 'Revenge', waterline => 80, gun_count => 24,ship_type_id => $frigate_type_id,);
    foreach my $color (qw(Red Blue Black)) {
        Pirate->create( name => $color . ' Beard', ship_id => $ship->id );
    }
}


if ($TEST_THIS{FIELDS_AND_METHODS}) {

    # These seem contradictory.
    # So pirates should not appear on the real fields list.....
    my @seen = CrormTest::Model::Ship->field_names();
    is(scalar (grep { $_ eq 'pirates' } @seen), 0, "has_many fields should not appear on field list");
    

    # but pirates should be a has_many field????
    ok(CrormTest::Model::Ship->is_field_has_many('pirates'), "pirates should be detected as a has_many field");
    

    # There should be a pirates method
    can_ok('CrormTest::Model::Ship', qw(pirates fetch_pirates));
    
}
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }

#....
# Fetching and counting an empty collection
#....
if ($TEST_THIS{FETCH_EMPTY}) {
    my (@seen, @expected, $ship, $count, $collection);
    $ship = CrormTest::Model::Ship->fetch_by_name('Black Pearl');
    lives_ok {
        $collection = $ship->pirates();
    } 'accessing a collection is not an exception'; 
    ok(defined($collection), 'accessing a collection returns something'); 

    throws_ok {
        @seen = $collection->all();
    } 'Class::ReluctantORM::Exception::Data::FetchRequired', 'calling all on a collection is an exception until it is fetched'; 
    @seen = ();

    throws_ok {
        $count = $collection->count();
    } 'Class::ReluctantORM::Exception::Data::FetchRequired', 'calling count on an unfetched collection is an exception'; 
    lives_ok {
        $count = $collection->fetch_count();
    } 'fetch_count works'; 

    is($count, 0, 'fetch_count is correct on empty collections'); 
    ok(!$collection->is_populated(), 'calling count does not fetch a collection'); 

    lives_ok {
        $count = $collection->count();
    } 'count permitted on unfetched collection after a fetch_count'; 
    lives_ok {
        @seen = $collection->fetch_all();
    } 'calling fetch_all works'; 
    @expected = ();
    is_deeply(\@seen, \@expected, 'fetch_all on an empty collection gives an empty array'); 
}
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }

#....
# Fetching and counting an non-empty collection
#....
if ($TEST_THIS{FETCH_NON_EMPTY}) {
    my (@seen, @expected, $ship, $count, $collection);

    $ship = CrormTest::Model::Ship->fetch_by_name('Revenge');
    lives_ok {
        $collection = $ship->pirates();
    } 'accessing a collection is not an exception'; 
    ok(defined($collection), 'accessing a collection returns something'); 
    throws_ok {
        @seen = $collection->all();
    } 'Class::ReluctantORM::Exception::Data::FetchRequired', 'calling all on a collection is an exception until it is fetched'; 
    @seen = ();

    throws_ok {
        $count = $collection->count();
    } 'Class::ReluctantORM::Exception::Data::FetchRequired', 'calling count on an unfetched collection is an exception'; 
    lives_ok {
        $count = $collection->fetch_count();
    } 'fetch_count works'; 
    is($count, 3, 'fetch_count is correct on non-empty collections'); 
    ok(!$collection->is_populated(), 'calling count does not fetch a collection'); 
    lives_ok {
        $count = $collection->count();
    } 'count permitted on unfetched collection after a fetch_count'; 
    lives_ok {
        @seen = $collection->fetch_all();
    } 'calling fetch_all works'; 
    @expected = sort map { $_ . ' Beard'} qw(Red Blue Black);
    @seen = sort map { $_->name } @seen;
    is_deeply(\@seen, \@expected, 'fetch_all on a non-empty collection gives the correct result'); 
}

if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }

#....
# Test $parent->childrens->fetch_deep(...);
#....
if ($TEST_THIS{FETCH_DEEP}) {
    my ($ship, @seen, $seen);

    my $reset = sub {
        $ship = undef;
        @seen = ();
        Ship->registry->purge_all();
        Pirate->registry->purge_all();
        $ship = Ship->fetch_by_name('Revenge');
    };
    $reset->();

    lives_ok {
        @seen = $ship->pirates->fetch_deep(
                                           with => {},
                                          );
    } "fetch_deep on a collection (empty with) should live"; 
    is(scalar(@seen), 3, "Should have 3 pirates from the fetch_deep"); 
    is($ship->pirates->count(), 3, "count() should be accurate after a fetch_deep"); 
    $reset->();

    lives_ok {
        @seen = $ship->pirates->fetch_deep(
                                           with => { booties => {} },
                                          );
    } "fetch_deep on a collection (with booties) should live"; 
    is(scalar(@seen), 3, "Should have 3 pirates from the fetch_deep"); 
    is($ship->pirates->count(), 3, "count() should be accurate after a fetch_deep"); 
    ok($ship->pirates->first->booties->is_populated(), "Booties should be populated after the fetch deep"); 
    $reset->();

    throws_ok {
        @seen = $ship->pirates->fetch_deep(
                                           with => {},
                                           where => 'leg_count > 2',
                                          );
    } 'Class::ReluctantORM::Exception::Param::Spurious', "fetch_deep should reject the 'where' option"; 
    $reset->();


}



#....
# Set on child
#....
if ($TEST_THIS{SET_ON_CHILD}) {
    my ($ship, $ship2, $pirate);

    $ship = CrormTest::Model::Ship->fetch_by_name('Black Pearl');
    $ship->pirates->fetch_count();
    $ship2 = CrormTest::Model::Ship->fetch_by_name('Revenge');
    $ship2->pirates->fetch_count();
    $pirate = CrormTest::Model::Pirate->fetch_by_name('Blue Beard');
    $pirate->ship($ship);
    $pirate->update();

    is($ship->pirates->count(),  0, 'parent should not initially be aware of changes via child'); 
    is($ship2->pirates->count(), 3, 'parent should not initially be aware of changes via child'); 

    $ship->pirates->depopulate();
    $ship2->pirates->depopulate();

    is($ship->pirates->fetch_count(), 1, 'parent should see change after a new fetch'); 
    is($ship2->pirates->fetch_count(), 2, 'parent should see change after a new fetch'); 
}
Ship->registry->purge_all();   Pirate->registry->purge_all(); # TODO - fix cyclic memory leak in above section
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }

#....
#  Test generated methods like fetch_with_pirates(id), etc
#....
if ($TEST_THIS{FETCH_WITH}) {
    my ($id, $ship, @seen);

    $id = Ship->fetch_by_name('Revenge')->id();
    lives_ok {
        $ship = CrormTest::Model::Ship->fetch_with_pirates($id);
    } 'fetch_with_foo works'; 
    can_ok('CrormTest::Model::Ship', qw(fetch_with_pirates));
    ok($ship->is_fetched('pirates'), "pirates should be fetched after fetch_with_foo"); 

    lives_ok {
        @seen = $ship->pirates->all();
    } 'all after fetch_with_foo should work'; 

    @seen = ();
    $ship = undef;

    lives_ok {
        $ship = CrormTest::Model::Ship->fetch_by_name_with_pirates('Revenge');
    } 'fetch_by_name_with_foo works'; 

    ok($ship->is_fetched('pirates'), "pirates should be fetched after fetch_by_name_with_foo"); 

    lives_ok {
        @seen = $ship->pirates->all();
    } 'all after fetch_by_name_with_foo should work'; 
}
Ship->registry->purge_all();   Pirate->registry->purge_all(); # TODO - fix cyclic memory leak in above section
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }




if ($TEST_THIS{LIST_CONTEXT}) {
    my ($ship, @seen);

    $ship = Ship->fetch_by_name('Revenge');

    # In TB/CRO v0.04 and later, this should call ->all() in list context if fetched, and return the collection otherwise
    throws_ok {
        @seen = $ship->pirates();
    } 'Class::ReluctantORM::Exception::Data::FetchRequired', 'calling the relation method in list context on an unfetched collection is an exception'; 
    @seen = ();

    $ship->pirates->fetch_all();
    lives_ok {
        @seen = $ship->pirates();
    } 'calling the relation method in list context on a fetched collection should live'; 
    is(scalar(@seen), $ship->pirates->count(), "list context result should contain right number of child objects"); 
    isa_ok($seen[0], Pirate); 
}
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }

if ($TEST_THIS{IMPLICIT_CREATION}) {
    my ($id, $ship, $ship2, @seen, @expected, @pirates);

    $id = Ship->fetch_by_name('Revenge')->id();
    foreach my $color (qw(Purple Orange Puce)) {
        push @pirates, CrormTest::Model::Pirate->create(
                                                        name => $color . ' Beard',
                                                        ship_id => $id, # Only here because ship_id is required
                                                       );
    }

    lives_ok {
        $ship2 = CrormTest::Model::Ship->create(
                                                name => 'Floaty McBoaty',
                                                waterline => 80,
                                                gun_count => 24,
                                                pirates => \@pirates,
                                                ship_type_id => $frigate_type_id,
                                               );
    } 'implicit linkage to existing children on create should work'; 
    ok(defined($ship2), 'create should return something'); 

    lives_ok {
        @seen = $ship2->pirates->all();
    } 'implicit linkage should result in a pre-fetched collection'; 
    @expected = sort map {$_->name} @pirates;
    @seen = sort map {$_->name} @seen;
    is_deeply(\@seen, \@expected, 'Prefetched collection should be accurate'); 

    # Check crosslinking
    lives_ok {
        $ship = $pirates[0]->ship();
    } 'implicity set child should have a prepopulated parent'; 

    # Children should be dirty, since thier FK to the parent just changed
    ok($pirates[0]->is_dirty, 'implicitly set child should be dirty'); 
    ok($ship && ($ship->id eq $ship2->id()), 'implicitly set parent ID should be correct'); 
    is($pirates[0]->ship_id, $ship2->id, 'implicitly set parent ID should be correct in child key field'); 
}
Ship->registry->purge_all();   Pirate->registry->purge_all(); # TODO - fix cyclic memory leak in above section
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }

#....
# Implicit creation - cascading
#....
if ($TEST_THIS{CASCADE}) {
    my (@pirates, $ship);

    foreach my $color (qw(Curly Long Fluffy)) {
        push @pirates, CrormTest::Model::Pirate->new(
                                                     name => $color . ' Beard',
                                                    );
    }

    throws_ok {
        $ship = CrormTest::Model::Ship->new(
                                            name => 'Floaty McBoaty',
                                            waterline => 80,
                                            gun_count => 24,
                                            pirates => \@pirates,
                                           );
    } 'Class::ReluctantORM::Exception::Data::UnsupportedCascade', 'cascading implicit new should fail'; 
    throws_ok {
        $ship = CrormTest::Model::Ship->create(
                                               name => 'Floaty McBoaty',
                                               waterline => 80,
                                               gun_count => 24,
                                               pirates => \@pirates,
                                              );
    } 'Class::ReluctantORM::Exception::Data::UnsupportedCascade', 'cascading implicit create should fail'; 
}
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }

#...
# Attach
#...
if ($TEST_THIS{ATTACH}) {
    my ($ship, $ship2, $pirate, $count);

    $ship = CrormTest::Model::Ship->fetch_by_name('Revenge');
    $ship2 = CrormTest::Model::Ship->create(name => 'Golden Hind', waterline => 75, gun_count => 22,ship_type_id => $frigate_type_id,);
    $ship->pirates->fetch_all();
    $ship2->pirates->fetch_all();
    $pirate = $ship->pirates->first();

    $query_counter->reset();
    $count = $ship2->pirates->count();
    lives_ok {
        $ship2->pirates->attach($pirate);
    } 'attach should live'; 
    is($query_counter->last_measured_value(), 0, "attach should be 0 queries"); 
    ok($pirate->is_dirty(), "Child should be dirty after attach"); 
    ok(!$ship2->is_dirty(), "Parent should not be dirty after attach"); 
    is($count + 1, $ship2->pirates->count(), "Collection count should have increased by one"); 
    $count = grep { $_->id() eq $pirate->id() } $ship2->pirates->all();
    is($count, 1, "New collection should now contain one copy of the child"); 
    $count = grep { $_->id() eq $pirate->id() } $ship->pirates->all();
    is($count, 0, "Old collection should now contain zero copies of the child"); 
}
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }

#...
# Remove
#...
if ($TEST_THIS{REMOVE}) {
    my ($ship, $pirate, $count);
    $ship = Ship->fetch_by_name_with_pirates('Revenge');
    $pirate = Pirate->fetch_by_name('Black Beard');

    #foreach my $p ($ship->pirates()) { diag('Revenge has pirate: ' . $p->name); }

    is((scalar grep { $_->id eq $pirate->id } $ship->pirates()), 1, "test fixture assertion: Revenge has one " . $pirate->name . " aboard"); 

    $query_counter->reset();
    $count = $ship->pirates->count();
    lives_ok {
        $ship->pirates->remove($pirate);
    } "remove should live"; 
    is($query_counter->last_measured_value(), 0, "remove should be 0 queries"); 
    ok($pirate->is_dirty(), "Child should be dirty after remove"); 
    ok(!$ship->is_dirty(), "Parent should not be dirty after remove"); 
    is($count - 1, $ship->pirates->count(), "Collection count should have decreased by one"); 
    $count = grep { nz($_->id(),0) eq nz($pirate->id(),0) } $ship->pirates->all();
    is($count, 0, "Collection should now contain zero copies of the child"); 

    {
        my $all_undef = 1;
        foreach my $key_field (Ship->relationships('pirates')->remote_key_fields()) {
            $all_undef &&= !defined($pirate->raw_field_value($key_field));
        }
        ok($all_undef, "All FK fields on a removed child should be undef"); 
    }
}
Ship->registry->purge_all();   Pirate->registry->purge_all(); # TODO - fix cyclic memory leak in above section
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }

#....
# Add
#....
if ($TEST_THIS{ADD}) {
    my ($revenge, $pearl, $pirate, $count);

    $revenge = Ship->fetch_by_name_with_pirates('Revenge');
    $pearl   = Ship->fetch_by_name_with_pirates('Black Pearl');
    $pirate  = Pirate->fetch_by_name('Red Beard');

    #foreach my $p ($revenge->pirates()) { diag('Revenge has pirate: ' . $p->name); }

    # Transfer the pirate from Revenge to Black Pearl
    $query_counter->reset();
    $count = $pearl->pirates->count();
    lives_ok {
        $pearl->pirates->add($pirate);
    } 'add works'; 
    is($query_counter->last_measured_value(), 2, "add should be 2 queries (audited pirate)"); 
    ok(!$pirate->is_dirty(), "Child should not be dirty after add"); 
    ok(!$pearl->is_dirty(), "Parent should not be dirty after add"); 
    is($count + 1, $pearl->pirates->count(), "Collection should increase by one"); 
    $count = grep { nz($_->id(),0) eq nz($pirate->id(),0) } $pearl->pirates->all();
    is($count, 1, "Collection should contain exactly one copy of the child"); 

    # Should add work on a new pirate?
    $pirate = CrormTest::Model::Pirate->new(
                                            name => 'Glue Beard',
                                           );
    ok(!$pirate->is_inserted(), "a new child should not be inserted prior to an add"); 
    lives_ok {
        $revenge->pirates->add($pirate);
    } 'adding a unsaved child should work, because add() saves the child'; 
    ok($pirate->is_inserted(), "add() should cause a new child to be inserted"); 

    # Add a pirate to ship implicitly via Pirate->new()
    $revenge->pirates->fetch_all();
    $count = $revenge->pirates->count();
    $pirate = CrormTest::Model::Pirate->create(
                                               name => "Shoe Beard",
                                               ship => $revenge,
                                              );
    is($revenge->pirates->count(), $count + 1, "Collection count should go up by one after an implicit add"); 
    ok(!$revenge->is_dirty(), "Parent should not be dirty after an implicit add"); 
    ok(!$pirate->is_dirty(), "Child should not be dirty after an implicit add"); 
    $count = grep { nz($_->id(),0) eq nz($pirate->id(),0) } $revenge->pirates->all();
    is($count, 1, "Collection should contain exactly one copy of the child"); 
}
Ship->registry->purge_all();   Pirate->registry->purge_all(); # TODO - fix cyclic memory leak in above section
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }

#....
# is_present
#....
if ($TEST_THIS{IS_PRESENT}) {
    my ($ship, $pirate);
    $ship  = CrormTest::Model::Ship->fetch_by_name('Revenge');
    $pirate = CrormTest::Model::Pirate->fetch_by_name('Black Beard');

    throws_ok {
        $ship->pirates->is_present($pirate);
    } 'Class::ReluctantORM::Exception::Data::FetchRequired', 'must fetch before doing a is_present'; 

    $ship->pirates->fetch_all();
    ok($ship->pirates->is_present($pirate), 'is present works positively'); 

    $pirate = CrormTest::Model::Pirate->fetch_by_name('Blue Beard');
    ok(not($ship->pirates->is_present($pirate)), 'is present works negatively'); 
}
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }


#...
# Delete
#...
if ($TEST_THIS{IS_PRESENT}) {
    my ($ship, $ship2, $pirate, $count);

    $ship  = CrormTest::Model::Ship->fetch_by_name('Revenge');
    $ship2  = CrormTest::Model::Ship->fetch_by_name('Black Pearl');
    $ship2->pirates->fetch_all();
    $pirate = $ship2->pirates->first();

    throws_ok {
        $ship->pirates->delete($pirate);
    } 'Class::ReluctantORM::Exception::Data::FetchRequired', 'must fetch before doing a delete'; 
    $ship->pirates->fetch_all();

    # We know this is the wrong ship
    $count = $ship->pirates->fetch_count();
    $query_counter->reset();
    lives_ok {
        # DOCS
        $ship->pirates->delete($pirate);
    } 'deleting a nonexistent pirate is not an exception'; 
    is($query_counter->last_measured_value(), 0, "A miss delete should result in 0 queries");  
    is($ship->pirates->fetch_count(), $count, 'A miss delete should not delete anything'); 

    # An unsuccessful delete should not deplopulate the collection...
    ok($ship->pirates->is_populated(), 'a miss delete should not depopulate the collection'); 

    # Add a pirate to ship
    $pirate = CrormTest::Model::Pirate->create(
                                               name => "White Beard",
                                               ship => $ship,
                                              );
    $count = $ship->pirates->count();
    $query_counter->reset();
    lives_ok {
        $ship->pirates->delete($pirate);
    } 'deleting a existing pirate works'; 
    is($query_counter->last_measured_value(), 2, "delete hit should be two queries (audited pirate)");  
    is($ship->pirates->fetch_count, $count -1, 'delete hit reduces collection count by 1'); 
}
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }


#....
# delete_where()
#....
if ($TEST_THIS{IS_PRESENT}) {
    my ($ship, $count);

    $ship  = CrormTest::Model::Ship->fetch_by_name_with_pirates('Revenge');

    $count = $fixture->count_rows('caribbean.pirates');
    $query_counter->reset();
    lives_ok {
        $ship->pirates->delete_where("name LIKE 'fuzzybritches'");
    } 'delete_where should work'; 
    is($query_counter->last_measured_value(), 1, "delete where should be one query");  
    ok(!$ship->pirates->is_populated(), 'delete_where should depopulate the collection'); 
    is($fixture->count_rows('caribbean.pirates'), $count, 'a delete_where matching nothing should not delete anything'); 

    $count = $fixture->count_rows('caribbean.pirates');
    lives_ok {
        $ship->pirates->delete_where(where => 'leg_count > ?', execargs => [7]);
    } 'delete_where should permit execargs'; 

    ok(!$ship->pirates->is_populated(), 'delete_where should depopulate the collection'); 

    $ship->pirates->delete_where(where => "name = 'Glue Beard'");
    is($fixture->count_rows('caribbean.pirates'), $count - 1, 'narrow scoped where should only delete one row'); 
    $count = $fixture->count_rows('caribbean.pirates');
    $ship->pirates->delete_where(where => "name LIKE '%Beard'");
    is($fixture->count_rows('caribbean.pirates'), 2, 'broad scoped where should only delete rows associated with this parent'); 
}
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": ship registry size: " . Ship->registry->count()); }
if ($DEBUG_REGS_SIZE) { diag(__LINE__ . ": pirate registry size: " . Pirate->registry->count()); }


done_testing();



