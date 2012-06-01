# -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's has_many_many relationship support
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

my $all = 1;
my %TEST_THIS = (
                 INIT     => 1,
                 FIELDS_AND_METHODS => $all,
                 FETCH_EMPTY        => $all,
                 ATTACH             => $all,
                 FETCH_DEEP         => $all,  # ATTACH must be enabled for the counting checks to pass
                 DELETE_ALL         => $all,
                );
my $DEBUG_REGS_SIZE = 0;

sub registry_check {
    my $line = (caller())[2];
    foreach my $class (Ship, Pirate, Booty) {
        if ($DEBUG_REGS_SIZE) {
            diag("line " . $line . ": " . $class . " pre-purge registry size is " . $class->registry->count());
        }
        $class->registry->purge_all();
    }
}

my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;
if ($TEST_THIS{INIT}) {
    my $ship = Ship->create(
                            name => 'Revenge',
                            waterline => 80,
                            gun_count => 24,
                            ship_type_id => $frigate_type_id,
                           );
    foreach my $color (qw(Red Blue Black Mint Chartreuse Lime)) {
        Pirate->create(
                       name => $color . ' Beard',
                       ship_id => $ship->id,
                      );
    }
    foreach my $place ('Bermuda', 'Skull Island', 'Pegleg Lagoon', 'Land of 1K Dances') {
        Booty->create(
                      place => $place,
                      cash_value => int(1000*rand()),
                     );
    }
    my $bt = Booty->fetch_by_place_with_pirates('Skull Island');
    foreach my $color (qw(Mint Chartreuse Lime)) {
        my $pirate = Pirate->create(
                                    name => $color . ' Beard',
                                    ship_id => $ship->id,
                                   );
        $bt->pirates->add($pirate);
    }

}
registry_check();

#....
# Class method generation
#....
if ($TEST_THIS{FIELDS_AND_METHODS}) {
    my (@seen);

    # These seem contradictory.
    # So pirates should not appear on the real fields list.....
    @seen = Pirate->field_names();
    is(scalar (grep { $_ eq 'booties' } @seen), 0, "has_many_many fields should not appear on field list");
    

    # but booties should be a has_many field????
    ok(Pirate->is_field_has_many_many('booties'), "booties should be detected as a has_many_many field");
    

    # There should be a pirates method
    can_ok(Pirate, qw(booties fetch_booties)); 
    can_ok(Booty, qw(pirates fetch_pirates)); 
}
registry_check();

#....
# Fetching and counting an empty collection
#....
if ($TEST_THIS{FETCH_EMPTY}) {
    my ($pirate, $booty_count, $collection, @seen, @expected);
    $pirate = Pirate->fetch_by_name('Red Beard');

    $query_counter->reset();
    lives_ok {
        $collection = $pirate->booties();
    } 'accessing a collection is not an exception'; 
    ok(defined($collection), 'accessing a collection returns something'); 
    is($query_counter->last_measured_value(), 0, "Accessing a collection should be 0 queries"); 
    isa_ok($collection, 'Class::ReluctantORM::Collection::ManyToMany');  

    throws_ok {
        @seen = $collection->all();
    } 'Class::ReluctantORM::Exception::Data::FetchRequired', 'calling all on a collection is an exception until it is fetched'; 

    throws_ok {
        $booty_count = $collection->count();
    } 'Class::ReluctantORM::Exception::Data::FetchRequired', 'calling count on an unfetched collection is an exception'; 

    $query_counter->reset();
    lives_ok {
        $booty_count = $collection->fetch_count();
    } 'fetch_count works'; 


    is($query_counter->last_measured_value(), 1, "fetch_count on a collection should be 1 query"); 
    is($booty_count, 0, 'fetch_count should be correct on empty collections'); 

    ok(!$collection->is_populated(), 'calling count does not fetch a collection'); 

    lives_ok {
        $booty_count = $collection->count();
    } 'count permitted on unfetched collection after a fetch_count'; 
    lives_ok {
        @seen = $collection->fetch_all();
    } 'calling fetch_all lives'; 
    @expected = ();
    is_deeply(\@seen, \@expected, 'fetch_all on an empty collection gives an empty array'); 
}
registry_check();

#....
# Attach
#....
if ($TEST_THIS{ATTACH}) {
    my ($pirate, $pirate2, $pirate_count, $booty, $booty_count, $p2booty_count, @seen);

    $pirate = Pirate->fetch_by_name('Red Beard');
    $pirate2 = Pirate->fetch_by_name('Blue Beard');
    $booty  = Booty->fetch_by_place('Bermuda');

    $pirate->booties->fetch_all();
    $pirate2->booties->fetch_all();
    $booty->pirates->fetch_all();
    $booty_count = $pirate->booties->count();
    $p2booty_count = $pirate2->booties->count();
    $pirate_count = $booty->pirates->count();

    is($booty_count, 0, "prior to attach, booty count should be 0"); 
    is($pirate_count, 0, "prior to attach, pirate count should be 0"); 
    lives_ok {
        $pirate->booties->attach($booty);
    } "attach should live"; 
    is($pirate->booties->count(), $booty_count + 1, "attach should have caused collection count to increase by 1"); 
    is($booty->pirates->count(), $pirate_count + 1, "attach should have caused inverse collection count to increase by 1"); 

    # Attach a second
    lives_ok {
        $pirate2->booties->attach($booty);
    } "attach should live"; 
    is($pirate2->booties->count(), $p2booty_count + 1, "attach should have caused collection count to increase by 1"); 
    is($booty->pirates->count(), $pirate_count + 2, "attach should have caused inverse collection count to increase by 1 + 1"); 
    

    $query_counter->reset();
    lives_ok {
        $pirate->booties->commit_pending_attachments();
    } "commit_pending_attachments should live"; 
    is($query_counter->last_measured_value(), 1, "commit_pending_attachments should be one query"); 

    TODO: 
    {
        local $TODO = 'Multiple commits from diverse children are broken';
        $query_counter->reset();
        lives_ok {
            $booty->pirates->commit_pending_attachments();
        } "commit_pending_attachments should live"; 
        is($query_counter->last_measured_value(), 1, "commit_pending_attachments should be one query"); 
        registry_check();
    }

    $pirate->booties->depopulate();
    @seen = ();
    lives_ok {
        @seen = $pirate->booties->fetch_all();
    } "pirate->booties->fetch_all should live when fetching a non-empty collection"; 
    is((scalar @seen), 1, "pirate->booties->fetch_all should return one entry");  
    is(($seen[0] ? $seen[0]->place() : undef), $booty->place(), "fetch_all should have returned the right child"); 
    registry_check();

    TODO: 
    {
        local $TODO = 'Multiple commits from diverse children are broken';

        $booty = undef;
        registry_check();
        $booty = Booty->fetch_by_place('Bermuda');
        @seen = ();
        lives_ok {
            @seen = $booty->pirates->fetch_all();
        } "booty->pirates->fetch_all should live when fetching a non-empty collection"; 
        is((scalar @seen), 2, "booty->pirates->fetch_all should return two entries");  
        registry_check();
    }

}
registry_check();


#....
# Test $parent->childrens->fetch_deep(...);
#....
if ($TEST_THIS{FETCH_DEEP}) {
    my ($booty, @booties, @pirates);

    my $reset = sub {
        $booty = undef;
        @booties = ();
        @pirates = ();
        Ship->registry->purge_all();
        Pirate->registry->purge_all();
        Booty->registry->purge_all();
        $booty = Booty->fetch_by_place('Skull Island');
    };
    $reset->();

    lives_ok {
        @pirates = $booty->pirates->fetch_deep(
                                               with => {},
                                              );
    } "fetch_deep on a collection (empty with) should live"; 
    is(scalar(@pirates), 3, "Should have 3 pirates from the fetch_deep"); 
    is($booty->pirates->count(), 3, "count() should be accurate after a fetch_deep"); 
    $reset->();

    lives_ok {
        @pirates = $booty->pirates->fetch_deep(
                                            with => { ship => {} },
                                           );
    } "fetch_deep on a collection (with ship) should live"; 
    is(scalar(@pirates), 3, "Should have 3 pirates from the fetch_deep"); 
    is($booty->pirates->count(), 3, "count() should be accurate after a fetch_deep"); 
    ok($booty->pirates->first->is_fetched('ship'), "Ship should be populated after the fetch deep"); 
    $reset->();

    throws_ok {
        @pirates = $booty->pirates->fetch_deep(
                                               with => {},
                                               where => 'leg_count > 2',
                                              );
    } 'Class::ReluctantORM::Exception::Param::Spurious', "fetch_deep should reject the 'where' option"; 
    $reset->();


}

#....
# Test $parent->childrens->delete_all(...);
#....
if ($TEST_THIS{DELETE_ALL}) {
    my ($pirate);

    $pirate = Pirate->fetch_by_name_with_booties('Lime Beard');
    #is($pirate->booties->count(), 3, "count() should be accurate after a fetch_by..with"); 
    lives_ok {
        $pirate->booties->delete_all();
    } "delete_all on a fetched collection should live"; 

}

done_testing();
__END__


#....
# Add
#....

($pirate) = grep { $_->name eq 'Blue Beard' } @pirates;
($booty) = grep { $_->place eq 'Bermuda' } @booties;
$pirate->booties->fetch_all();
$booty->pirates->fetch_all();
$booty_count = $pirate->booties->count();
$pirate_count = $booty->pirates->count();

$query_counter->reset();
lives_ok {
    $pirate->booties->add($booty);
} 'add lives'; 

is($query_counter->last_measured_value(), 1, "Add should be one query"); 
is($pirate->booties->fetch_count(), $booty_count + 1, 'Add should affect the near-end relation count'); 
is($booty->pirates->fetch_count(), $pirate_count + 1, 'Add should affect the far-end relation count'); 

($booty) = grep { $_->place eq 'Skull Island' } @booties;
$pirate->booties->add($booty);
$pirate->booties->depopulate();
@seen = ();
lives_ok {
    @seen = $pirate->booties->fetch_all();
} "fetch_all should live when fetching a non-empty collection"; 
is((scalar @seen), 2, "fetch_all should return one entry");  
is(($seen[1] ? $seen[1]->place() : undef), $booty->place(), "fetch_all should have returned the right child"); 


throws_ok {
    $pirate->booties->add($booty);
} 'Class::ReluctantORM::Exception::Data::UniquenessViolation', 'prevent duplicates on many-to-many'; 


is($fixture->count_rows('caribbean.booties2pirates'), 3, 'right number of rows in the join table'); 

#....
# is_present
#....
$pirate->booties->depopulate();
throws_ok {
    $pirate->booties->is_present($booty);
} 'Class::ReluctantORM::Exception::Data::FetchRequired', 'must fetch before doing a is_present'; 
$pirate->booties->fetch_all();
ok($pirate->booties->is_present($booty), 'is_present should work positively'); 
$booty = Booty->fetch_by_place('Land of 1K Dances');
ok(!$pirate->booties->is_present($booty), 'is present works negatively'); 

#....
# Fetching and counting a non-empty collection
#....
$booty = CrormTest::Model::Booty->fetch($booties[1]->id);
lives_ok {
    $collection = $booty->pirates();
} 'accessing a collection is not an exception'; 
ok(defined($collection), 'accessing a collection returns something'); 
throws_ok {
    @seen = $collection->all();
} 'Class::ReluctantORM::Exception::Data::FetchRequired', 'calling all on a collection is an exception until it is fetched'; 
throws_ok {
    $pirate_count = $collection->count();
} 'Class::ReluctantORM::Exception::Data::FetchRequired', 'calling count on an unfetched collection is an exception'; 
lives_ok {
    $pirate_count = $collection->fetch_count();
} 'fetch_count works'; 
is($pirate_count, 1, 'fetch_count is correct on non-empty collections'); 
ok(!$collection->is_populated(), 'calling count does not fetch a collection'); 

lives_ok {
    $count1 = $collection->count();
} 'count permitted on unfetched collection after a fetch_count'; 
lives_ok {
    @seen = $collection->fetch_all();
} 'calling fetch_all works'; 
@expected = ('Blue Beard');
@seen = sort map { $_->name } @seen;
is_deeply(\@seen, \@expected, 'fetch_all on a non-empty collection gives the correct result'); 

#....
# Fetch with fetch_with
#...
$id = $pirate->id;
undef $pirate;
lives_ok {
    $pirate = CrormTest::Model::Pirate->fetch_with_booties($id);
} 'fetch_with_foo works'; 
can_ok(Pirate, qw(fetch_with_booties)); 
lives_ok {
    @seen = $pirate->booties->all();
} 'fetch all after fetch_with_foo should work'; 

#....
# Implicit creation
#....
lives_ok {
    $pirate = CrormTest::Model::Pirate->create(
                                               name => 'Glue Beard',
                                               ship => $ship,
                                               booties => \@booties,
                                              );
} 'implicit linkage to existing children on create should work'; 
ok(defined($pirate), 'create should return something'); 

lives_ok {
    @seen = $pirate->booties->all();
} 'implicit linkage should result in a pre-fetched collection'; 
@expected = sort map {$_->place} @booties;
@seen = sort map {$_->place} @seen;
is_deeply(\@seen, \@expected, 'Prefetched collection should be accurate'); 



foreach my $b (@booties) {
    $booty = CrormTest::Model::Booty->fetch_with_pirates($b->id);
    ok($booty->pirates->is_present($pirate), 'implicitly created pirate should be present in sibling relation'); 
    last;
}

ok(!$booties[0]->is_dirty, 'implicitly set sibling should not be dirty'); 


#....
# Implicit creation - cascading
#....
my @fresh_booties;
foreach my $place (qw(Jamaica Haiti Palookaville)) {
    push @fresh_booties, CrormTest::Model::Booty->new(
                                                      place => $place,
                                                     );
}

throws_ok {
    $pirate = CrormTest::Model::Pirate->new(
                                            name => 'Purple Beard',
                                            ship => $ship,
                                            booties => \@fresh_booties,
                                           );
} 'Class::ReluctantORM::Exception::Data::UnsupportedCascade', 'cascading implicit new should fail'; 
throws_ok {
    $pirate = CrormTest::Model::Pirate->create(
                                               name => 'Pink Beard',
                                               ship => $ship,
                                               booties => \@fresh_booties,
                                              );
} 'Class::ReluctantORM::Exception::Data::UnsupportedCascade', 'cascading implicit create should fail'; 

#....
# remove
#....
$booty = CrormTest::Model::Booty->fetch($booties[0]->id);
# this guy has no claim to booty
$pirate = CrormTest::Model::Pirate->create(
                                           name => 'Magenta Beard',
                                           ship => $ship,
                                          );
throws_ok {
    $booty->pirates->remove($pirate);
} 'Class::ReluctantORM::Exception::Data::FetchRequired', 'must fetch before doing a remove'; 

$booty->pirates->fetch_all();

# We know this is the wrong booty
$pirate_count = $booty->pirates->fetch_count();
lives_ok {
    $booty->pirates->remove($pirate);
} 'removing a nonexistent pirate is not an exception'; 
is($booty->pirates->fetch_count(), $pirate_count, 'A miss remove should not remove anything'); 

# An unsuccessful remove should not deplopulate the collection...
ok($booty->pirates->is_populated(), 'a miss remove should not depopulate the collection'); 

lives_ok {
    $booty->pirates->commit_pending_removals();
} 'commit pending removals should live, when no remvals are pending'; 


# Add a pirate to ship
$pirate = CrormTest::Model::Pirate->fetch_by_name('Red Beard');
$pirate_count = $booty->pirates->count();

lives_ok {
    $booty->pirates->remove($pirate);
} 'removing an existing pirate should live'; 
is($booty->pirates->count(), $pirate_count -1, 'remove hit reduces apparent collection count by 1'); 
is($booty->pirates->fetch_count(), $pirate_count, 'remove hit reduces does not affect actual collection'); 
ok(!$booty->pirates->is_present($pirate), 'pirate not present after remove'); 

lives_ok {
    $booty->pirates->commit_pending_removals();
} 'commit pending removals should live when one removal is pending'; 
is($booty->pirates->fetch_count(), $pirate_count -1, 'committed removal reduces affects actual collection size'); 


#....
# delete
#....
$booty = CrormTest::Model::Booty->fetch($booties[1]->id);
# this guy has no claim to booty
$pirate = CrormTest::Model::Pirate->create(
                                           name => 'Chartreuse Beard',
                                           ship => $ship,
                                          );
throws_ok {
    $booty->pirates->delete($pirate);
} 'Class::ReluctantORM::Exception::Data::FetchRequired', 'must fetch before doing a delete'; 

$booty->pirates->fetch_all();

# We know this is the wrong booty
$pirate_count = $booty->pirates->fetch_count();
lives_ok {
    $booty->pirates->delete($pirate);
} 'deleting a nonexistent pirate is not an exception'; 
is($booty->pirates->fetch_count(), $pirate_count, 'A miss delete should not delete anything'); 

# An unsuccessful delete should not deplopulate the collection...
ok($booty->pirates->is_populated(), 'a miss delete should not depopulate the collection'); 

$pirate = CrormTest::Model::Pirate->fetch_by_name('Blue Beard');
$pirate_count = $booty->pirates->count();

lives_ok {
    $booty->pirates->delete($pirate);
} 'deleting a existing pirate works'; 
is($booty->pirates->fetch_count, $pirate_count - 1, 'delete hit reduces collection count by 1'); 
ok(!$booty->pirates->is_present($pirate), 'pirate not present after delete'); 

#....
# delete_where
#....

$booty = CrormTest::Model::Booty->fetch_with_pirates($booties[0]->id);

$count1 = $fixture->count_rows('caribbean.booties2pirates');
lives_ok {
    $booty->pirates->delete_where("name LIKE 'fuzzybritches'");
} 'delete_where should work'; 
ok(not($booty->pirates->is_populated()), 'delete_where should depopulate the collection'); 

is($fixture->count_rows('caribbean.booties2pirates'), $count1, 'a delete_where matching nothing should not delete anything'); 
lives_ok {
    $booty->pirates->delete_where(where => 'leg_count > ?', execargs => [7]);
} 'delete_where should permit execargs'; 

ok(not($booty->pirates->is_populated()), 'delete_where should depopulate the collection'); 


# Add a pirate to booty to ensure we have two
$pirate = CrormTest::Model::Pirate->create(
                                           name => "White Beard",
                                           ship => $ship,
                                          );
$booty->pirates->add($pirate);
$count1 = $fixture->count_rows('caribbean.booties2pirates');

$booty->pirates->delete_where(where => "name = 'White Beard'");
is($fixture->count_rows('caribbean.booties2pirates'), $count1 - 1, 'narrow scoped where should only delete one row'); 
$count1 = $fixture->count_rows('caribbean.booties2pirates');

$booty->pirates->delete_where(where => "name LIKE '%Beard'");
is($fixture->count_rows('caribbean.booties2pirates'), $count1 - 2, 'broad scoped where should only delete rows associated with this parent'); 

done_testing();
