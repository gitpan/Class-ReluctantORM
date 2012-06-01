#  -*-cperl-*-
use strict;
use warnings;
#use Devel::Leak::Object qw{ GLOBAL_bless };
#$Devel::Leak::Object::TRACKSOURCELINES = 1;

# Test suite to test Class::ReluctantORM's Resgistry functionality
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }

use CrormTest::Model;

use Class::ReluctantORM::SQL::Aliases;

use Scalar::Util qw(isweak refaddr);

my (@expected, @seen, $seen, $expected);
my %TEST_THIS = (
                 INIT          => 1,
                 FETCH         => 1,
                 DESTROY       => 1,
                 LEAKS         => 1,
                 WEAK          => 1,
                 HAS_ONE       => 1,
                 HAS_MANY      => 1,
                 HAS_MANY_MANY => 1,
                );
my $ITERATIONS = 50;


# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;

# Init Block
if ($TEST_THIS{INIT}) {

    my $ship = Ship->create(
                            name => 'Revenge',
                            waterline => 80,
                            ship_type_id => $frigate_type_id,
                            gun_count => 24,
                           );
    my @pirates;
    foreach my $color (qw(Red Blue Black)) {
        push @pirates, Pirate->create(
                                      name => $color . ' Beard',
                                      ship_id => $ship->id,
                                     );
    }

    my @booties;
    foreach my $island (qw(StKitts Skull Bermuda)) {
        push @booties, Booty->create(
                                     place => $island,
                                     pirates => \@pirates,
                                     cash_value => 23,
                                    );
    }
}



if ($TEST_THIS{FETCH}) {
    my $reg = Pirate->registry();
    $reg->purge_all();

    # Get a list of all Pirates
    my @pirates = Pirate->fetch_all();
    is($reg->count(), (scalar @pirates), "registry should contain the correct number of entries after a fetch_all");
    $test_count++;

    my $id = $pirates[0]->id();
    my $pirate1 = $pirates[0];
    my $pirate2 = Pirate->fetch($id);
    is(refaddr($pirate1), refaddr($pirate2), "a pirate fetched by fetch_all() and a pirate fetched by fetch() should have the same memory location");
    $test_count++;

    my $name = $pirates[0]->name();
    $pirate2 = Pirate->search(where => 'name = ? ', execargs => [ $name ]);
    is(refaddr($pirate1), refaddr($pirate2), "a pirate fetched by fetch_all() and a pirate fetched by search() should have the same memory location");
    $test_count++;
}

if ($TEST_THIS{DESTROY}) {
    my $initial_count = Ship->registry->count();
    my $iterations = $ITERATIONS;

    # Create several hundred ships, and keep links to them in a scope
    my $mem_after_create = 0;
    {
        my @ships;
        foreach (1..$iterations) {
            push @ships, Ship->create(name => 'Ship ' . $_, waterline => 80, gun_count => 16, ship_type_id => $frigate_type_id);
        }
        is(Ship->registry->count(), $initial_count +  $iterations, "Creating $iterations ships should increase Ship registry count");
        $test_count++;
    }


    # Left scope.  Purge should have happened.
    is(Ship->registry->count(), $initial_count, "After leaving scope, Ship registry should return to initial count");
    $test_count++;

}

if ($TEST_THIS{LEAKS}) {
    #eval "use Devel::Size";
  SKIP: {
        if (1 || $@) {
            $test_count += 3;
            skip("Can't test for memory leaks without Devel::Size", 3);
        }

        my $LEAK_DUMP_REG_REPORT = 0;

        my $reg = Ship->registry();
        my $iterations = $ITERATIONS;
        my $initial_size = Devel::Size::total_size($reg);

        if ($LEAK_DUMP_REG_REPORT) {
            eval "use Devel::Size::Report;";
            diag(" Initial registry size report:");
            diag(" Reg key count: " . (scalar keys %{$reg->_hash_by_id()}));
            #diag(Devel::Size::Report::report_size($reg, { indent => "  " }));
        }

        # Create several hundred ships, and keep links to them in a scope
        my $size_after_create = 0;
        {
            my @ships;
            foreach (1..$iterations) {
                push @ships, Ship->create(name => 'Ship ' . $_, waterline => 80, gun_count => 16);
            }
            $size_after_create = Devel::Size::total_size($reg);
            ok($size_after_create > $initial_size, "Memory usage should be increase after bulk create (reg size: $size_after_create)");
            $test_count++;
        }

        if ($LEAK_DUMP_REG_REPORT) {
            diag(" Post-purge registry size report:");
            diag(" Reg key count: " . (scalar keys %{$reg->_hash_by_id()}));
            #diag(Devel::Size::Report::report_size($reg, { indent => "  " }));
        }


        my $size_after_purge = Devel::Size::total_size($reg);
        ok($size_after_purge < $size_after_create, "Memory usage should be reduced after purge (now: $size_after_purge)");
        $test_count++;
        is($size_after_purge, $initial_size, "Memory usage should be equal to initial size before purge");
        $test_count++;
    } # end skip

}

if ($TEST_THIS{WEAK}) {
    # This was created in the init block
    my $ship = Ship->fetch_by_name('Revenge');
    ok(!isweak($ship), "A CRO object should not be weak upon being fetched");
    $test_count++;

    # This is a known registry hit
    my $ship2 = Ship->fetch_by_name('Revenge');
    ok(!isweak($ship2), "A CRO object should not be weak upon being fetched on a registry hit");
    $test_count++;

    # WHITEBOX
    my $reg = Ship->registry();
    if ($reg->isa('Class::ReluctantORM::Registry::Hash')) {
        my $hash = $reg->_hash_by_id();
        ok(isweak($hash->{$ship->id()}), "A Hash registry should have weakened ref values in its hash");
        $test_count++;
    }

}

if ($TEST_THIS{HAS_ONE}) {
    # Child registry tests
    {
        my $ship = Ship->fetch_by_name('Revenge');
        my $pirate = Pirate->fetch_with_ship(Pirate->fetch_by_name('Red Beard')->id());

        # check that $pirate->ship is the same as the fetched ship
        is(refaddr($pirate->ship()), refaddr($ship), "A ship fetched by name and a pirate fetched with a ship should refer to the same ship");
        $test_count++;

        $pirate = Pirate->fetch_deep(
                                     name => 'Red Beard',
                                     with => { ship => {}},
                                    );
        is(refaddr($pirate->ship()), refaddr($ship), "A ship fetched by name and a pirate fetch_deep'd with a ship should refer to the same ship");
        $test_count++;
    }

    # Shallow existing parent tests
    {
        my $pirate1 = Pirate->fetch_by_name('Red Beard');
        my $pirate2;
        lives_ok {
            $pirate2 = Pirate->fetch_deep(
                                          name => 'Red Beard',
                                          with => { ship => {}},
                                         );
        } "Fetching an existing pirate again but deeply should live"; $test_count++;

        is(refaddr($pirate1), refaddr($pirate2), "Pirates should be same object"); $test_count++;
        ok($pirate1->is_fetched('ship'), "Deep-fetching an existing Pirate should cause its ship to be fetched"); $test_count++;
    }

    # Deep existing parent tests
    {
        my $pirate1 = Pirate->fetch_deep(
                                         name => 'Red Beard',
                                         with => { ship => {}},
                                        );
        my $pirate2;
        lives_ok {
            $pirate2 = Pirate->fetch_by_name('Red Beard');
        } "Fetching an existing pirate again but shallowly should live"; $test_count++;

        is(refaddr($pirate1), refaddr($pirate2), "Pirates should be same object"); $test_count++;
        ok($pirate2->is_fetched('ship'), "Shallow-fetching an existing deep Pirate should cause its ship to still be fetched"); $test_count++;
    }

    # Mixed existing parent tests
    {
        my $pirate1 = Pirate->fetch_deep(
                                         name => 'Red Beard',
                                         with => { booties => {}},
                                        );
        my $pirate2;
        lives_ok {
            $pirate2 = Pirate->fetch_deep(
                                          name => 'Red Beard',
                                          with => { ship => {}},
                                         );
        } "Fetching an existing pirate again but deeply (mixed) should live"; $test_count++;

        is(refaddr($pirate1), refaddr($pirate2), "Pirates should be same object"); $test_count++;
        ok($pirate1->is_fetched('ship'), "Deep-fetching an existing deep Pirate should cause its ship to still be fetched"); $test_count++;
        ok($pirate2->is_fetched('booties'), "Deep-fetching an existing deep Pirate should cause its booties to now be fetched"); $test_count++;
    }


}

if ($TEST_THIS{HAS_MANY}) {
    # Child registry tests
    {
        my $pirate = Pirate->fetch_by_name('Red Beard');
        my $ship = Ship->fetch_by_name_with_pirates('Revenge');
        my ($pirate2) = grep { $_->name eq 'Red Beard' } $ship->pirates;

        is(refaddr($pirate), refaddr($pirate2), "A pirate fetched by name and a ship fetched with pirates should refer to the same pirate");
        $test_count++;

        $ship = Ship->fetch_deep(
                                 name => 'Revenge',
                                 with => { pirates => {}},
                                );
        ($pirate2) = grep { $_->name eq 'Red Beard' } $ship->pirates;

        is(refaddr($pirate), refaddr($pirate2), "A pirate fetched by name and a ship fetch_deep'd with pirates should refer to the same pirate");
        $test_count++;
    }

    # Shallow existing parent tests
    {
        my $ship1 = Ship->fetch_by_name('Revenge');
        my $ship2;
        lives_ok {
            $ship2 = Ship->fetch_deep(
                                      name => 'Revenge',
                                      with => { pirates => {}},
                                     );
        } "Fetching an existing ship again but deeply should live"; $test_count++;
        is(refaddr($ship1), refaddr($ship2), "Ships should be same object"); $test_count++;
        ok($ship1->is_fetched('pirates'), "Deep-fetching an existing Ship should cause its pirates to be fetched"); $test_count++;
    }

    # Deep existing parent tests
    {
        my $ship1 = Ship->fetch_deep(
                                      name => 'Revenge',
                                      with => { pirates => {}},
                                     );
        my $ship2;
        lives_ok {
            $ship2 = Ship->fetch_by_name('Revenge');
        } "Fetching an existing ship again but shallowly should live"; $test_count++;
        is(refaddr($ship1), refaddr($ship2), "Ships should be same object"); $test_count++;
        ok($ship2->is_fetched('pirates'), "Shallow-fetching an existing deep Ship should cause its pirates to still be fetched"); $test_count++;
    }

    # Mixed existing parent tests
    # Currently no relations with which to test this....
}


if ($TEST_THIS{HAS_MANY_MANY}) {
    # Child registry tests
    {
        my $pirate = Pirate->fetch_by_name('Red Beard');
        my $booty = Booty->fetch_by_place_with_pirates('Bermuda');
        my ($pirate2) = grep { $_->name eq 'Red Beard' } $booty->pirates;

        is(refaddr($pirate), refaddr($pirate2), "A pirate fetched by name and a booty fetched with pirates should refer to the same pirate"); $test_count++;

        $booty = Booty->fetch_deep(
                                   place => 'Bermuda',
                                   with => { pirates => {}},
                                  );
        ($pirate2) = grep { $_->name eq 'Red Beard' } $booty->pirates;
        is(refaddr($pirate), refaddr($pirate2), "A pirate fetched by name and a booty fetch_deep'd with pirates should refer to the same pirate"); $test_count++;
    }

    # Shallow existing parent tests
    {
        my $booty1 = Booty->fetch_by_place('Bermuda');
        my $booty2;
        lives_ok {
            $booty2 = Booty->fetch_deep(
                                        place => 'Bermuda',
                                        with => { pirates => {}},
                                       );
        } "Fetching an existing Booty again but deeply should live"; $test_count++;
        is(refaddr($booty1), refaddr($booty2), "Booties should be same object"); $test_count++;
        ok($booty1->is_fetched('pirates'), "Deep-fetching an existing Booty should cause its pirates to be fetched"); $test_count++;
    }

    # Deep existing parent tests
    {
        my $booty1 = Booty->fetch_deep(
                                       place => 'Bermuda',
                                       with => { pirates => {}},
                                      );
        my $booty2;
        lives_ok {
            $booty2 = Booty->fetch_by_place('Bermuda');
        } "Fetching an existing Booty again but shallowly should live"; $test_count++;
        is(refaddr($booty1), refaddr($booty2), "Booties should be same object"); $test_count++;
        ok($booty2->is_fetched('pirates'), "Shallow-fetching an existing deep Booty should cause its pirates to still be fetched"); $test_count++;
    }

    # Mixed existing parent tests
    {
        my $pirate1 = Pirate->fetch_deep(
                                         name => 'Red Beard',
                                         with => { ship => {}},
                                        );
        my $pirate2;
        lives_ok {
            $pirate2 = Pirate->fetch_deep(
                                          name => 'Red Beard',
                                          with => { booties => {}},
                                         );
        } "Fetching an existing pirate again but deeply (mixed) should live"; $test_count++;

        is(refaddr($pirate1), refaddr($pirate2), "Pirates should be same object"); $test_count++;
        ok($pirate2->is_fetched('ship'), "Deep-fetching an existing deep Pirate should cause its ship to still be fetched"); $test_count++;
        ok($pirate1->is_fetched('booties'), "Deep-fetching an existing deep Pirate should cause its booties to now be fetched"); $test_count++;
    }

}




done_testing($test_count);
