#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's fetch_deep support a la v0.03 syntax
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;
use Scalar::Util qw(refaddr);


# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

my (@expected, @seen);
my ($id, $count, $collection);
my ($ship, $pirate, $booty, $rank);

my $all = 1;
my %TEST_THIS = (
                 INIT => 1,
                 SHALLOW   => $all,
                 ONE_LEVEL => $all,
                 BROAD     => $all,
                 DEEP      => $all,
                 WHERE     => $all,
                 ORDER_BY  => $all,
                 LIMIT     => $all,
                );

#....
# Populate Fixture Database
#....
if ($TEST_THIS{INIT}) {
    require "$FindBin::Bin/test-init.pl";
}

if ($TEST_THIS{SHALLOW}) {
    my (@seen, @expected);
    my ($ship);
    lives_ok {
        @seen = CrormTest::Model::Ship->fetch_deep(
                                                   name => 'Black Pearl',
                                                   with => {},
                                                  );
    } 'fetch_deep with empty with works'; $test_count++;
    is(scalar(@seen), 1, 'fetch_deep in list context returned right number of objects'); $test_count++;

    @seen = ();
    lives_ok {
        $ship = CrormTest::Model::Ship->fetch_deep(
                                                   name => 'Black Pearl',
                                                   with => {},
                                                  );
    } 'fetch_deep with empty with works'; $test_count++;
    ok($ship,'fetch_deep in scalar context returned something'); $test_count++;

    @seen = ();
    throws_ok {
        @seen = CrormTest::Model::Ship->fetch_deep(
                                                   name => 'No Such Boat',
                                                   with => {},
                                                  );
    } 'Class::ReluctantORM::Exception::Data::NotFound', 'fetch_deep throws an exception on a miss'; $test_count++;

    @seen = ();
    lives_ok {
        @seen = CrormTest::Model::Ship->search_deep(
                                                    name => 'No Such Boat',
                                                    with => {},
                                                   );
    } 'search_deep does not die on a miss'; $test_count++;
    is(scalar(@seen), 0, 'search_deep returned returns an empty list on a miss'); $test_count++;
}


if ($TEST_THIS{ONE_LEVEL}) {
    my (@seen, @expected);
    my ($ship, $pirate, $rank, $ship_id);

    lives_ok {
        $pirate = CrormTest::Model::Pirate->fetch_deep(
                                                       name => 'Sir Francis Drake',
                                                       with => {
                                                                ship => {},
                                                               },
                                                      );
    } '1-level fetch_deep works'; $test_count++;
    ok($pirate, '1-level fetch_deep returned something'); $test_count++;

    lives_ok {
        $ship = $pirate->ship();
    } 'relation access after fetch deep does not throw an exception'; $test_count++;
    ok($ship, 'relation access after fetch deep returns something'); $test_count++;
    $ship_id = Ship->fetch_by_name('Golden Hind')->id();
    is($ship ? $ship->id : undef, $ship_id, 'relation access after fetch deep returns the right thing'); $test_count++;

    lives_ok {
        $rank = $pirate->rank();
    } 'unfetched static relation access after fetch deep is OK'; $test_count++;

    undef $pirate;
    undef $rank;
    undef $ship;
    Pirate->registry->purge_all();
    lives_ok {
        $pirate = CrormTest::Model::Pirate->fetch_deep(
                                                       name => 'Sir Francis Drake',
                                                       with => {
                                                                rank => {},
                                                               },
                                                      );
    } '1-level fetch_deep works against a static relation'; $test_count++;
    ok($pirate, '1-level fetch_deep returned something'); $test_count++;
    lives_ok {
        $rank = $pirate->rank();
    } 'static relation access after fetch deep does not throw an exception'; $test_count++;
    ok($rank, 'static relation access after fetch deep returns something'); $test_count++;
    is($rank->name, 'Captain', 'static relation access after fetch deep returns the right thing'); $test_count++;

    is(refaddr($rank), refaddr(Rank->fetch_by_name('Captain')), 'static access returns the same object'); $test_count++;

    throws_ok {
        $ship = $pirate->ship();
    } 'Class::ReluctantORM::Exception::Data::FetchRequired', 'unfetched relation access after fetch deep dies'; $test_count++;
}



if ($TEST_THIS{BROAD}) {
    my (@seen, @expected);
    my ($ship, $pirate, $rank, $ship_id);

    # Note that Drake has no booty
    lives_ok {
        @seen = CrormTest::Model::Pirate->fetch_deep(
                                                     name => 'Sir Francis Drake',
                                                     with => {
                                                              ship => {},
                                                              booties => {},
                                                              rank => {},
                                                             },
                                                    );
    } '1-level fetch_deep works'; $test_count++;
    is(scalar(@seen), 1, 'broad fetch_deep returned something'); $test_count++;
    $pirate = $seen[0];

    lives_ok {
        $pirate->ship();
    } 'access relation after broad fetch_deep works'; $test_count++;
    $ship_id = Ship->fetch_by_name('Golden Hind')->id();
    is($pirate->ship->id, $ship_id, 'access relation after broad fetch_deep returns the right thing'); $test_count++;
    lives_ok {
        @seen = $pirate->booties->all();
    } 'access has_many relation after broad fetch_deep works'; $test_count++;
    is(scalar(@seen), 0, 'access empty has_many relation after broad fetch_deep return nothing'); $test_count++;

    # Wesley does have 2 booties
    @seen = ();
    undef $pirate;
    lives_ok {
        @seen = CrormTest::Model::Pirate->fetch_deep(
                                                     name => 'Wesley',
                                                     with => {
                                                              ship => {},
                                                              booties => {},
                                                              rank => {},
                                                             },
                                                    );
    } '1-level fetch_deep works'; $test_count++;
    is(scalar(@seen), 1, 'broad fetch_deep with multiple has_many_many returned right number of things'); $test_count++;
    $pirate = $seen[0];
    lives_ok {
        @seen = $pirate->booties->all();
    } 'access has_many relation after broad fetch_deep works'; $test_count++;
    is(scalar(@seen), 2, 'access empty has_many relation after broad fetch_deep returns 2 items'); $test_count++;
}


if ($TEST_THIS{DEEP}) {
    my (@seen);
    my ($ship, $pirate, $rank);

    lives_ok {
        @seen = CrormTest::Model::Ship->fetch_deep(
                                                   name => 'Revenge',
                                                   with => {
                                                            pirates => {
                                                                        rank => {},
                                                                        booties => {},
                                                                       },
                                                           },
                                                  );
    } 'Deep-level fetch_deep works'; $test_count++;
    is(scalar(@seen), 1, 'deep-level fetch returns right number of things'); $test_count++;
    $ship = $seen[0];

    lives_ok {
        @seen = $ship->pirates->all();
    } 'can access  first -level has-many after deep fetch'; $test_count++;
    is(scalar(@seen), 6, 'right number of first-level objects in deep fetch'); $test_count++;
    ($pirate) = grep {$_->name eq 'Wesley'} @seen;

    lives_ok {
        @seen = $pirate->booties->all();
    } 'can access  second-level has-many-many after deep fetch'; $test_count++;
    is(scalar(@seen), 2, 'right number of secodn-level objects in deep fetch'); $test_count++;

    lives_ok {
        $rank = $pirate->rank();
    } 'can access static second-level field after deep fetch'; $test_count++;

    is($rank, Rank->fetch_by_name('Cabin Boy'), 'static access returns the same object'); $test_count++;

}



if ($TEST_THIS{WHERE}) {
    my (@seen);
    my ($count);

    throws_ok {
        @seen = CrormTest::Model::Ship->fetch_deep(
                                                   where => "0=1",
                                                   with => {
                                                            pirates => {
                                                                        rank => {},
                                                                        booties => {},
                                                                       },
                                                           },
                                                  );
    } 'Class::ReluctantORM::Exception::Data::NotFound', 'fetch_deep with where throws an exception on a miss'; $test_count++;

    @seen = ();
    lives_ok {
        @seen = CrormTest::Model::Ship->search_deep(
                                                    where => "0=1",
                                                    with => {
                                                             pirates => {
                                                                         rank => {},
                                                                         booties => {},
                                                                        },
                                                            },
                                                   );
    } 'search_deep with where works on a miss'; $test_count++;
    is(scalar(@seen), 0, 'search_deep with where returns 0 objects on miss'); $test_count++;

    $count = Ship->count_of_ship_id();
    @seen = ();
    lives_ok {
        @seen = CrormTest::Model::Ship->search_deep(
                                                    where => "1=1",
                                                    with => {
                                                             pirates => {
                                                                         rank => {},
                                                                         booties => {},
                                                                        },
                                                            },
                                                   );
    } 'search_deep with where works on a hit'; $test_count++;
    is(scalar(@seen), $count, 'search_deep with where returns right number of objects on hit'); $test_count++;

  TODO:
    {
        local $TODO = "Column disambiguaotr seems to be broken here";
        @seen = ();
        lives_ok {
            @seen = Pirate->search_deep(
                                        where => "pirates.name = 'Red Beard'",
                                        with => {
                                                 ship => {},
                                                 captain => {},
                                                },
                                       );
        } 'search_deep with where works on a hit with a case transformed table.column crit'; $test_count++;
        is(scalar(@seen), 1, 'search_deep with where returns right number of objects'); $test_count++;
    }
}

if ($TEST_THIS{ORDER_BY}) {
    my (@seen, @expected, %ships);

    %ships = map { $_->name() => $_ } Ship->fetch_all();

    @expected = sort keys %ships;

    throws_ok {
        @seen = CrormTest::Model::Ship->search_deep(
                                                    where => "1=1",
                                                    with => {
                                                             pirates => {
                                                                         rank => {},
                                                                         booties => {},
                                                                        },
                                                            },
                                                    order_by => 'name',
                                                   );
    } 'Class::ReluctantORM::Exception::SQL::AmbiguousReference', 'search_deep dies when given an order_by clause with an ambiguous column'; $test_count++;

    @seen = ();
    @expected = sort keys %ships;
    lives_ok {
        @seen = CrormTest::Model::Ship->search_deep(
                                                    where => "1=1",
                                                    with => {
                                                             pirates => {
                                                                         rank => {},
                                                                         booties => {},
                                                                        },
                                                            },
                                                    order_by => 'ships.name',
                                                   );
    } 'search_deep works with an order_by clause'; $test_count++;
    @seen = map { $_->name } @seen;
    is_deeply(\@seen, \@expected, 'search_deep with order_by clause returned items in right order'); $test_count++;

    @seen = ();
    @expected = reverse sort keys %ships;
    lives_ok {
        @seen = CrormTest::Model::Ship->search_deep(
                                                    where => "1=1",
                                                    with => {
                                                             pirates => {
                                                                         rank => {},
                                                                         booties => {},
                                                                        },
                                                            },
                                                    order_by => 'ships.name DESC',
                                                   );
    } 'search_deep works with a DESC order_by clause'; $test_count++;
    @seen = map { $_->name } @seen;
    is_deeply(\@seen, \@expected, 'search_deep with DESC order_by clause returned items in right order'); $test_count++;
}



#....
# Limit/Offset
#....
if ($TEST_THIS{LIMIT}) {
    my (@seen, @expected, %ships);

    %ships = map { $_->name() => $_ } Ship->fetch_all();

    @expected = (sort keys %ships)[0..1];
    lives_ok {
        @seen = CrormTest::Model::Ship->search_deep(
                                                    where => "1=1",
                                                    with => {
                                                             pirates => {
                                                                         rank => {},
                                                                         booties => {},
                                                                        },
                                                            },
                                                    order_by => 'ships.name',
                                                    limit => 2,
                                                   );
    } 'search_deep works with an order_by/limit clause'; $test_count++;
    is(scalar(@seen), 2, 'search_deep with order_by/limit clause returned right number of items'); $test_count++;
    @seen = map { $_->name } @seen;
    is_deeply(\@seen, \@expected, 'search_deep with order_by/limit clause returned items in right order'); $test_count++;

    @seen = ();
    @expected = (sort keys %ships)[0..1];
    lives_ok {
        @seen = CrormTest::Model::Ship->search_deep(
                                                    where => "1=1",
                                                    with => {
                                                             pirates => {
                                                                         rank => {},
                                                                         booties => {},
                                                                        },
                                                        },
                                                    order_by => 'ships.name',
                                                    limit => 2,
                                                    offset => 0,
                                                   );
    } 'search_deep works with an order_by/limit/0 offset clause'; $test_count++;
    is(scalar(@seen), 2, 'search_deep with order_by/limit/0 offset clause returned right number of items'); $test_count++;
    @seen = map { $_->name } @seen;
    is_deeply(\@seen, \@expected, 'search_deep with order_by/limit/0 offset clause returned items in right order'); $test_count++;

    @seen = ();
    @expected = (sort keys %ships)[1..2];
    lives_ok {
        @seen = CrormTest::Model::Ship->search_deep(
                                                    where => "1=1",
                                                    with => {
                                                             pirates => {
                                                                         rank => {},
                                                                         booties => {},
                                                                        },
                                                            },
                                                    order_by => 'ships.name',
                                                    limit => 2,
                                                    offset => 1,
                                                   );
    } 'search_deep works with an order_by/limit/1 offset clause'; $test_count++;
    is(scalar(@seen), 2, 'search_deep with order_by/limit/1 offset clause returned right number of items'); $test_count++;
    @seen = map { $_->name } @seen;
    is_deeply(\@seen, \@expected, 'search_deep with order_by/limit/1 offset clause returned items in right order'); $test_count++;

    @seen = ();
    @expected = (sort keys %ships)[2..2];
    lives_ok {
        @seen = CrormTest::Model::Ship->search_deep(
                                                    where => "1=1",
                                                    with => {
                                                             pirates => {
                                                                         rank => {},
                                                                         booties => {},
                                                                        },
                                                            },
                                                    order_by => 'ships.name',
                                                    limit => 2,
                                                    offset => 2,
                                                   );
    } 'search_deep works with an order_by/limit/2 offset (underrun) clause'; $test_count++;
    is(scalar(@seen), 1, 'search_deep with order_by/limit/2 offset (underrun) clause returned right number of items'); $test_count++;
    @seen = map { $_->name } @seen;
    is_deeply(\@seen, \@expected, 'search_deep with order_by/limit/2 offset (underrun) clause returned items in right order'); $test_count++;
}

done_testing($test_count);
