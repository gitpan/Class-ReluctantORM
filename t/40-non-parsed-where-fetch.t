#  -*-cperl-*-
use strict;
use warnings;
no warnings 'once';

# Test suite to test Class::ReluctantORM's fetch_deep support with non-parsed wheres


use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }

use CrormTest::Model;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();


# Registries screw with the fetch map tests, so disable them.
foreach my $class (Class::ReluctantORM->list_all_classes()) {
    $class->_change_registry('Class::ReluctantORM::Registry::None');
}

# Leave where parsing turned ON at the global level, so it has to respect the prase_where option


my (@expected, @seen);
my (%ships, %pirates, %booties, %ranks);
my ($id, $count, $collection);
my (%dq_args, %seen_args, @killers, @fine);
my ($ship, $pirate, $booty, $rank);

my $all = 1;
my %TEST_THIS = (
                 INIT => 1,
                 CHECK_ARGS => $all,
                 SHALLOW    => $all,
                 BROAD      => 0, #$all,  # TODO
                 DEEP       => 0, #$all,  # TODO
                 ORDER_BY   => 0, #$all,  # TODO
                 LIMIT      => 0, #$all,  # TODO
                );
my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;
if ($TEST_THIS{INIT}) {
    foreach my $name ('Black Pearl', 'Revenge', 'Golden Hind') {
        $ships{$name} =  Ship->create(
                                      name => $name,
                                      waterline => 50 + int(50*rand()),
                                      gun_count => 12 + int(24*rand()),
                                      ship_type_id => $frigate_type_id,
                                     );
    }
    # Need to be able to pick out a ship by a unique field
    $ships{'Black Pearl'}->waterline(49);
    $ships{'Black Pearl'}->save();
    

    foreach my $rank (Rank->fetch_all()) {
        $ranks{$rank->name} = $rank;
    }

    $pirates{'Dread Pirate Roberts'} = Pirate->create(
                                                      name => 'Dread Pirate Roberts',
                                                      ship => $ships{Revenge},
                                                      rank => $ranks{Captain},
                                                     );

    $pirates{'Wesley'} = Pirate->create(
                                        name => 'Wesley',
                                        ship => $ships{Revenge},
                                        rank => $ranks{'Cabin Boy'},
                                        captain => $pirates{'Dread Pirate Roberts'},
                                       );

    $pirates{'Sir Francis Drake'} = Pirate->create(
                                                   name => 'Sir Francis Drake',
                                                   ship => $ships{'Golden Hind'},
                                                   rank => $ranks{Captain},
                                                  );
    $ships{'Golden Hind'}->captain($pirates{'Sir Francis Drake'});
    $ships{'Golden Hind'}->save();

    foreach my $place ('Shores of Guilder', 'Bermuda', 'Montego', 'Kokomo', 'Skull Island') {
        $booties{$place} = Booty->create(
                                         place => $place,
                                         cash_value => int(10000*rand()),
                                        );
    }

    $pirates{'Dread Pirate Roberts'}->booties->add($booties{'Shores of Guilder'});
    $pirates{'Wesley'}->booties->add($booties{'Shores of Guilder'});
    $pirates{'Wesley'}->booties->add($booties{'Montego'});
}


if ($TEST_THIS{CHECK_ARGS}) {
    # These are all against Ship

    @fine = (

             {
              args =>  {name => 'Black Pearl'},
              msg => 'single column query permitted',
             },
             {
              args => {where => '1=1', order_by => 'name ASC', offset => 1, limit => 4},
              msg => 'order_by and limit and offset permitted',
             },
             {
              args => {where => '1=1', order_by => 'name ASC', limit => 4},
              msg => 'order_by and limit permitted',
             },
             {
              args => {where => '1=1', order_by => 'name ASC'},
              msg => 'order_by permitted',
             },
             {
              args => {where => '1=1', execargs => []},
              msg => 'zero-length execargs permitted',
             },
             {
              args => {where => 'name=?', execargs => ['Black Pearl']},
              msg => 'question mark allowed as placeholder',
             },
             {
              args => {where => '1=1', with => {}},
              msg => 'empty with permitted',
             },
             {
              args => {where => '1=1', with => { pirates => {}}},
              msg => 'simple v0.3 with permitted',
              only => 1, # DEBUG
             },
             {
              args => {where => '1=1', with => { pirates => { booties => {}, captain => {}}}},
              msg => 'complex v0.3 with permitted',
             },
             {
              msg => 'advanced with options permitted',
              args => {
                       where => '1=1',
                       with => {
                                pirates => {
                                            join_type => 'LEFT OUTER',
                                            join_on => 'ships.ship_id = pirates.ship_id AND pirates.leg_count > 2',
                                           },
                               },
                      },
              todo => 1, # TODO - parse error on join_on clause
             },
             {
              msg => 'advanced with options deeply permitted (degenerate)',
              args => {
                       where => '1=1',
                       with => {
                                pirates => {
                                            join_type => 'LEFT OUTER',
                                            join_on => 'ships.ship_id = pirates.ship_id AND pirates.leg_count > 2',
                                            with => {},
                                           },
                               },
                      },
              todo => 1, # TODO - parse error on join_on clause
             },
             {
              msg => 'advanced-then-simple with options deeply permitted',
              args => {
                       where => '1=1',
                       with => {
                                pirates => {
                                            join_type => 'LEFT OUTER',
                                            join_on => 'ships.ship_id = pirates.ship_id AND pirates.leg_count > 2',
                                            with => {
                                                     booties => {},
                                                    },
                                           },
                               },
                      },
              todo => 1, # TODO - parse error on join_on clause
             },
             {
              msg => 'advanced with options deeply permitted',
              args => {
                       where => '1=1',
                       with => {
                                pirates => {
                                            join_type => 'LEFT OUTER',
                                            join_on   => 'ships.ship_id = pirates.ship_id AND pirates.leg_count > 2',
                                            with => {
                                                     booties => {
                                                                 where => "cash_value > 1000",
                                                                },
                                                    },
                                           },
                               },
                      },
              todo => 1, # TODO - parse error on join_on clause
             },
            );

    my @nonparsed_permutations = (
                                  { parse_where => 1, },
                                  { parse_where => 0, },
                                 );
    foreach my $test (@fine) {
        #next unless $test->{only}; # DEBUG

        foreach my $permutation (@nonparsed_permutations) {
            my %additional_nonparsed_where_options = %$permutation;

            if ($test->{todo}) {
              TODO: {
                    local $TODO = $test->{msg};
                    lives_ok {
                        %seen_args = Ship->__dq_check_args(%{$test->{args}}, %additional_nonparsed_where_options);
                    } $test->{msg};
                    $test_count++;
                }
            } else {
                lives_ok {
                    %seen_args = Ship->__dq_check_args(%{$test->{args}}, %additional_nonparsed_where_options);
                } $test->{msg};
                $test_count++;
            }
        }
    }
}


if ($TEST_THIS{SHALLOW}) {
    my $saw_black_pearl = sub {
        my $ships = $_[0];
        unless (@$ships) { fail("no ship to check"); return; }
        my $ship = $ships->[0];
        is($ship->name, 'Black Pearl', "Should get the right ship");
    };


    my @tests =
      (
       {
        label => 'empty "with" parser works on list result',
        class => Ship,
        args => {
                 where => 'gun_count > 10',
                 with => {},
                },
        count => 3,
        fetch_map => {},
       },
       {
        label => 'empty with, one param',
        class => Ship,
        args => {
                 where => 'gun_count > ?',
                 execargs => [10],
                 with => {},
                },
        count => 3,
        fetch_map => {},
       },
       {
        label => 'empty with, two param',
        class => Ship,
        args => {
                 where => 'gun_count > ? AND waterline > ?',
                 execargs => [10, 40],
                 with => {},
                },
        count => 3,
        fetch_map => {},
       },

       {
        label => 'one join, unambiguous column',
        class => Ship,
        args => {
                 where => "waterline = 49",
                 with => { pirates => {}, },
                },
        count => 1, fetch_map => { pirates => {} }, extra => [ $saw_black_pearl ],
       },
       {
        label => 'one join, ambiguous column, table.col',
        class => Ship,
        args => {
                 where => "ships.name = 'Black Pearl'",
                 with => { pirates => {}, },
                },
        count => 1, fetch_map => { pirates => {} }, extra => [ $saw_black_pearl ],
       },
       {
        label => 'one join, ambiguous column, schema.table.col',
        class => Ship,
        args => {
                 where => "caribbean.ships.name = 'Black Pearl'",
                 with => { pirates => {}, },
                },
        count => 1, fetch_map => { pirates => {} }, extra => [ $saw_black_pearl ],
       },
       {
        label => 'one join, ambiguous column, "table".col',
        class => Ship,
        args => {
                 where => q{ "ships".name = 'Black Pearl' },
                 with => { pirates => {}, },
                },
        count => 1, fetch_map => { pirates => {} }, extra => [ $saw_black_pearl ],
       },
       {
        label => 'one join, ambiguous column, MACRO__base__.col',
        class => Ship,
        args => {
                 where => q{ MACRO__base__.name = 'Black Pearl' },
                 with => { pirates => {}, },
                },
        count => 1, fetch_map => { pirates => {} }, extra => [ $saw_black_pearl ],
       },
       {
        label => 'one join, ambiguous column, MACRO__parent__REL.col',
        class => Ship,
        args => {
                 where => q{ MACRO__parent__pirates__.name = 'Black Pearl' },
                 with => { pirates => {}, },
                },
        count => 1, fetch_map => { pirates => {} }, extra => [ $saw_black_pearl ],
       },
       # MACRO__child__pirates__
      );
    my @leftovers = 
      (
       {
        label => 'one step single result, inner join',
        class => Ship,
        args => {
                 name => 'Black Pearl',
                 with => {
                          pirates => {
                                      join => 'INNER',
                                      join_on => 'ships.ship_id = pirates.pirate_id AND pirates.leg_count = 400'
                                     },
                         },
                },
        count => 0,
        fetch_map => { pirates => {} },
        todo => 1, # TODO - parse error on join_on clause
       },
       {
        label => 'shallow - has_one reflexive single result',
        class => Pirate,
        args => {
                 name => 'Wesley',
                 with => { captain => {} },
                },
        count => 1,
        fetch_map => { captain => {} },
       },

      );
    run_fetch_deep_tests(@tests);
}

if ($TEST_THIS{BROAD}) {
    my @tests =
      (
       {
        label => 'broad - has_one, has_one static single result',
        class => Pirate,
        args => {
                 name => 'Wesley',
                 with => { ship => {}, rank => {} },
                },
        count => 1,
        fetch_map => { ship => {}, rank => {}},
       },
       {
        label => 'broad - has_one, has_one static multi result',
        class => Pirate,
        args => {
                 where => 'leg_count > 1',
                 with => { ship => {}, rank => {} },
                },
        count => 3,
        fetch_map => {ship => {}, rank => {}},
       },
       {
        label => 'broad - has_one, has_many single result',
        class => Pirate,
        args => {
                 name => 'Wesley',
                 with => { ship => {}, booties => {} },
                },
        count => 1,
        fetch_map => { ship => {}, booties => {}},
       },
       {
        label => 'broad - has_one, has_many static multi result',
        class => Pirate,
        args => {
                 where => 'leg_count > 1',
                 with => { ship => {}, booties => {} },
                },
        count => 3,
        fetch_map => {ship => {}, booties => {}},
       },
       {
        label => 'broad - has_one reflexive, has_many single result',
        class => Pirate,
        args => {
                 name => 'Wesley',
                 with => { captain => {}, booties => {} },
                },
        count => 1,
        fetch_map => { captain => {}, booties => {}},
       },
       {
        label => 'broad - has_one reflexive, has_many multi result',
        class => Pirate,
        args => {
                 where => 'MACRO__base__.leg_count > 1',
                 with => { captain => {}, booties => {} },
                },
        count => 3,
        fetch_map => {captain => {}, booties => {}},
       },
       {
        label => 'broad - has_one with join crit',
        class => Pirate,
        args => {
                 name => 'Wesley',
                 with => { ship => { join_on => 'pirates.ship_id = ships.ship_id AND ships.waterline = 11.5' } },
                },
        count => 1,
        fetch_map => { captain => {}, booties => {}},
        extra => [ sub { ok($_[0]->[0] && !defined($_[0]->[0]->ship), "ship should not be defined") } ],
        todo => 1, # TODO - parse error on join_on clause
       },
       # TODO - more tests on advanced where syntax
      );

    run_fetch_deep_tests(@tests);
}

if ($TEST_THIS{DEEP}) {
    my @tests =
      (
       {
        label => 'deep - has_many then has_many - single result',
        class => Ship,
        args => {
                 name => 'Black Pearl',
                 with => {
                          pirates => {
                                      with => {
                                               booties => {},
                                              },
                                     },
                         },
                },
        count => 1,
        fetch_map => { pirates => { booties => {} } },
       },
       {
        label => 'deep - has_many then has_many - multi result',
        class => Ship,
        args => {
                 where => 'gun_count > 10',
                 with => {
                          pirates => {
                                      with => {
                                               booties => {},
                                              },
                                     },
                         },
                },
        count => 3,
        fetch_map => { pirates => { booties => {} } },
       },

       {
        label => 'deep - one-of ambiguous has_one then has_many - single result',
        class => Ship,
        args => {
                 name => 'Golden Hind',
                 with => {
                          captain => {
                                      with => {
                                               booties => {},
                                              },
                                     },
                         },
                },
        count => 1,
        fetch_map => { captain => { booties => {} } },
        extra => [
                  sub {
                      my $ship = $_[0]->[0];
                      is($ship && $ship->captain && $ship->captain->name, 'Sir Francis Drake', "should have correct captain's name");
                  },
                 ],
       },
       {
        label => 'deep - (ambiguous has_one, ambiguous has_many) then has_many - single result',
        class => Ship,
        args => {
                 name => 'Golden Hind',
                 with => {
                          pirates => {},
                          captain => {
                                      with => {
                                               booties => {},
                                              },
                                     },
                         },
                },
        count => 1,
        # Note that pirate and ship have circular references due to relationship inversion
        fetch_map => { pirates => { ship => 'STOP' }, captain => { booties => {} } },
        extra => [
                  sub {
                      my $ship = $_[0]->[0];
                      is($ship && $ship->captain && $ship->captain->name, 'Sir Francis Drake', "should have correct captain's name");
                  },
                 ],
       },


       {
        label => 'deep - has_many then has_many - multi result',
        class => Ship,
        args => {
                 where => 'gun_count > 10',
                 with => {
                          captain => {
                                      with => {
                                               booties => {},
                                              },
                                     },
                         },
                },
        count => 3,
        fetch_map => { captain => { booties => {} } },
       },

       {
        label => 'deep - has_one reflexive, then has_one reflexive again - single result',
        class => Pirate,
        args => {
                 name => 'Wesley',
                 with => {
                          captain => {
                                      with => {
                                               captain => {},
                                              },
                                     },
                         },
                },
        count => 1,
        fetch_map => { captain => { captain => {} } },
       },



       # TODO - more tests on advanced where syntax
      );

    run_fetch_deep_tests(@tests);
}




























#====================================================================#

done_testing($test_count);

#====================================================================#

sub run_fetch_deep_tests {
    foreach my $test (@_) {
        next if $test->{skip};
        #next unless $test->{only}; # DEBUG
        if ($test->{todo}) {
          TODO: {
                local $TODO = $test->{label};
                run_fetch_deep_test_one($test);
            }
        } else {
            run_fetch_deep_test_one($test);
        }
    }
}

sub run_fetch_deep_test_one {
    my $test = shift;
    my $class = $test->{class};
    my $label = $test->{label} . ' where: [' . $test->{args}{where} . ']';
    @seen = ();
    lives_ok {
        @seen = $class->search_deep(%{$test->{args}}, parse_where => 0);
    } "search_deep on '$label' lives"; $test_count++;
    is((scalar @seen), $test->{count}, "'$label' result count"); $test_count++;
    if (@seen) {
        ok(check_fetch_map($seen[0], $test->{fetch_map}), "'$label' fetch map check");  $test_count++;
    }
    foreach my $extra (@{$test->{extra} || []}) {
        $extra->(\@seen, $test);
        $test_count++;
    }
}

sub check_fetch_map {
    my $cro = shift;
    my %expected_map = %{shift()};

    my @fetched_first_level =
      sort
        grep { $cro->is_fetched($_); }
          $cro->relationship_names();

    my @expected_first_level = sort keys %expected_map;
    unless (@expected_first_level == @fetched_first_level) {
        diag("Fetch map count mismatch - have ",ref($cro),"'s fetched list as ", explain(\@fetched_first_level), " but expected ", explain(\@expected_first_level));
        return 0;
    }

    foreach my $i (0..$#expected_first_level) {
        unless ($expected_first_level[$i] eq $fetched_first_level[$i]) {
            diag("Fetch map field name mismatch - have ",ref($cro),"'s fetched list as ", explain(\@fetched_first_level), " but expected ", explain(\@expected_first_level));
            return 0;
        }
    }

    my $rv = 1;
    if (@expected_first_level) {
        return 0 unless $rv;
        foreach my $relname (@expected_first_level) {
            # Check for stopper
            if ($expected_map{$relname} eq 'STOP') {
                next;
            }
            my $rel = $cro->relationships($relname);
            if ((!defined($rel->upper_multiplicity())) || $rel->upper_multiplicity() > 1) {
                my $coll = $cro->$relname();
                if ($coll->count()) {
                    # Recurse on the first entry
                    $rv &&= check_fetch_map($coll->first(), $expected_map{$relname});
                }
            } elsif ($rel->upper_multiplicity == 1) {
                my $child = $cro->$relname();
                if (defined $child) {
                    # Recurse on the single child object
                    $rv &&= check_fetch_map($child, $expected_map{$relname});
                }
            } else {
                # Has lazy or similar
            }
        }
    }
    return $rv;

}
