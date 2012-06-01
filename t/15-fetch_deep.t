#  -*-cperl-*-
use strict;
use warnings;
no warnings 'once';

# Test suite to test Class::ReluctantORM's fetch_deep support a la v0.4 syntax and above

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

# Inverse relationships screw up the fetch map expectations
Class::ReluctantORM->set_global_option('populate_inverse_relationships', 0);

my (@expected, @seen);
our (%ships, %pirates, %booties, %ranks);
my ($id, $count, $collection);
my (%dq_args, %seen_args, @killers, @fine);
my ($ship, $pirate, $booty, $rank);

my %TEST_THIS = (
                 INIT => 1,
                 CHECK_ARGS => 1,
                 SHALLOW => 1,
                 BROAD => 1,
                 DEEP => 1,
                 ORDER_BY => 1,
                 LIMIT => 1,
                );

if ($TEST_THIS{INIT}) {
    require "$FindBin::Bin/test-init.pl";
}


if ($TEST_THIS{CHECK_ARGS}) {
    # These are all against Ship
    # Args checking - these should all die
    @killers = (
                {
                 args => {},
                 msg => 'empty args not permitted',
                },
                {
                 args =>  {foo => 'bar'},
                 msg => 'nonexistant column not permitted',
                },
                {
                 args =>  {where => '1=1', name => 'Black Pearl'},
                 msg => 'where and single column combo not permitted',
                },
                {
                 args =>  {where => '1=1', limit => 4},
                 msg => 'limit without order by not permitted',
                },
                {
                 args =>  {where => '1=1', offset => 4},
                 msg => 'offset without order by not permitted',
                },
                {
                 args =>  {where => '1=1', offset => 1, limit => 4},
                 msg => 'offset and limit  without order by not permitted',
                },
                {
                 args =>  {where => 'name=?'},
                 msg => 'question mark as placeholder makes execargs required',
                },
                {
                 args =>  {where => 'name=? AND gun_count=?', execargs => ['Black Pearl']},
                 msg => 'question mark as placeholder makes execargs length be checked',
                },
                {
                 args => {name => 'foo', with => { bar => {} }},
                 msg => 'nonexistant relationship detected',
                },
                {
                 args => {name => 'foo', with => { pirates => { bar => {}}}},
                 msg => 'nonexistant deep relationship detected',
                },
                {
                 args => {name => 'foo', with => []},
                 msg => 'with must be a hashref, not arrayref',
                },
                {
                 args => {name => 'foo', with => 'bar'},
                 msg => 'with must be a hashref, not string',
                },
                {
                 msg => 'mixed simple and advanced keys in a with not permitted',
                 args => {
                          where => '1=1',
                          with => {
                                   pirates => {
                                               join => 'LEFT OUTER',
                                               join_on => 'ships.ship_id = pirates.ship_id AND pirates.leg_count > 2',
                                               goatse => 1,
                                              },
                                  },
                         },
                },
               );
    foreach my $test (@killers) {
        throws_ok {
            %seen_args = Ship->__dq_check_args(%{$test->{args}});
        } 'Class::ReluctantORM::Exception::Param', $test-> {msg};
        $test_count++;
    }

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
                                            join => 'LEFT OUTER',
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
                                            join => 'LEFT OUTER',
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
                                            join => 'LEFT OUTER',
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
                                            join => 'LEFT OUTER',
                                            join_on => 'ships.ship_id = pirates.ship_id AND pirates.leg_count > 2',
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
    foreach my $test (@fine) {
        if ($test->{todo}) {
          TODO: {
                local $TODO = $test->{msg};
                lives_ok {
                    %seen_args = Ship->__dq_check_args(%{$test->{args}});
                } $test->{msg};
                $test_count++;
            }
        } else {
            lives_ok {
                %seen_args = Ship->__dq_check_args(%{$test->{args}});
            } $test->{msg};
            $test_count++;
        }
    }
}


if ($TEST_THIS{SHALLOW}) {
    my @tests =
      (
       {
        label => 'empty "with" gives single result',
        class => Ship,
        args => {
                 name => 'Black Pearl',
                 with => {},
                },
        count => 1,
        fetch_map => {},
       },
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
        label => 'one step single result, no adv',
        class => Ship,
        args => {
                 name => 'Black Pearl',
                 with => {
                          pirates => {},
                         },
                },
        count => 1,
        fetch_map => { pirates => {} },
       },
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
        count => 7,
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
        count => 7,
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
        count => 7,
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
        fetch_map => { pirates => { }, captain => { booties => {} } },
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
    my $label = $test->{label};
    @seen = ();
    lives_ok {
        @seen = $class->search_deep(%{$test->{args}});
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

=pod

if (0) {
    #Ship->create(name => 'Black Pearl', waterline => 30, gun_count => 84);
    @seen = Pirate->search_deep(
                                                  name => 'Foo Beard',
                                                  with => {captain => {captain => {}}},
                                                 );
} 'non-empty "with" parser works on 0.3 syntax'; $test_count++;

=cut
