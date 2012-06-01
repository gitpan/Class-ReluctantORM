#  -*-cperl-*-
use strict;
use warnings;
no warnings 'once';

# Test suite to test Class::ReluctantORM's fetch_deep_overlay

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

my (@expected, @seen);
our (%ships, %pirates, %booties, %ranks);
my ($id, $count, $collection);
my (%dq_args, %seen_args, @killers, @fine);
my ($ship, $pirate, $booty, $rank);


my $all = 0;
my %TEST_THIS = (
                 INIT => 1,
                 CHECK_ARGS => 1,#$all,
                 BASIC      => 1,#$all,
                 FRESHEN    => $all,
                 OVERLAY    => $all,
                 MULTI      => $all,
                );

if ($TEST_THIS{INIT}) {
    require "$FindBin::Bin/test-init.pl";
}


if ($TEST_THIS{CHECK_ARGS}) {
    my $ship = Ship->fetch_by_name('Revenge');

    # These are all against a single $ship
    my @killers = 
      (
       {
        args => [],
        msg => 'empty args not permitted',
       },
       {
        args => [ with => {} ],
        msg => 'empty with not permitted',
       },
       {
        args =>  [ { captain => {} } ],
        msg => 'single hashref not permitted',
       },
       {
        args =>  [ with => {captain => {} }, where => '1=1'],
        msg => 'where not permitted',
       },
       {
        args =>  [ with => {captain => {} }, objects => []],
        msg => 'objects not permitted on instance call',
       },
       {
        args =>  [ with => {rocket => {} }],
        msg => 'relationship name must be valid',
       },
      );

    foreach my $test (@killers) {
        my @args = @{$test->{args}};
        throws_ok {
            $ship->fetch_deep_overlay(@{$test->{args}});
        } 'Class::ReluctantORM::Exception::Param', $test-> {msg};
        $test_count++;

        # These should all be bad for the class, too, if it doesn't already have an objects line
        unless (grep {$_ eq 'objects' } @args) {
            throws_ok {
                Ship->fetch_deep_overlay(@{$test->{args}});
            } 'Class::ReluctantORM::Exception::Param', $test-> {msg};
            $test_count++;
        }
    }

    my @fine =
      (
       {
        args =>  { with => { captain => {} } },
        msg => 'single with permitted ',
       },
       {
        args =>  { with => {captain => { booties => {} } }, },
        msg => 'deep with permitted',
       },
      );

    foreach my $test (@fine) {
        lives_ok {
            $ship->fetch_deep_overlay(%{$test->{args}});
        } $test->{msg} . " in instance mode";
        $test_count++;

        lives_ok {
            Ship->fetch_deep_overlay(%{$test->{args}}, objects => []);
        } $test->{msg} . " in plural mode";
        $test_count++;
    }
}


if ($TEST_THIS{BASIC}) {
    # Instance basic overlay.  We always start with a $pirate 
    # that has its ship fetched but nothing else. In this series, the new fetches are always
    # disjoint from the old fetches - no merge conflicts.
    my @tests =
      (
       {
        label => ' one-step to has-many-many',
        args => {
                 with => {
                          booties => {},
                         },
                },
        fetch_map => { ship => {}, booties => {} },
       },
       {
        label => ' one-step to has-one reflexive',
        args => {
                 with => {
                          captain => {},
                         },
                },
        fetch_map => { ship => {}, captain => {} },
       },
       {
        label => ' one-step to static',
        args => {
                 with => {
                          rank => {},
                         },
                },
        fetch_map => { ship => {}, rank => {} },
       },
      );
    run_overlay_tests(@tests);
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

sub run_overlay_tests {
    foreach my $test (@_) {
        next if $test->{skip};
        #next unless $test->{only}; # DEBUG
        if ($test->{todo}) {
          TODO: {
                local $TODO = $test->{label};
                run_overlay_test_one($test);
                run_overlay_test_plural($test);
            }
        } else {
            run_overlay_test_one($test);
            run_overlay_test_plural($test);
        }
    }
}

sub run_overlay_test_one {
    my $test = shift;
    my $label = $test->{label};

    # initial object
    my $pirate = Pirate->fetch_deep(name => 'Wesley', with => { ship => {} });

    my $initial_dirty_field_count = scalar $pirate->dirty_fields();

    lives_ok {
        $pirate->fetch_deep_overlay(%{$test->{args}});
    } "fetch_deep_overlay (instance) on '$label' lives"; $test_count++;

    is((scalar $pirate->dirty_fields()), $initial_dirty_field_count, "'$label' dirty field count should be stable after overlay"); $test_count++;
    ok(check_fetch_map($pirate, $test->{fetch_map}), "'$label' fetch map check");  $test_count++;

    foreach my $extra (@{$test->{extra} || []}) {
        $extra->($pirate, $test);
        $test_count++;
    }
}

sub run_overlay_test_plural {
    my $test = shift;
    my $label = $test->{label};

    # initial objects
    my @pirates = Pirate->fetch_deep(where => "MACRO__base__.name LIKE '\%Beard'", with => { ship => {} });

    my $initial_dirty_field_count = scalar $pirates[0]->dirty_fields();

    lives_ok {
        Pirate->fetch_deep_overlay(%{$test->{args}}, objects => \@pirates);
    } "fetch_deep_overlay (plural) on '$label' lives"; $test_count++;

    is((scalar $pirates[0]->dirty_fields()), $initial_dirty_field_count, "'$label' dirty field count should be stable after overlay"); $test_count++;
    my $ok = 1;
    foreach my $p (@pirates) {
        $ok &&= check_fetch_map($p, $test->{fetch_map});
    }
    ok($ok, "'$label' plural fetch map check");  $test_count++;

    foreach my $extra (@{$test->{extra} || []}) {
        $extra->(\@pirates, $test);
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
