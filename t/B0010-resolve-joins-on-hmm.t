#  -*-cperl-*-
use strict;
use warnings;

# Regression test for bug "Can't resolve table names uniquely when fetch-deeping two or more HasManyMany"
#   https://svn.omniti.com/trac/omniti-redteam/ticket/10

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }

use CrormTest::Model;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

my $all = 1;
my %TEST_THIS = (
                 INIT => 1,
                 NORMAL_HMM         => $all,
                 STATIC_HMM         => $all,
                 DUAL_HMM           => $all,
                 HM_NORMAL_HMM      => $all,
                 HM_STATIC_HMM      => $all,
                 HM_DUAL_HMM        => $all,
                 SCBR_STATIC_HMM    => $all,
                 HO_SCBR_STATIC_HMM => $all,
                );

my (%ships, %pirates, %booties, %ranks, %ship_types, %nationalities);


#==========================================================================#
#                              Init
#==========================================================================#

if ($TEST_THIS{INIT}) {

    # It is calling fetch_all here that causes the bug to exhibit.
    ShipType->fetch_all();

    $ships{'Revenge'} =  Ship->create(
                                      name => 'Revenge',
                                      ship_type_id => 2, # Be careful not to load a Shiptype yet - affects the test
                                                         # so hardcode a FK
                                      waterline => 50 + int(50*rand()),
                                      gun_count => 12 + int(24*rand()),
                                     );
    $ships{'Black Pearl'} =  Ship->create(
                                          name => 'Black Pearl',
                                          ship_type_id => 3, # Be careful not to load a Shiptype yet - affects the test
                                                             # so hardcode a FK
                                          waterline => 50 + int(50*rand()),
                                          gun_count => 12 + int(24*rand()),
                                         );


    foreach my $rank (Rank->fetch_all()) {
        $ranks{$rank->name} = $rank;
    }

    foreach my $nationality (Nationality->fetch_all()) {
        $nationalities{$nationality->name} = $nationality;
    }

    $pirates{'Dread Pirate Roberts'} = Pirate->create(
                                                      name => 'Dread Pirate Roberts',
                                                      ship => $ships{Revenge},
                                                      rank => $ranks{Captain},
                                                     );

    foreach my $place ('Shores of Guilder', 'Bermuda', 'Montego', 'Kokomo', 'Skull Island') {
        $booties{$place} = Booty->create(
                                         place => $place,
                                         cash_value => int(10000*rand()),
                                        );
    }

    $pirates{'Dread Pirate Roberts'}->booties->add($booties{'Shores of Guilder'});
    $pirates{'Dread Pirate Roberts'}->booties->add($booties{'Montego'});
    $pirates{'Dread Pirate Roberts'}->booties->add($booties{'Skull Island'});

    $pirates{'Dread Pirate Roberts'}->nationalities->add($nationalities{British});
    $pirates{'Dread Pirate Roberts'}->nationalities->add($nationalities{French});
}

#==========================================================================#
#                             Normal HMM
#==========================================================================#

if ($TEST_THIS{NORMAL_HMM}) {
    my $pirate;

    lives_ok {
        $pirate = Pirate->fetch_deep
          (
           name => 'Dread Pirate Roberts',
           with => {
                    booties => {},
                   },
          );
    } "Should be able to fetch_deep directly to a normal HMM";
    $test_count++;
}

if ($TEST_THIS{STATIC_HMM}) {
    my $pirate;

    lives_ok {
        $pirate = Pirate->fetch_deep
          (
           name => 'Dread Pirate Roberts',
           with => {
                    nationalities => {},
                   },
          );
    } "Should be able to fetch_deep directly to a static HMM";
    $test_count++;
}

if ($TEST_THIS{DUAL_HMM}) {
    my $pirate;

    lives_ok {
        $pirate = Pirate->fetch_deep
          (
           name => 'Dread Pirate Roberts',
           with => {
                    booties => {},
                    nationalities => {},
                   },
          );
    } "Should be able to fetch_deep directly to a normal HMM and to a static HMM";
    $test_count++;
}

if ($TEST_THIS{HM_NORMAL_HMM}) {
    my $ship;

    lives_ok {
        $ship = Ship->fetch_deep
          (
           name => 'Revenge',
           with => {
                    pirates => {
                                booties => {},
                               },
                   },
          );
    } "Should be able to fetch_deep via HM to a static HMM";
    $test_count++;
}

if ($TEST_THIS{HM_STATIC_HMM}) {
    my $ship;

    lives_ok {
        $ship = Ship->fetch_deep
          (
           name => 'Revenge',
           with => {
                    pirates => {
                                nationalities => {},
                               },
                   },
          );
    } "Should be able to fetch_deep via HM to a static HMM";
    $test_count++;
}

if ($TEST_THIS{HM_DUAL_HMM}) {
    my $ship;

    lives_ok {
        $ship = Ship->fetch_deep
          (
           name => 'Revenge',
           with => {
                    pirates => {
                                booties => {},
                                nationalities => {},
                               },
                   },
          );
    } "Should be able to fetch_deep via HM to a normal HMM and static HMM";
    $test_count++;
}

if ($TEST_THIS{SCBR_STATIC_HMM}) {
    my $ship_type;

    # Note that the SCBR classes have already been preloaded

    lives_ok {
        $ship_type = ShipType->fetch_deep
          (
           name => 'Frigate',
           with => {
                    masts => {},
                   },
          );
    } "Should be able to fetch_deep a SCBR class directly to a static HMM";
    $test_count++;
}

if ($TEST_THIS{HO_SCBR_STATIC_HMM}) {
    my $ship;

    # Note that the SCBR classes have already been preloaded

    lives_ok {
        $ship = Ship->fetch_deep
          (
           name => 'Black Pearl',
           with => {
                    ship_type => {
                                  masts => {},
                                 },
                   },
          );
    } "Should be able to fetch_deep a SCBR class via a HO to a static HMM";
    $test_count++;
}

done_testing($test_count);
