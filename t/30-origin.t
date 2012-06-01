#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's SQL parsing
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;
use Class::ReluctantORM::SQL::Aliases;

my $all = 1;
my %TEST_THIS = (
                 INIT => 1,
                 DISABLED         => $all,
                 ENABLED          => $all,
                 MULTI_CHILD_HO   => $all,
                 MULTI_CHILD_HM   => $all,
                 MULTI_CHILD_HMM  => $all,
                 MULTI_CHILD_HL   => $all,
                 FETCH_EXCEPTION  => $all,
                );

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();


my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;

if ($TEST_THIS{INIT}) {
    my $ship = Ship->create(
                            name => 'Revenge',
                            gun_count => 80,
                            waterline => 25,
                            ship_type_id => $frigate_type_id,
                           );
    my @pirates;
    foreach my $color (qw(Red Blue Green)) {
        push @pirates, Pirate->create(
                                      name => $color . ' Beard',
                                      ship => $ship,
                                     );
    }
    foreach my $island (qw(Skull Bermuda)) {
        Booty->create(
                      place => $island,
                      cash_value => 23,
                      pirates => \@pirates,
                     );
    }
}

if ($TEST_THIS{DISABLED}) {
    my ($ship, $frame, @seen, @expected);

    $ship = Ship->fetch_by_name('Revenge');

    # Should be disabled by default
    ok(!$ship->is_origin_tracking_enabled(), "tracking should be disabled by default, instance method"); $test_count++;
    ok(!Ship->is_origin_tracking_enabled(), "tracking should be disabled by default, class method"); $test_count++;
    ok(!Class::ReluctantORM->is_origin_tracking_enabled(), "tracking should be disabled by default, CRO class method"); $test_count++;
    ok(!SQL->is_origin_tracking_enabled(), "tracking should be disabled by default, SQL class"); $test_count++;

    lives_ok {
        $frame = $ship->last_origin_frame();
    } "last_origin_frame should live when tracking is disabled"; $test_count++;
    is($frame, undef, "tracking should be disabled by default"); $test_count++;

    lives_ok {
        @seen = $ship->all_origin_traces();
    } "all_origin_frames should live in list context when tracking is disabled"; $test_count++;
    @expected = ();
    is_deeply(\@seen, \@expected, "tracking should be disabled by default"); $test_count++;
}

# Probably a bad idea to do this at runtime
lives_ok {
    Class::ReluctantORM->enable_origin_tracking(1);
} "enable_origin_tracking(1) should live"; $test_count++;


if ($TEST_THIS{ENABLED}) {
    my ($ship, $line, $frame);

    ok(Class::ReluctantORM->is_origin_tracking_enabled(), "tracking should be enabled, CRO class method"); $test_count++;
    ok(Ship->is_origin_tracking_enabled(), "tracking should be enabled, for CRO subclass method"); $test_count++;

    lives_ok {
        $line = __LINE__; # this line must appear directly above the next one, no gap
        $ship = Ship->fetch_by_name('Revenge');
    } "fetch_by_name should live with origin tracking enabled"; $test_count++;

    ok($ship->is_origin_tracking_enabled(), "tracking should be enabled on an instance"); $test_count++;

    lives_ok {
        $frame = $ship->last_origin_frame();
    } "origin_frame should live when tracking is enabled"; $test_count++;
    ok(defined($frame), "origin_frame should return something when tracking is enabled"); $test_count++;
    is($frame->{file}, __FILE__, "origin_frame should have correct file"); $test_count++;
    is($frame->{line}, $line + 1, "origin_frame should have correct line number"); $test_count++;
    is($frame->{package}, __PACKAGE__, "origin_frame should have correct package"); $test_count++;

}

# Has One
if ($TEST_THIS{MULTI_CHILD_HO}) {
    my ($ship, $pirate, $line1, $line2, $frame1, $frame2, @seen);
    for my $c (Ship, Pirate, Booty) { $c->registry->purge_all(); }

    $line1 = __LINE__; # this line must appear directly above the next one, no gap
    $pirate = Pirate->fetch_by_name('Blue Beard');

    lives_ok {
        $line2 = __LINE__; # this line must appear directly above the next one, no gap
        $pirate->fetch_ship();
    } "has one fetch should live when tracking is enabled"; $test_count++;
    lives_ok {
        @seen = $pirate->all_origin_traces();
    } "all_origin_traces should live"; $test_count++;
    is((scalar @seen), 2, "pirate should have two origin traces after an afterthought fetch"); $test_count++;
    ($frame1, $frame2) = ($seen[0][0], $seen[1][0]);
    is($frame1->{file}, __FILE__, "first frame on multi-origin should have correct file"); $test_count++;
    is($frame1->{line}, $line1 + 1, "first frame on multi-origin should have correct line number"); $test_count++;
    is($frame1->{package}, __PACKAGE__, "first frame on multi-origin should have correct package"); $test_count++;
    is($frame2->{file}, __FILE__, "second frame on multi-origin should have correct file"); $test_count++;
    is($frame2->{line}, $line2 + 1, "second frame on multi-origin should have correct line number"); $test_count++;
    is($frame2->{package}, __PACKAGE__, "second frame on multi-origin should have correct package"); $test_count++;

    $ship = $pirate->ship();
    $frame1 = $ship->last_origin_frame();
    is($frame1->{file}, __FILE__, "child fetch origin should have correct file"); $test_count++;
    is($frame1->{line}, $line2 + 1, "child fetch origin should have correct line number"); $test_count++;
    is($frame1->{package}, __PACKAGE__, "child fetch should have correct package"); $test_count++;


}


# Has Many
if ($TEST_THIS{MULTI_CHILD_HM}) {
    my ($ship, $pirate, $line1, $line2, $frame1, $frame2, @seen);
    for my $c (Ship, Pirate, Booty) { $c->registry->purge_all(); }

    $line1 = __LINE__; # this line must appear directly above the next one, no gap
    $ship = Ship->fetch_by_name('Revenge');

    lives_ok {
        $line2 = __LINE__; # this line must appear directly above the next one, no gap
        $ship->pirates->fetch_all();
    } "has many fetch all should live when tracking is enabled"; $test_count++;
    lives_ok {
        @seen = $ship->all_origin_traces();
    } "all_origin_traces should live"; $test_count++;
    is((scalar @seen), 2, "ship should have two origins after an afterthought fetch"); $test_count++;
    ($frame1, $frame2) = ($seen[0][0], $seen[1][0]);
    is($frame1->{file}, __FILE__, "first frame on multi-origin should have correct file"); $test_count++;
    is($frame1->{line}, $line1 + 1, "first frame on multi-origin should have correct line number"); $test_count++;
    is($frame1->{package}, __PACKAGE__, "first frame on multi-origin should have correct package"); $test_count++;
    is($frame2->{file}, __FILE__, "second frame on multi-origin should have correct file"); $test_count++;
    is($frame2->{line}, $line2 + 1, "second frame on multi-origin should have correct line number"); $test_count++;
    is($frame2->{package}, __PACKAGE__, "second frame on multi-origin should have correct package"); $test_count++;

    $pirate = $ship->pirates->first();
    $frame1 = $pirate->last_origin_frame();
    is($frame1->{file}, __FILE__, "child fetch origin should have correct file"); $test_count++;
    is($frame1->{line}, $line2 + 1, "child fetch origin should have correct line number"); $test_count++;
    is($frame1->{package}, __PACKAGE__, "child fetch should have correct package"); $test_count++;
}

# Has Many Many
if ($TEST_THIS{MULTI_CHILD_HMM}) {
    my ($pirate, $booty, $line1, $line2, $frame1, $frame2, @seen);
    for my $c (Ship, Pirate, Booty) { $c->registry->purge_all(); }

    $line1 = __LINE__; # this line must appear directly above the next one, no gap
    $pirate = Pirate->fetch_by_name('Blue Beard');

    my $ok = 0;
    lives_ok {
        $line2 = __LINE__; # this line must appear directly above the next one, no gap
        $pirate->booties->fetch_all();
        $ok = 1;
    } "has many many fetch_all should live when tracking is enabled"; $test_count++;

    if ($ok) {
        lives_ok {
            @seen = $pirate->all_origin_traces();
        } "all_origin_traces should live"; $test_count++;
        is((scalar @seen), 2, "pirate should have two origins after an afterthought fetch"); $test_count++;
        ($frame1, $frame2) = ($seen[0][0], $seen[1][0]);
        is($frame1->{file}, __FILE__, "first frame on multi-origin should have correct file"); $test_count++;
        is($frame1->{line}, $line1 + 1, "first frame on multi-origin should have correct line number"); $test_count++;
        is($frame1->{package}, __PACKAGE__, "first frame on multi-origin should have correct package"); $test_count++;
        is($frame2->{file}, __FILE__, "second frame on multi-origin should have correct file"); $test_count++;
        is($frame2->{line}, $line2 + 1, "second frame on multi-origin should have correct line number"); $test_count++;
        is($frame2->{package}, __PACKAGE__, "second frame on multi-origin should have correct package"); $test_count++;

        $booty = $pirate->booties->first();
        $frame1 = $booty->last_origin_frame();
        is($frame1->{file}, __FILE__, "child fetch origin should have correct file"); $test_count++;
        is($frame1->{line}, $line2 + 1, "child fetch origin should have correct line number"); $test_count++;
        is($frame1->{package}, __PACKAGE__, "child fetch should have correct package"); $test_count++;
    }
}

if ($TEST_THIS{MULTI_CHILD_HL}) {
    my ($ship, $pirate, $line1, $line2, $frame1, $frame2, @seen);
    for my $c (Ship, Pirate, Booty) { $c->registry->purge_all(); }

    $line1 = __LINE__; # this line must appear directly above the next one, no gap
    $pirate = Pirate->fetch_by_name('Blue Beard');

    lives_ok {
        $line2 = __LINE__; # this line must appear directly above the next one, no gap
        $pirate->fetch_diary();
    } "has lazy fetch should live when tracking is enabled"; $test_count++;
    lives_ok {
        @seen = $pirate->all_origin_traces();
    } "all_origin_traces should live"; $test_count++;
    is((scalar @seen), 2, "pirate should have two origins after an afterthought fetch"); $test_count++;
    ($frame1, $frame2) = ($seen[0][0], $seen[1][0]);
    is($frame1->{file}, __FILE__, "first frame on multi-origin should have correct file"); $test_count++;
    is($frame1->{line}, $line1 + 1, "first frame on multi-origin should have correct line number"); $test_count++;
    is($frame1->{package}, __PACKAGE__, "first frame on multi-origin should have correct package"); $test_count++;
    is($frame2->{file}, __FILE__, "second frame on multi-origin should have correct file"); $test_count++;
    is($frame2->{line}, $line2 + 1, "second frame on multi-origin should have correct line number"); $test_count++;
    is($frame2->{package}, __PACKAGE__, "second frame on multi-origin should have correct package"); $test_count++;

}

if ($TEST_THIS{FETCH_EXCEPTION}) {
    for my $c (Ship, Pirate, Booty) { $c->registry->purge_all(); }
    my ($ship, $exception, $line, $frames, $traces);

    can_ok('Class::ReluctantORM::Exception::Data::FetchRequired', 'fetch_locations'); $test_count++;

    $line = __LINE__; # this line must appear directly above the next one, no gap
    $ship = Ship->fetch_by_name('Revenge');

    # this is supposed to die
    eval {
        $ship->pirates->all();
    };
    $exception = $@;

    ok($exception, "with origin tracking enabled, a FetchRequired should still be thrown"); $test_count++;
    isa_ok($exception, 'Class::ReluctantORM::Exception::Data::FetchRequired'); $test_count++;
    $traces = $exception->fetch_locations;
    ok(defined($traces), "fetch_locations should return something"); $test_count++;
    is(ref($traces), 'ARRAY', "traces should be an array ref"); $test_count++;
    if (ref($traces) eq 'ARRAY') {
        is((scalar @$traces), 1, "should be one origin"); $test_count++;
        is($traces->[0]->[0]->{file}, __FILE__, "exception origin should have correct file"); $test_count++;
        is($traces->[0]->[0]->{line}, $line + 1, "exception origin should have correct line number"); $test_count++;
        is($traces->[0]->[0]->{package}, __PACKAGE__, "exception origin should have correct package"); $test_count++;
    }

}


done_testing($test_count);
