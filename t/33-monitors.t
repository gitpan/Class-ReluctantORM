#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's SQL->inflate()
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;
use Class::ReluctantORM::SQL::Aliases;
use List::Util qw(max);
use IO::Scalar;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

my $all = 1;
my %TEST_THIS = (
                 INIT => 1,
                 DUMP            => $all,
                 QUERY_COUNTER   => $all,
                 COLUMN_COUNTER  => $all,
                 JOIN_COUNTER    => $all,
                 ROW_COUNTER     => $all,
                 ROW_SIZE        => $all,
                 QUERY_SIZE      => $all,
                 TIMER           => $all,
                );

my %QUERIES;

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
                                      diary => "I wish I had a pony.",
                                     );
    }
    foreach my $island (qw(Skull Bermuda)) {
        Booty->create(
                      place => $island,
                      cash_value => 23,
                      pirates => \@pirates,
                     );
    }
    init_queries();
}

if ($TEST_THIS{DUMP}) {
    my $info = {
                monitor => 'Dump',
                args => { },
                defaults => { },
                queries => [
                            { l => 'Single table', },
                            { c => 2, l => 'Two table',    },
                            { c => 3, l => 'Multi-Row 2 Table', },
                            { c => 4, l => '1 Table without HL', },
                            { c => 5, l => '1 Table with HL', },
                            { c => 6, l => 'Multi-Row 3 Table with Self Join',  },
                            { c => 7, l => 'Single-Row 3 Table with Self Join', },
                            { c => 8, l => 'Kitchen Sink', },
                           ],
               };
    run_log_tests($info);
}

if ($TEST_THIS{QUERY_COUNTER}) {
    my $info = {
        monitor => 'QueryCount',
        defaults => {},
        queries => [
                    { c => 1, l => 'Single table', },
                    { c => 2, l => 'Two table',    },
                    { c => 3, l => 'Multi-Row 2 Table', },
                    { c => 4, l => '1 Table without HL', },
                    { c => 5, l => '1 Table with HL', },
                    { c => 6, l => 'Multi-Row 3 Table with Self Join',  },
                    { c => 7, l => 'Single-Row 3 Table with Self Join', },
                    { c => 8, l => 'Kitchen Sink', },
                   ],
               };
    run_counter_tests($info);

}

if ($TEST_THIS{COLUMN_COUNTER}) {
    run_counter_tests
      ({
        monitor => 'ColumnCount',
        defaults => {
                     highwater_count => 5,
                     fatal_threshold => undef,
                    },
        warn  => 15,
        fatal => 20,
        queries => [
                    { c => 6,  l => 'Single table', },
                    { c => 12, l => 'Two table',    },
                    { c => 12, l => 'Multi-Row 2 Table', },
                    { c => 6,  l => '1 Table without HL', },
                    { c => 7,  l => '1 Table with HL', },
                    { c => 18, l => 'Multi-Row 3 Table with Self Join',  },
                    { c => 18, l => 'Single-Row 3 Table with Self Join', },
                    { c => 25, l => 'Kitchen Sink', },
                   ],
       });
}

if ($TEST_THIS{JOIN_COUNTER}) {
    run_counter_tests
      ({
        monitor => 'JoinCount',
        defaults => {
                     highwater_count => 5,
                     fatal_threshold => undef,
                    },
        warn  => 2,
        fatal => 4,
        queries => [
                    { c => 0,  l => 'Single table', },
                    { c => 1, l => 'Multi-Row 2 Table', },
                    { c => 1, l => 'Two table',    },
                    { c => 0,  l => '1 Table without HL', },
                    { c => 0,  l => '1 Table with HL', },
                    { c => 2,  l => 'Multi-Row 3 Table with HMM', },
                    { c => 2, l => 'Multi-Row 3 Table with Self Join',  },
                    { c => 2, l => 'Single-Row 3 Table with Self Join', },
                    { c => 5, l => 'Kitchen Sink', },
                   ],
       });
}

if ($TEST_THIS{ROW_COUNTER}) {
    run_counter_tests
      ({
        monitor => 'RowCount',
        defaults => {
                     highwater_count => 5,
                     fatal_threshold => undef,
                    },
        warn  => 3,
        fatal => 4,
        queries => [
                    { c => 1,  l => 'Single table', },
                    { c => 3, l => 'Two table',    },
                    { c => 3, l => 'Multi-Row 2 Table', },
                    { c => 3,  l => '1 Table without HL', },
                    { c => 3,  l => '1 Table with HL', },
                    { c => 6,  l => 'Multi-Row 3 Table with HMM', },
                    { c => 3, l => 'Multi-Row 3 Table with Self Join',  },
                    { c => 1, l => 'Single-Row 3 Table with Self Join', },
                    { c => 1, l => 'Kitchen Sink', },
                   ],
       });
}

if ($TEST_THIS{ROW_SIZE}) {
  TODO:
    {
        local $TODO = "These check numbers may be brittle - may be dependent on database encoding";
        run_counter_tests
          ({
            monitor => 'RowSize',
            defaults => {
                         highwater_count => 5,
                         fatal_threshold => undef,
                        },
            warn  => 25,
            fatal => 30,
            queries => [
                        { c => 14,  l => 'Single table', },
                        { c => 33, l => 'Multi-Row 2 Table', },
                        { c => 33, l => 'Two table',    },
                        { c => 19,  l => '1 Table without HL', },
                        { c => 39,  l => '1 Table with HL', },
                        { c => 29,  l => 'Multi-Row 3 Table with HMM', },
                        { c => 19, l => 'Multi-Row 3 Table with Self Join',  },
                        { c => 17, l => 'Single-Row 3 Table with Self Join', },
                        { c => 17, l => 'Kitchen Sink', },
                       ],
           });
    }
}

if ($TEST_THIS{QUERY_SIZE}) {
  TODO:
    {
        local $TODO = "These check numbers may be brittle - may be dependent on database encoding";

        run_counter_tests
          ({
            monitor => 'QuerySize',
            defaults => {
                         highwater_count => 5,
                         fatal_threshold => undef,
                        },
            warn  => 50,
            fatal => 26,
            queries => [
                        { c => 14,  l => 'Single table', },
                        { c => 96,  l => 'Multi-Row 2 Table', },
                        { c => 96,  l => 'Two table',    },
                        { c => 54,  l => '1 Table without HL', },
                        { c => 114, l => '1 Table with HL', },
                        { c => 174, l => 'Multi-Row 3 Table with HMM', },
                        { c => 54,  l => 'Multi-Row 3 Table with Self Join',  },
                        { c => 17,  l => 'Single-Row 3 Table with Self Join', },
                        { c => 17,  l => 'Kitchen Sink', },
                       ],
           });
    }
}

if ($TEST_THIS{TIMER}) {
  TODO:
    {
        local $TODO = "These check numbers are insanely brittle";
        run_counter_tests
          ({
            monitor => 'Timer',
            defaults => {
                         highwater_count => 5,
                         fatal_threshold => undef,
                        },
            warn  => 0.0002,
            fatal => 0.0010,
            queries => [
                        { r => [0.0001, 0.5000],  l => 'Single table', },
                        { r => [0.0001, 0.5000],  l => 'Multi-Row 2 Table', },
                        { r => [0.0001, 0.5000],  l => 'Two table',    },
                        { r => [0.0001, 0.5000],  l => '1 Table without HL', },
                        { r => [0.0001, 0.5000],  l => '1 Table with HL', f => 0.0001 },
                        { r => [0.0001, 0.5000],  l => 'Multi-Row 3 Table with HMM', },
                        { r => [0.0001, 0.5000],  l => 'Multi-Row 3 Table with Self Join',  },
                        { r => [0.0001, 0.5000],  l => 'Single-Row 3 Table with Self Join', },
                        { r => [0.0001, 0.5000],  l => 'Kitchen Sink', f => 0.0010 },
                       ],
           });
    }
}

sub run_log_tests {
    my $test = shift;
    my $name = $test->{monitor};
    my $mon_class = 'Class::ReluctantORM::Monitor::' . $test->{monitor};
    my $mon;

    my $log = '';
    my $io = IO::Scalar->new(\$log);

    # Constructor checks
    lives_ok {
        $mon = $mon_class->new(log => $io);
    } "$name constructor should live with only log arg"; $test_count++;
    ok($mon, "$name constructor should return something "); $test_count++;
    if ($test->{defaults}) {
        foreach my $opt (keys %{$test->{defaults}}) {
            is($mon->$opt, $test->{defaults}->{$opt}, "$name default for $opt should be correct"); $test_count++;
        }
    }
    lives_ok {
        $mon = $mon_class->new(log => $io, %{$test->{args} || {}});
    } "$name constructor should live with supplied args"; $test_count++;
    ok($mon, "$name constructor should return something "); $test_count++;

    # Global install checks
    lives_ok {
        Class::ReluctantORM->install_global_monitor($mon);
    } "$name should be able to be installed as a global monitor"; $test_count++;
    my $seen = grep { $_ eq $mon } Class::ReluctantORM->global_monitors();
    is($seen, 1, "Exactly one copy of the $name monitor should be on the global monitor list"); $test_count++;


    # run one query and confirm results
    lives_ok {
        $QUERIES{$test->{queries}->[0]->{l}}->();
    } "Single query should live with the monitor installed"; $test_count++;

    check_log($mon, $log, $test->{queries}->[0], $name, 'Single');
    $log = '';

    # Run all queries and check
    foreach my $query (@{$test->{queries}}) {
        $log = '';
        lives_ok {
            $QUERIES{$query->{l}}->();
        } ($query->{l} . " query should live with the monitor installed"); $test_count++;
        check_log($mon, $log, $query, $name, $query->{l});
    }


}

sub check_log {
    my ($mon, $log, $query, $name, $qname) = @_;
    #diag('Log size: ' . length($log));

    my $check_log = 
      !$mon->supports_measuring || 
        ($mon->log_threshold && $mon->last_measured_value >= $mon->log_threshold);

    if (!$mon->supports_measuring()) {
        foreach my $when (@Class::ReluctantORM::Monitor::WHENS) {
            if (exists $mon->when->{$when}) {
                like($log, qr{$when}, "$qname query should contain data for $when event"); $test_count++;
            }
        }
    } else {
        if ($mon->log_threshold && $mon->last_measured_value >= $mon->log_threshold) {
            my $re = $mon->measurement_label();
            like($log, qr{$re}, "$qname query log should contain a warning"); $test_count++;
        } else {
            # Log should be empty
            is($log, '', "Log should be empty if warn threshold not reached for $qname query"); $test_count++;
        }
    }

}

sub run_counter_tests {
    my $test = shift;
    my $name = $test->{monitor};
    my $mon_class = 'Class::ReluctantORM::Monitor::' . $test->{monitor};
    my $mon;

    # Constructor checks
    lives_ok {
        $mon = $mon_class->new();
    } "$name constructor should live with no args"; $test_count++;
    ok($mon, "$name constructor should return something "); $test_count++;

    if ($test->{defaults}) {
        foreach my $opt (keys %{$test->{defaults}}) {
            is($mon->$opt, $test->{defaults}->{$opt}, "$name default for $opt should be correct"); $test_count++;
        }
    }
    my $log = '';
    my $io = IO::Scalar->new(\$log);

    if ($name ne 'QueryCount') {
        lives_ok {
            $mon = $mon_class->new(log => $io, log_threshold => $test->{warn}, %{$test->{args} || {}});
        } "$name constructor should live with log and supplied args"; $test_count++;
        ok($mon, "$name constructor should return something "); $test_count++;
    }
    is($mon->last_measured_value(), 0, "$name counter should initialize at 0"); $test_count++;

    # Global install checks
    lives_ok {
        Class::ReluctantORM->install_global_monitor($mon);
    } "$name should be able to be installed as a global monitor"; $test_count++;
    my $seen = grep { $_ eq $mon } Class::ReluctantORM->global_monitors();
    is($seen, 1, "Exactly one copy of the $name monitor should be on the global monitor list"); $test_count++;


    # run one query and confirm results
    lives_ok {
        $QUERIES{$test->{queries}->[0]->{l}}->();
    } "Single query should live with the monitor installed"; $test_count++;
    check_monitor_result($mon, $test->{queries}->[0], $name, 'Single');


    lives_ok {
        $mon->reset();
    } "Reset should live after performing a query"; $test_count++;
    is($mon->last_measured_value(), 0, "$name counter should reset to 0"); $test_count++;

    # run a bunch of different queries and check results
    foreach my $query (@{$test->{queries}}) {
        $log = '';
        lives_ok {
            $QUERIES{$query->{l}}->();
        } ($query->{l} . " query should live with the monitor installed"); $test_count++;
        check_monitor_result($mon, $query, $name, $query->{l});
        check_log($mon, $log, $query, $name, $query->{l});
    }

    # OK, should have highwater info now
    if ($mon->supports_measuring) {
        my @marks = $mon->highwater_marks();        is(scalar(@marks), $mon->highwater_count(), "$name should have the correct number of highwater marks"); $test_count++;
        my $expected = max(map { $_->{measured_value} } @marks);
        is($marks[0]->{measured_value}, $expected, "$name should have the worst offender in the number 1 spot"); $test_count++;
    }

    # clear globals
    lives_ok {
        Class::ReluctantORM->remove_global_monitors();
    } "Removing all global monitors should live"; $test_count++;

    # make new monitor with low fatal limit
    if ($test->{fatal}) {
        $mon = $mon_class->new(fatal_threshold => $test->{fatal});
    } else {
        $mon = $mon_class->new();
    }


    # install as class monitor
    lives_ok {
        Pirate->install_class_monitor($mon);
    } "installing $name as a class monitor on Pirate should live"; $test_count++;

    # check on class monitor list
    $seen = grep { $_ eq $mon } Pirate->class_monitors();
    is($seen, 1, "Should be exactly 1 copy of $name on the class monitor list for Pirate"); $test_count++;
    $seen = grep { $_ eq $mon } Ship->class_monitors();
    is($seen, 0, "Should be exactly no copies of $name on the class monitor list for Ship"); $test_count++;
    $seen = grep { $_ eq $mon } Class::ReluctantORM->global_monitors();
    is($seen, 0, "Should be exactly no copies of $name on the global monitor list"); $test_count++;

    # Run and check normal
    my $query;
    if ($test->{fatal}) {
        ($query) = grep { my $d = $_->{c} || $_->{f}; defined($d) && ($d < $test->{fatal}) } @{$test->{queries}};
    } else {
        $query = $test->{queries}->[0];
    }

    lives_ok {
        $QUERIES{$query->{l}}->();
    } ($query->{l} . " (below fatal) query should live with the monitor installed"); $test_count++;

    # Run large-column query and check fatality
    if ($test->{fatal}) {
        ($query) = grep { my $d = $_->{c} || $_->{f}; defined($d) && ($d > $test->{fatal}) } @{$test->{queries}};

        if ($query) {
            throws_ok {
                $QUERIES{$query->{l}}->();
            } 'Class::ReluctantORM::Exception::SQL::AbortedByMonitor',
              ($query->{l} . " (above fatal) query should throw exception with the monitor installed"); $test_count++;
        }
    }

    lives_ok {
        Pirate->remove_class_monitors();
    } "removing class monitors for $name should live"; $test_count++;

}

sub check_monitor_result {
    my ($mon, $query, $name, $qname) = @_;
    if ($query->{r}) {
        my ($lo, $hi, $result) = ($query->{r}->[0], $query->{r}->[1], $mon->last_measured_value());
        my $ok = ($lo <= $result) && ($result <= $hi);
        unless ($ok) {
            diag("Observed: $result, Lo: $lo, Hi: $hi");
        }
        ok($ok, "$qname query result should be correct for $name"); $test_count++;
    } else {
        is($mon->last_measured_value, $query->{c}, "$qname query result should be correct for $name"); $test_count++;
    }
}


sub init_queries {
    $QUERIES{'Single table'} = sub {
          Ship->fetch_by_name('Revenge');
      };

    $QUERIES{'Two table'} = sub {
        Ship->fetch_by_name_with_pirates('Revenge');
    };


    $QUERIES{'Multi-Row 2 Table'} = sub {
        Pirate->fetch_deep(
                           where => '1=1',
                           with => {ship => {}},
                          );
    };

    $QUERIES{'1 Table without HL'} = sub {
        Pirate->fetch_deep(
                           where => '1=1',
                           with => {}
                          );
    };

    $QUERIES{'1 Table with HL'} = sub {
        Pirate->fetch_deep(
                           where => '1=1', 
                           with => {diary => {}},
                          );
    };

    $QUERIES{'Multi-Row 3 Table with Self Join'} = sub {
        Pirate->fetch_deep(
                           where => '1=1',
                           with => {
                                    ship => {},
                                    captain => {},
                                   }
                          );
    };

    $QUERIES{'Multi-Row 3 Table with HMM'} = sub {
        Pirate->fetch_deep(
                           where => '1=1',
                           with => {
                                    booties => {},
                                   }
                          );
    };

    $QUERIES{'Single-Row 3 Table with Self Join'} = sub {
        Pirate->fetch_deep(
                           where => "MACRO__base__.name = 'Red Beard'",
                           with => {
                                    ship => {},
                                    captain => {},
                                   });
    };

    $QUERIES{'Kitchen Sink'} = sub {
        Pirate->fetch_deep(
                           where => "MACRO__base__.name = 'Red Beard'",
                           with => {
                                    ship => {},
                                    diary => {},
                                    booties => {
                                                secret_map => {},
                                               },
                                    rank => {},
                                    captain => {
                                                diary => {},
                                               },
                                   });
    };
}


done_testing($test_count);
