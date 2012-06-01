#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test trigger support
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;
use Class::ReluctantORM::SQL::Aliases;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();





my %TEST_THIS = (
                 INIT => 1,
                 after_retrieve => 1,
                 before_insert  => 1,
                 after_insert   => 1,
                 before_update  => 1,
                 after_update   => 1,
                 before_save    => 1,
                 after_save     => 1,
                 before_delete  => 1,
                 after_delete   => 1,
                 before_refresh => 1,
                 after_refresh  => 1,
                );

my @EVENTS = keys %Class::ReluctantORM::TRIGGER_EVENTS;
my %NONE = map { $_ => 0 } @EVENTS;
my %TEMPLATE = (
                insert => { %NONE },
                update => { %NONE },
                delete => { %NONE },
                fetch  => { %NONE },
                save_insert => { %NONE },
                save_update => { %NONE },
                save_noop => { %NONE },
               );

my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;
if ($TEST_THIS{INIT}) {
    my $ship = Ship->create(
                            name => 'Revenge',
                            gun_count => 80,
                            waterline => 25,
                            ship_type_id => $frigate_type_id,
                           );
}


#==============
#  Main tests
#==============

if ($TEST_THIS{after_retrieve}) {
    my $triggered = { %NONE, after_retrieve => 1 };
    run_trigger_tests({
                       event => 'after_retrieve',
                       expected => {
                                    %TEMPLATE,
                                    fetch => $triggered,
                                   },
                      });
}

if ($TEST_THIS{before_refresh}) {
    my $triggered = { %NONE, before_refresh => 1 };
    run_trigger_tests({
                       event => 'before_refresh',
                       expected => {
                                    %TEMPLATE,
                                    insert => $triggered,
                                    update => $triggered,
                                    save_insert => $triggered,
                                    save_update => $triggered,
                                    # TODO - refresh()
                                   },
                      });
}

if ($TEST_THIS{after_refresh}) {
    my $triggered = { %NONE, after_refresh => 1 };
    run_trigger_tests({
                       event => 'after_refresh',
                       expected => {
                                    %TEMPLATE,
                                    insert => $triggered,
                                    update => $triggered,
                                    save_insert => $triggered,
                                    save_update => $triggered,
                                    # TODO - refresh()
                                   },
                      });
}


if ($TEST_THIS{before_insert}) {
    my $triggered = { %NONE, before_insert => 1 };
    run_trigger_tests({
                       event => 'before_insert',
                       expected => {
                                    %TEMPLATE,
                                    insert => $triggered,
                                    save_insert => $triggered,
                                   },
                      });
}

if ($TEST_THIS{after_insert}) {
    my $triggered = { %NONE, after_insert => 1 };
    run_trigger_tests({
                       event => 'after_insert',
                       expected => {
                                    %TEMPLATE,
                                    insert => $triggered,
                                    save_insert => $triggered,
                                   },
                      });
}

if ($TEST_THIS{before_update}) {
    my $triggered = { %NONE, before_update => 1 };
    run_trigger_tests({
                       event => 'before_update',
                       expected => {
                                    %TEMPLATE,
                                    update => $triggered,
                                    save_update => $triggered,
                                   },
                      });
}

if ($TEST_THIS{after_update}) {
    my $triggered = { %NONE, after_update => 1 };
    run_trigger_tests({
                       event => 'after_update',
                       expected => {
                                    %TEMPLATE,
                                    update => $triggered,
                                    save_update => $triggered,
                                   },
                      });
}

if ($TEST_THIS{before_save}) {
    my $triggered = { %NONE, before_save => 1 };
    run_trigger_tests({
                       event => 'before_save',
                       expected => {
                                    %TEMPLATE,
                                    save_insert => $triggered,
                                    save_update => $triggered,
                                   },
                      });
}

if ($TEST_THIS{after_save}) {
    my $triggered = { %NONE, after_save => 1 };
    run_trigger_tests({
                       event => 'after_save',
                       expected => {
                                    %TEMPLATE,
                                    save_insert => $triggered,
                                    save_update => $triggered,
                                   },
                      });
}

if ($TEST_THIS{before_delete}) {
    my $triggered = { %NONE, before_delete => 1 };
    run_trigger_tests({
                       event => 'before_delete',
                       expected => {
                                    %TEMPLATE,
                                    delete => $triggered,
                                   },
                      });
}

if ($TEST_THIS{after_delete}) {
    my $triggered = { %NONE, after_delete => 1 };
    run_trigger_tests({
                       event => 'after_delete',
                       expected => {
                                    %TEMPLATE,
                                    delete => $triggered,
                                   },
                      });
}




#==============
#  Test Subs
#==============


my %TRIGGERS_SEEN;

sub run_trigger_tests {
    my $info = shift;

    my $event = $info->{event};
    my $expected = $info->{expected};

    # Attach our triggers
    lives_ok {
        Ship->add_trigger($event, \&record_trigger_activity);
    } "($event) add_trigger should live";
    $test_count++;

    # Look for trigger
    my @seen = Ship->list_triggers($event);
    is(scalar(@seen), 1, "($event) Should be exactly one trigger for this event"); $test_count++;
    is($seen[0], \&record_trigger_activity, "($event) Trigger assignment should be correct"); $test_count++;

    # Do a retrieve
    %TRIGGERS_SEEN = %NONE;
    my $fetched_ship = Ship->fetch_by_name('Revenge');
    is_deeply(\%TRIGGERS_SEEN, $expected->{fetch}, "($event) should see the correct triggers fire on a fetch");
    $test_count++;

    # Do a new
    %TRIGGERS_SEEN = %NONE;
    my $ship = shipyard();
    is_deeply(\%TRIGGERS_SEEN, \%NONE, "($event) new() should never fire a trigger");
    $test_count++;

    # Do an insert
    %TRIGGERS_SEEN = %NONE;
    $ship->insert();
    is_deeply(\%TRIGGERS_SEEN, $expected->{insert}, "($event) should see the correct triggers fire on an insert");
    $test_count++;

    # Do an update
    %TRIGGERS_SEEN = %NONE;
    $ship->gun_count(3);
    $ship->update();
    is_deeply(\%TRIGGERS_SEEN, $expected->{update}, "($event) should see the correct triggers fire on an update");
    $test_count++;

    # do a save/insert
    my $si_ship = shipyard();
    %TRIGGERS_SEEN = %NONE;
    $si_ship->save();
    is_deeply(\%TRIGGERS_SEEN, $expected->{save_insert}, "($event) should see the correct triggers fire on an save-insert");
    $test_count++;

    # do a save/update
    my $su_ship = shipyard();
    $su_ship->insert();
    $su_ship->gun_count(3);
    %TRIGGERS_SEEN = %NONE;
    $su_ship->save();
    is_deeply(\%TRIGGERS_SEEN, $expected->{save_update}, "($event) should see the correct triggers fire on an save-update");
    $test_count++;

    # do a save/noop
    my $sn_ship = shipyard();
    $sn_ship->insert();
    %TRIGGERS_SEEN = %NONE;
    $sn_ship->save();
    is_deeply(\%TRIGGERS_SEEN, $expected->{save_noop}, "($event) should see the correct triggers fire on an save-noop");
    $test_count++;

    # Do a delete
    %TRIGGERS_SEEN = %NONE;
    $ship->delete();
    is_deeply(\%TRIGGERS_SEEN, $expected->{delete}, "($event) should see the correct triggers fire on a delete");
    $test_count++;

    lives_ok {
        Ship->remove_trigger($event, \&record_trigger_activity);
    } "($event) remove_trigger should live";
    $test_count++;
    is(scalar(Ship->list_triggers($event)), 0, "($event) Should have 0 triggers after remove_trigger");     $test_count++;

}

sub shipyard {
    return Ship->new(
                     name => 'Sea Spray ' . int(rand(100)),
                     gun_count => 0,
                     waterline => 45,
                     ship_type_id => $frigate_type_id,
                    );
}

sub record_trigger_activity {
    my $obj = shift;
    my $event = shift;
    $TRIGGERS_SEEN{$event} = 1;
}





done_testing($test_count);
