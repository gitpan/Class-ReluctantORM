#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's Create Retreieve Update Delete Functionality

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

my %TEST_THIS = (
                 INIT   => 1,
                 NEW    => 1,
                 INSERT => 1, SEAMLESS_INSERT => 1,
                 FETCH  => 1,
                 CREATE => 1, SEAMLESS_CREATE => 1,
                 UPDATE => 1, SEAMLESS_UPDATE => 1,
                 DELETE => 1, SEAMLESS_DELETE => 1,
                );

my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;

if ($TEST_THIS{INIT}) {
    my $ship = Ship->create(
                            name => 'Revenge',
                            gun_count => 32,
                            waterline => 64,
                            ship_type_id => $frigate_type_id,
                           );
    foreach my $color (qw(Red RedD Green Blue BlueD Purple)) {
        Pirate->create(
                       name => $color . ' Beard',
                       ship => $ship,
                      );
    }
}

if ($TEST_THIS{NEW}) {
    my $pirate;

    # Call new with no extra args
    $query_counter->reset();
    lives_ok {
        $pirate = Pirate->new(
                              name => 'Puce Beard',
                             );
    } "new Pirate, seamless mode should live"; $test_count++;
    is($query_counter->last_measured_value(), 0, "Pirate->new() should be 0 queries");  $test_count++;

    # Audit args should not be permitted to new()
    throws_ok {
        $pirate = Pirate->new(
                              name => 'Tater Beard',
                              audit_pid => $$,
                             );
    } 'Class::ReluctantORM::Exception::Param::Spurious', 
      "new Pirate, seamless mode, audit args should die"; $test_count++;
}

if ($TEST_THIS{SEAMLESS_INSERT}) {
    my ($pirate, $overall_log_count, $this_log_count);

    # Call new with no extra args
    $pirate = Pirate->new(
                          name => 'Pas de Burre Beard',
                         );
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate->insert();
    } "Pirate->insert() seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->insert() should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_pid = $$ AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;

    # Audit args should be permitted to insert, and should override seamless effect
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    $pirate = Pirate->new(
                          name => 'Eggplant Beard',
                         );
    lives_ok {
        $pirate->insert(8888888);
    } "Pirate->insert() seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->insert(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_pid = 8888888 AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;

    # Count of audit args to insert must be restricted
    $pirate = Pirate->new(
                          name => 'Robot Beard',
                         );
    throws_ok {
        $pirate->insert(123123,123123);
    } 'Class::ReluctantORM::Exception::Param', "Calling insert with the wrong number of params should be an exception";
    $test_count++;

}

if ($TEST_THIS{FETCH}) {
    my ($ship, $pirate, $log_count);

    # Simple fetch
    $query_counter->reset();
    $log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate = Pirate->fetch_by_name('Red Beard');
    } "Pirate->fetch() should live"; $test_count++;
    is($query_counter->last_measured_value(), 1, "Pirate->fetch() should be 1 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $log_count, "A fetch should not impact the log"); $test_count++;

    # Deep fetch
    $query_counter->reset();
    $log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate = Pirate->fetch_by_name_with_ship('Red Beard');
    } "Pirate->fetch() should live"; $test_count++;
    is($query_counter->last_measured_value(), 1, "Pirate->fetch() should be 1 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $log_count, "A fetch should not impact the log"); $test_count++;

}

if ($TEST_THIS{SEAMLESS_CREATE}) {
    my ($pirate, $overall_log_count, $this_log_count);

    # Call create with no extra args
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate = Pirate->create(
                                 name => 'Glasschin Beard',
                                );
    } "Pirate->create() seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->create() should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_action='INSERT' AND audit_pid = $$ AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;

    # Audit args should be permitted to create, and should override seamless effect
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate = Pirate->create(
                                 name => 'Eggplant Beard',
                                 audit_pid => 5555555,
                                );
    } "Pirate->create(audit_arg) seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->create(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_action='INSERT' AND audit_pid = 5555555 AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;

}

if ($TEST_THIS{SEAMLESS_UPDATE}) {
    my ($pirate, $overall_log_count, $this_log_count);

    $pirate = Pirate->fetch_by_name('Red Beard');
    $pirate->leg_count(8);
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate->update();
    } "Pirate->update() seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->update() should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_action='UPDATE' AND audit_pid = $$ AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;

    # Audit args should be permitted to update, and should override seamless effect
    $pirate = Pirate->fetch_by_name('Blue Beard');
    $pirate->leg_count(6);
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate->update(1212_1212);
    } "Pirate->update(ARG) seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->update(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_action='UPDATE' AND audit_pid = 12121212 AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;

    # Count of audit args to update must be restricted
    $pirate = Pirate->fetch_by_name('Blue Beard');
    $pirate->leg_count(54);
    throws_ok {
        $pirate->update(123123,123123);
    } 'Class::ReluctantORM::Exception::Param', "Calling update with the wrong number of params should be an exception";
    $test_count++;

}

if ($TEST_THIS{SEAMLESS_DELETE}) {
    my ($pirate, $id, $overall_log_count, $this_log_count);

    $pirate = Pirate->fetch_by_name('BlueD Beard');
    $id = $pirate->id();
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate->delete();
    } "Pirate->delete() seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->delete() should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_action='DELETE' AND audit_pid = $$ AND pirate_id = " . $id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the deleted pirate");  $test_count++;

    # Audit args should be permitted to delete, and should override seamless effect
    $pirate = Pirate->fetch_by_name('RedD Beard');
    $id = $pirate->id();
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate->delete(987654321);
    } "Pirate->delete(ARG) seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->delete(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_action='DELETE' AND audit_pid = 987654321 AND pirate_id = " . $id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the deleted pirate");  $test_count++;

    # Count of audit args to delete must be restricted
    $pirate = Pirate->fetch_by_name('Green Beard');
    $pirate->leg_count(54);
    throws_ok {
        $pirate->delete(123123,123123);
    } 'Class::ReluctantORM::Exception::Param', "Calling delete with the wrong number of params should be an exception";
    $test_count++;

}



#=======================================================#
#                 SEAMLESS-LESS MODE
#=======================================================#
{
    # WHITEBOX

    # Strip out audit metadata generator from symbol table
    ok(CrormTest::Model::Pirate->can('get_audit_metadata_audit_pid'), "test fixture assertion: should have an audit metadata generator");  $test_count++;
    delete $CrormTest::Model::Pirate::{get_audit_metadata_audit_pid};
    $Class::ReluctantORM::CLASS_METADATA{'CrormTest::Model::Pirate'}->{audit}->{fetchers} = {};
    ok(!CrormTest::Model::Pirate->can('get_audit_metadata_audit_pid'), "test fixture assertion: should have cleared the audit metadata generator");  $test_count++;

    ok(Pirate->audit_seamless_mode(), "test fixture assertion: Pirate should be seamlessly audited");  $test_count++;
    $Class::ReluctantORM::CLASS_METADATA{'CrormTest::Model::Pirate'}->{audit}->{seamless_mode} = 0;
    ok(!Pirate->audit_seamless_mode(), "test fixture assertion: Pirate seamless mode should be disabled");  $test_count++;

}

if ($TEST_THIS{INSERT}) {
    my ($pirate, $overall_log_count, $this_log_count);

    $pirate = Pirate->new(name => 'Butter Beard' );

    # Call insert with no args
    throws_ok {
        $pirate->insert();
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->insert() non-seamless should die";  $test_count++;

    throws_ok {
        $pirate->audited_insert();
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->audited_insert() with no args should die";  $test_count++;

    throws_ok {
        $pirate->audited_insert(123, 456);
    } 'Class::ReluctantORM::Exception::Param::Spurious',
      "Pirate->audited_insert(ARG, ARG) with spurious arg should die";  $test_count++;

    throws_ok {
        $pirate->insert(123);
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->insert(ARG) nonseamless should die";  $test_count++;

    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate->audited_insert($$);
    } "Pirate->audited_insert(ARG) non-seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->audited_insert(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_pid = $$ AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;


    # Make sure we can set whatever we want for audit metadata
    $pirate = Pirate->new(name => 'Soup-Saver Beard' );
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate->audited_insert(7777777);
    } "Pirate->audited_insert(ARG) non-seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->audited_insert(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_pid = 7777777 AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;

}

if ($TEST_THIS{CREATE}) {
    my ($pirate, $overall_log_count, $this_log_count);

    # Call create with no args
    throws_ok {
        Pirate->create();
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->create() non-seamless should die";  $test_count++;

    throws_ok {
        Pirate->audited_create();
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->audited_create() with no args should die";  $test_count++;

    throws_ok {
        Pirate->audited_create(name => "Scruff Beard");
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->audited_create() with no audit metadata should die";  $test_count++;

    throws_ok {
        Pirate->audited_create(name => 123, audit_pid => 456, audit_pants => "chinos");
    } 'Class::ReluctantORM::Exception::Param::Spurious',
      "Pirate->audited_create(ARG, ARG) with spurious arg should die";  $test_count++;

    throws_ok {
        Pirate->create(name => "Shoe Beard", audit_pid => 123);
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->create(ARG) nonseamless should die";  $test_count++;

    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate = Pirate->audited_create(
                                         name => "Neck Beard",
                                         audit_pid => $$,
                                        );
    } "Pirate->audited_create(ARG) non-seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->audited_create(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_pid = $$ AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;


    # Make sure we can set whatever we want for audit metadata
    $pirate = Pirate->new(name => 'Soup-Saver Beard' );
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate = Pirate->audited_create(
                                         name => "Neck Beard",
                                         audit_pid => 4444444,
                                        );
    } "Pirate->audited_create(ARG) non-seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->audited_create(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_pid = 4444444 AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;

}

if ($TEST_THIS{UPDATE}) {
    my ($pirate, $overall_log_count, $this_log_count);

    $pirate = Pirate->fetch_by_name('Green Beard');
    $pirate->leg_count(53);

    # Call update with no args
    throws_ok {
        $pirate->update();
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->update() non-seamless should die";  $test_count++;

    throws_ok {
        $pirate->audited_update();
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->audited_update() with no args should die";  $test_count++;

    throws_ok {
        $pirate->audited_update(123, 456);
    } 'Class::ReluctantORM::Exception::Param::Spurious',
      "Pirate->audited_update(ARG, ARG) with spurious arg should die";  $test_count++;

    throws_ok {
        $pirate->update(123);
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->update(ARG) nonseamless should die";  $test_count++;

    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate->audited_update($$);
    } "Pirate->audited_update(ARG) non-seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->audited_update(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_action='UPDATE' AND audit_pid = $$ AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;


    # Make sure we can set whatever we want for audit metadata
    $pirate = Pirate->fetch_by_name('Red Beard');
    $pirate->leg_count(22);
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate->audited_update(1234567);
    } "Pirate->audited_update(ARG) non-seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->audited_update(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_action='UPDATE' AND audit_pid = 1234567 AND pirate_id = " . $pirate->id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;

}

if ($TEST_THIS{DELETE}) {
    my ($pirate, $id, $overall_log_count, $this_log_count);

    $pirate = Pirate->fetch_by_name('Green Beard');
    $id = $pirate->id;

    # Call delete with no args
    throws_ok {
        $pirate->delete();
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->delete() non-seamless should die";  $test_count++;

    throws_ok {
        $pirate->audited_delete();
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->audited_delete() with no args should die";  $test_count++;

    throws_ok {
        $pirate->audited_delete(123, 456);
    } 'Class::ReluctantORM::Exception::Param::Spurious',
      "Pirate->audited_delete(ARG, ARG) with spurious arg should die";  $test_count++;

    throws_ok {
        $pirate->delete(123);
    } 'Class::ReluctantORM::Exception::Call::NotPermitted',
      "Pirate->delete(ARG) nonseamless should die";  $test_count++;

    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate->audited_delete($$);
    } "Pirate->audited_delete(ARG) non-seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->audited_delete(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_action='DELETE' AND audit_pid = $$ AND pirate_id = " . $id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;


    # Make sure we can set whatever we want for audit metadata
    $pirate = Pirate->fetch_by_name('Purple Beard');
    $id = $pirate->id;
    $query_counter->reset();
    $overall_log_count = $fixture->count_rows('caribbean.pirates_log');
    lives_ok {
        $pirate->audited_delete(1234567);
    } "Pirate->audited_delete(ARG) non-seamless should live";  $test_count++;
    is($query_counter->last_measured_value(), 2, "Pirate->audited_delete(ARG) should be 2 queries");  $test_count++;
    is($fixture->count_rows('caribbean.pirates_log'), $overall_log_count + 1, "Should be one more row in pirate log");   $test_count++;
    $this_log_count = $fixture->count_rows('caribbean.pirates_log', "audit_action='DELETE' AND audit_pid = 1234567 AND pirate_id = " . $id);
    is($this_log_count, 1, "Should be exactly one row in pirate log matching the new pirate");  $test_count++;

}




done_testing($test_count);

