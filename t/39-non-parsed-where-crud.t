#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's (!Create) Retreieve Update Delete Functionality
#    with non-parsed WHERE clauses

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;
use CrormTest::Monitor::RawWhereDetector;
use Class::ReluctantORM::SQL::Aliases;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

# Set default parsing mode to off
Class::ReluctantORM->set_global_option('parse_where', 0);

my $canary = CrormTest::Monitor::RawWhereDetector->new(); # a canary in a CRO-mine
Class::ReluctantORM->install_global_monitor($canary);

# Registries screw with the result checking
foreach my $class (Class::ReluctantORM->list_all_classes()) {
    $class->_change_registry('Class::ReluctantORM::Registry::None');
}

my %TEST_THIS = (
                 INIT   => 1,
                 FETCH  => 1,
                 UPDATE_METHOD => 1,
                 UPDATE_SQL    => 1,
                 DELETE_METHOD => 1,
                 DELETE_SQL    => 1,
                );

my (%ships, %ranks, %pirates, %booties);
my ($new_ship, $new_pirate, $new_rank);

if ($TEST_THIS{INIT}) {
    my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;
    foreach my $name ('Black Pearl', 'Revenge', 'Golden Hind') {
        $ships{$name} =  Ship->create(
                                      name => $name,
                                      waterline => 50 + int(50*rand()),
                                      gun_count => 12 + int(24*rand()),
                                      ship_type_id => $frigate_type_id,
                                     );
    }

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
                                        rank => undef,
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



#....
# Fetch/Search
#....
if ($TEST_THIS{FETCH}) {
    my (@expected, @seen);
    my ($id, $ship, $pirate, $booty, $rank);

    # Fetch by ID
    throws_ok {
        $ship = CrormTest::Model::Ship->fetch(990099);
    } 'Class::ReluctantORM::Exception::Data::NotFound', "fetch misses throw exceptions"; $test_count++;
    lives_ok { $ship = CrormTest::Model::Ship->search(990099); } "search misses don't throw exceptions";  $test_count++;
    lives_ok {
        $ship = CrormTest::Model::Ship->search($ships{'Revenge'}->id);
    } "fetch hits don't throw exceptions"; $test_count++;
    ok(defined($ship), "fetch returned an object"); $test_count++;
    is($ship->name, "Revenge", "fetched ship's name is correct"); $test_count++;
    ok(!$ship->is_dirty(), "Should not be dirty immediately after insert()"); $test_count++;

    # Attribute searches.  CRO should use the SQL object system, so our raw monitor 
    # should see object wheres, not raw wheres.
    $canary->reset();
    lives_ok { $ship = Ship->fetch_by_name('Revenge'); } 'Fetch by name works on Ships'; $test_count++;
    is($canary->raw_where_count, 0, "Fetch by name on non-static should be zero raw where queries");  $test_count++;
    is($canary->object_where_count, 1, "Fetch by name on non-static should be 1 object where queries");  $test_count++;

    $canary->reset();
    lives_ok { $rank = Rank->fetch_by_name('Cabin Boy'); } 'Fetch by name works on Ranks'; $test_count++;
    is($canary->raw_where_count, 0, "Fetch by name on static should be zero raw where queries");  $test_count++;
    is($canary->object_where_count, 0, "Fetch by name on static should be zero object where queries");  $test_count++;


    # Search with where clause
    $canary->reset();
    lives_ok {
        $ship = Ship->search(
                             where => "name = 'Revenge'",
                            );
    } "Unparsed where on a search should live"; $test_count++;
    is($ship && $ship->name(), 'Revenge', "Unparsed where should give correct result");  $test_count++;
    is($canary->raw_where_count, 1, "Unparsed where should be 1 raw where queries");  $test_count++;
    is($canary->object_where_count, 0, "Unparsed where should be zero object where queries");  $test_count++;

    $ship = undef;

    # Search with raw where clause and param
    $canary->reset();
    lives_ok {
        $ship = Ship->search(
                             where => "name = ?",
                             execargs => [ 'Revenge' ],
                            );
    } "Unparsed where with param on a search should live"; $test_count++;
    is($ship && $ship->name(), 'Revenge', "Unparsed where with param should give correct result");  $test_count++;
    is($canary->raw_where_count, 1, "Unparsed where with param should be 1 raw where queries");  $test_count++;
    is($canary->object_where_count, 0, "Unparsed where with param should be zero object where queries");  $test_count++;

    # Search with double-referenced raw where clause, requiring no-re-alias-where
    $canary->reset();
    lives_ok {
        $ship = Ship->search(
                             where => <<EOS,
name IN (SELECT name FROM caribbean.ships WHERE name = 'Revenge')
EOS
                             no_re_alias_where => 1,
                            );
    } "Unparsed where with no-re-alias-where on a search should live"; $test_count++;
    is($ship && $ship->name(), 'Revenge', "Unparsed where with  no-re-alias-where should give correct result");  $test_count++;
    is($canary->raw_where_count, 1, "Unparsed where with no-re-alias-where should be 1 raw where queries");  $test_count++;
    is($canary->object_where_count, 0, "Unparsed where with no-re-alias-where should be zero object where queries");  $test_count++;

    # Search with double-referenced raw where clause, requiring no-re-alias-where
    $canary->reset();
    lives_ok {
        $ship = Ship->search(
                             where => <<EOS,
name IN (SELECT name FROM caribbean.ships WHERE name = 'Revenge' AND 3=?) AND 'foo'=?
EOS
                             no_re_alias_where => 1,
                             execargs => [ 3, 'foo'],
                            );
    } "Unparsed where with no-re-alias-where and two params on a search should live"; $test_count++;
    is($ship && $ship->name(), 'Revenge', "Unparsed where with no-re-alias-where and two params should give correct result");  $test_count++;
    is($canary->raw_where_count, 1, "Unparsed where with no-re-alias-where and two params should be 1 raw where queries");  $test_count++;
    is($canary->object_where_count, 0, "Unparsed where with no-re-alias-where and two params should be zero object where queries");  $test_count++;
    


}


#....
# Update
#....
if ($TEST_THIS{UPDATE_METHOD}) {
    my (@expected, @seen);
    my ($id, $ship, $pirate, $booty, $rank, $msg);

    $pirate = $pirates{Wesley};
    $rank = $ranks{'Cabin Boy'};

    # Wesley started out as a cabin boy aboard _Revenge_.
    # NOTE: don't use rank() - that would test relations.  Instead use rank_id.

    $pirate->rank_id($rank->id);
    is($pirate->rank_id, $rank->id, "mutator should work"); $test_count++;
    ok($pirate->is_dirty(), "Should be dirty immediately after mutator call"); $test_count++;
    $msg = 'audited update on single field FK via mutator';
    $canary->reset();
    lives_ok {
        $pirate->update();
    } $msg . ' should not throw an exception'; $test_count++;
    is($canary->raw_where_count,    0, "$msg should be 0 raw where queries");  $test_count++;
    is($canary->no_where_count,     1, "$msg should be 1 no-where queries");  $test_count++;
    is($canary->object_where_count, 1, "$msg should be 1 object where queries");  $test_count++;

    # Later Wesley was promoted to captain, and he changed his name.
    $rank = CrormTest::Model::Rank->fetch_by_name('Captain');
    $pirate->rank_id($rank->id);
    $pirate->name('Dread Pirate Roberts');

    # Confirm that the dirty fields list is sensible
    @expected = sort qw(rank_id name);
    @seen = sort $pirate->dirty_fields();
    is_deeply(\@seen, \@expected, 'dirty field list is sensible'); $test_count++;

    # Use save() to do the update this time
    $msg = 'audited save() on two fields via mutator';
    $canary->reset();
    lives_ok { $pirate->save(); } 'save() does not throw an exception when an update is needed'; $test_count++;
    is($canary->raw_where_count,    0, "$msg should be 0 raw where queries");  $test_count++;
    is($canary->no_where_count,     1, "$msg should be 1 no-where queries");  $test_count++;
    is($canary->object_where_count, 1, "$msg should be 1 object where queries");  $test_count++;

    throws_ok { $pirate->insert(); } 'Class::ReluctantORM::Exception::Data::AlreadyInserted', 'insert should complain if the object has already been inserted'; $test_count++;
}

if ($TEST_THIS{UPDATE_SQL}) {
    my (@expected, @seen);
    my ($id, $ship, $pirate, $booty, $rank, $msg);

    # plain SQL Updates
    my $make_update = sub {
        my $s = SQL->new('UPDATE');
        my $t = Table->new(Pirate);
        $s->table($t);
        $s->add_input(Column->new(column => 'leg_count', table => $t), Param->new());
        return $s;
    };
    my @tests = (
                 {
                  label    => 'update with empty where',
                  where    => '',
                  execargs => [4],
                  no_count => 1,
                  raw_count => 0,
                  check    => {
                               all_updated => sub {
                                   my @all = Pirate->fetch_all();
                                   my $ok = 1;
                                   for (@all) { $ok &&= $_->leg_count(); }
                                   return $ok;
                               },
                  },
                 },
                 {
                  label    => 'update with one column',
                  where    => "name = 'Sir Francis Drake'",
                  execargs => [47],
                  check    => {
                               precise => sub { return Pirate->fetch_by_name('Sir Francis Drake')->leg_count == 47; },
                              }
                 },
                 {
                  label    => 'update with one colum, one param',
                  where    => "name = ?",
                  execargs => [23, 'Sir Francis Drake'],
                  check    => {
                               precise => sub { return Pirate->fetch_by_name('Sir Francis Drake')->leg_count == 23; },
                              },
                 },
                 {
                  label    => 'update with one column, table.column spec',
                  where    => "pirates.name = 'Sir Francis Drake'",
                  execargs => [119],
                  check    => {
                               precise => sub { return Pirate->fetch_by_name('Sir Francis Drake')->leg_count == 119; },
                               # On updates, we don't currently do re-aliasing because driver support varies widely for this, and SQL objects don't support multiple FROMs for UPDATE anyway, yet.
                               #aliaser => sub { return $canary->orig_sql->_cooked_where !~ 'pirates\.name'; },
                               aliaser => sub { return $canary->orig_sql->_cooked_where =~ 'pirates\.name'; },
                              },
                 },
                 {
                  label    => 'update with bad column',
                  where    => "glockenspeil_affinity = 'a great deal'",
                  execargs => [4],
                  fail     => 'Class::ReluctantORM::Exception::SQL::ExecutionError',
                 },
                 {
                  label    => 'update with bad table, good column',
                  where    => "ships.name = 'Somethin'",
                  execargs => [4],
                  fail     => 'Class::ReluctantORM::Exception::SQL::ExecutionError',
                 },
                );
    for (@tests) { $_->{sql_maker} ||= $make_update; }

    run_tests(@tests);
}


#....
# Delete
#....
if ($TEST_THIS{DELETE_METHOD}) {
    my (@expected, @seen);
    my ($id, $ship, $pirate, $booty, $rank);

    $ship   = $ships{Revenge};
    $pirate = $pirates{Wesley};
    $rank   = $ranks{'Able Seaman'};

    # Try to delete the ship.  Should fail due to DB constraints.
    throws_ok { $ship->delete(); } 'Class::ReluctantORM::Exception::SQL::ExecutionError', 'deleting a referred-to ship should result in an exception.'; $test_count++;

    # Try to delete the rank.  Should fail due to CRO constraints.
    throws_ok { $rank->delete(); } 'Class::ReluctantORM::Exception::Call::NotPermitted', 'deleting a undeletable item should result in an exception.'; $test_count++;

    # OK, delete Wesley.
    $canary->reset();
    lives_ok { 
        $pirate->delete();
    } 'deleting a pirate should work.'; # aaaaaaasssss yoooooouuuu wiiiiiiiiiiishhhh.......
    $test_count++;
}

done_testing($test_count);

































sub run_tests {
    my @tests = @_;

    foreach my $test (@tests) {
        #next unless $test->{only}; # DEBUG
        my $label = $test->{label};
        unless (defined $test->{raw_count}) { $test->{raw_count} = 1; }
        unless (defined $test->{no_count})  { $test->{no_count} = 0; }
        my $sql = $test->{sql_maker}->();
        $sql->raw_where($test->{where});
        if ($test->{fail}) {
            throws_ok {
                $sql->set_bind_values(@{$test->{execargs}});
                Pirate->driver->run_sql($sql);
            } $test->{fail}, "$label should fail with a $test->{fail}"; $test_count++;
        } else {
            $canary->reset();
            lives_ok {
                $sql->set_bind_values(@{$test->{execargs}});
                Pirate->driver->run_sql($sql);
            } "$label should live"; $test_count++;
            is($canary->raw_where_count,    $test->{raw_count}, "$label should be $test->{raw_count} raw where queries");  $test_count++;
            is($canary->no_where_count,     $test->{no_count},  "$label should be $test->{no_count} no where queries");  $test_count++;
            is($canary->object_where_count, 0, "$label should be 0 object where queries");  $test_count++;
            if ($test->{check}) {
                foreach my $check_name (keys %{$test->{check}}) {
                    ok($test->{check}{$check_name}->(), "$label should pass its post-query check '$check_name'");  $test_count++;
                }
            }

        }
    }
}

