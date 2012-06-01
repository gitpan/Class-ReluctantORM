#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's prepare-execute-fetch functionality from the SQL object

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;
use Class::ReluctantORM::SQL::Aliases;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

use aliased 'Class::ReluctantORM::Monitor::QueryCount';
my $query_counter = QueryCount->new();
Class::ReluctantORM->install_global_monitor($query_counter);

my %SQL;
my %TEST_THIS = (
                 INIT     => 1,
                 PREPARE  => 1,
                 FINISH   => 1,
                 EXECUTE  => 1,
                );

my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;

if ($TEST_THIS{INIT}) {
    my $ship = Ship->create(
                            name => 'Revenge',
                            gun_count => 32,
                            waterline => 64,
                            ship_type_id => $frigate_type_id,
                           );
    foreach my $color (qw(Red Green Blue Purple)) {
        Pirate->create(
                       name => $color . ' Beard',
                       ship => $ship,
                      );
    }
    make_sql();
}

if ($TEST_THIS{PREPARE}) {
    my $driver = Pirate->driver();

    foreach my $sql_name (sort keys %SQL) { # sort for test number consistency
        my $sql = $SQL{$sql_name}[0];
        lives_ok {
            $driver->prepare($sql)
        } "prepare of '$sql_name' should live"; $test_count++;
        ok($sql->is_prepared(), "'$sql_name' should now be prepared"); $test_count++;
    }
}

if ($TEST_THIS{FINISH}) {
    my $driver = Pirate->driver();

    foreach my $sql_name (sort keys %SQL) { # sort for test number consistency
        my $sql = $SQL{$sql_name}[0];
        $driver->prepare($sql);
        lives_ok {
            $sql->finish();
        } "finish of '$sql_name' should live"; $test_count++;
        ok(!$sql->is_prepared(), "'$sql_name' should now NOT be prepared"); $test_count++;
    }
}

if ($TEST_THIS{EXECUTE}) {
    my $driver = Pirate->driver();

    foreach my $sql_name (sort keys %SQL) { # sort for test number consistency
        my $sql = $SQL{$sql_name}[0];
        $driver->prepare($sql);
        my @params = @{$SQL{$sql_name}[1]};
        my $return_count = $SQL{$sql_name}[2];

        if (@params) {
            # Params expected
            lives_ok {
                $sql->execute(@params);
            } "execute of '$sql_name' should live"; $test_count++;

            lives_ok {
                $sql->fetchrow(@params);
            } "fetchrow on '$sql_name' should live"; $test_count++;

            if ($return_count) {
                ok($sql->has_results, "'$sql_name' should have produced at least one output"); $test_count++;
            }

            if (@params > 1) {
                # Too few params
                throws_ok {
                    $sql->execute($params[0]);
                } 'Class::ReluctantORM::Exception::Param::Missing', "execute of '$sql_name' with too few bind params should die"; $test_count++;
            }

            # Too many params
            throws_ok {
                $sql->execute(@params, 1,2,3);
            } 'Class::ReluctantORM::Exception::Param::Spurious', "execute of '$sql_name' with extra bind params should die"; $test_count++;


        } else {
            # No params expected
            lives_ok {
                $sql->execute();
            } "execute of '$sql_name' should live"; $test_count++;

            lives_ok {
                $sql->fetchrow(@params);
            } "fetchrow on '$sql_name' should live"; $test_count++;


            if ($return_count) {
                ok($sql->has_results, "'$sql_name' should have produced at least one output"); $test_count++;
            }

            # Try running with params - should die
            throws_ok {
                $sql->execute(1,2,3);
            } 'Class::ReluctantORM::Exception::Param::Spurious', "execute of '$sql_name' with extra bind params should die"; $test_count++;

        }
    }

}


done_testing($test_count);


sub make_sql {
    my $sql;

    $sql = SQL->new('SELECT');
    $sql->from(From->new(Table->new(table => 'ships')));
    $sql->where(Where->new());
    $sql->add_output(Column->new(column => 'ship_id'));
    $SQL{'01 one-column, one-table select'} = [ $sql, [], 1];

    $sql = SQL->new('SELECT');
    $sql->from(From->new(Table->new(table => 'pirates')));
    $sql->where(Where->new(Criterion->new('=', Column->new(column => 'name'), Param->new())));
    $sql->add_output(Column->new(column => 'pirate_id'));
    $SQL{'02 one-column, one-table select, one param'} = [$sql, ['Red Beard'], 1];

    $sql = SQL->new('SELECT');
    $sql->from(From->new(Table->new(table => 'booties2pirates')));
    $sql->where(Where->new());
    $sql->add_output(Column->new(table => Table->new(table => 'booties2pirates'), column => 'pirate_id'));
    $SQL{'03 one-column, one-table select from a join table'} = [$sql, [], 0];

    $sql = SQL->new('UPDATE');
    $sql->table(Table->new(table => 'ships'));
    $sql->where(Where->new(Criterion->new('=', Column->new(column => 'name'), Param->new('Revenge'))));
    $sql->add_input(Column->new(column => 'gun_count'), Param->new());
    $SQL{'04 one-param, one-table update, no returns'} = [$sql, [22, 'Revenge'], 0];

    $sql = SQL->new('UPDATE');
    $sql->table(Table->new(table => 'ships'));
    $sql->where(Where->new(Criterion->new('=', 1, 1)));
    $sql->add_input(Column->new(column => 'name'), Param->new('Awesome Boat'));
    $sql->add_output(Column->new(column => 'ship_id'));
    $SQL{'05 one-param, one-returning, one-table update'} = [$sql, ['Awesome Boat'], 1];

}
