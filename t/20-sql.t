#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's Abstract SQL functionality
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;

use aliased 'Class::ReluctantORM::Monitor::Dump' => 'Monitor::Dump';
my $mon = Monitor::Dump->new(
                             when => ['execute_begin'],
                            );
use Class::ReluctantORM;
Class::ReluctantORM->install_global_monitor($mon);

use Class::ReluctantORM::SQL;
use Class::ReluctantORM::SQL::Aliases;

my $driver = CrormTest::Model::Pirate->driver();

my ($seen, $expected);
my ($sql, $from, $table, $column, $join, $where, $crit);

$sql = SQL->new('SELECT');
ok(defined($sql), "SQL constructor works"); $test_count++;

ok(!$sql->has_results, "Newborn SQL has no results"); $test_count++;

#.......
# Check operations
#.......
foreach my $op (qw(select SELECT insert update delete)) {
    lives_ok { $sql = SQL->new($op); } "$op is a permitted operation"; $test_count++;
    ok($sql && $sql->operation() eq uc($op), "operation() is correct and uppercase"); $test_count++;
}
foreach my $op (qw(GRANT create udpate)) {
    throws_ok {  SQL->new($op);  } "Class::ReluctantORM::Exception::Param::BadValue", "$op is not permitted as an operation"; $test_count++;
}
$sql = SQL->new('SELECT');
throws_ok { $sql->operation('INSERT'); } "Class::ReluctantORM::Exception::Call::NotMutator", "operator() is not a mutator"; $test_count++;


#......
#  Check Table
#......
$table = Table->new('CrormTest::Model::Pirate');
ok(defined($table), "SQL table constructor works"); $test_count++;

$sql = SQL->new('SELECT');
throws_ok { $sql->table($table); } "Class::ReluctantORM::Exception::Call::NotPermitted", "table() not permitted for SELECT"; $test_count++;
throws_ok { $sql->table(); } "Class::ReluctantORM::Exception::Call::NotPermitted", "table() not permitted for SELECT"; $test_count++;

foreach my $op (qw(INSERT UPDATE DELETE)) {
    $sql = SQL->new($op);
    is($sql->table(), undef, "table() starts off undef for $op"); $test_count++;
    lives_ok { $sql->table($table) } "table() mutator allowed for $op"; $test_count++;
    $seen = $sql->table();
    is_deeply($seen, $table, "table() accessor returns correct table for $op"); $test_count++;
}

# TODO - tests?

#......
# Column Disambiguation
#......
$sql = SQL->new('SELECT');
$table = Table->new('CrormTest::Model::Pirate');
$sql->from(From->new($table));
$column = Column->new(
                      column => 'pirate_id',
                      table => $table,
                     );
$sql->where(Where->new(Criterion->new('=', $column, 1)));

lives_ok { $sql->reconcile(); } "reconcile() should live on explicit statements"; $test_count++;
$seen = $sql->tables();
is($seen, 1, "should have only one table for explicit statements"); $test_count++;

$column = Column->new(
                      column => 'pirate_id',
                      table => Table->new(table => 'pirates'),
                     );
$sql->where(Where->new(Criterion->new('=', $column, 1)));
lives_ok { $sql->reconcile(); } "reconcile() should live on explicit statements"; $test_count++;
$seen = $sql->tables();
is($seen, 1, "should have only one table for explicit statements"); $test_count++;

done_testing($test_count);
