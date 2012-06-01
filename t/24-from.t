#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's Abstract SQL FROM functionality
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;

use Class::ReluctantORM::SQL::Aliases;

my (@expected, @seen, $seen, $expected);
my ($sql, $from, $str, $with, $rel, $crit, $lit, $join);
my (@passes, @fails);

my $ships_table = Table->new('CrormTest::Model::Ship');
my $pirates_table = Table->new('CrormTest::Model::Pirate');

#....
#  Join Tests
#....
$crit = Criterion->new('=', 1, 1);
$join = Join->new('INNER', $ships_table, $pirates_table, $crit);
isa_ok($join, Relation); $test_count++;
isa_ok($join, Join); $test_count++;
ok($join->knows_all_columns, "A join on two tables should know its own columns"); $test_count++;
ok($join->has_column('gun_count'), 'join has_column should work on left table'); $test_count++;
ok($join->has_column('leg_count'), 'join has_column should work on right table'); $test_count++;
# ...
    
throws_ok { Join->new('RIGHT OUTER', $ships_table, $pirates_table, $crit); } 'Class::ReluctantORM::Exception::Param', 'Right outer should be rejected as a join type'; $test_count++;
throws_ok { Join->new('NATURAL', $ships_table, $pirates_table, $crit); } 'Class::ReluctantORM::Exception::Param', 'Natural should be rejected as a join type'; $test_count++;
lives_ok { Join->new('CROSS', $ships_table, $pirates_table, $crit); } 'CROSS should be accepted as a join type'; $test_count++;
lives_ok { Join->new('LEFT OUTER', $ships_table, $pirates_table, $crit); } 'LEFT OUTER should be accepted as a join type'; $test_count++;
lives_ok { Join->new('cross', $ships_table, $pirates_table, $crit); } 'new should ignore join type case'; $test_count++;

#....
# From constructor tests
#....
$from = From->new($ships_table);
isa_ok($from, From); $test_count++;
is_deeply($from->root_relation, $ships_table, "root of a from built from a table should be that table"); $test_count++;
@expected = ($ships_table);
@seen = $from->tables();
is_deeply(\@seen, \@expected, "tables() of a from built from a table should be that table"); $test_count++;
throws_ok { From->new('some_table'); } 'Class::ReluctantORM::Exception::Param', 'from constructor should require arg to be a real relation'; $test_count++;


#....
# From 'with' parser tests
#....
@passes = (
           {                    # 0
            class => 'CrormTest::Model::Ship',
            args => {where => '1=1', with => {}},
            msg => 'empty with permitted',
           },
           {                    # 1
            class => 'CrormTest::Model::Pirate',
            args => {where => '1=1', with => { rank => {}}},
            msg => 'simple v0.3 with permitted (has_one static)',
           },
           {                    # 2
            class => 'CrormTest::Model::Ship',
            args => {where => '1=1', with => { pirates => {}}},
            msg => 'simple v0.3 with permitted (has_many)',
           },
           {                    # 3
            class => 'CrormTest::Model::Pirate',
            args => {where => '1=1', with => { ship => {}}},
            msg => 'simple v0.3 with permitted (has_one)',
           },
           {                    # 4
            class => 'CrormTest::Model::Pirate',
            args => {where => '1=1', with => { captain => {}}},
            msg => 'simple v0.3 with permitted, self-join',
           },
           {                    # 5
            class => 'CrormTest::Model::Pirate',
            args => {where => '1=1', with => { captain => {}, rank => {}}},
            msg => 'broad v0.3 with permitted',
           },
           {                    # 6
            class => 'CrormTest::Model::Ship',
            args => {where => '1=1', with => { pirates => { booties => {}}}},
            msg => 'complex v0.3 with permitted, variant 1',
           },
           {                    # 7
            class => 'CrormTest::Model::Pirate',
            args => {where => '1=1', with => { captain => { captain => {}}}},
            msg => 'complex v0.3 with permitted, double self-join',
           },
           {                    # 8
            class => 'CrormTest::Model::Ship',
            args => {where => '1=1', with => { pirates => { booties => {}, rank => {}}}},
            msg => 'complex v0.3 with permitted, variant 2',
           },
           {                    # 9
            class => 'CrormTest::Model::Ship',
            args => {where => '1=1', with => { pirates => { booties => {}, captain => {}}}},
            msg => 'complex v0.3 with permitted, variant 3',
           },

          );
foreach my $test (@passes) {
    # This uses the FetchDeep.pm args checker to canonicalize the 'with' clause
    my $tb_class = $test->{class};
    my %canonical_args = $tb_class->__dq_check_args(%{$test->{args}});
    my $with = $canonical_args{with};
    undef $from;
    #print "Generating FROM clause for '$test->{msg}':\n";
    lives_ok { $from = From->_new_from_with($tb_class, $with); } 'From::_new_from_with should survive for ' . $test->{msg};  $test_count++;
    #print $from->root_relation->pretty_print();
    #print Dumper($from);

    # TODO - more tests
}
done_testing($test_count);

