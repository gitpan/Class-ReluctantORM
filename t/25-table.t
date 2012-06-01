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
my ($sql, $table);


#....
# Table Tests
#....
my $ships_table = Table->new('CrormTest::Model::Ship');
my $pirates_table = Table->new('CrormTest::Model::Pirate');
isa_ok($ships_table, Relation); $test_count++;
isa_ok($pirates_table, Relation); $test_count++;
ok($ships_table->knows_all_columns, "a TB-Class based table should know its columns"); $test_count++;
ok($ships_table->has_column("gun_count"), "ship table should have a gun_count column"); $test_count++;
ok($ships_table->has_column("GUN_COUNT"), "has_column should ignore case"); $test_count++;
ok($ships_table->has_column("gUN_COuNT"), "has_column should ignore case"); $test_count++;
@seen = $ships_table->columns;
isa_ok($seen[0], Column); $test_count++;
@seen = sort map { $_->column } @seen;
@expected = sort CrormTest::Model::Ship->column_names();
is_deeply(\@seen, \@expected, "table columns should be accurate"); $test_count++;
ok($ships_table->is_leaf_relation, "Tables are leaves"); $test_count++;
ok($ships_table->is_table, "Tables are tables"); $test_count++;
ok(!$ships_table->is_join, "Tables are not Joins"); $test_count++;
@expected = ();
@seen = $ships_table->child_relations();
is_deeply(\@seen, \@expected, "tables should not have children"); $test_count++;

#....
# Column checking
#....

# CRO based Table
ok($ships_table->knows_any_columns(), "A CRO class-derived table should know any columns");  $test_count++;
ok($ships_table->knows_all_columns(), "A CRO class-derived table should know all columns");  $test_count++;

lives_ok {
    @seen = $ships_table->columns();
} "Should be able to call columns() on a CRO-derived table";  $test_count++;

lives_ok {
    $seen = $ships_table->has_column('foo');
} "Calling has_column on a CRO-derived table should live for fake columns";  $test_count++;
ok(!$seen, "Calling has_column on a CRO-derived table should return false for fake columns");  $test_count++;
lives_ok {
    $seen = $ships_table->has_column('ship_id');
} "Calling has_column on a CRO-derived table should live for real columns";  $test_count++;
ok($seen, "Calling has_column on a CRO-derived table should return true for real columns");  $test_count++;

# Unconfigged Table
$table = Table->new(table => 'fake');
ok(!$table->knows_any_columns(), "An unconfigged table should not know any columns");  $test_count++;
ok(!$table->knows_all_columns(), "An unconfigged table should not know all columns");  $test_count++;
throws_ok {
    @seen = $table->columns();
} "Class::ReluctantORM::Exception::Call::NotPermitted", "Calling columns on a unconfigged table should be an exception";  $test_count++;
throws_ok {
    @seen = $table->has_column('foo');
} "Class::ReluctantORM::Exception::Call::NotPermitted", "Calling has_column on a unconfigged table should be an exception";  $test_count++;


# Manual Table
@seen = ();
$table = Table->new(
                    table => 'manual',
                    columns => [qw(foo bar baz)],
                   );
ok($table->knows_any_columns(), "A manual table should know some columns");  $test_count++;
ok(!$table->knows_all_columns(), "A blank table should not know all columns");  $test_count++;
lives_ok {
    @seen = $table->columns();
} "Calling columns on a manual table should not be an exception";  $test_count++;
is((scalar @seen), 3, "calling columns on a manual table should give the right column count"); $test_count++;
lives_ok {
    $seen = $table->has_column('notthere');
} "Calling has_column on a manual table should live for fake columns";  $test_count++;
ok(!$seen, "Calling has_column on a manual table should return false for fake columns");  $test_count++;
lives_ok {
    $seen = $table->has_column('foo');
} "Calling has_column on a manual table should live for real columns";  $test_count++;
ok($seen, "Calling has_column on a manual table should return true for real columns");  $test_count++;

# Blank Table
@seen = ();
$table = Table->new(
                    table => 'blank',
                    columns => [],
                   );
ok($table->knows_any_columns(), "A blank table should know some columns");  $test_count++;
ok(!$table->knows_all_columns(), "A blank table should not know all columns");  $test_count++;
lives_ok {
    @seen = $table->columns();
} "Calling columns on a blank table should not be an exception";  $test_count++;
is((scalar @seen), 0, "calling columns on a blank table should give the right column count"); $test_count++;
lives_ok {
    $seen = $ships_table->has_column('notthere');
} "Calling has_column on a blank table should live for fake columns";  $test_count++;
ok(!$seen, "Calling has_column on a blank table should return false for fake columns");  $test_count++;

done_testing($test_count);

