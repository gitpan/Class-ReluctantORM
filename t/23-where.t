#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's Abstract SQL Where functionality
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;

use Class::ReluctantORM::SQL::Aliases;
use_ok('Class::ReluctantORM::SQL::Where'); $test_count++;

my %TEST_THIS = (
                 CRITERION => 1,
                 EMPTY     => 1,
                );


#....
# Criterion Tests
#....
if ($TEST_THIS{CRITERION}) {
    my ($crit, $lit, @seen, $seen);
    $crit = Criterion->new('=', 1, 1);
    ok(defined $crit, "New Criterion should be defined"); $test_count++;
    ok(!$crit->is_leaf_expression(), "Criterion should not be a leaf"); $test_count++;
    ok($crit->is_criterion(), "Criterion should be a criterion"); $test_count++;
    foreach my $type (qw(param literal column subquery)) {
        my $method = 'is_' . $type;
        ok(!$crit->$method, "Criterion should not be a $type"); $test_count++;
    }
    is($crit->function->name, '=', "Criterion operation should be correct"); $test_count++;
    @seen = $crit->child_expressions();
    is((scalar @seen), 2, "Criterion should have two children"); $test_count++;

    $lit = $seen[0];
    ok($lit->is_leaf_expression, "Literal should be a leaf"); $test_count++;
    is($lit->value, 1, "Literal should carry correct value"); $test_count++;
    ok($lit->is_literal, "Literal should be a literal"); $test_count++;
    foreach my $type (qw(criterion param column function_call subquery)) {
        my $method = 'is_' . $type;
        ok(!$lit->$method, "Literal should not be a $type"); $test_count++;
    }
    $seen = $lit->parent_expression();
    is_deeply($seen, $crit, "The parent of the Literal should be the Criterion"); $test_count++;
}

#....
# Empty Where Contructor
#....
if ($TEST_THIS{EMPTY}) {
    my ($where, @expected, @seen, $seen, $expected);

    $where = Class::ReluctantORM::SQL::Where->new();
    ok(defined($where), "where constructor works"); $test_count++;

    @expected = ();
    @seen = $where->params();
    is_deeply(\@seen, \@expected, "params should be empty on a newborn where"); $test_count++;

    @expected = ();
    @seen = $where->columns();
    is_deeply(\@seen, \@expected, "columns should be empty on a newborn where"); $test_count++;

    @expected = ();
    @seen = $where->tables();
    is_deeply(\@seen, \@expected, "tables should be empty on a newborn where"); $test_count++;

    $expected = Criterion->new_tautology();
    $seen = $where->root_criterion();
    is_deeply($seen, $expected, 'newborn where clause should have a tautological criteria'); $test_count++;
}


done_testing($test_count);

