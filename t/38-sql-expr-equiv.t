#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test CRO's is_equivalent function on SQL Expressions

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }

use CrormTest::Model;
use Class::ReluctantORM::SQL::Aliases;

my $DEBUG = 0;

my %TEST_THIS = (
                 LITERAL       => 1,
                 PARAM         => 1,
                 COLUMN        => 1,
                 FUNCTION_CALL => 1,
                 CRITERION     => 1,
                 SUBQUERY      => 1,

                 COMPOSITE     => 1,

                );

if ($TEST_THIS{LITERAL}) {
    my @expr = (
                # Each group forms an equivalence class
                {
                 name => 'nulls',
                 expr => {
                          plain   => Literal->new(undef),
                          typed   => Literal->new(undef, 'NULL'),
                          prefab  => Literal->NULL,
                         },
                 todo => 0,
                },
                {
                 name => 'integer',
                 expr => {
                          plain   => Literal->new(1),
                          typed   => Literal->new(1, 'NUMBER'),
                         },
                 todo => 0,
                },
                {
                 name => 'float',
                 expr => {
                          plain   => Literal->new(42.5),
                          typed   => Literal->new(42.5, 'NUMBER'),
                         },
                 todo => 0,
                },
                {
                 name => 'str',
                 expr => {
                          plain   => Literal->new('ponies'),
                          typed   => Literal->new('ponies', 'STRING'),
                         },
                 todo => 0,
                },
                {
                 name => 'boolean true',
                 expr => {
                          prefab     => Literal->TRUE(),
                          typed_1    => Literal->new(1, 'BOOLEAN'),
                          typed_TRUE => Literal->new('TRUE', 'BOOLEAN'),
                         },
                 todo => 0,
                },
                {
                 name => 'boolean false',
                 expr => {
                          prefab     => Literal->FALSE(),
                          typed_1    => Literal->new(0, 'BOOLEAN'),
                          typed_FALSE => Literal->new('FALSE', 'BOOLEAN'),
                         },
                 todo => 0,
                },
                {
                 name => 'custom type',
                 expr => {
                          typed_glen    => Literal->new('giggity', 'QUAGMIRISM'),
                         },
                 todo => 0,
                },
               );
    run_tests('Literal', \@expr);
}


if ($TEST_THIS{PARAM}) {
    my @expr = (
                # Each group forms an equivalence class
                {
                 name => 'unbound',
                 expr => {
                          new  => Param->new(),
                         },
                 todo => 0,
                },
                {
                 name => 'null',
                 expr => {
                          new  => Param->new(undef),
                         },
                 todo => 0,
                },
                {
                 name => 'integer',
                 expr => {
                          new   => Param->new(1),
                         },
                 todo => 0,
                },
               );
    run_tests('Param', \@expr);
}

if ($TEST_THIS{COLUMN}) {
    my @expr = (
                # Each group forms an equivalence class
                {
                 name => 'indeterminate',
                 expr => {
                          new  => Column->new(),
                         },
                 todo => 0,
                },
                {
                 name => 'no-table',
                 expr => {
                          named_arg_new  => Column->new(column => 'foo'),
                          aliased_1  => Column->new(column => 'foo', alias => 'glack'),
                          aliased_2  => Column->new(column => 'foo', alias => 'gleep'),
                          # Include ONE with a table here, so that we can test against that
                          tabled => Column->new(column => 'foo', table => Table->new(table => 'bar1')),
                         },
                 todo => 0,
                },
                {
                 name => 'tabled-no-schema',
                 expr => {
                          tabled => Column->new(column => 'foo2', table => Table->new(table => 'bar2')),
                          tabled_schema => Column->new(column => 'foo2', table => Table->new(table => 'bar2', schema => 'wiggity')),
                          tabled_alias1_schema => Column->new(column => 'foo2', table => Table->new(table => 'bar2', schema => 'wiggity', alias => 'beep1')),
                          tabled_alias2_schema => Column->new(column => 'foo2', table => Table->new(table => 'bar2', schema => 'wiggity', alias => 'beep2')),
                         },
                 todo => 0,
                },
                {
                 name => 'different-table1',
                 expr => {
                          table3 => Column->new(column => 'foo3', table => Table->new(table => 'bar3')),
                         },
                 todo => 0,
                },
                {
                 name => 'different-table2',
                 expr => {
                          table4 => Column->new(column => 'foo3', table => Table->new(table => 'bar4')),
                         },
                 todo => 0,
                },

               );
    run_tests('Column', \@expr);
}

if ($TEST_THIS{FUNCTION_CALL}) {
    my @expr = (
                # Each group forms an equivalence class
                {
                 name => 'add2arg',
                 expr => {
                          add12  => FunctionCall->new('+', 1, 2),
                          add21  => FunctionCall->new('+', 2, 1),
                         },
                 todo => 0,
                },
                {
                 name => 'add3arg_commute',
                 expr => {
                          add123  => FunctionCall->new('+', 1, 2, 3),
                          add132  => FunctionCall->new('+', 1, 3, 2),
                          add213  => FunctionCall->new('+', 2, 1, 3),
                          add231  => FunctionCall->new('+', 2, 3, 1),
                          add321  => FunctionCall->new('+', 3, 2, 1),
                          add312  => FunctionCall->new('+', 3, 1, 2),
                         },
                 todo => 0,
                },
                {
                 name => 'add3arg_associate',
                 expr => {
                          add45_6  => FunctionCall->new('+', FunctionCall->new('+', 4, 5), 6),
                          add4_56  => FunctionCall->new('+', 4, FunctionCall->new('+', 5, 6)),
                         },
                 todo => 1, # TODO no support for associative transformation yet
                },
                {
                 name => 'subtr2arg_comm_1',
                 expr => {
                          subtr12  => FunctionCall->new('-', 1, 2),
                         },
                 todo => 0,
                },
                {
                 name => 'subtr2arg_comm_2',
                 expr => {
                          subtr21  => FunctionCall->new('-', 2, 1),
                         },
                 todo => 0,
                },
               );
    run_tests('FunctionCall', \@expr);
}

if ($TEST_THIS{CRITERION}) {
    my @expr = (
                # Each group forms an equivalence class
                {
                 name => 'and2arg',
                 expr => {
                          andTF  => Criterion->new('AND', Literal->TRUE, Literal->FALSE),
                          andFT  => Criterion->new('AND', Literal->FALSE, Literal->TRUE),
                         },
                 todo => 0,
                },
                # TODO - more, presumably
               );
    run_tests('Criterion', \@expr);
}



done_testing($test_count);

sub run_tests {
    my $series = shift;
    my $expr = shift;
    test_reflexivity($series, $expr);
    test_equivalence_within_classes($series, $expr);
    test_non_equivalence_across_classes($series, $expr);
}

sub test_reflexivity {
    my $series = shift;
    my $groups = shift;
    foreach my $group (@$groups) {
        my $group_name = $group->{name};
        if ($group->{todo}) {
          TODO: {
                local $TODO = "$series: group $group_name is TODO";
                run_reflexivity_tests_on_group($series, $group);
            }
        } else {
            run_reflexivity_tests_on_group($series, $group);
        }
    }
}

sub run_reflexivity_tests_on_group {
    my $series = shift;
    my $group = shift;
    my $group_name = $group->{name};
    foreach my $expr_name (keys %{$group->{expr}}) {
        my $expr = $group->{expr}{$expr_name};
        my $result;
        lives_ok {
            $result = $expr->is_equivalent($expr);
        } "$series: $group_name/$expr_name self-equivalence should live"; $test_count++;
        ok(defined($result) && $result, "$series: $group_name/$expr_name self-equivalence should be true");  $test_count++;
    }
}

sub test_equivalence_within_classes {
    my $series = shift;
    my $groups = shift;
    foreach my $group (@$groups) {
        my $group_name = $group->{name};
        if ($group->{todo}) {
          TODO: {
                local $TODO = "$series: group $group_name is TODO";
                run_class_equivalence_tests_on_group($series, $group);
            }
        } else {
            run_class_equivalence_tests_on_group($series, $group);
        }
    }
}

sub run_class_equivalence_tests_on_group {
    my $series = shift;
    my $group = shift;
    my $group_name = $group->{name};
    foreach my $left_name (keys %{$group->{expr}}) {
        foreach my $right_name (keys %{$group->{expr}}) {
            next if ($left_name eq $right_name); # Don't re-test self-equivalence
            my $left   = $group->{expr}{$left_name};
            my $right  = $group->{expr}{$right_name};

            my $result;

            lives_ok {
                $result = $left->is_equivalent($right);
            } "$series: $group_name/$left_name equiv to $group_name/$right_name should live "; $test_count++;
            ok(defined($result) && $result, "$series: $group_name/$left_name equiv to $group_name/$right_name should be true");  $test_count++;

            lives_ok {
                $result = $right->is_equivalent($left);
            } "$series: $group_name/$right_name equiv to $group_name/$left_name should live "; $test_count++;
            ok(defined($result) && $result, "$series: $group_name/$right_name equiv to $group_name/$left_name should be true");  $test_count++;
        }
    }
}

sub test_non_equivalence_across_classes {
    my $series = shift;
    my $groups = shift;
    foreach my $group (@$groups) {
        my $group_name = $group->{name};
        if ($group->{todo}) {
          TODO: {
                local $TODO = "$series: group $group_name is TODO";
                run_non_equivalence_tests_on_group($series, $group, $groups);
            }
        } else {
            run_non_equivalence_tests_on_group($series, $group, $groups);
        }
    }
}

sub run_non_equivalence_tests_on_group {
    my $series = shift;
    my $this_group = shift;
    my $all_groups = shift;
    my $this_group_name = $this_group->{name};
    foreach my $other_group (@$all_groups) {
        my $other_group_name = $other_group->{name};
        next if ($this_group_name eq $other_group_name);

        foreach my $this_expr_name (keys %{$this_group->{expr}}) {
            foreach my $other_expr_name (keys %{$other_group->{expr}}) {
                my $left   = $this_group->{expr}{$this_expr_name};
                my $right  = $other_group->{expr}{$other_expr_name};

                my $result;

                lives_ok {
                    $result = $left->is_equivalent($right);
                } "$series: $this_group_name/$this_expr_name equiv to $other_group_name/$other_expr_name should live"; $test_count++;
                ok(defined($result) && !$result, "$series: $this_group_name/$this_expr_name equiv to $other_group_name/$other_expr_name should be false");  $test_count++;

                lives_ok {
                    $result = $right->is_equivalent($left);
                } "$series: $other_group_name/$other_expr_name equiv to $this_group_name/$this_expr_name should live "; $test_count++;
                ok(defined($result) && !$result, "$series: $other_group_name/$other_expr_name equiv to $this_group_name/$this_expr_name should be false");  $test_count++;
            }
        }
    }
}


