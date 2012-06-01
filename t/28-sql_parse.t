#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's SQL parsing
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;
use Class::ReluctantORM::SQL::Aliases;
use Class::ReluctantORM::SQL;

my $driver = Ship->driver();
my $driver_name = driver_name();

my (@PASS_STATEMENTS, @FAIL_STATEMENTS, @PASS_WHERE, @FAIL_WHERE);

SKIP:
{
    unless ($driver->supports_parsing()) {
        skip("$driver_name() driver does not support SQL parsing",1);
    }

    init_sql();

    #run_pass_statement_tests();
    #run_fail_statement_tests();

    run_pass_where_tests();
    run_fail_where_tests();

    # For reasons too dumb to get into, order_by parsing is tested in 22-order_by.t

}
done_testing($test_count);


#======================================================#
#                      Test SQL                        #
#======================================================#


sub init_sql {

    @PASS_STATEMENTS =
      (
       {
        label => 'unquoted unaliased single crit single output',
        op => 'SELECT', out_count => 1, table_count => 1,
        str => q|SELECT ship_id FROM ships WHERE gun_count > 11|,
       },
       {
        label => 'quoted unaliased single crit single output',
        op => 'SELECT', out_count => 1, table_count => 1,
        str => q|SELECT ship_id FROM ships WHERE "gun_count" > 11|,
       },
       {
        label => 'unquoted explicit table single crit single output',
        op => 'SELECT', out_count => 1, table_count => 1,
        str => q|SELECT ship_id FROM ships WHERE ships.gun_count > 11|,
       },
       {
        label => 'unquoted explicit table single crit explicit table on single output',
        op => 'SELECT', out_count => 1, table_count => 1,
        str => q|SELECT ships.ship_id FROM ships WHERE ships.gun_count > 11|,
       },
       {
        label => 'quoted explicit table single crit single output',
        op => 'SELECT', out_count => 1, table_count => 1,
        str => q|SELECT ship_id FROM ships WHERE "ships"."gun_count" > 11|,
       },
       {
        label => 'quoted explicit table single crit single output',
        op => 'SELECT', out_count => 1, table_count => 1,
        str => q|SELECT ship_id FROM ships WHERE "ships"."gun_count" > 11|,
       },
       {
        label => 'unambiguous name in WHERE',
        op => 'SELECT', out_count => 1, table_count => 2,
        str => <<EOS
SELECT ship_id
  FROM ships
  INNER JOIN pirates ON (pirates.ship_id = ships.ship_id)
 WHERE ships.name = 'Revenge'
EOS
       },
       {
        label => 'unambiguous name in WHERE',
        op => 'SELECT', out_count => 1, table_count => 2,
        str => <<EOS
SELECT ship_id
  FROM ships
  INNER JOIN pirates ON (pirates.ship_id = ships.ship_id)
 WHERE pirates.name = 'Wesley'
EOS
       },

      );

    @FAIL_STATEMENTS = 
      (
       {
        label => 'ambiguous name in WHERE',
        str => <<EOS
SELECT ship_id
  FROM ships
  INNER JOIN pirates ON (pirates.ship_id = ships.ship_id)
 WHERE name = 'Revenge'
EOS
       },
      );


    @PASS_WHERE =
      (

       {
        str => "ships.name = 'Black Pearl'",
        crit => Criterion->new('=', Column->new(column =>'name', table => Table->new(table => 'ships')), Literal->new('Black Pearl')),
        cols => [ 'name' ],
        param_count => 0,
        tables => ['ships'],
       },


       {
        str => '1=1',
        param_count => 0,
        crit => Criterion->new('=', Literal->new(1), Literal->new(1)),
        cols => [],
        tables => [],
       },

       {
        str => "name = 'Black Pearl'",
        crit => Criterion->new('=',Column->new(column =>'name'),Literal->new('Black Pearl')),
        cols => [ 'name' ],
        param_count => 0,
        tables => [],
       },

       {
        str => "flag = TRUE",
        crit => Criterion->new('=',Column->new(column =>'flag'),Literal->TRUE()),
        cols => [ 'flag' ],
        param_count => 0,
        tables => [],
       },

       # ships

       {
        str => "PIRATES.name = 'Red Beard'",
        crit => Criterion->new('=', Column->new(column =>'name', table => Table->new(table => 'pirates')), Literal->new('Red Beard')),
        cols => [ 'name' ],
        param_count => 0,
        tables => ['pirates'],
       },
       {
        label => 'one schema.table.column criteria with no placeholders',
        crit => Criterion->new('=', Column->new(column =>'name', table => Table->new(table => 'ships')), Literal->new('Black Pearl')),
        # This is more correct, but SQL::Parser currently drops schema names
        #crit => Criterion->new('=', Column->new(column =>'name', table => Table->new(table => 'ships', schema => 'caribbean')), Literal->new('Black Pearl')),
        str => "caribbean.ships.name = 'Black Pearl'",
        cols => [ 'name' ],
        param_count => 0,
        tables => ['ships'],
       },
       {
        label => 'one column=column criteria with no placeholders',
        str => "name = gun_count",
        crit => Criterion->new('=', Column->new(column =>'name'), Column->new(column =>'gun_count')),
        cols => [ 'name', 'gun_count' ],
        param_count => 0,
        tables => [],
       },
       {
        label => 'one column=? criteria',
        str => "name = ?",
        crit => Criterion->new('=', Column->new(column =>'name'), Param->new()),
        cols => [ 'name' ],
        param_count => 1,
        tables => [],
       },
       {
        label => 'two criteria, column=? AND column < ?',
        str => "name = ? AND gun_count < ?",
        crit => Criterion->new(
                               'AND',
                               Criterion->new('=', Column->new(column =>'name'), Param->new()),
                               Criterion->new('<', Column->new(column =>'gun_count'), Param->new()),
                              ),
        cols => [ 'name', 'gun_count' ],
        param_count => 2,
        tables => [],
       },
       {
        label => ' WE headache #2',
        str => " users.user_id = ? AND quote_statuses.name = ? ",
        crit => Criterion->new(
                               'AND',
                               Criterion->new('=', Column->new(column =>'user_id', table => Table->new(table => 'users')), Param->new()),
                               Criterion->new('=', Column->new(column =>'name', table => Table->new(table => 'quote_statuses')), Param->new()),
                              ),
        cols => [ 'user_id', 'name' ],
        param_count => 2,
        tables => [],
        todo => [qw(PostgreSQL)],
       },




      );


}

#======================================================#
#                      Utils                           #
#======================================================#

sub run_pass_statement_tests {
    foreach my $test (@PASS_STATEMENTS) {
        if ($test->{todo} && grep {$driver_name eq $_ } @{$test->{todo}}) {
          TODO: {
                local $TODO = $test->{label} . " is TODO for driver $driver_name ";
                run_pass_statement_test($test);
            }
        } else {
            run_pass_statement_test($test);
        }
    }
}

sub run_pass_statement_test {
    my $test = shift;
    my $label = $test->{label};
    my $sql;
    lives_ok {
        $sql = $driver->parse_statement($test->{str}, ($test->{opts} ? $test->{opts} : ()));
    } "parse_statement on '$label' should live"; $test_count++;
    ok(defined($sql), "SQL object from parse_statement on '$label' should be defined"); $test_count++;
    if ($sql && $test->{op}) {
        is($test->{op}, $sql->operation, "SQL operation from parse_statement on '$label' should be correct"); $test_count++;
    }
    if ($sql && defined($test->{out_count})) {
        is($test->{out_count}, (scalar $sql->output_columns()), "SQL output column count from parse_statement on '$label' should be correct"); $test_count++;
    }
    if ($sql && defined($test->{table_count})) {
        is($test->{table_count}, (scalar $sql->tables()), "SQL table count from parse_statement on '$label' should be correct"); $test_count++;
    }
}

sub run_pass_where_tests {
    foreach my $test (@PASS_WHERE) {
        #next unless ($test->{only}); # DEBUG
        if ($test->{todo} && grep {$driver_name eq $_ } @{$test->{todo}}) {
          TODO: {
                local $TODO = $test->{label} . " is TODO for driver $driver_name ";
                run_pass_where_test($test);
            }
        } else {
            run_pass_where_test($test);
        }
    }
}

sub run_pass_where_test {
    my $test = shift;
    my $label = $test->{label} || $test->{str};
    my ($where, @seen, @expected, $seen, $expected);
    lives_ok {
        $where = $driver->parse_where($test->{str}, ($test->{opts} ? $test->{opts} : ()));
    } "parse_where on '$label' should live"; $test_count++;
    ok(defined($where), "SQL::Where object from parse_where on '$label' should be defined"); $test_count++;
    return unless $where;

    @seen = map { uc($_) } map { $_->column } $where->columns();
    @expected = map { uc($_) } @{$test->{cols}};
    is_deeply(\@seen, \@expected, "$label has right columns"); $test_count++;

    $seen = scalar $where->params();
    is_deeply($seen, $test->{param_count}, "$label has right param count"); $test_count++;

    @seen = map { uc($_) } map { $_->table } $where->tables();
    @expected = map { uc($_) } @{$test->{tables}};
    is_deeply(\@seen, \@expected, "$label has right tables"); $test_count++;

    $seen = $where->root_criterion;
    $expected = $test->{crit};
    # sooooo retarded
    $seen->walk_leaf_expressions(\&uppercase_table_names);
    $expected->walk_leaf_expressions(\&uppercase_table_names);
    is_deeply($seen, $expected, "$label criterion structure is correct"); $test_count++;
}

sub run_fail_statement_tests {
    foreach my $test (@FAIL_STATEMENTS) {
        my $label = $test->{label};
        my $ex = $test->{exception} || 'Class::ReluctantORM::Exception::SQL::ParseError';
        throws_ok {
            my $sql = $driver->parse_statement($test->{str}, ($test->{opts} ? $test->{opts} : ()));
        } $ex, "parse_statement on '$label' should fail"; $test_count++;
    }
}

sub run_fail_where_tests {
    foreach my $test (@FAIL_WHERE) {
        my $label = $test->{label} || $test->{str};
        my $ex = $test->{exception} || 'Class::ReluctantORM::Exception::SQL::ParseError';
        throws_ok {
            my $where = $driver->parse_where($test->{str}, ($test->{opts} ? $test->{opts} : ()));
        } $ex, "parse_where on '$label' should fail"; $test_count++;
    }
}



sub driver_name {
    my $class = ref($driver);
    $class =~ s/Class::ReluctantORM::Driver:://;
    return $class;
}

sub uppercase_table_names {
    my $expr = shift;
    return unless $expr->is_column();
    return unless $expr->table();
    $expr->table->table(uc($expr->table->table()));
}

