#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's SQL->annotate() facility
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;
use Class::ReluctantORM::SQL;
use Class::ReluctantORM::SQL::Aliases;
sub CRO { return 'Class::ReluctantORM'; }

my @TESTS = 
  (
   { table => 'ships',   class => 'Ship',   is_join => 0, exists => 1, schema => 'caribbean', },
   { table => 'ships',   class => 'Ship',   is_join => 0, exists => 1,                        },
   { table => 'pirates', class => 'Pirate', is_join => 0, exists => 1, schema => 'caribbean', },
   { table => 'booties', class => 'Booty',  is_join => 0, exists => 1, schema => 'caribbean', },
   { table => 'ranks',   class => 'Rank',   is_join => 0, exists => 1, schema => 'caribbean', },
   { table => 'booties2pirates',            is_join => 1, exists => 1, schema => 'caribbean', },
   { table => 'fluffy_clouds',              is_join => 0, exists => 0, schema => 'caribbean', },
   { table => 'rainbows',                   is_join => 0, exists => 0,                        },
  );



my %TEST_THIS = (
                 IS_JOIN_TABLE       => 1,
                 FIND_CLASS_BY_TABLE => 1,
                 ANNOTATE_SIMPLE     => 1,
                 ANNOTATE_RETURNING  => 0,
                );

if ($TEST_THIS{IS_JOIN_TABLE}) {
    foreach my $test (@TESTS) {
        my $label = ($test->{schema} ? $test->{schema} : '(no schema)') . '.' . $test->{table};
        my $expected = !$test->{exists} ? undef : $test->{is_join};
        my $seen;

        if (1) {
            $seen = -1;
            lives_ok {
                $seen = CRO->_is_join_table(table_name => $test->{table});
            } "is_join_table (table_name) for $label should live"; $test_count++;
            is($seen, $expected, "is_join_table (table_name) for $label should be correct"); $test_count++;
        }

        if (1) {
            $seen = -1;
            lives_ok {
                $seen = CRO->_is_join_table(table_name => $test->{table}, schema_name => $test->{schema});
            } "is_join_table (table_name, schema_name) for $label should live"; $test_count++;
            is($seen, $expected, "is_join_table (table_name, schema_name) for $label should be correct"); $test_count++;
        }

        if (1) {
            $seen = -1;
            lives_ok {
                $seen = CRO->_is_join_table(table_obj => Table->new(table => $test->{table}, schema => $test->{schema}));
            } "is_join_table (table_obj) for $label should live"; $test_count++;
            is($seen, $expected, "is_join_table (table_obj) for $label should be correct"); $test_count++;
        }
    }
}

if ($TEST_THIS{FIND_CLASS_BY_TABLE}) {
    foreach my $test (@TESTS) {
        my $label = ($test->{schema} ? $test->{schema} : '(no schema)') . '.' . $test->{table};
        my $expected = (!$test->{exists} || $test->{is_join}) ? undef : ('CrormTest::Model::' . $test->{class});
        my $seen;

        $seen = -1;
        lives_ok {
            $seen = CRO->_find_class_by_table(table_name => $test->{table});
        } "find_class_by_table (table_name) for $label should live"; $test_count++;
        is($seen, $expected, "find_class_by_table (table_name) for $label should be correct"); $test_count++;

        $seen = -1;
        lives_ok {
            $seen = CRO->_find_class_by_table(table_name => $test->{table}, schema_name => $test->{schema});
        } "find_class_by_table (table_name, schema_name) for $label should live"; $test_count++;
        is($seen, $expected, "find_class_by_table (table_name, schema_name) for $label should be correct"); $test_count++;

        $seen = -1;
        lives_ok {
            $seen = CRO->_find_class_by_table(table_obj => Table->new(table => $test->{table}, schema => $test->{schema}));
        } "find_class_by_table (table_obj) for $label should live"; $test_count++;
        is($seen, $expected, "find_class_by_table (table_obj) for $label should be correct"); $test_count++;
    }
}

if ($TEST_THIS{ANNOTATE_SIMPLE}) {
    my ($sql, $class, $label);

    $class = Ship;
    $label = 'one-column, one-table select';
    $sql = SQL->new('SELECT');
    $sql->from(From->new(Table->new(table => 'ships')));
    $sql->where(Where->new());
    $sql->add_output(Column->new(column => 'ship_id'));
    annotate_checks($sql, $class, $label, \$test_count);

    $class = undef;
    $label = 'one-column, one-table select from a join table';
    $sql = SQL->new('SELECT');
    $sql->from(From->new(Table->new(table => 'booties2pirates')));
    $sql->where(Where->new());
    $sql->add_output(Column->new(column => 'pirate_id'));
    annotate_checks($sql, $class, $label, \$test_count);

    $class = Ship;
    $label = 'one-param, one-table update';
    $sql = SQL->new('UPDATE');
    $sql->table(Table->new(table => 'ships'));
    $sql->where(Where->new(Criterion->new('=', 0, 1)));
    $sql->add_input(Column->new(column => 'name'), Param->new('Awesome Boat'));
    annotate_checks($sql, $class, $label, \$test_count);

    $class = Ship;
    $label = 'one-param, one-returning, one-table update';
    $sql = SQL->new('UPDATE');
    $sql->table(Table->new(table => 'ships'));
    $sql->where(Where->new(Criterion->new('=', 0, 1)));
    $sql->add_input(Column->new(column => 'name'), Param->new('Awesome Boat'));
    $sql->add_output(Column->new(column => 'ship_id'));
    annotate_checks($sql, $class, $label, \$test_count);

}

done_testing($test_count);

sub annotate_checks {
    my ($sql, $base_class, $label, $test_count_ref) = @_;

    lives_ok {
        $sql->annotate();
    } "annotate on a $label should live"; $$test_count_ref++;

    # all tables should be either join tables or have class defined
    my $all_ok = 1;
    foreach my $table ($sql->tables()) {
        $all_ok &&= defined($table->class()) || CRO->_is_join_table(table_obj => $table);
    }
    ok($all_ok, "all tables in a $label should either have a class or be a join table"); $$test_count_ref++;

    my $base_table = $sql->base_table();
    ok(defined($base_table), "$label should have a base table"); $$test_count_ref++;
    is(($base_table ? $base_table->class() : undef), $base_class, "$label should have the correct base class"); $$test_count_ref++;
    


}
