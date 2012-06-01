#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test CRO's cloning of SQL objects

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }

use CrormTest::Model;
use Class::ReluctantORM::SQL::Aliases;
use Scalar::Util qw(refaddr);

my $DEBUG = 0;

my $all = 1;
my %TEST_THIS = (
                 FUNCTION    => $all,
                 EXPRESSION  => $all,
                 RELATION    => $all,
                 WHERE       => $all,
                 FROM        => $all,
                 OUTPUT      => $all,
                 ORDERBY     => $all,

                 INSERT      => $all,
                 UPDATE      => $all,
                 DELETE      => $all,
                 SELECT      => $all,

                );

my %CLONE_CHECK_IGNORE_CLASSES = map { $_ => 1 } 
  qw(
        Class::ReluctantORM::SQL::Function
        Class::ReluctantORM::Relationship::HasOne
        Class::ReluctantORM::Relationship::HasMany
        Class::ReluctantORM::Relationship::HasManyMany
   );

my %CLONE_CHECK_IGNORE_KEYS = map { $_ => 1 } 
  qw(
        unique_alias_prefix
   );




if ($TEST_THIS{EXPRESSION}) {
    my %expressions = (
                       literal_int    => Literal->new(1),
                       literal_str    => Literal->new('ponies'),
                       literal_float  => Literal->new(42.5),
                       literal_null   => Literal->new(undef),

                       param_unbound     => Param->new(),
                       param_bound       => Param->new(14),
                       param_bound_null  => Param->new(undef),

                       column_unspec     => Column->new(),
                       column_col        => Column->new(column => 'foo'),
                       column_alias      => Column->new(column => 'foo', alias => 'f'),
                       column_table      => Column->new(column => 'foo', table => Table->new(table => 'bar')),

                       function_call_autobox  => FunctionCall->new('=', 1, 1),
                       function_call_expr     => FunctionCall->new('=', Literal->new(1), 1),

                       criterion_taut => Criterion->new(),

                      );
    {
        my $select = SQL->new('select');
        $select->from(From->new(Table->new(table => 'foo')));
        $select->add_output(
                            Column->new(column => 'baz'),
                           );
        $expressions{subquery} = SubQuery->new($select);
    }

    run_tests('expressions', \%expressions);
}

if ($TEST_THIS{FUNCTION}) {
    my %funcs = (
                 equal_by_name => Function->by_name('='),
                );
    run_tests('functions', \%funcs);
}

if ($TEST_THIS{RELATION}) {
    my %rels = (
                table_simple    => Table->new(table => 'foo'),
                table_schema    => Table->new(table => 'foo', alias => 'f', schema => 'blar'),
                table_class     => Table->new(Ship),

                join => Join->new('INNER', Table->new(table => 'foo'), Table->new(table => 'bar'), Criterion->new()),

               );

    {
        my $select = SQL->new('select');
        $select->from(From->new(Table->new(table => 'foo')));
        $select->add_output(
                            Column->new(column => 'baz'),
                           );
        $rels{subquery} = SubQuery->new($select);
    }

    run_tests('relations', \%rels);
}

if ($TEST_THIS{WHERE}) {
    my %funcs = (
                 empty => Where->new(),
                 taut => Where->new(Criterion->new('=',1,1)),
                );
    run_tests('where', \%funcs);
}

if ($TEST_THIS{FROM}) {
    my %funcs = (
                 table => From->new(Table->new(table => 'foo')),
                 join => From->new(Join->new('INNER', Table->new(table => 'foo'), Table->new(table => 'bar'), Criterion->new())),
                );

    {
        my $select = SQL->new('select');
        $select->from(From->new(Table->new(table => 'foo')));
        $select->add_output(
                            Column->new(column => 'baz'),
                           );
        $funcs{subquery} = From->new(SubQuery->new($select));
    }


    run_tests('from', \%funcs);
}

if ($TEST_THIS{OUTPUT}) {
    my %funcs = (
                 expression => OutputColumn->new(
                                                 expression => FunctionCall->new('=', Literal->new(1), Column->new(column => 'foo')),
                                                ),
                 literal => OutputColumn->new(Literal->new(1)),
                 column => OutputColumn->new(Column->new(column => 'foo')),
                 column_alias => OutputColumn->new(
                                                   expression => Column->new(column => 'foo'),
                                                   alias => 'bar',
                                                  ),
                 pk_true => OutputColumn->new(
                                              expression => Column->new(column => 'foo'),
                                              is_primary_key => 1,
                                             ),
                 pk_false => OutputColumn->new(
                                               expression => Column->new(column => 'foo'),
                                               is_primary_key => 0,
                                              ),
                 pk_undef => OutputColumn->new(
                                               expression => Column->new(column => 'foo'),
                                               is_primary_key => undef,
                                              ),

                );
    run_tests('output_col', \%funcs);
}

if ($TEST_THIS{ORDERBY}) {
    my %funcs = ();

    $funcs{empty} = OrderBy->new();

    $funcs{one_implicit_dir} = OrderBy->new();
    $funcs{one_implicit_dir}->add(Column->new(column => 'foo'));

    $funcs{two_implicit_dir} = OrderBy->new();
    $funcs{two_implicit_dir}->add(Column->new(column => 'foo'));
    $funcs{two_implicit_dir}->add(Column->new(column => 'bar'));

    $funcs{one_asc} = OrderBy->new();
    $funcs{one_asc}->add(Column->new(column => 'foo'), 'ASC');

    $funcs{one_desc} = OrderBy->new();
    $funcs{one_desc}->add(Column->new(column => 'foo'), 'DESC');

    $funcs{many} = OrderBy->new();
    $funcs{many}->add(Column->new(column => 'foo'));
    $funcs{many}->add(Column->new(column => 'bar'));
    $funcs{many}->add(Column->new(column => 'baz'));
    $funcs{many}->add(Column->new(column => 'fliggity'));
    $funcs{many}->add(Column->new(column => 'floggity'));
    $funcs{many}->add(Column->new(column => 'floo'));

    run_tests('order_by', \%funcs);
}

if ($TEST_THIS{SELECT}) {
    my %funcs = ();

    $funcs{degenerate} = SQL->new('select');

    $funcs{simple} = SQL->new('select');
    $funcs{simple}->from(From->new(Table->new(table => 'foo')));
    $funcs{simple}->add_output(Column->new(column => 'baz'));
    $funcs{simple}->where(Where->new(Criterion->new('=', Column->new(column => 'glepp'), 3)));

    {
        $funcs{order_by} = SQL->new('select');
        $funcs{order_by}->from(From->new(Table->new(table => 'foo')));
        $funcs{order_by}->add_output(Column->new(column => 'baz'));
        $funcs{order_by}->where(Where->new(Criterion->new('=', Column->new(column => 'glepp'), 3)));
        my $ob = OrderBy->new();
        $ob->add(Column->new(column => 'baz'));
        $funcs{order_by}->order_by($ob);
    }

    {
        $funcs{limit} = SQL->new('select');
        $funcs{limit}->from(From->new(Table->new(table => 'foo')));
        $funcs{limit}->add_output(Column->new(column => 'baz'));
        $funcs{limit}->where(Where->new(Criterion->new('=', Column->new(column => 'glepp'), 3)));
        $funcs{limit}->limit(3);
    }

    {
        $funcs{offset} = SQL->new('select');
        $funcs{offset}->from(From->new(Table->new(table => 'foo')));
        $funcs{offset}->add_output(Column->new(column => 'baz'));
        $funcs{offset}->where(Where->new(Criterion->new('=', Column->new(column => 'glepp'), 3)));
        $funcs{offset}->limit(3);
        $funcs{offset}->offset(5);
    }

    {
        $funcs{raw_where} = SQL->new('select');
        $funcs{raw_where}->from(From->new(Table->new(table => 'foo')));
        $funcs{raw_where}->add_output(Column->new(column => 'baz'));
        $funcs{raw_where}->raw_where('glepp = 3');
    }

    run_tests('select', \%funcs);
}

if ($TEST_THIS{INSERT}) {
    my %funcs = ();

    $funcs{degenerate} = SQL->new('insert');

    $funcs{simple} = SQL->new('insert');
    $funcs{simple}->table(Table->new(table => 'foo'));
    $funcs{simple}->add_input(
                              Column->new(column => 'bar'),
                              Param->new(),
                             );

    $funcs{with_return} = SQL->new('insert');
    $funcs{with_return}->table(Table->new(table => 'foo'));
    $funcs{with_return}->add_input(
                                   Column->new(column => 'bar'),
                                   Param->new(),
                                  );
    $funcs{with_return}->add_output(
                                    Column->new(column => 'baz'),
                                   );

    {
        $funcs{from_subselect} = SQL->new('insert');
        $funcs{from_subselect}->table(Table->new(table => 'foo'));
        my $select = SQL->new('SELECT');
        $select->from(From->new(Table->new(table => 'doo')));
        $select->add_output(Column->new(column => 'gleepy'));
        $funcs{from_subselect}->input_subquery(SubQuery->new($select));
    }

    run_tests('insert', \%funcs);
}

if ($TEST_THIS{UPDATE}) {
    my %funcs = ();

    $funcs{degenerate} = SQL->new('update');

    $funcs{simple} = SQL->new('update');
    $funcs{simple}->table(Table->new(table => 'foo'));
    $funcs{simple}->where(Where->new(Criterion->new('=', Column->new(column => 'glepp'), 3)));
    $funcs{simple}->add_input(
                              Column->new(column => 'bar'),
                              Param->new(),
                             );

    $funcs{with_return} = SQL->new('update');
    $funcs{with_return}->table(Table->new(table => 'foo'));
    $funcs{with_return}->where(Where->new(Criterion->new('=', Column->new(column => 'glepp'), 3)));
    $funcs{with_return}->add_input(
                                   Column->new(column => 'bar'),
                                   Param->new(),
                                  );
    $funcs{with_return}->add_output(
                                    Column->new(column => 'baz'),
                                   );

    run_tests('update', \%funcs);
}

if ($TEST_THIS{DELETE}) {
    my %funcs = ();

    $funcs{degenerate} = SQL->new('delete');

    $funcs{simple} = SQL->new('delete');
    $funcs{simple}->from(From->new(Table->new(table => 'foo')));
    $funcs{simple}->where(Where->new(Criterion->new('=', Column->new(column => 'glepp'), 3)));

    run_tests('delete', \%funcs);
}



done_testing($test_count);










sub run_tests {
    my $group = shift;
    my $what = shift;
    foreach my $name (sort keys %$what) { # sort for test number consistency
        my $orig = $what->{$name};
        my $class = ref($orig);
        my $clone;
        lives_ok {
            $clone = $orig->clone();
        } "cloning of $group/$name should live"; $test_count++;
        ok(defined($clone), "clone of $group/$name should be defined"); $test_count++;
        different_deeply($orig, $clone, "clone should be deeply different - $group/$name"); $test_count++;
    }
}

sub different_deeply {
    my ($orig, $clone, $msg) = @_;
    ok(different_deeply_rec($orig, $clone), $msg);
}




sub different_deeply_rec {
    my ($l, $r) = @_;

    if (0) {
        note("\nComparing:\n", explain($l), explain($r));
    }

    unless (ref($l) eq ref($r)) {
        note explain($l) . " and " . explain($r) . " have different classes";
        return 0;
    }

    unless (exists $CLONE_CHECK_IGNORE_CLASSES{ref($l)}) {
        if (refaddr($l) eq refaddr($r)) {
            note explain($l) . " and " . explain($r) . " have the same memory location";
            return 0;
        }
    }

    my @lvals;
    my @rvals;
    my @rkeys;
    my @lkeys;

    if (ref($l) eq 'ARRAY') {
        @lvals = @$l;
        @rvals = @$r;
    } elsif (ref($l) eq 'REF') {
        # weak ref entry - assume it matches
        return 1;
    } else {
        @lkeys = sort keys %$l;
        @rkeys = sort keys %$r;

        my %rhash = %$r;
        my %lhash = %$l;

        @lvals = @lhash{@lkeys};
        @rvals = @rhash{@rkeys};

        for my $i (0..$#lkeys) {
            unless ($lkeys[$i] eq $rkeys[$i]) {
                note(explain($l),explain($r), " have different keys: $lkeys[$i] vs $rkeys[$i]");
                return 0;
            }
        }
    }


    unless (@lvals == @rvals) {
        note("\ndifferent element counts:\n", explain($l), "\n", explain($r));
        return 0;
    }


    my $outcome = 1;
    foreach my $i (0..$#lvals) {
        last unless $outcome;
        my ($lv, $rv) = ($lvals[$i], $rvals[$i]);

        unless ((ref($lv) && ref($rv)) ||
                !(ref($lv) || ref($rv))) {
            note(explain($lv),explain($rv), " - not both refs or neither");
            return 0;
        }

        if (!ref($lv)) {
            # Ok, it's a scalar.  Check to see if we should ignore it.
            if ((!$lkeys[$i]) || ($lkeys[$i] && !(exists $CLONE_CHECK_IGNORE_KEYS{$lkeys[$i]}))) {
                if (!defined($lv) && !defined($rv)) {
                    $outcome &&= 1; # both undef
                } elsif ($lv eq $rv) {
                    $outcome &&= 1; # yeah yeah
                } else {
                    if ($lkeys[$i]) {
                        note("\ndifferent non-ref values on key $lkeys[$i]:\n", explain($lv), "\n", explain($rv));
                    } else {
                        note("\ndifferent non-ref values on $i th element:\n", explain($lv), "\n", explain($rv));
                    }

                    return 0;
                }
            } else {
                # Ignored key
                $outcome &&= 1;
            }
        } else {
            $outcome &&= different_deeply_rec($lv, $rv);
        }
    }
    return $outcome;

}
