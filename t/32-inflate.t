#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's SQL->inflate()
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;
use Class::ReluctantORM::SQL::Aliases;

my $all = 1;
my %TEST_THIS = (
                 INIT => 1,
                 IS_INFLATABLE   => $all,
                 SIMPLE          => $all,
                 REL_HO          => $all,
                 REL_HM          => $all,
                 REL_HMM         => $all,
                 REL_HL          => $all,
                 COMPLEX         => $all,
                );

my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;
if ($TEST_THIS{INIT}) {
    my $ship = Ship->create(
                            name => 'Revenge',
                            gun_count => 80,
                            waterline => 25,
                            ship_type_id => $frigate_type_id,
                           );
    my @pirates;
    foreach my $color (qw(Red Blue Green)) {
        push @pirates, Pirate->create(
                                      name => $color . ' Beard',
                                      ship => $ship,
                                     );
    }
    foreach my $island (qw(Skull Bermuda)) {
        Booty->create(
                      place => $island,
                      cash_value => 23,
                      pirates => \@pirates,
                     );
    }
}

if ($TEST_THIS{IS_INFLATABLE}) {
    my @tests =
      (
       {
        label => [ 'one-column, one-table select', 'with default opts' ],
        result => 1, opts => {},
        #skip => 1,
       },
       {
        label => [ 'one-column, one-table select', 'with auto ann, auto rec, yes add col' ],
        result => 1, opts => { auto_annotate => 1, auto_reconcile => 1, add_output_columns => 1},
        #skip => 1,
       },
       {
        label => [ 'one-column, one-table select', 'with auto ann, auto rec, no add col' ],
        result => 0, opts => { auto_annotate => 1, auto_reconcile => 1, add_output_columns => 0},
        #skip => 1,
       },
       {
        label => [ 'one-column, one-table select', 'with no ann, no rec, no add col'],
        result => 0, opts => { auto_annotate => 0, auto_reconcile => 0, add_output_columns => 0},
        #skip => 1,
       },
       {
        label => [ 'one-column, one-table select', 'with manual ann, man rec'],
        result => 1, opts => { auto_annotate => 0, auto_reconcile => 0},
        pre => sub { $_[0]->annotate(); $_[0]->reconcile(add_output_columns => 1); },
        #skip => 1,
       },
       {
        label => [ 'one-column, one-table select', 'with manual ann, no rec'],
        result => 0, opts => { auto_annotate => 0, auto_reconcile => 0},
        pre => sub { $_[0]->annotate() },
        #skip => 1,
       },
       {
        label => [ 'one-column, one-table select from a join table', 'with default opts' ],
        result => 0, opts => {},
        #skip => 1,
       },
       {
        label => [ 'one-column, one-table select from a join table', 'with auto ann, no rec' ],
        result => 0, opts => { auto_annotate => 1, auto_reconcile => 0},
        #skip => 1,
       },
       {
        label => [ 'one-param, one-table update', 'with default opts' ],
        result => 1, opts => {}, # add columns will add essential columns, creating a RETURNING clause
        #skip => 1,
       },
       {
        label => [ 'one-param, one-table update', 'with auto ann, auto rec, no add col' ],
        result => 0, opts => { auto_annotate => 1, auto_reconcile => 1, add_output_columns => 0},
        #skip => 1,
       },
       {
        label => [ 'one-param, one-returning, one-table update', 'with default opts' ],
        result => 1, opts => {},
        #skip => 1,
       },
       {
        label => [ 'two table select related by has-one', 'with default opts' ],
        result => 1, opts => {},
        #skip => 1,
       },
       {
        label => [ 'two table select related by has-one', 'without reconcile' ],
        result => 0, opts => {auto_reconcile => 0},
        #skip => 1,
       },
       {
        label => [ 'two table select related by has-one', 'without ann' ],
        result => 0, opts => {auto_annotate => 0},
       },
      );

    foreach my $test (@tests) {
        next if $test->{skip};
        my ($result, $exception);
        my $label = $test->{label}->[0] . ' ' . $test->{label}->[1];
        my $sql = make_sql($test->{label}->[0]);
        if ($test->{pre}) { $test->{pre}->($sql); }
        lives_ok {
            ($result, $exception) = $sql->is_inflatable(%{$test->{opts}});
        } "is_inflatable with $label should live"; $test_count++;

        if ($exception && !(ref($exception) && $exception->isa('Class::ReluctantORM::Exception::SQL'))) {
            diag("Exception is: $exception");
        }

        is($result, $test->{result}, "is_inflatable with $label should be correct"); $test_count++;

        unless ($test->{result}) {
            ok((ref($exception) && $exception->isa('Class::ReluctantORM::Exception::SQL')),
               "any exception thrown with a $label should be of the right type"); $test_count++;
        }
    }
}

if ($TEST_THIS{SIMPLE}) {
    my @tests = 
      (
       {
        label => 'one-column, one-table select',
        results => sub { Ship->fetch_all() },
        #skip => 1,
       },
       {
        label => 'one-column, one-table select from a join table',
        fail => 1,
        #skip => 1,
       },
       {
        label => 'one-table select with where clause',
        results => sub { Ship->fetch_by_name('Revenge') },
       },
       {
        label => 'one-param, one-table update',
        results => sub { Ship->fetch_by_name('Revenge') },
       }
      );
    inflate_tests(@tests);
}

# Has One
if ($TEST_THIS{REL_HO}) {
    my @tests = 
      (
       {
        label => 'two table select related by has-one',
        results => sub { Pirate->fetch_deep(where => '1=1', with => { ship => {}}); },
        skip => 1,
       },
       {
        label => 'two table select related by has-one, self-join',
        results => sub { Pirate->fetch_deep(where => '1=1', with => { captain => {}}); },
        #skip => 1,
       },
       {
        label => 'three table select, pirate with rank and ship, has-one',
        results => sub { Pirate->fetch_deep(where => '1=1', with => { ship => {}, rank => {}}); },
        #skip => 1,
       },
      );
    inflate_tests(@tests);
}


# Has Many
if ($TEST_THIS{REL_HM}) {
    my @tests = 
      (
       {
        label => 'two table select related by has-many',
        results => sub { Ship->fetch_deep(where => '1=1', with => { pirates => {}}); },
        #skip => 1,
       },
      );
    inflate_tests(@tests);
}

# Has Many Many
if ($TEST_THIS{REL_HMM}) {
    my @tests =
      (
       {
        label => 'three-table HMM, booty to pirates',
        results => sub { Booty->fetch_deep(where => '1=1', with => {pirates => {}}); },
        #skip => 1,
       },
       {
        label => 'three-table HMM, pirates to booty, reverse LIJ',
        results => sub { Pirate->fetch_deep(where => '1=1', with => {booties => {}}); },
        #skip => 1,
       },
      );
    inflate_tests(@tests);
}

if ($TEST_THIS{REL_HL}) {
    my @tests = 
      (
       {
        label => 'one table select related by has-lazy',
        results => sub { Pirate->fetch_deep(where => '1=1', with => { diary => {}}); },
        #skip => 1,
       },
      );
    inflate_tests(@tests);
}

if ($TEST_THIS{COMPLEX}) {
    my @tests = 
      (
       {
        label => 'three table select, ship, pirates, rank; has-many and has-one',
        results => sub {
            Ship->fetch_deep
              (
               where => '1=1',
               with => {
                        pirates => {
                                    rank => {},
                                   },
                       },
              );
        },
        #skip => 1,
       },

       {
        label => 'two table select, ship, pirates, diary; has-many and has-lazy',
        results => sub {
            Ship->fetch_deep
              (
               where => '1=1',
               with => {
                        pirates => {
                                    diary => {},
                                   },
                       },
              );
        },
        #skip => 1,
       },

       {
        label => 'four-table SELECT, booty to pirates to ranks, HMM and HO',
        results => sub {
            Booty->fetch_deep
              (
               where => '1=1',
               with => {
                        pirates => {
                                    rank => {},
                                   },
                       },
              );
        },
        #skip => 1,
       },

       {
        label => 'five-table SELECT, ship to pirates to rank, booty (HM, HMM and HO)',
        results => sub {
            Ship->fetch_deep
              (
               where => '1=1',
               with => {
                        pirates => {
                                    rank => {},
                                    booties => {},
                                   },
                       },
              );
        },
        #skip => 1,
       },

      );
    inflate_tests(@tests);
}


done_testing($test_count);

sub inflate_tests {
    my @tests = @_;

    foreach my $test (@tests) {
        next if $test->{skip};
        my $label = $test->{label};
        my $sql = make_sql($label);

        if ($test->{fail}) {
            throws_ok {
                $sql->make_inflatable();
            } 'Class::ReluctantORM::Exception::SQL', "$label should fail inflation";  $test_count++;
        } else {
            $sql->make_inflatable();

            my @seen;
            lives_ok {
                @seen = $sql->inflate();
            } "inflating $label should live"; $test_count++;
            my @expected = $test->{results}->();

            is_deeply(\@seen, \@expected, "$label should give correct results"); $test_count++;
        }
    }
}

sub make_sql {
    my $label = shift;
    my $sql;

    if (0) {
    } elsif ($label eq 'one-column, one-table select') {
        $sql = SQL->new('SELECT');
        $sql->from(From->new(Table->new(table => 'ships')));
        $sql->where(Where->new());
        $sql->add_output(Column->new(column => 'ship_id'));
    } elsif ($label eq 'one-column, one-table select from a join table') {
        $sql = SQL->new('SELECT');
        $sql->from(From->new(Table->new(table => 'booties2pirates')));
        $sql->where(Where->new());
        $sql->add_output(Column->new(column => 'pirate_id'));
    } elsif ($label eq 'one-param, one-table update') {
        $sql = SQL->new('UPDATE');
        $sql->table(Table->new(table => 'ships'));
        $sql->where(Where->new(Criterion->new('=', Column->new(column => 'name'), Param->new('Revenge'))));
        $sql->add_input(Column->new(column => 'gun_count'), Param->new(22));
    } elsif ($label eq 'one-param, one-returning, one-table update') {
        $sql = SQL->new('UPDATE');
        $sql->table(Table->new(table => 'ships'));
        $sql->where(Where->new(Criterion->new('=', 0, 1)));
        $sql->add_input(Column->new(column => 'name'), Param->new('Awesome Boat'));
        $sql->add_output(Column->new(column => 'ship_id'));
    } elsif ($label eq 'one-table select with where clause') {
        $sql = SQL->new('SELECT');
        $sql->from(From->new(Table->new(table => 'ships')));
        $sql->where
          (Where->new
           (
            Criterion->new('=',
                           Column->new(column => 'name'),
                           Param->new('Revenge'),
                          )
           ));
    } elsif ($label eq 'two table select related by has-one') {
        # Pirate->fetch_deep(where => '1=1', with => { ship => {} });
        $sql = SQL->new('SELECT');
        $sql->from
          (From->new
           (Join->new
            (
             'LEFT OUTER',
             Table->new(table => 'pirates'),
             Table->new(table => 'ships'),
             Criterion->new
             ('=',
              Column->new(column => 'ship_id', table => Table->new(table => 'pirates')),
              Column->new(column => 'ship_id', table => Table->new(table => 'ships')),
             )
            )
           )
          );
        $sql->where(Where->new());
    } elsif ($label eq 'three table select, pirate with rank and ship, has-one') {
        # Pirate->fetch_deep(where => '1=1', with => { ship => {}, rank => {} });
        $sql = SQL->new('SELECT');
        $sql->from
          (From->new
           (Join->new
            (
             'LEFT OUTER',
             Join->new
             (
              'LEFT OUTER',
              Table->new(table => 'pirates'),
              Table->new(table => 'ranks'),
              Criterion->new
              ('=',
               Column->new(column => 'rank_id', table => Table->new(table => 'pirates')),
               Column->new(column => 'rank_id', table => Table->new(table => 'ranks')),
              )
             ),
             Table->new(table => 'ships'),
             Criterion->new
             ('=',
              Column->new(column => 'ship_id', table => Table->new(table => 'pirates')),
              Column->new(column => 'ship_id', table => Table->new(table => 'ships')),
             )
            )
           )
          );
        $sql->where(Where->new());
    } elsif ($label eq 'two table select related by has-many') {
        # Ship->fetch_deep(where => '1=1', with => { pirates => {} });
        $sql = SQL->new('SELECT');
        $sql->from
          (From->new
           (Join->new
            (
             'LEFT OUTER',
             Table->new(table => 'ships'),
             Table->new(table => 'pirates'),
             Criterion->new
             ('=',
              Column->new(column => 'ship_id', table => Table->new(table => 'pirates')),
              Column->new(column => 'ship_id', table => Table->new(table => 'ships')),
             )
            )
           )
          );
        $sql->where(Where->new());
    } elsif ($label eq 'three table select, ship, pirates, rank; has-many and has-one') {
        # Ship->fetch_deep(where => '1=1', with => { pirates => { rank => {}} });
        $sql = SQL->new('SELECT');
        $sql->from
          (From->new
           (Join->new
            (
             'LEFT OUTER',
             Table->new(table => 'ships'),
             Join->new(
                       'LEFT OUTER',
                       Table->new(table => 'pirates'),
                       Table->new(table => 'ranks'),
                       Criterion->new
                       ('=',
                        Column->new(column => 'rank_id', table => Table->new(table => 'pirates')),
                        Column->new(column => 'rank_id', table => Table->new(table => 'ranks')),
                       )
                      ),
             Criterion->new
             ('=',
              Column->new(column => 'ship_id', table => Table->new(table => 'pirates')),
              Column->new(column => 'ship_id', table => Table->new(table => 'ships')),
             )
            )
           )
          );
    } elsif ($label eq 'two table select related by has-one, self-join') {
        # Pirate->fetch_deep(where => '1=1', with => { captain => {} });
        $sql = SQL->new('SELECT');
        $sql->from
          (From->new
           (Join->new
            (
             'LEFT OUTER',
             Table->new(table => 'pirates', alias => 'p'),
             Table->new(table => 'pirates', alias => 'c'),
             Criterion->new
             ('=',
              Column->new(column => 'pirate_id', table => Table->new(table => 'pirates', alias => 'c')),
              Column->new(column => 'captain_id', table => Table->new(table => 'pirates', alias => 'p')),
             )
            )
           )
          );
        $sql->where(Where->new());

    } elsif ($label eq 'three-table HMM, booty to pirates') {
        # Booty->fetch_deep(where => '1=1', with => {pirates => {}});
        $sql = SQL->new('SELECT');
        $sql->from
          (From->new
           (Join->new
            (
             'LEFT OUTER',
             Table->new(table => 'booties'),
             Join->new(
                       'INNER',
                       Table->new(table => 'booties2pirates'),
                       Table->new(table => 'pirates'),
                       Criterion->new
                       ('=',
                        Column->new(column => 'pirate_id', table => Table->new(table => 'booties2pirates')),
                        Column->new(column => 'pirate_id', table => Table->new(table => 'pirates')),
                       )
                      ),
             Criterion->new
             ('=',
              Column->new(column => 'booty_id', table => Table->new(table => 'booties2pirates')),
              Column->new(column => 'booty_id', table => Table->new(table => 'booties')),
             )
            )
           )
          );
    } elsif ($label eq 'three-table HMM, pirates to booty, reverse LIJ') {
        # Pirate->fetch_deep(where => '1=1', with => {booties => {}});
        $sql = SQL->new('SELECT');
        $sql->from
          (From->new
           (Join->new
            (
             'LEFT OUTER',
             Table->new(table => 'pirates'),
             Join->new(
                       'INNER',
                       Table->new(table => 'booties'),           #  swapped
                       Table->new(table => 'booties2pirates'),   #  swapped
                       Criterion->new
                       ('=',
                        Column->new(column => 'booty_id', table => Table->new(table => 'booties2pirates')),
                        Column->new(column => 'booty_id', table => Table->new(table => 'booties')),
                       )
                      ),
             Criterion->new
             ('=',
              Column->new(column => 'pirate_id', table => Table->new(table => 'booties2pirates')),
              Column->new(column => 'pirate_id', table => Table->new(table => 'pirates')),
             )
            )
           )
          );
    } elsif ($label eq 'four-table SELECT, booty to pirates to ranks, HMM and HO') {
        # Booty->fetch_deep(where => '1=1', with => {pirates => { rank => {} }});
        $sql = SQL->new('SELECT');
        $sql->from
          (From->new
           (Join->new
            (
             'LEFT OUTER',
             Table->new(table => 'booties'),
             Join->new(
                       'INNER',
                       Table->new(table => 'booties2pirates'),
                       Join->new(
                                 'LEFT OUTER',
                                 Table->new(table => 'pirates'),
                                 Table->new(table => 'ranks'),
                                 Criterion->new
                                 ('=',
                                  Column->new(column => 'rank_id', table => Table->new(table => 'pirates')),
                                  Column->new(column => 'rank_id', table => Table->new(table => 'ranks')),
                                 )
                                ),
                       Criterion->new
                       ('=',
                        Column->new(column => 'pirate_id', table => Table->new(table => 'booties2pirates')),
                        Column->new(column => 'pirate_id', table => Table->new(table => 'pirates')),
                       )
                      ),
             Criterion->new
             ('=',
              Column->new(column => 'booty_id', table => Table->new(table => 'booties2pirates')),
              Column->new(column => 'booty_id', table => Table->new(table => 'booties')),
             )
            )
           )
          );
    } elsif ($label eq 'five-table SELECT, ship to pirates to rank, booty (HM, HMM and HO)') {
        # Ship->fetch_deep(where => '1=1', with => {pirates => { rank => {}, booties => {}}});
        $sql = SQL->new('SELECT');
        $sql->from
          (From->new
           (Join->new
            (
             'LEFT OUTER',
             Table->new(table => 'ships'),
             Join->new
             (
              'LEFT OUTER',
              Join->new
              (
               'LEFT OUTER',
               Table->new(table => 'pirates'),
               Join->new
               (
                'INNER',
                Table->new(table => 'booties2pirates'),
                Table->new(table => 'booties'),
                Criterion->new
                ('=',
                 Column->new(column => 'booty_id', table => Table->new(table => 'booties2pirates')),
                 Column->new(column => 'booty_id', table => Table->new(table => 'booties')),
                )
               ),
               Criterion->new
               ('=',
                Column->new(column => 'pirate_id', table => Table->new(table => 'booties2pirates')),
                Column->new(column => 'pirate_id', table => Table->new(table => 'pirates')),
               )
              ),
              Table->new(table => 'ranks'),
              Criterion->new
              ('=',
               Column->new(column => 'rank_id', table => Table->new(table => 'pirates')),
               Column->new(column => 'rank_id', table => Table->new(table => 'ranks')),
              )
             ),
             Criterion->new
             ('=',
              Column->new(column => 'ship_id', table => Table->new(table => 'pirates')),
              Column->new(column => 'ship_id', table => Table->new(table => 'ships')),
             )
            )
           )
          );

    } elsif ($label eq 'one table select related by has-lazy') {
        $sql = SQL->new('SELECT');
        $sql->from(From->new(Table->new(table => 'pirates')));
        $sql->where(Where->new());
        $sql->add_output(Column->new(column => 'diary'));

    } elsif ($label eq 'two table select, ship, pirates, diary; has-many and has-lazy') {
        # Ship->fetch_deep(where => '1=1', with => { pirates => { diary => {} } });
        $sql = SQL->new('SELECT');
        $sql->from
          (From->new
           (Join->new
            (
             'LEFT OUTER',
             Table->new(table => 'ships'),
             Table->new(table => 'pirates'),
             Criterion->new
             ('=',
              Column->new(column => 'ship_id', table => Table->new(table => 'pirates')),
              Column->new(column => 'ship_id', table => Table->new(table => 'ships')),
             )
            )
           )
          );
        $sql->add_output(Column->new(column => 'diary', table => Table->new(table => 'pirates')));

    } elsif ($label eq '') {
    } elsif ($label eq '') {
    } elsif ($label eq '') {
    }
    return $sql;


}
