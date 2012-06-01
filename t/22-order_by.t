#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's order_by options

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

my (@expected, @seen, @tests);
my ($id, $order_by, $str);

my $driver = Ship->driver();
my $driver_name = driver_name();

#....
# SQL::OrderBy tests
#....
use_ok('Class::ReluctantORM::SQL::OrderBy'); $test_count++;
$str = '';
@tests = (
          {
           msg => 'empty stings',
           sql => '',
           cols => [],
           dirs => [],
          },
          {
           msg => 'one column',
           sql => 'foo',
           cols => ['foo'],
           dirs => ['ASC'],
          },
          {
           msg => 'one column, space padded',
           sql => ' foo ',
           cols => ['foo'],
           dirs => ['ASC'],
          },
          {
           msg => 'one column with asc',
           sql => 'foo asc',
           cols => ['foo'],
           dirs => ['ASC'],
          },
          {
           msg => 'one column with ASC',
           sql => 'foo ASC',
           cols => ['foo'],
           dirs => ['ASC'],
          },
          {
           msg => 'one column with DESC',
           sql => 'foo DESC',
           cols => ['foo'],
           dirs => ['DESC'],
          },
          {
           msg => 'one column with desc',
           sql => 'foo desc',
           cols => ['foo'],
           dirs => ['DESC'],
          },
          {
           msg => 'one column with desc space padded',
           sql => '    foo desc  ',
           cols => ['foo'],
           dirs => ['DESC'],
          },
          {
           msg => 'two column',
           sql => 'foo, bar',
           cols => ['foo', 'bar'],
           dirs => ['ASC', 'ASC'],
          },
          {
           msg => 'two column space padded',
           sql => '    foo   ,    bar    ',
           cols => ['foo', 'bar'],
           dirs => ['ASC', 'ASC'],
          },
          {
           msg => 'two column with desc', 
           sql => 'foo, bar desc',
           cols => ['foo', 'bar'],
           dirs => ['ASC', 'DESC'],
          },
          {
           msg =>'two column with both directions',
           sql => 'foo desc, bar asc',
           cols => ['foo', 'bar'],
           dirs => ['DESC', 'ASC'],
          },
          {
           msg => 'explicit table name',
           sql => 'ships.foo',
           cols => ['foo'],
           tables => ['ships'],
           dirs => ['ASC'],
          },
          {
           msg => 'quoted column name',
           sql => '"foo"',
           cols => ['foo'],
           dirs => ['ASC'],
          },
          {
           msg => 'explicit quoted table name',
           sql => '"ships".foo',
           cols => ['foo'],
           tables => ['ships'],
           dirs => ['ASC'],
          },
          {
           msg => 'schema table column name',
           sql => 'carribean.ships.foo',
           cols => ['foo'],
           tables => ['ships'],
           schemas => ['carribean'],
           dirs => ['ASC'],
          },
          {
           msg => '"schema" "table" column name',
           sql => '"carribean"."ships".foo',
           cols => ['foo'],
           tables => ['ships'],
           schemas => ['carribean'],
           dirs => ['ASC'],
          },
         );

SKIP:
{
    unless ($driver->supports_parsing()) {
        skip("$driver_name() driver does not support SQL parsing",1);
    }

    foreach my $test (@tests) {
        $order_by = undef;

        lives_ok {
            $order_by = $driver->parse_order_by($test->{sql});
        } ($test->{msg} . " should live"); $test_count++;
        ok(defined($order_by), $test->{msg} . " should return something"); $test_count++;
        next unless $order_by;

        @expected = @{$test->{cols}};
        @seen = map { $_->column } $order_by->columns;
        is_deeply(\@seen, \@expected, $test->{msg} . " should have correct column names"); $test_count++;
        
        @expected = @{$test->{dirs}};
        @seen = map { $_->[1] } $order_by->columns_with_directions;
        is_deeply(\@seen, \@expected, $test->{msg} . " should have correct directions"); $test_count++;

        if ($test->{tables}) {
            @expected = @{$test->{tables}};
            @seen = map { $_->table ? $_->table->table() : undef } $order_by->columns;
            is_deeply(\@seen, \@expected, $test->{msg} . " should have correct table names"); $test_count++;
        }
        if ($test->{schemas}) {
            @expected = @{$test->{schemas}};
            @seen = map { $_->table ? $_->table->schema() : undef } $order_by->columns;
            is_deeply(\@seen, \@expected, $test->{msg} . " should have correct schema names"); $test_count++;
        }
    }
}

#....
# Parser tests - parse fails
#....
@tests = (
          #{
          # msg => '',
          # sql => '',
          #},
         );

SKIP:
{
    unless ($driver->supports_parsing()) {
        skip("$driver_name() driver does not support SQL parsing",1);
    }

    foreach my $test (@tests) {
        throws_ok {
            $order_by = $driver->parse_order_by($test->{sql});
        } 'Class::ReluctantORM::Exception::SQL::ParseError', $test->{msg}; $test_count++;
    }
}
done_testing($test_count);



sub driver_name {
    my $class = ref($driver);
    $class =~ s/Class::ReluctantORM::Driver:://;
    return $class;
}
