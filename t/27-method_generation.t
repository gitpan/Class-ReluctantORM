#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's method generator functionality
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }

use CrormTest::Model;

my %TEST_THIS = (
                 SEARCH_BY => 1,
                 FETCH_WITH => 1,
                 SEARCH_BY_WITH => 1,
                 NO_AUTOLOAD => 1,
                );

my @classes = (Pirate, Ship, Booty, Rank);

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();


if ($TEST_THIS{SEARCH_BY}) {
    foreach my $class (@classes) {
        foreach my $mode (qw(search fetch)) {
            foreach my $field ($class->fields()) {
                my $method = $mode . '_by_' . $field;

                # Should not already exist
                ok(!$class->can($method), "$class should not already have a $method"); 

                # Call in eval, ignore errors - triggers method generation
                eval { $class->$method(); };

                ok($class->can($method), "$class should have $method after an attempt"); 
                ok(UNIVERSAL::can($class, $method), "UNIVERSAL colon can $method (naughty)"); 

            }
        }
    }
}

if ($TEST_THIS{FETCH_WITH}) {
    foreach my $class (@classes) {
        foreach my $mode (qw(search fetch)) {
            foreach my $rel ($class->relationships()) {
                my $method = $mode . '_with_' . $rel->name();

                # Should not already exist
                ok(!$class->can($method), "$class should not already have a $method"); 

                # Call in eval, ignore errors - triggers method generation
                eval { $class->$method(); };

                ok($class->can($method), "$class should have $method after an attempt"); 
                ok(UNIVERSAL::can($class, $method), "UNIVERSAL colon can $method (naughty)"); 
            }
        }
    }
}

if ($TEST_THIS{SEARCH_BY_WITH}) {
    foreach my $class (@classes) {
        foreach my $mode (qw(search fetch)) {
            foreach my $field ($class->fields()) {
                foreach my $rel ($class->relationships()) {
                    my $method = $mode . '_by_' . $field . '_with_' . $rel->name();

                    # Should not already exist
                    ok(!$class->can($method), "$class should not already have a $method"); 

                    # Call in eval, ignore errors - triggers method generation
                    eval { $class->$method(); };

                    ok($class->can($method), "$class should have $method after an attempt"); 
                    ok(UNIVERSAL::can($class, $method), "UNIVERSAL colon can $method (naughty)"); 
                }
            }
        }
    }
}

if ($TEST_THIS{NO_AUTOLOAD}) {
    foreach my $class (@classes) {
        throws_ok {
            $class->no_such_method_234();
        } 'Class::ReluctantORM::Exception::Call::NoSuchMethod', "AUTOLOADer should reject unknown methods with an exception";
    }
}

done_testing();
