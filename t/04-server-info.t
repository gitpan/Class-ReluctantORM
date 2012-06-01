#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's server interrogator

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;

my $driver = Pirate->driver();
ok(defined($driver), "Can get a driver object for Pirate");
$test_count++;

ok(defined($driver->{brand}),   "database brand detection ($driver->{brand})");
$test_count++;
ok(defined($driver->{version}), "database version detection ($driver->{version})");
$test_count++;

done_testing($test_count);
