#  -*-cperl-*-
use strict;
use warnings;
use blib;
use FindBin;
use lib "$FindBin::Bin/../inc";       # Bundled build dependencies
use lib "$FindBin::Bin/tlib";         # Test libraries


# Test suite to ensure dependencies are installed 

my @modules;
my @need_ones;

BEGIN {
    @modules = (
                'NEXT',
                'Data::Diff',
                'Class::Accessor',
                'Regexp::Common',
                'DBI',
                'Test::Exception',
                'Exception::Class',
                'aliased',
                'SQL::Statement',
                'Lingua::EN::Inflect',
                'IO::Scalar',
               );
    @need_ones = (
                  [ 'DBD::Pg', 'DBD::SQLite' ] # list drivers here
                 );
}

use Test::More tests => ((scalar @modules) + (scalar @need_ones));

foreach my $module (@modules) {
   use_ok($module);
}

foreach my $set (@need_ones) {
    my $have_one = 0;
  MODULE_IN_SET:
    foreach my $module (@$set) {
        eval "use $module;";
        unless ($@) {
            $have_one = $module;
            last MODULE_IN_SET;
        }
    }
    my $names = join ' ', @$set;
    if ($have_one) {
        pass("Needed one of ($names), found $have_one");
    } else {
        fail("Needed one of ($names), none found!");
    }

}

