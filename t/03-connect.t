#  -*-cperl-*-
use strict;
use warnings;
use blib;
use FindBin;
use lib "$FindBin::Bin/../inc";       # Bundled build dependencies
use lib "$FindBin::Bin/tlib";         # Test libraries

# Make sure we can connect to the text fixture database

use Test::More tests => 4;
use Test::Exception;
use FindBin;
use lib "$FindBin::Bin/lib";            # Test libraries
use lib "$FindBin::Bin/../../lib/perl"; # Install Location

use CrormTest::DB;
use Class::ReluctantORM::SQL::Aliases;

SKIP:
{
    skip('Database testing skipped', 4) if $CrormTest::DB::SKIP_ALL;
    my $dbh = CrormTest::DB->new();
    ok($dbh, "database adaptor creation");
    use_ok('CrormTest::Model');


    # Disconnect the database handle and see if it can reconnect

    my $driver = CrormTest::Model::Ship->driver();
    #ok($driver->is_connected(), "Driver should be able to report that it is connected");

    my $sql = 'SELECT 1;';
    my $cro_dbh = $driver->cro_dbh();
    my $sth;
    lives_ok {
        $sth = $cro_dbh->prepare($sql);
    } "Driver's cro_dbh should be able to prepare() a simple SQL statement";

    lives_ok {
        $sth = $cro_dbh->execute($sth);
    } "SQL should be able to be executed";


}


1;
