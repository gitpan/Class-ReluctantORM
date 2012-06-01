#  -*-cperl-*-
use strict;
use warnings;

use File::Temp qw(mktemp);

# Test suite to test CRO's schema-caching facility
use FindBin;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }

use Class::ReluctantORM::SchemaCache;

my %TEST_THIS = (
                 INIT            => 1,
                 OPTIONS         => 1,
                 POLICY_NONE     => 1,
                 POLICY_SIMPLE   => 1,
                 POLICY_CLEAR    => 1,
                );

if ($TEST_THIS{OPTIONS}) {
    foreach my $opt (qw(schema_cache_file schema_cache_policy)) {
        my $val;
        lives_ok {
            $val = Class::ReluctantORM->get_global_option($opt);
        } "Option '$opt' should exist"
    }

    # Defaults
    is(Class::ReluctantORM->get_global_option('schema_cache_policy'), 'None', "Default policy should be None");
    is(Class::ReluctantORM->get_global_option('schema_cache_file'), undef, 'Default location should be undef');

    # Can only set legit policies
    foreach my $policy (Class::ReluctantORM::SchemaCache->policy_names()) {
        lives_ok {
            Class::ReluctantORM->set_global_option('schema_cache_policy', $policy);
        } "Policy '$policy' should be permitted"
    }

    # Can't set bad policies
    foreach my $policy (qw(NOPE WELP RESCAN_ON_ERROR)) {
        throws_ok {
            Class::ReluctantORM->set_global_option('schema_cache_policy', $policy);
        } 'Class::ReluctantORM::Exception::Param::BadValue',  "Policy '$policy' should NOT be permitted";
    }

    # Reset to default
    Class::ReluctantORM->set_global_option('schema_cache_policy', 'None');
    $Class::ReluctantORM::SchemaCache::SCHEMA_CACHE = undef;

}

if ($TEST_THIS{POLICY_NONE}) {
    is(subshell('None'), 'OK', 'None policy should work in subshell');
    is(subshell('None', '/some/path/that/does/not/exist/42/foo.json'), 'OK', 'None policy should work in subshell with nonsense file location');
    my $tempfile = mktemp('/tmp/cro-schema-cache-45-None-XXXXX');
    is(subshell('None', $tempfile), 'OK', 'None policy should work in subshell with temp file');
    ok(!(-e $tempfile), 'None policy should not have caused the temp file to be created');
}

foreach my $policy (qw(Simple ClearOnError)) {
    if ($policy eq 'Simple' && !$TEST_THIS{POLICY_SIMPLE}) { next; }
    if ($policy eq 'ClearOnError' && !$TEST_THIS{POLICY_CLEAR}) { next; }

    is(subshell($policy), 'OK', "$policy policy should work in subshell with default path");

    # verify the we can create a cache file and then load it
    my $path = mktemp("/tmp/cro-schema-cache-45-$policy-01-XXXXX");
    is(subshell($policy, $path), 'OK', "$policy policy should work in subshell with temp path");
    ok(-e $path, "$policy policy should result in a file in the temp location $path");
    my $modtime = (stat($path))[9];
    sleep(2);
    is(subshell($policy, $path), 'OK', "$policy policy should work in subshell with existing file");
    is((stat($path))[9], $modtime, "$policy policy should not change mtime of existing file");
    my $colinfo_check = 'print "" . ($CrormTest::DB::COLINFO_CALLS == 0 ? "OK" : "Have $CrormTest::DB::COLINFO_CALLS") . "\n";';
    is(subshell($policy, $path, $colinfo_check), 'OK', "$policy policy with existing cache file should result in zero column info calls");
    unlink($path);

    # OK, verify the column info calls happen if no file is present
    $path = mktemp("/tmp/cro-schema-cache-45-$policy-02-XXXXX");
    $colinfo_check = 'print "" . ($CrormTest::DB::COLINFO_CALLS > 0 ? "OK" : "Have $CrormTest::DB::COLINFO_CALLS") . "\n";';
    is(subshell($policy, $path, $colinfo_check), 'OK', "$policy policy with missing file should result in nonzero column info calls");

    # Change a column name, which should break the schema
    rename_column('caribbean', 'ranks', 'name', 'nomenclature');
    my $kaboom = <<'EOP';
eval {
   my @ranks = CrormTest::Model::Rank->fetch_all();
};
if ($@ =~ /column t0\.name does not exist/) {
   print "OK\n";
} else {
   print $@ . "\n";
}
EOP
    is(subshell($policy, $path, $kaboom), 'OK', "$policy policy should die when encountering a SQL error");

    if ($policy eq 'Simple') {
        ok((-e $path), "Simple policy should keep broken cache file at $path");
        is(subshell($policy, $path, $kaboom), 'OK', "Simple policy should stay broken on next run");
    } elsif ($policy eq 'ClearOnError') {
        ok(!(-e $path), "ClearOnError policy should delete broken cache file at $path");
        is(subshell($policy, $path), 'OK', "ClearOnError policy should recover on next run");
    }
    unlink($path);

    rename_column('caribbean', 'ranks', 'nomenclature', 'name');

}




done_testing();

sub rename_column {
    my ($schema, $table, $oldname, $newname) = @_;
    my $dbh = CrormTest::Model::Pirate->driver->dbi_dbh();
    my $sql = "ALTER TABLE $schema.$table RENAME COLUMN $oldname TO $newname";
    $dbh->do($sql);
}

sub subshell {
    my $policy   = shift;
    my $location = shift || '';
    my $code     = shift || 'print "OK\n";';
    my $perl = <<EOP;
BEGIN { \$| = 1; }
use warnings;
use blib;
use lib "$FindBin::Bin/../inc";       # Bundled build dependencies
use lib "$FindBin::Bin/tlib";         # Test libraries
use Class::ReluctantORM;
BEGIN {
  Class::ReluctantORM->set_global_option('schema_cache_policy', '$policy');
  if ('$location') {
    Class::ReluctantORM->set_global_option('schema_cache_file', '$location');
  }
}
use CrormTest::Model;
$code
EOP
   my $tempfile = File::Temp->new();
   $tempfile->print($perl);
   $tempfile->unlink_on_destroy(0); # DEBUG
   close $tempfile;
   my $out = `CRO_DB_DSN_FILE=$CrormTest::DB::DSN_FILE CRO_DB_INITTED_FILE=$CrormTest::DB::INITTED_FLAG_FILE $^X $tempfile 2>&1`;
   chomp($out);
   return $out;
}
