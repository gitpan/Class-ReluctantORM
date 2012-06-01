# -*-cperl-*-
use strict;
use warnings;

use FindBin;
our $test_count = 1;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;

use IO::File;

our %DB_OPTS;
our $SKIP_ALL;

my $settings_file = "$FindBin::Bin/test.dsn";
my $initted_flag = "$FindBin::Bin/test-db-initted.flag";

SKIP:
{
    skip("No DSN file found", 1) unless (-f $settings_file);

    # Load settings from file
    my $io = IO::File->new();
    $io->open("< $settings_file");
    my $code = join '', <$io>;
    $io->close();
    eval($code);
    unless ($DB_OPTS{dsn}) {
        $SKIP_ALL = 1;
    }

    skip("No database testing", 1) if ($SKIP_ALL);
    skip("No type field in DSN file, assuming no local DB generated", 1) unless ($DB_OPTS{type});
    skip("Database shutdown disabled by testing env var CRO_DB_KEEP_RUNNING", 1) if ($ENV{CRO_DB_KEEP_RUNNING});

    my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
    $fixture->stop_local_database();

    # Go ahead and remove DSN, as the server is no longer running
    unlink($settings_file) || die("Could not unlink $settings_file: $!");
    unlink($initted_flag) || die("Could not unlink $initted_flag: $!");

    pass("Stopped test database");
}
done_testing($test_count);
