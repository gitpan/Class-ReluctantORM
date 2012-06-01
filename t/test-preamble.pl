package main;
our $test_count = 0;
use blib;
use FindBin;
use lib "$FindBin::Bin/../inc";       # Bundled build dependencies
use lib "$FindBin::Bin/tlib";         # Test libraries


# using this will cause the DB options to be determined
use CrormTest::DB;

unless (CrormTest::DB->test_db_initialized()) {
    my $fixture = CrormTest::DB->get_fixture();
    $fixture->start_local_database();
}

BEGIN {
    if ($CrormTest::DB::SKIP_ALL) {
        eval "use Test::More skip_all => 'Test database required for testing';";
    } else {
        eval "use Test::More;";
    }
}


# Eval this so that the uses happen at runtime
my @packages = qw(
                     CrormTest::Fixture
                     CrormTest::Model
                     Test::Exception
                );
foreach my $pkg (@packages) {
    eval "use $pkg;";
    if ($@) {
        die "Error while loading $pkg: $@";
    }
}

if ($ENV{CRO_TRACE}) {
    eval "use Class::ReluctantORM::Exception;";
    $Class::ReluctantORM::Exception::TRACE = 1;
}



if ($ENV{CRO_DUMP_SQL}) {
    eval 'use Class::ReluctantORM::Monitor::Dump;';
    unless ($@) {
        my $mon = Class::ReluctantORM::Monitor::Dump->new
          (
           what => [
                    'statement',
                    #'row',
                    'binds',
                    #'sql_object', # Via Data::Dumper
                    'sql_object_pretty', # As prettyprint string
                   ],
           when => [
                    'render_begin',
                    #'render_transform',
                    #'render_finish',
                    'execute_begin'
                    #'fetch_row',
                    #'execute_finish',
                    #'finish',
                   ],
          );
        Class::ReluctantORM->install_global_monitor($mon);
    }
}

1;
