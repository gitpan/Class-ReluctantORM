use strict; #-*-cperl-*-
use warnings;
use Test::More;



# Ensure a recent version of Test::Pod::Coverage
my $min_tpc = 1.08;
eval "use Test::Pod::Coverage $min_tpc";
plan skip_all => "Test::Pod::Coverage $min_tpc required for testing POD coverage"
    if $@;

# Test::Pod::Coverage doesn't require a minimum Pod::Coverage version,
# but older versions don't recognize some common documentation styles
my $min_pc = 0.18;
eval "use Pod::Coverage $min_pc";
plan skip_all => "Pod::Coverage $min_pc required for testing POD coverage"
    if $@;

# CRO custom options
my $cro_pod_opts = {
                    also_private => [
                                     qr/^notify_(render|finish|fetch|execute)/, # Ignore Monitor hooks
                                    ],
                    trust_me => [
                                ],
                   };

my %ignore_modules = map { $_ => 1 }
  qw(
        Class::ReluctantORM::Exception
        Class::ReluctantORM::FetchDeep::Results
        Class::ReluctantORM::Base
        Class::ReluctantORM::Utilities
        Class::ReluctantORM::SQL::Aliases
        Class::ReluctantORM::Monitor::RowCount
        Class::ReluctantORM::Monitor::RowSize
        Class::ReluctantORM::Monitor::ColumnCount
        Class::ReluctantORM::Monitor::JoinCount
        Class::ReluctantORM::Monitor::QueryCount
        Class::ReluctantORM::Monitor::QuerySize
        Class::ReluctantORM::Monitor::Timer
   );
my %todo_modules = map { $_ => 1 }
  qw(
        Class::ReluctantORM::Driver::Oracle8i
        Class::ReluctantORM::Driver::Oracle
        
        Class::ReluctantORM::Driver::PostgreSQL
        Class::ReluctantORM::Driver::SQLite
        Class::ReluctantORM::DBH::WrapDBI
        Class::ReluctantORM::Registry::Hash
        Class::ReluctantORM::Registry::None
        Class::ReluctantORM::Static
        Class::ReluctantORM::Audited
        Class::ReluctantORM::Collection
        Class::ReluctantORM::Relationship
        Class::ReluctantORM::Driver
        Class::ReluctantORM::SQL
        Class::ReluctantORM::SQL::Parser
   );


my @modules = all_modules();

my $tests = 0;
foreach my $module (@modules) {
    next if $ignore_modules{$module};
    if ($todo_modules{$module}) {
      TODO:
        {
            local $TODO = "need POD coverage on $module";
            pod_coverage_ok($module, $cro_pod_opts);
            $tests++;
        }
    } else {
        pod_coverage_ok($module, $cro_pod_opts);
        $tests++;
    }
}

done_testing($tests);
