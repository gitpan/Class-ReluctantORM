#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's SQL parsing
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use Class::ReluctantORM::Utilities;

my %TEST_THIS = (
                 install_method => 1,
                 #conditional_load => 1, # TODO
                 #conditional_load_subdir => 1, # TODO
                 camel_case_to_underscore_case => 1,
                 last_non_cro_stack_frame => 1,
                );

if ($TEST_THIS{install_method}) {
    is_export_ok('install_method'); $test_count++;
    my $test_sub = sub { return "worked"; };

    lives_ok {
        Class::ReluctantORM::Utilities::install_method('Class::ReluctantORM::Utilities', 'testsub1', $test_sub);
    } "install_method should live"; $test_count++;
    can_ok('Class::ReluctantORM::Utilities', 'testsub1'); $test_count++;

    my $result;
    lives_ok {
        $result = Class::ReluctantORM::Utilities->testsub1();
    } "calling installed method should live"; $test_count++;
    is($result, "worked", "installed method should return correct result when called"); $test_count++;

    # TODO - test with nonexistant class

}

if ($TEST_THIS{camel_case_to_underscore_case}) {
    is_export_ok('camel_case_to_underscore_case'); $test_count++;
    my %tests = (
                 ''        => '',
                 'Foo'     => 'foo',
                 'FooBar'  => 'foo_bar',
                 'foo_bar' => 'foo_bar',
                 'fooBar'  => 'foo_bar',
                );
    while (my($input, $expected) = each %tests) {
        is(
           Class::ReluctantORM::Utilities::camel_case_to_underscore_case($input),
           $expected,
           "camel_case_to_underscore should turn '$input' into '$expected'",
          );
        $test_count++;
    }
}

if ($TEST_THIS{last_non_cro_stack_frame}) {
    is_export_ok('last_non_cro_stack_frame'); $test_count++;

    my ($line, $frame);

    # Direct call
    ($line, $frame) = (undef, undef);
    lives_ok {
        $line = __LINE__; # this line must appear directly above the next one, no gap
        $frame = Class::ReluctantORM::Utilities::last_non_cro_stack_frame();
    } "LNCSF direct call should live"; $test_count++;
    ok(defined($frame), "LNCSF direct call should return something"); $test_count++;
    if (defined($frame)) {
        is($frame->{file}, __FILE__, "LNCSF direct call should have correct file"); $test_count++;
        is($frame->{line}, $line + 1, "LNCSF direct call should have correct line number"); $test_count++;
        is($frame->{package}, __PACKAGE__, "LNCSF direct call should have correct package"); $test_count++;
        is($frame->{frames}, 0, "LNCSF direct call should have correct framecount"); $test_count++;
    }


    # Wrapped
    ($line, $frame) = (undef, undef);
    lives_ok {
        $line = __LINE__; # this line must appear directly above the next one, no gap
        $frame = Class::ReluctantORM::Utilities::__testsub_lncsf1();
    } "LNCSF wrapped call should live"; $test_count++;
    ok(defined($frame), "LNCSF wrapped call should return something"); $test_count++;
    if (defined($frame)) {
        is($frame->{file}, __FILE__, "LNCSF wrapped call should have correct file"); $test_count++;
        is($frame->{line}, $line + 1, "LNCSF wrapped call should have correct line number"); $test_count++;
        is($frame->{package}, __PACKAGE__, "LNCSF wrapped call should have correct package"); $test_count++;
        is($frame->{frames}, 1, "LNCSF wrapped call should have correct framecount"); $test_count++;
    }


}

done_testing($test_count);

sub is_export_ok {
    my $sub = shift;
    my $found = grep { $_ eq $sub } @Class::ReluctantORM::Utilities::EXPORT_OK;
    ok($found == 1, "$sub should be on the EXPORT_OK list of Class::ReluctantORM::Utilities");
}
