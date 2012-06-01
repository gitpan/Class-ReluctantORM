#  -*-cperl-*-
use strict;
use warnings;
no warnings 'once';

# Test suite to test Class::ReluctantORM's support for SubClassByRow

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }

use CrormTest::Model;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();


# Registries screw with the fetch map tests, so disable them.
foreach my $class (Class::ReluctantORM->list_all_classes()) {
    $class->_change_registry('Class::ReluctantORM::Registry::None');
}

# Leave where parsing turned ON at the global level, so it has to respect the prase_where option
my (@expected, @seen);
our (%ships, %pirates, %booties, %ranks, %ship_types);

my $all = 1;
my %TEST_THIS = (
                 INIT => 1,
                 DIRECT_FETCH => $all,
                 SUBCLASS_METADATA => $all,
                 POLYMORPHIC  => $all,
                );

if ($TEST_THIS{INIT}) {
    require "$FindBin::Bin/test-init.pl";
    $ships{'Black Pearl'}->ship_type($ship_types{'Galleon'});
    $ships{'Black Pearl'}->save();
}


if ($TEST_THIS{SUBCLASS_METADATA}) {

    # Get the Frigate
    my $frigate = ShipType->fetch_by_name('Frigate');
    
    # Class metadata should match on all of these with ShipType in general
    my @metadata_accessors = qw(
                                   __metadata
                                   driver
                                   field_names
                                   column_names
                                   relationships
                              );
    foreach my $method (@metadata_accessors) {
        my @expected = ShipType->$method();
        my @seen = $frigate->$method();
        is_deeply(\@seen, \@expected, "metadata accessor $method should match"); $test_count++;
    }

}

#====================================================================#

done_testing($test_count);

#====================================================================#
