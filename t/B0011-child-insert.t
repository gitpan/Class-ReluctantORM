#  -*-cperl-*-
use strict;
use warnings;

# Regression test for bug "Unclear behavior when a child object is inserted into a collection"
#   https://svn.omniti.com/trac/omniti-redteam/ticket/11

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }

use CrormTest::Model;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();


# Create a ship and a pirate in memory
my $s = Ship->new(
                  name => 'Hispanola',
                  waterline => 80,
                  gun_count => 14,
                  ship_type => ShipType->fetch_by_name('Frigate'),
                 );

my $p = Pirate->new(
                    name => 'Bluebeard',
                    leg_count => 1,
                   );

# Save the ship
$s->insert();

# Set the pirate to refer to the ship using its ID
$p->ship_id($s->ship_id);

# Save the pirate
$p->insert();

# The Ship's pirates collection - should it know about the pirate?
my $pirates_collection = $s->pirates();

# Good question! TODO - decide this, and write tests to match.


TODO: {
    $test_count++;
    local $TODO = "Decide proper behavior and implement it";
    ok(0, "Test parent notification of child insert");
}

done_testing($test_count);
