package CrormTest::Model::ShipType::Frigate;
use base 'CrormTest::Model::ShipType';
use strict;
use warnings;

sub set_sail {
    my $ship_type = shift;
    return "A pillagin' we shall go!";
}

1;
