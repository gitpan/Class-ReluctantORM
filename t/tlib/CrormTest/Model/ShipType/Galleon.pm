package CrormTest::Model::ShipType::Galleon;
use base 'CrormTest::Model::ShipType';
use strict;
use warnings;

sub set_sail {
    my $ship_type = shift;
    return "Off to Spain with my dubloons!";
}

1;
