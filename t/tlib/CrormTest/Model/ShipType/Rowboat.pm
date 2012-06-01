package CrormTest::Model::ShipType::Rowboat;
use base 'CrormTest::Model::ShipType';
use strict;
use warnings;

sub set_sail {
    my $ship_type = shift;
    return "merrily merrily merrily";
}

1;
