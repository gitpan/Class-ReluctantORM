our (%ships, %pirates, %booties, %ranks, %ship_types);

foreach my $ship_type (ShipType->fetch_all()) {
    $ship_types{$ship_type->name} = $ship_type;
}

foreach my $name ('Black Pearl', 'Revenge', 'Golden Hind') {
    $ships{$name} =  Ship->create(
                                  name => $name,
                                  ship_type => $ship_types{Frigate},
                                  waterline => 50 + int(50*rand()),
                                  gun_count => 12 + int(24*rand()),
                                 );
}

foreach my $rank (Rank->fetch_all()) {
    $ranks{$rank->name} = $rank;
}


foreach my $color (qw(Red Green Blue Black)) {
    my $name = $color . ' Beard';
    $pirates{$name} = Pirate->create(
                                     name => $name,
                                     ship => $ships{Revenge},
                                     rank => $ranks{'Able Seaman'},
                                    );

}

$pirates{'Dread Pirate Roberts'} = Pirate->create(
                                                  name => 'Dread Pirate Roberts',
                                                  ship => $ships{Revenge},
                                                  rank => $ranks{Captain},
                                                 );

$pirates{'Wesley'} = Pirate->create(
                                    name => 'Wesley',
                                    ship => $ships{Revenge},
                                    rank => $ranks{'Cabin Boy'},
                                    captain => $pirates{'Dread Pirate Roberts'},
                                   );

$pirates{'Sir Francis Drake'} = Pirate->create(
                                               name => 'Sir Francis Drake',
                                               ship => $ships{'Golden Hind'},
                                               rank => $ranks{Captain},
                                              );
$ships{'Golden Hind'}->captain($pirates{'Sir Francis Drake'});
$ships{'Golden Hind'}->save();

foreach my $place ('Shores of Guilder', 'Bermuda', 'Montego', 'Kokomo', 'Skull Island') {
    $booties{$place} = Booty->create(
                                     place => $place,
                                     cash_value => int(10000*rand()),
                                    );
}

$pirates{'Dread Pirate Roberts'}->booties->add($booties{'Shores of Guilder'});
$pirates{'Wesley'}->booties->add($booties{'Shores of Guilder'});
$pirates{'Wesley'}->booties->add($booties{'Montego'});

1;
