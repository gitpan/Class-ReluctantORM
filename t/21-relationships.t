#  -*-cperl-*-
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's abstract relationships functionality
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;


# Make sure all classes loaded
foreach my $class (qw(Booty Pirate Ship Rank)) {
    my $full = 'CrormTest::Model::' . $class;
    ok(Class::ReluctantORM->is_class_available($full), "$full should be available"); $test_count++;
}

# Check relationships() method of Ship
my $class = 'CrormTest::Model::Ship';
my @seen = sort map { $_->method_name } $class->relationships();
my @expected = sort qw(captain pirates ship_type);

is_deeply(\@seen, \@expected, "List context of relationships() should be accurate"); $test_count++;

my $rel = $class->relationships('pirates');
isa_ok($rel, 'Class::ReluctantORM::Relationship'); $test_count++;
isa_ok($rel, 'Class::ReluctantORM::Relationship::HasMany'); $test_count++;
is($rel->join_depth(), 1, "Join count on a has_many should be 1"); $test_count++;
is_deeply([$rel->local_key_fields()], [qw(ship_id)], "Local key list should be correct"); $test_count++;
is_deeply([$rel->remote_key_fields()], [qw(ship_id)], "Remote key list should be correct"); $test_count++;


# Check relationships() method of Pirates
$class = 'CrormTest::Model::Pirate';
@seen = sort map { $_->method_name } $class->relationships();
@expected = sort qw(ship booties captain rank diary nationalities);

is_deeply(\@seen, \@expected, "List context of relationships() should be accurate"); $test_count++;

$rel = $class->relationships('booties');
isa_ok($rel, 'Class::ReluctantORM::Relationship'); $test_count++;
isa_ok($rel, 'Class::ReluctantORM::Relationship::HasManyMany'); $test_count++;
is($rel->join_depth(), 2, "Join count on a has_many_many should be 2"); $test_count++;
is_deeply([$rel->local_key_fields()], [qw(pirate_id)], "Local key list should be correct"); $test_count++;
is_deeply([$rel->join_local_key_columns()], [qw(pirate_id)], "Join local column list should be correct"); $test_count++;
is_deeply([$rel->join_remote_key_columns()], [qw(booty_id)], "Join remote column list should be correct"); $test_count++;
is_deeply([$rel->remote_key_fields()], [qw(booty_id)], "Remote key list should be correct"); $test_count++;


$rel = $class->relationships('captain');
isa_ok($rel, 'Class::ReluctantORM::Relationship'); $test_count++;
isa_ok($rel, 'Class::ReluctantORM::Relationship::HasOne'); $test_count++;
is($rel->join_depth(), 1, "Join count on a has_one should be 1"); $test_count++;
is_deeply([$rel->local_key_fields()], [qw(captain_id)], "Local key list should be correct on self-ref has_one"); $test_count++;
is_deeply([$rel->remote_key_fields()], [qw(pirate_id)], "Remote key list should be correct on self-ref has_one"); $test_count++;

#=======
#  Inverse Support
#=======
if (1) {

    # Ship and Pirate should refer to each other (has_one vs has_many)
    my $p2s = Pirate->relationships('ship');
    my $s2p = Ship->relationships('pirates');
    is($s2p->inverse_relationship(), $p2s, "Inverse of Ship to Pirate should be Pirate to Ship");  $test_count++;
    is($p2s->inverse_relationship(), $s2p, "Inverse of Pirate to Ship should be Ship to Pirate");  $test_count++;

    # Pirate's captain relationship should have no inverse (unidirectional has_one)
    my $p2c = Pirate->relationships('captain');
    is($p2c->inverse_relationship(), undef, "Inverse of Pirate to Captain should be undef");  $test_count++;

    # Pirate to Booty and Booty to Pirate should be each other's inverse
    my $p2b = Pirate->relationships('booties');
    my $b2p = Booty->relationships('pirates');
    is($b2p->inverse_relationship(), $p2b, "Inverse of Booty to Pirate should be Pirate to Booty");  $test_count++;
    is($p2b->inverse_relationship(), $b2p, "Inverse of Pirate to Booty should be Booty to Pirate");  $test_count++;

}



done_testing($test_count);
