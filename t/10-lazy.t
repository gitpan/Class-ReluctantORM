#  -*-cperl-*-
use strict;
use warnings;
no warnings 'once';

# Test suite to test Class::ReluctantORM's lazy column support

use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;
use aliased 'Class::ReluctantORM::Monitor::QueryCount';

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

my $query_counter = QueryCount->new();
Class::ReluctantORM->install_global_monitor($query_counter);

our (%ships, %pirates, %booties, %ranks, %ship_types);

my %TEST_THIS = (
                 INIT => 1,
                 FIELDS       => 1,
                 BUILD_CLASS  => 1,
                 RELATIONSHIP => 1,
                 FETCH        => 1,
                 MUTATOR      => 1,
                 FETCH_DEEP   => 1,
                 FETCH_WITH   => 1,
                );

if ($TEST_THIS{INIT}) {
    require "$FindBin::Bin/test-init.pl";

    foreach my $color (qw(Red Blue Green Black)) {
        my $pirate = $pirates{$color . ' Beard'};
        $pirate->diary("My name is $color Beard.  Today we ravaged and pillaged.  We had fun.");
        $pirate->save();
    }
}

if ($TEST_THIS{FIELDS}) {
    my (@seen);

    @seen = CrormTest::Model::Pirate->field_names();
    is(scalar (grep { $_ eq 'diary' } @seen), 1, "has_lazy fields should appear on the field list");
    

    @seen = CrormTest::Model::Pirate->essential_fields();
    is(scalar (grep { $_ eq 'diary' } @seen), 0, "has_lazy fields should not appear on the essential field list");
    

    ok(CrormTest::Model::Pirate->is_field_has_lazy('diary'), "diary should be detected as a has_lazy field");
    

    # There should be a diary method
    can_ok('CrormTest::Model::Pirate', qw(diary fetch_diary)); # fetch_with_diary should be auto-generated
    

}

if ($TEST_THIS{BUILD_CLASS}) {
    my (@seen);

    @seen = Booty->field_names();
    is(scalar (grep { $_ eq 'secret_map' } @seen), 1, "has_lazy via build_class fields should appear on the field list");
    

    @seen = Booty->essential_fields();
    is(scalar (grep { $_ eq 'secret_map' } @seen), 0, "has_lazy via build_class fields should not appear on the essential field list");
    

    ok(Booty->is_field_has_lazy('secret_map'), "secret_map should be detected as a has_lazy field");
    

    # There should be a secret_map method
    can_ok(Booty, qw(secret_map fetch_secret_map));
    
}


if ($TEST_THIS{RELATIONSHIP}) {
    my ($rel);

    $rel = Pirate->relationships('diary');
    ok($rel, "Should have found a relationship for the slot 'diary'");    
    if ($rel) {
        ok($rel->is_has_lazy(), "relationship should be has_lazy");    
        is($rel->inverse_relationship(), undef, "should have no inverse rel");    
        is($rel->linking_class(), Pirate, "Pirate should be linking class");    
        is($rel->linked_class(), undef, "Pirate should be linking class");    
        is($rel->join_depth(), 0, "rel join depth should be zero");    
        is_deeply([$rel->local_key_fields()], [qw(diary)], "local key fields should be correct");    
        is_deeply([$rel->remote_key_fields()], [], "should have no remote key fields");    
    }
}
purge_registries();

if ($TEST_THIS{FETCH}) {
    my ($pirate, $diary);

    lives_ok {
        $pirate = Pirate->fetch_by_name('Red Beard');
    } "fetching without a diary should live";
    
    ok($pirate, "plain fetch should still return something"); 
    ok(!$pirate->is_fetched('diary'), "is_fetched should initially be false"); 

    throws_ok {
        $diary = $pirate->diary();
    } "Class::ReluctantORM::Exception::Data::FetchRequired", "Unfetched lazy accessor call should die";
    

    lives_ok {
        $diary = $pirate->fetch_diary();
    } "afterthought fetching should live";
    
    ok($diary, "afterthought fetch should return something"); 
    ok($pirate->is_fetched('diary'), "is_fetched should be true after afterhought fetch"); 
    ok(!$pirate->is_dirty(), "pirate should not be dirty after afterthought fetch"); 
    like($diary, qr{had fun}, "pirates should always have fun"); 

    lives_ok {
        $diary = $pirate->diary();
    } "lazy accessor call should live if previously set by afterthought fetch";
    
    like($diary, qr{had fun}, "pirates should always have fun"); 
}
purge_registries();


if ($TEST_THIS{MUTATOR}) {
    my ($pirate, $diary, $new_diary);

    my $auditted_split_count = $fixture->auditted_split_count();

    $new_diary = 'Today we had pancakes.  We had fun.';

    $pirate = Pirate->fetch_by_name('Red Beard');
    ok(!$pirate->is_dirty(), "TEST FIXTURE ASSERTION: pirate should not be dirty before lazy mutator call"); 
    lives_ok {
        $pirate = Pirate->fetch_by_name('Red Beard');
        $pirate->diary($new_diary);
    } "lazy mutator call should live";
    
    ok($pirate->is_dirty(), "pirate should be dirty after lazy mutator call"); 
    ok($pirate->is_fetched('diary'), "is_fetched should be true after mutator call"); 

    lives_ok {
        $diary = $pirate->diary();
    } "lazy accessor call should live if previously set by mutator";
    
    is($diary, $new_diary, "Accessor return value should be correct"); 
    like($diary, qr{had fun}, "pirates should always have fun"); 

    $query_counter->reset();
    lives_ok {
        $pirate->save();
    } "lazy accessor call should live if previously set by mutator";
    
    is($query_counter->last_measured_value(), $auditted_split_count, "Save query count should be $auditted_split_count (audited pirate)"); 
    ok(!$pirate->is_dirty(), "pirate should be no longer be dirty after save"); 
    ok($pirate->is_fetched('diary'), "is_fetched should still be true after save"); 
    like($diary, qr{had fun}, "pirates should always have fun"); 

    $pirate = undef;
    $pirate = Pirate->fetch_by_name('Red Beard');
    $diary  = $pirate->fetch_diary();
    is($diary, $new_diary, "Accessor return value should be correct"); 


}
purge_registries();

if ($TEST_THIS{FETCH_DEEP}) {
    my (@results, $diary);

    my @tests = (
                 {
                  label => "pirate to diary (HL)",
                  base => Pirate,
                  with => { diary => {}, },
                  check => sub { $_[0]->diary(); },
                 },
                 {
                  label => "pirate to diary, ship (HL, HO)",
                  base => Pirate,
                  with => { diary => {}, ship => {} },
                  check => sub { $_[0]->diary(); },
                  skip => 1,
                 },
                );

    foreach my $test (@tests) {
        next if $test->{skip};
        my $label = "fetch_deep on " . $test->{label};
        lives_ok {
            @results = $test->{base}->fetch_deep(
                                                 where => "name LIKE '\%Beard'",
                                                 with => $test->{with},
                                                );
        } "$label should live";
        
        ok((@results > 0), "$label should return at least one object"); 

        $diary = undef;
        lives_ok {
            $diary = $test->{check}->(@results);
        } "accessing diary after $label should live";
        
        like($diary, qr{had fun}, "pirates should always have fun"); 
    }
}
purge_registries();

if ($TEST_THIS{FETCH_WITH}) {
    my ($id, $pirate, $diary, $ship);

    $id = Pirate->fetch_by_name('Red Beard')->id();

    lives_ok {
        $pirate = Pirate->fetch_with_ship($id);
    } "fetching with ship without a diary should live";
    
    ok($pirate, "fetch with should return something"); 
    ok($pirate->is_fetched('ship'), "is_fetched(ship) should be true"); 
    ok(!$pirate->is_fetched('diary'), "is_fetched(diary) should be false"); 

    $pirate = undef;

    lives_ok {
        $pirate = Pirate->fetch_with_diary($id);
    } "fetching with diary should live";
    
    ok($pirate, "fetch with should return something"); 
    ok(!$pirate->is_fetched('ship'), "is_fetched(ship) should be false"); 
    ok($pirate->is_fetched('diary'), "is_fetched(diary) should be true"); 
    can_ok('CrormTest::Model::Pirate', qw(fetch_with_diary)); # fetch_with_diary should be auto-generated

    lives_ok {
        $diary = $pirate->diary();
    } "lazy accessor call should live if previously set by fetch_with";
    
    like($diary, qr{had fun}, "pirates should always have fun"); 

    $pirate = undef;

    lives_ok {
        $pirate = Pirate->fetch_by_name_with_diary('Blue Beard');
    } "fetching by name with diary should live";
    
    ok($pirate, "fetch with should return something"); 
    ok(!$pirate->is_fetched('ship'), "is_fetched(ship) should be false"); 
    ok($pirate->is_fetched('diary'), "is_fetched(diary) should be true"); 
}
purge_registries();

done_testing();

sub purge_registries {
    foreach my $class (Ship, Pirate) { $class->registry->purge_all(); }
}

