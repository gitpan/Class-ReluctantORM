#!/usr/bin/env perl
use strict;
use warnings;

# Test suite to test Class::ReluctantORM's filters facility
use FindBin;
our $test_count = 0;
BEGIN { require "$FindBin::Bin/test-preamble.pl"; }
use CrormTest::Model;

# Start with a clean slate
my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
$fixture->reset_schema();

# Load test filters
use_ok('CrormTest::Filter::MeterLabel');
$test_count++;
use_ok('CrormTest::Filter::ReverseWrite');
$test_count++;

my (@expected, @seen);
my ($revenge, $hind, $pirate);

my $frigate_type_id = ShipType->fetch_by_name('Frigate')->id;
# Make a few test objects
$revenge = CrormTest::Model::Ship->create(
                                          name => 'Revenge',
                                          waterline => 80,
                                          gun_count => 32,
                                          ship_type_id => $frigate_type_id,
                                         );
$pirate = CrormTest::Model::Pirate->create(
                                           name => "Wesley",
                                           ship => $revenge,
                                          );

#-----
#   No Filters by Default
#-----
{
    # Should be no filters at first on Ship on all fields
    foreach my $field (Ship->field_names_including_relations()) {
        @expected = ();
        @seen = $revenge->read_filters_on_field($field);
        is_deeply(\@seen, \@expected, "Should be initially no read filters on ship->$field()");
        $test_count++;

        @expected = ();
        @seen = $revenge->write_filters_on_field($field);
        is_deeply(\@seen, \@expected, "Should be initially no write filters on ship->$field()");
        $test_count++;
    }
}

#-----
# Attach Class Filters
#-----
{
    # Set meterlabel filter on waterline field at class level
    lives_ok {
        Ship->attach_filter(class => 'CrormTest::Filter::MeterLabel', fields => ['waterline']);
    } "Attaching a filter should live";
    $test_count++;

    @expected = ('CrormTest::Filter::MeterLabel');
    @seen = $revenge->read_filters_on_field('waterline');
    is_deeply(\@seen, \@expected, "Should have MeterLabel filter on waterline field on existing ship after attaching as class filter");
    $test_count++;

    @expected = ();
    @seen = $revenge->read_filters_on_field('name');
    is_deeply(\@seen, \@expected, "Should have no filters on name field on existing ship after attaching as class filter");
    $test_count++;

    # Create Golden Hind, ensure it gets new class filter
    $hind = CrormTest::Model::Ship->create(
                                           name => 'The Golden Hind',
                                           waterline => 64,
                                           ship_type_id => $frigate_type_id,
                                           gun_count => 128, # jeez
                                          );
    @expected = ('CrormTest::Filter::MeterLabel');
    @seen = $hind->read_filters_on_field('waterline');
    is_deeply(\@seen, \@expected, "Should have MeterLabel filter on waterline field on new ship after attaching as class filter");
    $test_count++;

    @expected = ();
    @seen = $revenge->read_filters_on_field('name');
    is_deeply(\@seen, \@expected, "Should have no filters on name field on new ship after attaching as class filter");
    $test_count++;

}

#-----
#  Test read filters
#-----
{
    is($revenge->waterline(), '80 meters', "Read filter should work correctly");
    $test_count++;

    is($revenge->raw_field_value('waterline'), '80', "Raw field value should still be untouched");
    $test_count++;

    ok(!$revenge->is_field_dirty('waterline'), "Read filter should not change dirty status of field");
    $test_count++;

    $revenge->waterline(23);
    is($revenge->raw_field_value('waterline'), '23', "Raw field value should still be changed in memory after mutate");
    $test_count++;

    is($revenge->waterline(), '23 meters', "Read filter should work correctly even after mutate");
    $test_count++;

    lives_ok {
        $revenge->update();
    } "Commit to database of a read-filtered field that changes datatype should live";
    $test_count++;

}

#----
# Attach object filter
#----
{
    lives_ok {
        # Note we're not giving fields param, so should attach to all non-primary-key fields
        $revenge->attach_filter(class => 'CrormTest::Filter::ReverseWrite');
    } "Attaching an object filter should live OK";
    $test_count++;

    # Should not have attached to PK field
    @expected = ();
    @seen = $revenge->write_filters_on_field('ship_id');
    is_deeply(\@seen, \@expected, "Should not attach filters to primary key fields by defualt");
    $test_count++;

    # Should have attached to name field
    @expected = ('CrormTest::Filter::ReverseWrite');
    @seen = $revenge->write_filters_on_field('name');
    is_deeply(\@seen, \@expected, "Attaching an object filter should work");
    $test_count++;

    # Should appear on both read and write lists
    @expected = ('CrormTest::Filter::ReverseWrite');
    @seen = $revenge->read_filters_on_field('name');
    is_deeply(\@seen, \@expected, "Attaching an object filter should work and appear on both lists");
    $test_count++;

    @expected = ('CrormTest::Filter::MeterLabel', 'CrormTest::Filter::ReverseWrite');
    @seen = $revenge->read_filters_on_field('waterline');
    is_deeply(\@seen, \@expected, "Attaching an object filter should append to existing filters");
    $test_count++;

    @expected = ('CrormTest::Filter::ReverseWrite', 'CrormTest::Filter::MeterLabel');
    @seen = $revenge->write_filters_on_field('waterline');
    is_deeply(\@seen, \@expected, "Write list should be reverse of read list");
    $test_count++;

    @expected = ();
    @seen = $hind->write_filters_on_field('gun_count');
    is_deeply(\@seen, \@expected, "Object attach should not affect other objects");
    $test_count++;

}

#----
# Test Write filters
#----
{
    is($revenge->name(), 'Revenge', "Write filter should not affect reads");
    $test_count++;

    $revenge->name('Forgiveness');
    is($revenge->name(), 'ssenevigroF', "Write filter should work");
    $test_count++;

    is($revenge->raw_field_value('name'), 'ssenevigroF', "Write filter should affect raw field value");
    $test_count++;

    ok($revenge->is_field_dirty('name'), "Write filter should mark field as dirty");
    $test_count++;

    lives_ok {
        $revenge->update();
    } "Update after write filter should live";
    $test_count++;

    $revenge->raw_field_value('name', 'Satisfaction');
    is($revenge->raw_field_value('name'), 'Satisfaction', "raw_field_value as mutator should bypass write filter");
    $test_count++;

    ok($revenge->is_field_dirty('name'), "raw_field_value as mutator should mark field as dirty");
    $test_count++;
}

#======
# Object set/clear/remove filters
#======
{

    my @existing_on_waterline = $revenge->read_filters_on_field('waterline');
    lives_ok {
        $revenge->append_filter(class => 'CrormTest::Filter::MeterLabel', fields => ['name']);
    } "append_filter should live";
    $test_count++;

    # Should have two at this point
    @expected = ('CrormTest::Filter::ReverseWrite', 'CrormTest::Filter::MeterLabel');
    @seen = $revenge->read_filters_on_field('name');
    is_deeply(\@seen, \@expected, "append_filter should work");
    $test_count++;

    # Should not have affected other fields
    @expected = @existing_on_waterline;
    @seen = $revenge->read_filters_on_field('waterline');
    is_deeply(\@seen, \@expected, "append_filter should not affect other fields");
    $test_count++;

    # Remove a filter
    lives_ok {
        $revenge->remove_filter(class => 'CrormTest::Filter::MeterLabel', fields => ['name']);
    } "remove_filter should live";
    $test_count++;

    # Should have one at this point
    @expected = ('CrormTest::Filter::ReverseWrite');
    @seen = $revenge->read_filters_on_field('name');
    is_deeply(\@seen, \@expected, "remove_filter should work");
    $test_count++;

    # Clear filters
    lives_ok {
        $revenge->clear_filters(fields => ['name']);
    } "clear_filters should live";
    $test_count++;

    # Should have none at this point
    @expected = ();
    @seen = $revenge->read_filters_on_field('name');
    is_deeply(\@seen, \@expected, "clear_filters should work");
    $test_count++;

    # Set filters
    lives_ok {
        $revenge->set_filters(
                              classes => [
                                          'CrormTest::Filter::ReverseWrite',
                                          'CrormTest::Filter::MeterLabel'
                                         ],
                              fields => ['name']
                             );
    } "set_filters should live";
    $test_count++;

    @expected = ('CrormTest::Filter::ReverseWrite', 'CrormTest::Filter::MeterLabel');
    @seen = $revenge->read_filters_on_field('name');
    is_deeply(\@seen, \@expected, "set_filters should work");
    $test_count++;

    # Check fields defaulting
    # Remove MeterLabel from all fields
    lives_ok {
        $revenge->remove_filter(class => 'CrormTest::Filter::MeterLabel');
    } "Remove a filter from all fields should live";
    $test_count++;

    my $saw_removed_filter = 0;
    foreach my $field ($revenge->field_names_including_relations()) {
        $saw_removed_filter ||= grep { $_ eq 'CrormTest::Filter::MeterLabel' } $revenge->read_filters_on_field($field);
    }
    ok(!$saw_removed_filter, "Remove a filter from all fields should work");
    $test_count++;

    # Clearing all filters should work
    lives_ok {
        $hind->clear_filters();
    } "clear_filters should live with no args";
    $test_count++;

    my $saw_any_filter = 0;
    foreach my $field ($hind->field_names_including_relations()) {
        my @filts = $hind->read_filters_on_field($field);
        $saw_any_filter ||= @filts;
    }
    ok(!$saw_any_filter, "clear_filters from all fields should work");
    $test_count++;

}

# Fetch deep integration
{
    my @ships;

    lives_ok {
        @ships = Ship->fetch_deep(
                                  where => '1=1',
                                  with => { pirates => {}},
                                  append_filter => {
                                                    class => 'CrormTest::Filter::MeterLabel',
                                                    fields => [qw(gun_count)],
                                                   },
                                 );
    } "append_filter option to fetch_deep should live";
    $test_count++;

    my $all_objects_have_filter = 1;
    foreach my $ship (@ships) {
        $all_objects_have_filter &&= grep { $_ eq 'CrormTest::Filter::MeterLabel' } $ship->read_filters_on_field('gun_count');
    }
    ok($all_objects_have_filter, "append_filter arg to fetch_deep should work");
    $test_count++;
}



done_testing($test_count);
