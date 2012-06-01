package CrormTest::Filter::MeterLabel;
use strict;
use warnings;

use base 'Class::ReluctantORM::Filter';

# Read Filter that appends " meters" to a field

sub apply_read_filter {
    my ($filter, $value, $object, $field) = @_;

    if (ref $value) {
        return $value;
    } elsif (defined $value) {
        return $value . " meters";
    } else {
        return undef;
    }
}

1;
