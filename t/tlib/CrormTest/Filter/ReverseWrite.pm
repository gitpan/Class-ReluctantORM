package CrormTest::Filter::ReverseWrite;
use strict;
use warnings;

use base 'Class::ReluctantORM::Filter';

# Write Filter that reverses (scalar) the field

sub apply_write_filter {
    my ($filter, $value, $object, $field) = @_;

    if (ref $value) {
        return $value;
    } elsif (defined $value) {
        return reverse $value;
    } else {
        return undef;
    }

}

1;
