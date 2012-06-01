# mix-in - set package to SL
package Class::ReluctantORM::Driver::SQLite;

use strict;
use warnings;
use Class::ReluctantORM::SQL::Aliases;
use Class::ReluctantORM::SQL::Function;

=head1 NAME

Class::ReluctantORM::Driver::SQLite::Functions - SQL function rendering library

=head1 DESCRIPTION

Provides a set of Functions, and how to render them, for the SQLite driver.

=cut


our %FUNCTION_RENDERERS;

our @UNARY = (
              'NOT',
              'EXISTS',
             );
foreach my $op (@UNARY) {
    $FUNCTION_RENDERERS{$op} = sub {
        my $arg = shift;
        return '(' . $op . ' ' . $arg . ')';
    };
}

our @INFIX_BINARY = (
                     'AND',
                     'OR',
                     '=',
                     '<>',
                     '>',
                     '<',
                     '>=',
                     '<=',
                     'IS',
                     'IS NOT',
                     'LIKE',
                     'ILIKE',
                     'IN',    # Custom, see below
                    );
foreach my $op (@INFIX_BINARY) {
    $FUNCTION_RENDERERS{$op} = sub {
        my @args = @_;
        return '(' . $args[0] . " $op " . $args[1] . ')';
    };
}



our @PREFIX_N_ARY = (
                     'REPLACE',

                     # Aggregates are in this catagory, generally
                     'SUM',
                     'MAX',
                     'MIN',
                     'STDDEV',
                     'COUNT',
                     'AVG',
                    );
foreach my $op (@PREFIX_N_ARY) {
    $FUNCTION_RENDERERS{$op} = sub {
        my @args = @_;
        return "$op(" . join(',', @args) . ')';
    };
}


# Completely wierd things go here
$FUNCTION_RENDERERS{KEY_COMPOSITOR_OUTSIDE_SUBQUERY} = sub {
    # This gets passed a list of FK or PK columns (Which have already been rendered)
    # If only one, should simply return that column
    my @cols = @_;
    if (@cols == 1) {
        return $cols[0];
    } else {
        return '(' . join(',',@cols) . ')';
    }
};
$FUNCTION_RENDERERS{KEY_COMPOSITOR_INSIDE_SUBQUERY} = sub {
    # This gets passed a list of FK or PK columns (Which have already been rendered)
    # Just return these as a unparanthesized list
    my @cols = @_;
    return join(',',@cols);
};

my @CUSTOM_FUNCTIONS = (
                        { name => 'IN', min_inputs => 2, max_inputs => 2 },
                       );
foreach my $def (@CUSTOM_FUNCTIONS) {
    my $name = $def->{name};
    unless (Function->is_registered($name)) {
        Function->register(%$def);
    }
}


1;
