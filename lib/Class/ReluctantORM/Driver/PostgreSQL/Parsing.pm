package Class::ReluctantORM::Driver::PostgreSQL;
# This is a continuation

use strict;
use warnings;

use Class::ReluctantORM::Exception;
use Class::ReluctantORM::SQL::Parser;

our $PARSER = Class::ReluctantORM::SQL::Parser->new();

=head1 NAME

Class::ReluctantORM::Driver::PostgreSQL::Parsing - Parse Support for CRO PG Driver

=head1 DESCRIPTION

This module provides Class::ReluctantORM::Driver parsing support for PostgreSQL.  It can parse some, but by no means all, DML statements.


=head1 LIMITATIONS AND LAMENTATIONS

Since this module is based on SQL::Parser 1.xx, it has all the strengths and weaknesses of that module.  In particular, we can't actually define a BNF grammar, instead we rely on Dialects and Features, which can be troublesome.

CRO introduces some limitations as well.

=over

=item Only supports SELECT, INSERT, UPDATE, DELETE statements.  No DDL.

=item Can only parse one statement - no enormous scripts.

=item No SQL support for transactional statements (BEGIN, COMMIT, ROLLBACK) though the API may support this.

=back

=cut

=head2 $true = $driver->supports_parsing();

Returns true.  Hubris!

=cut

sub supports_parsing { return 1; }

=head2 $sql_obj = $driver->parse_statement($string, \%options);

Tries to parse the given string as a single statement.  Returns a Class::ReluctantORM::SQL on success. Throws an exception if a problem occured.

No options as yet.

=cut

sub parse_statement {
    Class::ReluctantORM::Exception::NotImplemented->croak();
}

=head2 $sql_where = $driver->parse_where($string, \%options);

Tries to parse the given string as a where clause without the where.  Returns a Class::ReluctantORM::SQL::Where on success.  Throws an exception if a problem occured.

No options as yet.

=cut

sub parse_where {
    my $driver = shift;
    my $sql_str = shift;
    my $options = shift; # currently ignored, no options

    my $sql = $sql_str;

    # The approach here is to try to parse it using
    # SQL::Statement.  But since we don't have a table list, 
    # just a where clause, we have to make up a table list.
    # I know this is basically awful.



    my @fake_tables = qw(fake_table);
    $@ = 1;
  TABLE_NAME_TRY:
    while ($@) {
        my $str = 'SELECT * FROM ' . __make_fake_join(@fake_tables) . ' WHERE ' . $sql;
        eval {
            $PARSER->parse($str);
        };

        if ($@) {
            my ($new_table) = $@ =~ /Table '(\w+)' referenced but not found/;
            if ($new_table) {
                push @fake_tables, $new_table;
            } else {
                # Unknown parse error
                last TABLE_NAME_TRY;
            }
        }
    }

    if ($@) {
        Class::ReluctantORM::Exception::SQL::ParseError->croak(error => $@, sql => $sql_str);
    }

    my $horrid_struct = $PARSER->{struct};  # voids warranty

    my $where = Where->new();
    $where->{orig_str} = $sql_str;
    $where->{root} = $PARSER->__build_crit_from_parse_tree($horrid_struct, $where);

    return $where;
}

sub __make_fake_join {
    my @tables = @_;

    return join (' JOIN ', @tables); 

    my $str = shift @tables;
    while (@tables) {
        $str .= ' JOIN ' . (shift(@tables)) . ' USING foo ';
    }
    return $str;
}

# Inherit parse_order_by


1;


