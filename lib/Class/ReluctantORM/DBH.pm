package Class::ReluctantORM::DBH;
use strict;
use warnings;

use Class::ReluctantORM::DBH::WrapDBI;

=head1 NAME

Class::ReluctantORM::DBH - Database handle base class

=head1 DESCRIPTION

This class is used to define an interface for low-level calls to the database handle.  In most circumstances, it is a thin wrapper around a DBI database handle.  You can use this as a shim to intercept connection calls, manage server timeouts, etc.

Most users will simply use Class::ReluctantORM::DBH::WrapDBI, which directly wraps a DBI database handle.  In fact, if you simply pass a DBI database handle to build_class, WrapDBI will be loaded and used for you.

=cut

use Class::ReluctantORM::Exception;

=head1 ABSTRACT INTERFACE

=cut

=head2 $dbh = YourDBHClass->new();

Creates a new, connected database handle.  This can be a singleton or pooled connection.

=cut

sub new { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $value = $dbh->get_info($info_id);

A direct passthru to DBI::dbh->get_info.  Used to query things like database vendor, version, quote characters, etc.

=cut

sub get_info { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $dbh->set_handle_error($coderef);

Installs the coderef such that it will be called when a database error occurs.  Used to attach a hook to re-throw the error as a Class::ReluctantORM::Exception::SQL::ExecutionError.

=cut

sub set_handle_error { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $sth = $sbh->prepare($sql_string);

Turn the given SQL string into a DBI statement handle.

=cut

sub prepare { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }

=head2 $meta_sth = $dbh->column_info($catalog, $schema, $table, $column);

Returns a statement handle with data about the columns in the database.  See DBI::column_info.

=cut

sub column_info { Class::ReluctantORM::Exception::Call::PureVirtual->croak(); }


=head2 $dbi_dbh = $cro_dbh->dbi_dbh();

If the CRO DBH is based on DBI, this returns the DBI database handle.

=cut

sub dbi_dbh { return $_[0]->{_dbh}; }
sub __post_connect_hook { }


sub _boost_to_cro_dbh {
    my $crod = shift;
    my $dbh = shift;

    # It may already support CROD
    eval {
        $crod->_quack_check($dbh);
    };
    unless ($@) {
        # Looks OK
        return $dbh;
    }

    return Class::ReluctantORM::DBH::WrapDBI->new($dbh);

}



sub _quack_check {
    my $crod = shift;
    my $db_class = shift;
    foreach my $method (qw(new prepare execute set_handle_error get_info column_info dbi_dbh)) {
        unless ($db_class->can($method)) { OmniTI::Exception::Param->croak(message => "db_class must support $method method.",  param => 'db_class', value => $db_class);  }
    }
}

=head1 AUTHOR

Clinton Wolfe clwolfe@cpan.org March 2010


=cut

1;
