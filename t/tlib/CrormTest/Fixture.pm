package CrormTest::Fixture;

=head1 NAME

CrormTest::Fixture - make test databases for testing Class::ReluctantORM

=head1 SYNOPSIS



=head1 DESCRIPTION

=head1 METHODS

=head1 VIRTUAL METHODS

=head2 $f->start_local_database()

=head2 $f->stop_local_database()

=head2 $sql = $f->schema_sql()

=cut

use strict;
use FindBin;
use DBI;

sub new {
    my ($class, $opts_ref, $db_type) = @_;
    $opts_ref->{type} ||= $db_type;
    $db_type ||= $opts_ref->{type};

    my $db_fixture_class = "CrormTest::Fixture::$db_type";
    eval "use $db_fixture_class;";
    if ($@) { die $@; }
    my $self = bless {%{$opts_ref}}, $db_fixture_class;

    $self->init();

    return $self;
}

sub get_temp_dir {

    my $tmp = '/tmp'; # TODO - replace with soemthing portable
    $tmp .= '/cro-test';
    unless (-e $tmp) {
        mkdir $tmp;
    }

    return $tmp;
}

sub init {}

sub auditted_split_count { return 2; }
sub split_count { return 1; }

sub get_db_opts {
    my $self = shift;
    return (
            user => $self->{user},
            pass => $self->{pass},
            type => $self->{type},
            dsn  => $self->{dsn},
           );
}

sub dbh {
    my $self = shift;
    unless ($self->{dbh}) {
        $self->{dbh} = DBI->connect($self->{dsn}, $self->{user}, $self->{pass});
        $self->dbh_post_connect($self->{dbh});
    }
    return $self->{dbh};
}

#=======================================================================#
#                         Fixture Building
#=======================================================================#

sub create_schema {
    my $self = shift;
    my $dbh = $self->dbh();
    my $sql = $self->schema_sql;
    $dbh->do($sql);
}

sub reset_schema {
    my $self = shift;
    my $dbh = $self->dbh();
    my $sql = $self->reset_sql;
    $dbh->do($sql);
}

sub dbh_post_connect { }

#=======================================================================#
#                        DB Convenience
#=======================================================================#

sub count_rows {
    my $self = shift;
    my $table = shift;
    my $where = shift || '';
    my $dbh = $self->dbh;
    my $sql = "SELECT COUNT(*) FROM $table" . ($where ? " WHERE " : '') . $where;
    my ($count) = $dbh->selectrow_array($sql);
    return $count;
}



1;
