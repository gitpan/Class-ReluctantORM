package CrormTest::DB;

=head1 NAME

CrormTest::DB - Database adaptor for testing Class::ReluctantORM

=head1 SYNOPSIS



=head1 DESCRIPTION

This is a subclass of Class::ReluctantORM::DBH.  It has-a DBI database handle inside it.  The database handle is a class singleton - only one will ever be created in the life of the process.

Upon use()ing this module for the first time, the DBI connection params are determined by examining environment variables, and if that fails, by interactively asking the user.  Once the params have been determined, they are saved in a file, t/test.dsn .

Depending on whether the test database is to be skipped, remotely used, or generated, the database may be generated.

=cut

use strict;
use base 'Class::ReluctantORM::DBH';
use FindBin;
use Data::Dumper;
use CrormTest::Fixture;
use IO::File;
use Class::ReluctantORM::Exception;
use DBI;
BEGIN {
   $Class::ReluctantORM::Exception::TRACE = 1;
};

use base 'Exporter';

our $one_true_self;

# On load, check for options
our %DB_OPTS;
our $SKIP_ALL;

our @EXPORT = ('%DB_OPTS', '$SKIP_ALL');

our %RDBMS_ABBREVS;

our $DSN_FILE;
our $INITTED_FLAG_FILE;

our $COLINFO_CALLS; # used by test 45


BEGIN {
    $COLINFO_CALLS = 0;
    $DSN_FILE = $ENV{CRO_DB_DSN_FILE} || "$FindBin::Bin/test.dsn";
    $INITTED_FLAG_FILE = $ENV{CRO_DB_INITTED_FILE} || "$FindBin::Bin/test-db-initted.flag";
}

BEGIN {
    %RDBMS_ABBREVS = (
                      #'o' => 'Oracle',
                      'p' => 'PostgreSQL',
                      #'m' => 'MySQL',
                      's' => 'SQLite',
                     );

     if (-f $DSN_FILE) {
        # Load settings from file
        my $io = IO::File->new();
        $io->open("< $DSN_FILE");
        my $code = join '', <$io>;
        $io->close();
        eval($code);
        unless ($DB_OPTS{dsn}) {
            $SKIP_ALL = 1;
        }

    } elsif ($ENV{CRO_DB_MODE}) {
        my $mode = $ENV{CRO_DB_MODE};
        if ($mode eq 's') {
            $SKIP_ALL = 1;
        } elsif ($mode eq 'd') {
            $DB_OPTS{dsn} = $ENV{CRO_DB_DSN};
            $DB_OPTS{user} = $ENV{CRO_DB_USERNAME};
            $DB_OPTS{pass} = $ENV{CRO_DB_PASSWORD};
            # TODO - figure out type from DSN?

        } elsif ($mode eq 'g') {
            my $flavor = $ENV{CRO_DB_RDBMS};
            $DB_OPTS{type} = $RDBMS_ABBREVS{$ENV{CRO_DB_RDBMS}};
            # Need to reset DB opts from fixture
            my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
            %DB_OPTS = $fixture->get_db_opts();
        }
    } else {
        # Prompt user for dsn
      HOW_FIXTURE:
        do {
            print STDERR "\nI need to be able to connect to a database to test Class::ReluctantORM.\n";
            print STDERR "Do you want to (s)kip, enter (d)sn info, or (g)enerate a local test database?\n";
            print STDERR "Choose (Sdg):";
            my $choice = <>;
            chomp($choice);
            $choice = lc($choice);
            if ($choice eq 's' || !$choice) {
                $SKIP_ALL = 1;
            } elsif ($choice eq 'd') {
                print STDERR "Enter DBI DSN: ";
                $choice = <>;
                chomp($choice);
                $DB_OPTS{dsn} = $choice;
                print STDERR "Enter username: ";
                $choice = <>;
                chomp($choice);
                $DB_OPTS{user} = $choice;
                print STDERR "Enter password: ";
                $choice = <>;
                chomp($choice);
                $DB_OPTS{pass} = $choice;
            } elsif ($choice eq 'g') {

              WHICH_RDBMS:
                do {
                    print STDERR "Choose your RDBMS: (c)ancel, (s)qlite, (p)ostgresql\n";
                    print STDERR "(support for (o)racle, (m)ysql is vaporware as yet)\n";
                    print STDERR "Note: You must have a local server installed.\n";
                    print STDERR "Choose (Cps):";

                    $choice = <>;
                    chomp($choice);
                    $choice = lc($choice);
                    if (!$choice || $choice eq 'c') {
                        $SKIP_ALL = 1;
                    } elsif (exists $RDBMS_ABBREVS{$choice}) {
                        my $type = $RDBMS_ABBREVS{$choice};
                        $DB_OPTS{type} = $type;
                        my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
                        %DB_OPTS = $fixture->get_db_opts();
                    } else {
                        redo WHICH_RDBMS;
                    }
                };
            } else {
                redo HOW_FIXTURE;
            }
        };
    }

    # Save DB_OPTS
    my $io = IO::File->new();
    $io->open("> $DSN_FILE")  || die("Could not write to $DSN_FILE: $!");
    $io->print(Data::Dumper->Dump([\%DB_OPTS], ['*DB_OPTS']));
    $io->close();

}

sub __post_connect_hook {
    my $self = shift;
    my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
    $fixture->dbh_post_connect($self->dbi_dbh);
}


sub test_db_initialized {
    my $class = shift;
    return (-f $INITTED_FLAG_FILE);
}

sub get_fixture {
    my $class = shift;
    my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
    return $fixture;
}

sub dbdsn  { return $DB_OPTS{dsn}; }
sub dbuser { return $DB_OPTS{user}; }
sub dbpass { return $DB_OPTS{pass}; }

# Required API for superclass 'Class::ReluctantORM::DBH';
sub new {
    my $class = shift;
    if ($one_true_self) { return $one_true_self; }

    $one_true_self = bless {}, $class;

    if (!$one_true_self->test_db_initialized) {
        my $fixture = CrormTest::Fixture->new(\%DB_OPTS);
        $fixture->start_local_database();
    }

    $one_true_self->{_dbh} = DBI->connect(
                                          $class->dbdsn(),
                                          $class->dbuser(),
                                          $class->dbpass(),
                                          {
                                           RaiseError => 1,
                                          }
                                         ) || die $DBI::errstr;
    $one_true_self->__post_connect_hook();
    return $one_true_self;
}

sub dbi_dbh { return $_[0]->{_dbh}; }

sub column_info {
    my $self = shift;
    $COLINFO_CALLS++;
    return $self->{_dbh}->column_info(@_);
}
sub get_info {
    my $self = shift;
    return $self->{_dbh}->get_info(@_);
}
sub prepare {
    my $self = shift;
    return $self->{_dbh}->prepare(@_);
}
sub execute {
    my $self = shift;
    my $sth = shift;
    unless ($sth->execute(@_)) {
        return '';
    }
    return $sth;
}
sub set_handle_error {
    my $self = shift;
    my $handler = shift;
    $self->{_dbh}->{HandleError} = $handler;
}

1;

