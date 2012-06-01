package CrormTest::Fixture::SQLite;
use strict;

use FindBin;
use IO::File;
use File::Path qw(mkpath rmtree);

use base 'CrormTest::Fixture';

# Explicitly use this so we can check for the module
use DBD::SQLite;
use DBI;

my $DEBUG = 1;

sub auditted_split_count { return 3; }
sub split_count { return 2; }

sub db_dir {
    return $_[0]->get_temp_dir() . '/sqlite';
}

sub caribbean_file {
    return $_[0]->db_dir . '/caribbean';
}

sub main_file {
    return $_[0]->db_dir . '/main';
}

sub init {
    my $self = shift;

    my $main_file = $self->main_file();
    my $dsn = "dbi:SQLite:dbname=$main_file";

    $self->{user} = '';
    $self->{pass} = '';
    $self->{dsn}  = $dsn;

}

sub start_local_database {
    my ($self) = @_;

    my $tmp_dir = $self->db_dir();

    # Blow away target directory
    if (-d $tmp_dir) {
        rmtree($tmp_dir, { verbose => 1 });
    }
    mkpath($tmp_dir, { verbose => 1 });    
    

    # Open DBH, which will create the database file(s)
    my $dbi_dbh = DBI->connect(
                               $self->{dsn},
                               $self->{user},
                               $self->{pass},
                               { RaiseError => 1 },
                              );
    $self->{dbh} = $dbi_dbh;

    $self->dbh_post_connect($self->dbh);

    $self->create_schema();

    system("touch " . $CrormTest::DB::INITTED_FLAG_FILE);

}

sub dbh_post_connect {
    my $self = shift;
    my $dbh = shift;
    my $file = $self->caribbean_file();
    my $sql = "ATTACH DATABASE '$file' AS caribbean;";
    $dbh->do($sql);

    $sql = "PRAGMA foreign_keys = TRUE;";
    $dbh->do($sql);
}

sub stop_local_database {
    # no-op
}

sub create_schema {
    my $self = shift;
    my $dbh = $self->dbh();

    # We assume that post_connect has already been called, so the test DB has a 'main' and a 'caribbean' schema.
    my $entire_sql = $self->schema_sql;
    my @statements = split(';', $entire_sql);

    local $dbh->{RaiseError} = 1;
    foreach my $stmt (@statements) {
        if ($DEBUG) { print "Running create schema statements:\n$stmt\n"; }
        $dbh->do($stmt);
    }
}

sub reset_schema {
    my $self = shift;
    my $dbh = $self->dbh();

    # We assume that post_connect has already been called, so the test DB has a 'main' and a 'caribbean' schema.
    my $entire_sql = $self->reset_sql;
    my @statements = split(';', $entire_sql);

    local $dbh->{RaiseError} = 1;
    foreach my $stmt (@statements) {
        if ($DEBUG) { print "Running reset schema statements:\n$stmt\n"; }
        $dbh->do($stmt);
    }
}

sub reset_sql {
    my $self = shift;
    return <<EOSQL;
DELETE FROM caribbean.pirates_log;
DELETE FROM caribbean.booties2pirates;
DELETE FROM caribbean.booties;
UPDATE caribbean.pirates SET captain_id = NULL;
DELETE FROM caribbean.pirates;
DELETE FROM caribbean.ships;
EOSQL
}

sub schema_sql {
    my $self = shift;

    # See http://www.sqlite.org/lang.html

    return <<EOSQL;

----------
-- CREATE TABLE
----------
DROP TABLE IF EXISTS caribbean.ranks;
CREATE TABLE caribbean.ranks (
    rank_id integer PRIMARY KEY NOT NULL, -- sqlite auto-increment integer pk
    name text NOT NULL
);

DROP TABLE IF EXISTS caribbean.ships;
CREATE TABLE caribbean.ships (
    SHIP_ID integer PRIMARY KEY NOT NULL, -- sqlite auto-increment integer pk
    NAME text NOT NULL,
    WATERLINE integer NOT NULL,
    GUN_COUNT integer NOT NULL
);

DROP TABLE IF EXISTS caribbean.pirates;
CREATE TABLE caribbean.pirates (
    pirate_id integer PRIMARY KEY NOT NULL, -- sqlite auto-increment integer pk
    name text NOT NULL,
    leg_count integer DEFAULT 2 NOT NULL,
    rank_id integer             REFERENCES ranks   (rank_id)   ON DELETE RESTRICT,
    captain_id integer          REFERENCES pirates (pirate_id) ON DELETE RESTRICT,
    ship_id integer    NOT NULL REFERENCES ships   (ship_id)   ON DELETE RESTRICT,
    diary text -- large
);

DROP TABLE IF EXISTS caribbean.booties;
CREATE TABLE caribbean.booties (
    booty_id integer PRIMARY KEY NOT NULL, -- sqlite auto-increment integer pk
    cash_value integer NOT NULL,
    "location" text,
    secret_map text
);

DROP TABLE IF EXISTS caribbean.booties2pirates;
CREATE TABLE caribbean.booties2pirates (
    booty_id integer  NOT NULL REFERENCES booties (booty_id)  ON DELETE RESTRICT,
    pirate_id integer NOT NULL REFERENCES pirates (pirate_id) ON DELETE RESTRICT,
    PRIMARY KEY (pirate_id, booty_id)
);


DROP TABLE IF EXISTS caribbean.pirates_log;
CREATE TABLE caribbean.pirates_log (
    pirate_id integer NOT NULL,
    name text,
    leg_count integer,
    rank_id integer,
    captain_id integer, 
    ship_id integer,
    diary text, -- large
    audit_action text,
    audit_pid integer
);

--
-- Data for static ranks table
--
INSERT INTO ranks (name) VALUES ('Able Seaman');
INSERT INTO ranks (name) VALUES ('Cabin Boy');
INSERT INTO ranks (name) VALUES ('Captain');

EOSQL
}

1;

