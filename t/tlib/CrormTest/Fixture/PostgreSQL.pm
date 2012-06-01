package CrormTest::Fixture::PostgreSQL;
use strict;

use FindBin;
use IO::File;

use base 'CrormTest::Fixture';

# Detect local PG server installation on install

our $PG_BIN_DIR;

BEGIN {
    # Find pg_config
    my $pg_config = `which pg_config`;
    chomp($pg_config);
    unless ($pg_config) {
        die "Could not find $pg_config in $ENV{PATH}\n";
    }
    $PG_BIN_DIR = `$pg_config --bindir`;
    chomp($PG_BIN_DIR);
    unless (-f "$PG_BIN_DIR/initdb") {
        die "Could not find initdb in $PG_BIN_DIR\n";
    }
    unless (-f "$PG_BIN_DIR/pg_ctl") {
        die "Could not find pg_ctl in $PG_BIN_DIR\n";
    }
    unless (-f "$PG_BIN_DIR/createdb") {
        die "Could not find createdb in $PG_BIN_DIR\n";
    }
}


sub start_local_database {
    my ($self) = @_;

    my $tmp_dir = $self->get_temp_dir() . '/postgresql';

    # Blow away target directory
    system("rm -rf $tmp_dir");

    # Setup Postgres cluster
    system("$PG_BIN_DIR/initdb -D $tmp_dir -Upostgres");

    # Overwrite postgres conf
    rename("$tmp_dir/postgresql.conf", "$tmp_dir/postgresql.conf.orig");
    my $io = IO::File->new();
    $io->open("> $tmp_dir/postgresql.conf");
    $io->print($self->__postgresql_conf($tmp_dir));
    $io->close();

    # Startup PG
    system("$PG_BIN_DIR/pg_ctl -l$tmp_dir/server.log -D$tmp_dir start");
    sleep(2);

    # Set postgres user password
    system(qq{echo "CREATE USER tbtester PASSWORD 'tbtester' SUPERUSER LOGIN;" | $PG_BIN_DIR/psql -Upostgres -h$tmp_dir template1});

    # Create High Seas test database
    system("$PG_BIN_DIR/createdb -Utbtester -h$tmp_dir highseas");


    $self->create_schema();

    system("touch " . $CrormTest::DB::INITTED_FLAG_FILE);

}

sub init {
    my $self = shift;

    my $tmp_dir = $self->get_temp_dir() . '/postgresql';

    $self->{user} = 'tbtester';
    $self->{pass} = 'tbtester';
    $self->{dsn} = "DBI:Pg:host=$tmp_dir;database=highseas";

}


sub stop_local_database {
    my ($self) = @_;
    my $tmp_dir = $self->get_temp_dir() . '/postgresql';
    system("$PG_BIN_DIR/pg_ctl -l$tmp_dir/server.log -D$tmp_dir -mi stop");
    sleep(2);
    #system("rm -rf $tmp_dir");
}

sub __postgresql_conf {
    my ($class, $tmp_dir) = @_;
    return <<EOCONF;
listen_addresses = ''
max_connections = 20
superuser_reserved_connections = 1
unix_socket_directory = '$tmp_dir'
shared_buffers = 2MB
#max_fsm_pages = 5000
#max_fsm_relations = 200
lc_messages = 'en_US.UTF-8'
lc_monetary = 'en_US.UTF-8'
lc_numeric = 'en_US.UTF-8'
lc_time = 'en_US.UTF-8'
default_text_search_config = 'pg_catalog.english'

EOCONF
}

sub reset_sql {
    my $self = shift;
    return <<EOSQL;
UPDATE caribbean.ships SET captain_pirate_id = NULL;
DELETE FROM caribbean.pirates_log;
DELETE FROM caribbean.nationalities2pirates;
DELETE FROM caribbean.booties2pirates;
DELETE FROM caribbean.booties;
UPDATE caribbean.pirates SET captain_id = NULL;
DELETE FROM caribbean.pirates;
DELETE FROM caribbean.ships;
EOSQL
}


sub schema_sql {
    my $self = shift;
    my $username = $self->{user};
    return <<EOSQL;
--
-- PostgreSQL database dump
--

SET client_encoding = 'UTF8';
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: caribbean; Type: SCHEMA; Schema: -; Owner: $username
--

DROP SCHEMA IF EXISTS caribbean CASCADE;

CREATE SCHEMA caribbean;


ALTER SCHEMA caribbean OWNER TO $username;

SET search_path = caribbean, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: booties; Type: TABLE; Schema: caribbean; Owner: $username; Tablespace: 
--

CREATE TABLE booties (
    booty_id integer NOT NULL,
    cash_value integer NOT NULL,
    "location" text,
    secret_map text
);


ALTER TABLE caribbean.booties OWNER TO $username;

--
-- Name: booties2pirates; Type: TABLE; Schema: caribbean; Owner: $username; Tablespace: 
--

CREATE TABLE booties2pirates (
    booty_id integer NOT NULL,
    pirate_id integer NOT NULL
);

CREATE TABLE nationalities2pirates (
    nationality_id integer NOT NULL,
    pirate_id integer NOT NULL
);

CREATE TABLE masts2ship_types (
    mast_id integer NOT NULL,
    ship_type_id integer NOT NULL
);

ALTER TABLE caribbean.masts2ship_types OWNER TO $username;
ALTER TABLE caribbean.booties2pirates OWNER TO $username;
ALTER TABLE caribbean.nationalities2pirates OWNER TO $username;

--
-- Name: pirates; Type: TABLE; Schema: caribbean; Owner: $username; Tablespace: 
--

CREATE TABLE pirates (
    pirate_id integer NOT NULL,
    name text NOT NULL,
    leg_count integer DEFAULT 2 NOT NULL,
    rank_id integer DEFAULT 2 NOT NULL,
    captain_id integer,
    ship_id integer,
    diary text -- large
);


ALTER TABLE caribbean.pirates OWNER TO $username;

--
-- Name: pirates_log; Type: TABLE; Schema: caribbean; Owner: $username; Tablespace: 
--

CREATE TABLE pirates_log (
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

ALTER TABLE caribbean.pirates_log OWNER TO $username;


--
-- Name: ships; Type: TABLE; Schema: caribbean; Owner: $username; Tablespace: 
--

CREATE TABLE ships (
    SHIP_ID integer NOT NULL,
    NAME text NOT NULL,
    SHIP_TYPE_ID integer NOT NULL,
    WATERLINE integer NOT NULL,
    GUN_COUNT integer NOT NULL,
    CAPTAIN_PIRATE_ID integer
);

ALTER TABLE caribbean.ships OWNER TO $username;

CREATE TABLE ranks (
    rank_id integer NOT NULL,
    name text NOT NULL
);
ALTER TABLE caribbean.ranks OWNER TO $username;

CREATE TABLE ship_types (
    ship_type_id integer NOT NULL,
    name text NOT NULL,
    subclass_name text NOT NULL
);

ALTER TABLE caribbean.ship_types OWNER TO $username;

CREATE TABLE masts (
    mast_id integer NOT NULL,
    name text NOT NULL,
    sail_count integer NOT NULL
);
ALTER TABLE caribbean.masts OWNER TO $username;

CREATE SEQUENCE masts_mast_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

ALTER TABLE caribbean.masts_mast_id_seq OWNER TO $username;

CREATE TABLE caribbean.nationalities (
    nationality_id integer NOT NULL,
    name text NOT NULL
);
ALTER TABLE caribbean.nationalities OWNER TO $username;

CREATE SEQUENCE nationalities_nationality_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE caribbean.nationalities_nationality_id_seq OWNER TO $username;


--
-- Name: booties_booty_id_seq; Type: SEQUENCE; Schema: caribbean; Owner: $username
--

CREATE SEQUENCE booties_booty_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE caribbean.booties_booty_id_seq OWNER TO $username;


--
-- Name: booties_booty_id_seq; Type: SEQUENCE SET; Schema: caribbean; Owner: $username
--

SELECT pg_catalog.setval('booties_booty_id_seq', 1, false);


CREATE SEQUENCE ship_types_ship_type_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE caribbean.ship_types_ship_type_id_seq OWNER TO $username;


--
-- Name: ship_types_ship_type_id_seq; Type: SEQUENCE SET; Schema: caribbean; Owner: $username
--

SELECT pg_catalog.setval('ship_types_ship_type_id_seq', 1, false);


--
-- Name: pirates_pirate_id_seq; Type: SEQUENCE; Schema: caribbean; Owner: $username
--

CREATE SEQUENCE pirates_pirate_id_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE caribbean.pirates_pirate_id_seq OWNER TO $username;

--
-- Name: pirates_pirate_id_seq; Type: SEQUENCE SET; Schema: caribbean; Owner: $username
--

SELECT pg_catalog.setval('pirates_pirate_id_seq', 1, true);


--
-- Name: ships_ship_id_seq; Type: SEQUENCE; Schema: caribbean; Owner: $username
--

CREATE SEQUENCE ships_ship_id_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE caribbean.ships_ship_id_seq OWNER TO $username;

--
-- Name: ranks_rank_id_seq; Type: SEQUENCE; Schema: caribbean; Owner: $username
--

CREATE SEQUENCE ranks_rank_id_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


ALTER TABLE caribbean.ranks_rank_id_seq OWNER TO $username;

--
-- Name: ranks_rank_id_seq; Type: SEQUENCE SET; Schema: caribbean; Owner: $username
--

SELECT pg_catalog.setval('ranks_rank_id_seq', 1, true);

--
-- Name: booty_id; Type: DEFAULT; Schema: caribbean; Owner: $username
--

ALTER TABLE booties ALTER COLUMN booty_id SET DEFAULT nextval('booties_booty_id_seq'::regclass);

ALTER TABLE ship_types ALTER COLUMN ship_type_id SET DEFAULT nextval('ship_types_ship_type_id_seq'::regclass);

ALTER TABLE nationalities ALTER COLUMN nationality_id SET DEFAULT nextval('nationalities_nationality_id_seq'::regclass);

ALTER TABLE masts ALTER COLUMN mast_id SET DEFAULT nextval('masts_mast_id_seq'::regclass);

--
-- Name: pirate_id; Type: DEFAULT; Schema: caribbean; Owner: $username
--

ALTER TABLE pirates ALTER COLUMN pirate_id SET DEFAULT nextval('pirates_pirate_id_seq'::regclass);


--
-- Name: ship_id; Type: DEFAULT; Schema: caribbean; Owner: $username
--

ALTER TABLE ships ALTER COLUMN ship_id SET DEFAULT nextval('ships_ship_id_seq'::regclass);

--
-- Name: rank_id; Type: DEFAULT; Schema: caribbean; Owner: $username
--

ALTER TABLE ranks ALTER COLUMN rank_id SET DEFAULT nextval('ranks_rank_id_seq'::regclass);



--
-- Name: booties2pirates_pkey; Type: CONSTRAINT; Schema: caribbean; Owner: $username; Tablespace: 
--

ALTER TABLE ONLY booties2pirates
    ADD CONSTRAINT booties2pirates_pkey PRIMARY KEY (booty_id, pirate_id);

ALTER TABLE ONLY masts2ship_types
    ADD CONSTRAINT masts2ship_types_pkey PRIMARY KEY (mast_id, ship_type_id);


--
-- Name: booties_pkey; Type: CONSTRAINT; Schema: caribbean; Owner: $username; Tablespace: 
--

ALTER TABLE ONLY booties
    ADD CONSTRAINT booties_pkey PRIMARY KEY (booty_id);

ALTER TABLE ONLY ship_types
    ADD CONSTRAINT ship_types_pkey PRIMARY KEY (ship_type_id);

ALTER TABLE ONLY nationalities
    ADD CONSTRAINT nationalities_pkey PRIMARY KEY (nationality_id);

ALTER TABLE ONLY masts
    ADD CONSTRAINT masts_pkey PRIMARY KEY (mast_id);


--
-- Name: pirates_pkey; Type: CONSTRAINT; Schema: caribbean; Owner: $username; Tablespace: 
--

ALTER TABLE ONLY pirates
    ADD CONSTRAINT pirates_pkey PRIMARY KEY (pirate_id);


--
-- Name: ships_pkey; Type: CONSTRAINT; Schema: caribbean; Owner: $username; Tablespace: 
--

ALTER TABLE ONLY ships
    ADD CONSTRAINT ships_pkey PRIMARY KEY (ship_id);

--
-- Name: ranks_pkey; Type: CONSTRAINT; Schema: caribbean; Owner: $username; Tablespace: 
--

ALTER TABLE ONLY ranks
    ADD CONSTRAINT ranks_pkey PRIMARY KEY (rank_id);


--
-- Name: booties2pirates_booty_id_fkey; Type: FK CONSTRAINT; Schema: caribbean; Owner: $username
--

ALTER TABLE ONLY booties2pirates
    ADD CONSTRAINT booties2pirates_booty_id_fkey FOREIGN KEY (booty_id) REFERENCES booties(booty_id) ON DELETE CASCADE;

ALTER TABLE ONLY nationalities2pirates
    ADD CONSTRAINT nationalities2pirates_nationality_id_fkey FOREIGN KEY (nationality_id) REFERENCES nationalities(nationality_id) ON DELETE CASCADE;

ALTER TABLE ONLY nationalities2pirates
    ADD CONSTRAINT nationalities2pirates_pirate_id_fkey FOREIGN KEY (pirate_id) REFERENCES pirates(pirate_id) ON DELETE CASCADE;

ALTER TABLE ONLY masts2ship_types
    ADD CONSTRAINT masts2ship_types_mast_id_fkey FOREIGN KEY (mast_id) REFERENCES masts(mast_id) ON DELETE CASCADE;

ALTER TABLE ONLY masts2ship_types
    ADD CONSTRAINT masts2ship_types_ship_type_id_fkey FOREIGN KEY (ship_type_id) REFERENCES ship_types(ship_type_id) ON DELETE CASCADE;

--
-- Name: booties2pirates_pirate_id_fkey; Type: FK CONSTRAINT; Schema: caribbean; Owner: $username
--

ALTER TABLE ONLY booties2pirates
    ADD CONSTRAINT booties2pirates_pirate_id_fkey FOREIGN KEY (pirate_id) REFERENCES pirates(pirate_id) ON DELETE CASCADE;

ALTER TABLE ONLY ships
    ADD CONSTRAINT ships_pirates_captain_id_fkey FOREIGN KEY (captain_pirate_id) REFERENCES pirates(pirate_id) ON DELETE RESTRICT;

ALTER TABLE ONLY ships
    ADD CONSTRAINT ships_ship_type_id_fkey FOREIGN KEY (ship_type_id) REFERENCES ship_types(ship_type_id) ON DELETE RESTRICT;

--
-- Name: pirates_ship_id_fkey; Type: FK CONSTRAINT; Schema: caribbean; Owner: $username
--

ALTER TABLE ONLY pirates
    ADD CONSTRAINT pirates_ship_id_fkey FOREIGN KEY (ship_id) REFERENCES ships(ship_id) ON DELETE RESTRICT;

--
-- Name: pirates_captain_id_fkey; Type: FK CONSTRAINT; Schema: caribbean; Owner: $username
--

ALTER TABLE ONLY pirates
    ADD CONSTRAINT pirates_captain_id_fkey FOREIGN KEY (captain_id) REFERENCES pirates(pirate_id) ON DELETE SET NULL;

--
-- Name: pirates_rank_id_fkey; Type: FK CONSTRAINT; Schema: caribbean; Owner: $username
--

ALTER TABLE ONLY pirates
    ADD CONSTRAINT pirates_rank_id_fkey FOREIGN KEY (rank_id) REFERENCES ranks(rank_id) ON DELETE RESTRICT;


--
-- Name: caribbean; Type: ACL; Schema: -; Owner: $username
--

REVOKE ALL ON SCHEMA caribbean FROM PUBLIC;
REVOKE ALL ON SCHEMA caribbean FROM $username;
GRANT ALL ON SCHEMA caribbean TO $username;


--
-- Name: booties; Type: ACL; Schema: caribbean; Owner: $username
--

REVOKE ALL ON TABLE booties FROM PUBLIC;
REVOKE ALL ON TABLE booties FROM $username;
GRANT INSERT,SELECT,UPDATE,DELETE,REFERENCES,TRIGGER ON TABLE booties TO $username;


--
-- Name: booties2pirates; Type: ACL; Schema: caribbean; Owner: $username
--

REVOKE ALL ON TABLE booties2pirates FROM PUBLIC;
REVOKE ALL ON TABLE booties2pirates FROM $username;
GRANT INSERT,SELECT,UPDATE,DELETE,REFERENCES,TRIGGER ON TABLE booties2pirates TO $username;


--
-- Name: pirates; Type: ACL; Schema: caribbean; Owner: $username
--

REVOKE ALL ON TABLE pirates FROM PUBLIC;
REVOKE ALL ON TABLE pirates FROM $username;
GRANT INSERT,SELECT,UPDATE,DELETE,REFERENCES,TRIGGER ON TABLE pirates TO $username;


--
-- Name: ships; Type: ACL; Schema: caribbean; Owner: $username
--

REVOKE ALL ON TABLE ships FROM PUBLIC;
REVOKE ALL ON TABLE ships FROM $username;
GRANT INSERT,SELECT,UPDATE,DELETE,REFERENCES,TRIGGER ON TABLE ships TO $username;

--
-- Name: ship_types; Type: ACL; Schema: caribbean; Owner: $username
--

REVOKE ALL ON TABLE ship_types FROM PUBLIC;
REVOKE ALL ON TABLE ship_types FROM $username;
GRANT INSERT,SELECT,UPDATE,DELETE,REFERENCES,TRIGGER ON TABLE ship_types TO $username;

--
-- Name: ranks; Type: ACL; Schema: caribbean; Owner: $username
--

REVOKE ALL ON TABLE ranks FROM PUBLIC;
REVOKE ALL ON TABLE ranks FROM $username;
GRANT INSERT,SELECT,UPDATE,DELETE,REFERENCES,TRIGGER ON TABLE ranks TO $username;


--
-- Name: booties_booty_id_seq; Type: ACL; Schema: caribbean; Owner: $username
--

REVOKE ALL ON TABLE booties_booty_id_seq FROM PUBLIC;
REVOKE ALL ON TABLE booties_booty_id_seq FROM $username;
GRANT SELECT,UPDATE ON TABLE booties_booty_id_seq TO $username;


--
-- Name: pirates_pirate_id_seq; Type: ACL; Schema: caribbean; Owner: $username
--

REVOKE ALL ON TABLE pirates_pirate_id_seq FROM PUBLIC;
REVOKE ALL ON TABLE pirates_pirate_id_seq FROM $username;
GRANT SELECT,UPDATE ON TABLE pirates_pirate_id_seq TO $username;

--
-- Name: ships_ship_id_seq; Type: ACL; Schema: caribbean; Owner: $username
--

REVOKE ALL ON TABLE ships_ship_id_seq FROM PUBLIC;
REVOKE ALL ON TABLE ships_ship_id_seq FROM $username;
GRANT SELECT,UPDATE ON TABLE ships_ship_id_seq TO $username;

--
-- Name: ship_types_ship_type_id_seq; Type: ACL; Schema: caribbean; Owner: $username
--

REVOKE ALL ON TABLE ship_types_ship_type_id_seq FROM PUBLIC;
REVOKE ALL ON TABLE ship_types_ship_type_id_seq FROM $username;
GRANT SELECT,UPDATE ON TABLE ship_types_ship_type_id_seq TO $username;


--
-- Name: ranks_rank_id_seq; Type: ACL; Schema: caribbean; Owner: $username
--

REVOKE ALL ON TABLE ranks_rank_id_seq FROM PUBLIC;
REVOKE ALL ON TABLE ranks_rank_id_seq FROM $username;
GRANT SELECT,UPDATE ON TABLE ranks_rank_id_seq TO $username;


--
-- Data for static ranks table
--
INSERT INTO ranks (name) VALUES ('Able Seaman');
INSERT INTO ranks (name) VALUES ('Cabin Boy');
INSERT INTO ranks (name) VALUES ('Captain');

--
-- Data for static nationalities table
--
INSERT INTO nationalities (name) VALUES ('Spanish');
INSERT INTO nationalities (name) VALUES ('British');
INSERT INTO nationalities (name) VALUES ('French');


--
-- Data for static masts table
--
INSERT INTO masts (name, sail_count) VALUES ('Main', 4);
INSERT INTO masts (name, sail_count) VALUES ('Fore', 3);
INSERT INTO masts (name, sail_count) VALUES ('Mizzen', 3);
INSERT INTO masts (name, sail_count) VALUES ('Jigger', 3);

--
-- Data for SCBR ship_types table
--

INSERT INTO ship_types (name, subclass_name) VALUES ('Row Boat', 'Rowboat');
INSERT INTO ship_types (name, subclass_name) VALUES ('Frigate', 'Frigate');
INSERT INTO ship_types (name, subclass_name) VALUES ('Galleon', 'Galleon');

--
-- Data for HMM on ship_types to masts
--

INSERT INTO masts2ship_types (mast_id, ship_type_id)
  SELECT m.mast_id, st.ship_type_id
    FROM masts m, ship_types st, 
      (VALUES
          ('Fore','Frigate'),
          ('Main','Frigate'),
          ('Mizzen','Frigate'),
          ('Fore','Galleon'),
          ('Main','Galleon'),
          ('Mizzen','Galleon'),
          ('Jigger','Galleon')
       )
       AS x(ship_type_name, mast_name)
   WHERE x.ship_type_name = st.name AND x.mast_name = m.name;

--
-- PostgreSQL database dump complete
--


EOSQL
}

1;

