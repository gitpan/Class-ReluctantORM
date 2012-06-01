package CrormTest::Model;
use strict;
use warnings;

use base 'Exporter';
our @EXPORT;

use CrormTest::DB;

BEGIN {
    our %TABLE_DEFAULTS = (
                           schema => 'caribbean',
                           db_class => 'CrormTest::DB',
                           deletable => 1,
                          );
}


my @modules = qw(
                    ShipType
                    Ship
                    Pirate
                    Booty
                    Rank
                    Nationality
                    Mast
               );

foreach my $module (@modules) {
    # Poor man's 'use aliased'
    eval <<EOP;
  sub $module { return 'CrormTest::Model::' . '$module'; }
EOP
    push @EXPORT, $module;
}


1;

#----------------------------------------------#
#   Dumb Classes Here, Smart Classes Later
#----------------------------------------------#

package CrormTest::Model::ShipType;
use base 'Class::ReluctantORM::SubClassByRow';
__PACKAGE__->build_class(
                         %CrormTest::Model::TABLE_DEFAULTS,
                         table => 'ship_types',
                         primary_key => 'ship_type_id',
                         subclass_column => 'subclass_name',
                        );


package CrormTest::Model::Ship;
use base 'Class::ReluctantORM';
__PACKAGE__->build_class(
                         %CrormTest::Model::TABLE_DEFAULTS,
                         table => 'ships',
                         primary_key => 'ship_id',
                        );

package CrormTest::Model::Pirate;
use base 'Class::ReluctantORM::Audited';
__PACKAGE__->build_class(
                         %CrormTest::Model::TABLE_DEFAULTS,
                         table => 'pirates',
                         primary_key => 'pirate_id',
                         refresh_fields => ['rank_id', 'leg_count'],
                         audit_schema_name => 'caribbean',
                         audit_table_name => 'pirates_log',
                         audit_columns => [ qw (audit_pid) ],
                         audit_seamless_mode => 1,
                        );

sub get_audit_metadata_audit_pid { return $$; }

package CrormTest::Model::Booty;
use base 'Class::ReluctantORM';
__PACKAGE__->build_class(
                         %CrormTest::Model::TABLE_DEFAULTS,
                         table => 'booties',
                         primary_key => 'booty_id',
                         fields => {
                                    booty_id   => 'booty_id',
                                    cash_value => 'cash_value',
                                    place      => 'location',
                                    secret_map => 'secret_map',
                                   },
                         lazy_fields => [qw(secret_map)],
                        );

package CrormTest::Model::Rank;
use base 'Class::ReluctantORM::Static';
__PACKAGE__->build_class(
                         %CrormTest::Model::TABLE_DEFAULTS,
                         table => 'ranks',
                         primary_key => 'rank_id',
                         deletable => 0,
                         index => ['name'],
                        );

package CrormTest::Model::Nationality;
use base 'Class::ReluctantORM::Static';
__PACKAGE__->build_class(
                         %CrormTest::Model::TABLE_DEFAULTS,
                         table => 'nationalities',
                         primary_key => 'nationality_id',
                         deletable => 0,
                         index => ['name'],
                        );

package CrormTest::Model::Mast;
use base 'Class::ReluctantORM::Static';
__PACKAGE__->build_class(
                         %CrormTest::Model::TABLE_DEFAULTS,
                         table => 'masts',
                         primary_key => 'mast_id',
                         deletable => 0,
                         index => ['name'],
                        );


#----------------------------------------------#
# Relationships
#----------------------------------------------#

package CrormTest::Model;

CrormTest::Model::Pirate->has_one('CrormTest::Model::Ship');
CrormTest::Model::Ship->has_many('CrormTest::Model::Pirate');
CrormTest::Model::Ship->has_one(
                                class => 'CrormTest::Model::Pirate',
                                method_name => 'captain',
                                local_key => 'captain_pirate_id',
                                remote_key => 'pirate_id',
                               );
CrormTest::Model::Ship->has_one('CrormTest::Model::ShipType');


CrormTest::Model::Pirate->has_one('CrormTest::Model::Rank');
CrormTest::Model::Pirate->has_one(
                                  class => 'CrormTest::Model::Pirate',
                                  method_name => 'captain',
                                  local_key => 'captain_id',
                                  remote_key => 'pirate_id',
                                 );
CrormTest::Model::Pirate->has_many_many(
                                        class => 'CrormTest::Model::Booty',
                                        join_table => 'booties2pirates',
                                       );
CrormTest::Model::Booty->has_many_many(
                                       class => 'CrormTest::Model::Pirate',
                                       join_table => 'booties2pirates',
                                      );

CrormTest::Model::Pirate->has_lazy('diary');

CrormTest::Model::Pirate->has_many_many(
                                        class => 'CrormTest::Model::Nationality',
                                        join_table => 'nationalities2pirates',
                                       );

CrormTest::Model::ShipType->has_many_many(
                                          class => 'CrormTest::Model::Mast',
                                          join_table => 'masts2ship_types',
                                         );
1;

