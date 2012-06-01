package Class::ReluctantORM::SQL::Aliases;
use strict;
use warnings;

sub Column { return  'Class::ReluctantORM::SQL::Column'; }
sub Criterion { return  'Class::ReluctantORM::SQL::Expression::Criterion'; }
sub Expression { return  'Class::ReluctantORM::SQL::Expression'; }
sub From { return  'Class::ReluctantORM::SQL::From'; }
sub Function { return  'Class::ReluctantORM::SQL::Function'; }
sub FunctionCall { return  'Class::ReluctantORM::SQL::Expression::FunctionCall'; }
sub Join { return  'Class::ReluctantORM::SQL::From::Join'; }
sub Literal { return  'Class::ReluctantORM::SQL::Expression::Literal'; }
sub OrderBy { return  'Class::ReluctantORM::SQL::OrderBy'; }
sub OutputColumn { return  'Class::ReluctantORM::SQL::OutputColumn'; }
sub Param { return  'Class::ReluctantORM::SQL::Param'; }
sub Relation { return  'Class::ReluctantORM::SQL::From::Relation'; }
sub SQL { return 'Class::ReluctantORM::SQL'; }
sub SubQuery { return  'Class::ReluctantORM::SQL::SubQuery'; }
sub Table { return  'Class::ReluctantORM::SQL::Table'; }
sub Where { return  'Class::ReluctantORM::SQL::Where'; }



our $DEBUG ||= 0;

# Auto-alias facility (like 'use aliased', only lazier)
our @EXPORT = qw(
                    Column
                    Criterion
                    Expression
                    From
                    Function
                    FunctionCall
                    Join
                    Literal
                    OrderBy
                    OutputColumn
                    Param
                    Relation
                    SQL
                    SubQuery
                    Table
                    Where
               );
use Exporter;
our @ISA;
push @ISA, 'Exporter';

1;
