use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Class::ReluctantORM',
    AUTHOR              => 'Clinton Wolfe <CLWOLFE@cpan.org>',
    VERSION_FROM        => 'lib/Class/ReluctantORM.pm',
    ABSTRACT_FROM       => 'lib/Class/ReluctantORM.pm',
    PL_FILES            => {},
    LICENSE             => 'perl',
    PREREQ_PM => {
                  'NEXT'       => 0,
                  'Data::Diff' => 0,
                  'Class::Accessor' => 0,
                  'Regexp::Common' => 0,
                  'DBI' => 1.611,             # SQL::Statement needs 1.611 or better
                  'Test::Exception' => 0,
                  'Exception::Class' => 1.30, # Need Exception::Class::Base
                  'aliased' => 0,
                  'SQL::Statement' => 0,
                  'Lingua::EN::Inflect' => 0,
                  'IO::Scalar' => 0,
                  'DBI' => 0,
                  'Sub::Name' => 0.05,
                  'JSON' => 0,
                 },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Class-ReluctantORM-*' },
);
