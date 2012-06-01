package Class::ReluctantORM::Registry::None;
use strict;
use warnings;
use base 'Class::ReluctantORM::Registry';

=head1 NAME

  Class::ReluctantORM::Registry::None - Do-Nothing Registry

=head1 DESCRIPTION

This registry does not store anything, and never remembers anything.
Use it to disable Registry effects.

=head1 AUTHOR

 Clinton Wolfe clwolfe@cpan.org January 2010

=cut

sub fetch { return undef; }
sub store { } # do nothing
sub purge { } # do nothing
sub purge_all { } # do nothing
sub count { return 0; }
sub walk { } # do nothing

1;
