#  -*-cperl-*-
# Test suite to ensure Class::ReluctantORM itself can load

use strict;
use warnings;
use blib;
use FindBin;
use lib "$FindBin::Bin/../inc";       # Bundled build dependencies
use lib "$FindBin::Bin/tlib";         # Test libraries

use Test::More tests => 1;

# CRO has a CHECK block - use_ok() will trigger a 'Too late for CHECK block' warning (category: void operations)
# Silence that warning
no warnings qw(void);

use_ok('Class::ReluctantORM');
