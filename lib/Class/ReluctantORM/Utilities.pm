package Class::ReluctantORM::Utilities;
use warnings;
use strict;
use Data::Dumper;

=head1 NAME

Class::ReluctantORM::Utilities - Utility subroutines

=head1 SYNOPSIS

  use Class::ReluctantORM::Utilities qw(:all);

  install_method('Some::Class', 'method_name', $coderef);
  conditional_load('Class::Name');

  # Look for and load all modules under the location of Super::Class
  # (handy for loading driver subclasses)
  BEGIN {
    @classes = conditional_load_subdir('Super::Class');
  }


=head1 DESCRIPTION

An uncohesive set of utility methods.  Several are for test manipulation; some are for class loading or interface manipulation.

No subroutines are exported by default, but all are available by request.

=cut

use base 'Exporter';
our @EXPORT = ();
our @EXPORT_OK = ();
our %EXPORT_TAGS = ();

use Class::ReluctantORM::Exception;
use Lingua::EN::Inflect;
use Sub::Name;
use JSON;

our $DEBUG = 0;


=head1 SUBROUTINES

=cut

=head2 install_method('Class', 'method_name', $coderef, $clobber);

Installs a new method in a class.  The class need not exist.

$clobber determines what to do if the named method already exists.  If clobber
is true, the existing method is renamed to __orig_method_name,
and $coderef is installed as method_name.  If clobber is false, the existing method
is untouched, and the $coderef is installed as __new_method_name .

If the named method does not exist, the coderef is
installed and $clobber has no effect.

=cut

push @EXPORT_OK, 'install_method';
sub install_method {
    my ($class, $method_name, $coderef, $clobber) = @_;
    my $existing = $class->can($method_name);

    unless ($coderef) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'coderef');  }

    {
        no strict 'refs';
        if ($existing && $clobber) {
            no warnings 'redefine';
            my $backup_name = '__orig_' . $method_name;
            *{"${class}::$backup_name"} = $existing;
            *{"${class}::$method_name"} = $coderef;
            subname "${class}::$method_name", $coderef;
        } elsif ($existing) {
            my $new_name = '__new_' . $method_name;
            *{"${class}::$new_name"} = $coderef;
        } else {
            *{"${class}::$method_name"} = $coderef;
            subname "${class}::$method_name", $coderef;
        }
    }
}

push @EXPORT_OK, 'install_method_on_first_use';
sub install_method_on_first_use {
    my ($class, $method_name, $method_maker) = @_;
    $Class::ReluctantORM::METHODS_TO_BUILD_ON_FIRST_USE{$class}{$method_name} = $method_maker;
}

=head2 install_method_generator($class, $generator)

Installs a method generator for as-needed method creation.  An AUTOLOAD hook will check to see if any generator can make a method by the given name.  Multuple generators can be installed for each class.

$class is a Class::ReluctantORM subclass.

$generator is a coderef.  It will be called with two args: the class name, and the proposed method name.  If the generator can generate a method body, it should do so, and return a coderef which will then be installed under that name.  If no candidate can be created, it should return undef.

=cut

push @EXPORT_OK, 'install_method_generator';
sub install_method_generator {
    my ($class, $generator) = @_;
    push @{$Class::ReluctantORM::METHOD_GENERATORS{$class}}, $generator;
}

our %COND_LOADED = ();

=head2 conditional_load('Class::Name');

Loads Class::Name if it hasn't already been loaded.

=cut

push @EXPORT_OK, 'conditional_load';
sub conditional_load {
   my $targetClass = shift;
   if (defined $COND_LOADED{$targetClass}) {
       if ($COND_LOADED{$targetClass}) {
           # Already loaded OK
           if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- already loaded $targetClass OK\n"; }
           return;
       }

       Class::ReluctantORM::Exception::CannotLoadClass->croak(error => 'Failed once before, not trying again.', class => $targetClass);
   } else {

       # Check to see if it has already been loaded from file
       my $path = $targetClass;
       $path =~ s/::/\//g;
       $path .= '.pm';
       if (exists $INC{$path}) {
           if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- already loaded from file, according to \%INC, not loading $targetClass again.\n"; }
           $COND_LOADED{$targetClass} = 1;
           return;
       }

       # Hmm, check to see if the namespace is already occupied - don't reload if it is
       # (might have been loaded from a file not named like the class)
       my $symbol_table_name = $targetClass . '::';
       {
           no strict 'refs';
           # Bitten by this once: check to see if the the namespace contains anything
           # other than SYMBOLS IN ALL CAPS.  The idea here is that you're likely to 
           # set things like $PACKAGE::DEBUG, which will populate the symbol table.
           # We shouldn't consider that loaded, however; but if you have things in 
           # lowercase, you've probably got actual methods, meaning you actually already loaded.
           # BITTEN TWICE: make sure it doesn't have :: in  it either - that's a subclass that might already be loaded.
           if (grep {$_ !~ /^[A-Z_0-9]+$/ } grep { $_ !~ /::/ } keys %$symbol_table_name) {
               if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- hmmm, apparently symbol table is non-empty, not loading $targetClass!\n"; }
               if ($DEBUG > 2) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- symbol table contents:\n" . Dumper({%$symbol_table_name}) . "\n"; }
               $COND_LOADED{$targetClass} = 1;
               return;
           }
       }


       # OK, go ahead and load from file
       eval  " use $targetClass; ";
       if ($@) {
           $COND_LOADED{$targetClass} = 0;
           print STDERR __PACKAGE__ . ':' . __LINE__ . " - Exception thrown while trying to load $targetClass:\n$@\n";
           Class::ReluctantORM::Exception::CannotLoadClass->croak(error => $@, class => $targetClass);
       } else {
           if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- successfully compiled $targetClass \n"; }
           $COND_LOADED{$targetClass} = 1;
       }
   }
}

=head2 @classes = conditional_load_subdir('Super::Class', $depth);

Finds Super::Class on the filesystem, then looks for and loads all modules Super/Class/*.pm
If $depth is present, directories are searched up to $depth deep (so $depth =2 gives Super/Class/*.pm Super/Class/*/*.pm Super/Class/*/*/*.pm)

It's best to call this from within a BEGIN block, if you are trying to find driver modules.

Returns a list of loaded classes.

=cut

push @EXPORT_OK, 'conditional_load_subdir';
sub conditional_load_subdir {
    my $super = shift;
    my $depth = shift || 0;

    # Map super class name into filename
    my $super_fn = $super;
    $super_fn =~ s/::/\//g;
    my $super_stub = $super_fn;
    $super_fn .= '.pm';

    # Lookup where it was loaded from
    $super_fn = $INC{$super_fn};
    unless ($super_fn) {
        Class::ReluctantORM::Exception::CannotLoadClass->croak(error => 'Cannot find filesystem location, so cannot load subclasses', class => $super);
    }

    if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- for class $super got fs location\n$super_fn\n"; }

    # Build filesearch pattern
    my $super_dir = $super_fn;
    $super_dir =~ s/\.pm//;
    my $glob;
    for my $d (0..$depth) {
        $glob .= $super_dir;
        for (0..$d) { $glob .= '/*'; }
        $glob .= '.pm ';
    }

    if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- got glob \n $glob\n"; }

    # Find files and map their names to modules
    my @mod_files = glob($glob);
    map { $_ =~ s/^.*$super_stub/$super_stub/; $_ } @mod_files;
    map { $_ =~ s/\//::/g; $_ } @mod_files;
    map { $_ =~ s/\.pm$//; $_ } @mod_files;



    # Load the classes
    my @classes;
    for my $class (@mod_files) {
        if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "- trying to load module $class\n"; }
        conditional_load($class);
        push @classes, $class;
    }
    return @classes;
}

=head2 %args = check_args(%opts);

Given the args list (which is assumed to have $self or $class already shifted off),
check the args list for required, optional, and mutually exclusive options.

=over

=item args => [] or args => {} (required)

If a arrayref, checks to make sure it is even-numbered.  If a hashref, used as-is.

=item required => []

List of args that are required.

=item optional => []

List of args that are permitted but optional.

=item mutex => [ \@set1, \@set2, ...]

Listref of listrefs.  Each inner listref is a set of parameters that is mutually exclusive, that is, AT MOST one the params may appear.

=item one_of => [ \@set1, \@set2, ...]

Like mutex, but EXACTLY ONE of the params of each set must appear.

=item frames => 2

When throwing an exception, the number of frames to jump back.  Default is 2.

=back

=cut

push @EXPORT_OK, 'check_args';
sub check_args {
    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %opts = @_;

    my $frames = $opts{frames} || 2;

    my %raw_args;
    my %scrubbed_args;
    if (ref($opts{args}) eq 'ARRAY' ) {
        if (@{$opts{args}} % 2) {
            Class::ReluctantORM::Exception::Param::ExpectedHash->croak(frames => $frames);
        }
        %raw_args = @{$opts{args}};
    } else {
        %raw_args = %{$opts{args}};
    }

    if ($opts{debug}) { print STDERR "CA Have incoming raw_args:\n" . Dumper(\%raw_args); }

    # Required args
    foreach my $argname (@{$opts{required} || []}) {
        unless (exists $raw_args{$argname}) {
            Class::ReluctantORM::Exception::Param::Missing->croak(param => $argname, frames => $frames);
        }
        $scrubbed_args{$argname} = $raw_args{$argname};
        delete $raw_args{$argname};
    }
    if ($opts{debug}) { print STDERR "CA after required have raw_args:\n" . Dumper(\%raw_args); }
    if ($opts{debug}) { print STDERR "CA after required have scrubbed_args:\n" . Dumper(\%scrubbed_args); }

    # Mutex and one_of
    foreach my $mode (qw(mutex one_of)) {
        foreach my $set (@{$opts{$mode} || []}) {
            my $count_seen = 0;
            foreach my $argname (@{$set || []}) {
                if (exists $raw_args{$argname}) {
                    $count_seen++;
                    $scrubbed_args{$argname} = $raw_args{$argname};
                    delete $raw_args{$argname};
                }
            }
            if ($mode eq 'mutex' && $count_seen > 1) {
                Class::ReluctantORM::Exception::Param::MutuallyExclusive->croak
                    (error => 'At most one of the parameters may be supplied', param_set => $set, frames => $frames);
            } elsif ($mode eq 'one_of' && $count_seen == 0) {
                Class::ReluctantORM::Exception::Param::Missing->croak
                    (error => 'Exactly one of the parameters must be supplied',
                     param => join(',', @$set),
                     frames => $frames);
            } elsif ($mode eq 'one_of' && $count_seen > 1) {
                Class::ReluctantORM::Exception::Param::MutuallyExclusive->croak
                    (error => 'Exactly one of the parameters must be supplied',
                     param_set => $set,
                     frames => $frames);
            }
        }
    }
    if ($opts{debug}) { print STDERR "CA after mutex/OO have raw_args:\n" . Dumper(\%raw_args); }
    if ($opts{debug}) { print STDERR "CA after mutex/OO have scrubbed_args:\n" . Dumper(\%scrubbed_args); }

    # Optional
    my %still_allowed = map {$_ => 1 } @{$opts{optional} || []};
    foreach my $argname (keys %still_allowed) {
        if (exists $raw_args{$argname}) {
            $scrubbed_args{$argname} = $raw_args{$argname};
            delete $raw_args{$argname};
        }
    }

    if ($opts{debug}) { print STDERR "CA after optional have raw_args:\n" . Dumper(\%raw_args); }
    if ($opts{debug}) { print STDERR "CA after optional have scrubbed_args:\n" . Dumper(\%scrubbed_args); }


    # Spurious (raw_args should be empty now)
    if (keys %raw_args) {
        Class::ReluctantORM::Exception::Param::Spurious->croak(param => join(',', keys %raw_args), value => join(',', values %raw_args), frames => $frames);
    }

    return %scrubbed_args;
}

=head2 $usc = camel_case_to_underscore_case($camelCaseString);

Converts a string in camel case (LikeThis) to one 
in underscore case (like_this).

=cut

push @EXPORT_OK, 'camel_case_to_underscore_case';
sub camel_case_to_underscore_case {
    my $camel = shift;
    $camel =~ s/([A-Z]+)([a-z])/'_' . lc($1) . $2/ge;
    $camel =~ s/^_//;
    return $camel;
}

=head2 my $plural = pluralize($singular);

Returns the plural form of a word.

=cut

push @EXPORT_OK, 'pluralize';
sub pluralize {
    my $singular = shift;

    if ($singular =~ /staff$/i) {
        return $singular;
    }
    else {
        if ($singular =~ /^[A-Z]/) {
            return ucfirst( Lingua::EN::Inflect::PL( lcfirst($singular) ) );
        }
        else {
            return Lingua::EN::Inflect::PL($singular);
        }
    }
}

=head2 $output = nz($input, $output_if_undef);

If $input is defined, $outout = $input.

If $input is undef, $output = $output_if_undef.

Named after the same function in Visual Basic, where all good ideas originate.

=cut

push @EXPORT_OK, 'nz';

sub nz { return defined($_[0]) ? $_[0] : $_[1]; }


=head2 $bool = array_shallow_eq($ary1, $ary2);

Returns true if the arrays referred to by the arrayrefs $ary1 and $ary2 are identical in a shallow sense, using 'eq'.

=cut

push @EXPORT_OK, 'array_shallow_eq';
sub array_shallow_eq {
    my $ary1 = shift;
    my $ary2 = shift;
    unless (ref($ary1) eq 'ARRAY') { Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak(param => 'array1'); }
    unless (ref($ary2) eq 'ARRAY') { Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak(param => 'array2'); }

    # Element count check
    unless (@$ary1 == @$ary2) { return 0; }

    for my $i (0..(@$ary1 -1)) {
        my ($c, $d) = ($ary1->[$i], $ary2->[$i]);
        my $matched = (defined($c) && defined($d) && $c eq $d) || (!defined($c) && !defined($d));
        return 0 unless $matched;
    }
    return 1;
}

=head2 $info = last_non_cro_stack_frame();

=head2 @frames = last_non_cro_stack_frame();

Returns information about the the last call stack frame outside of Class::ReluctantORM.

In scalar context, returns only the last call frame.  In list context, returns the last stack frame and up.

$info will contain keys 'file', 'package', 'line', and 'frames'.  Frames indicates the value passed to caller() to obtain the information, which is the number of frames to unwind.

=cut

push @EXPORT_OK, 'last_non_cro_stack_frame';
our @PACKAGES_TO_CONSIDER_PART_OF_CRO =
  (
   qr{^Class::ReluctantORM},
  );
sub last_non_cro_stack_frame {
    my $frame = -1;
    my @frames;

  FRAME:
    while (1) {
        $frame++;
        my ($package, $file, $line) = caller($frame);
        unless ($package) {
            # out of frames?
            return @frames;
        }
        foreach my $re (@PACKAGES_TO_CONSIDER_PART_OF_CRO) {
            if ($package =~ $re) {
                next FRAME;
            }
        }
        # Didn't match anything, must not be CRO
        my %info = (
                 package => $package,
                 file    => $file,
                 line    => $line,
                 frames  => $frame,
                );
        push @frames, \%info;
        if (!wantarray) {
            return \%info;
        }
    }

}
# TEST WINDOW - see t/29-utils.t
sub __testsub_lncsf1 {
    return last_non_cro_stack_frame();
}


=head2 $int = row_size($hashref);

Calculates the size, in bytes, of the values of the given hashref.  This is used by the RowSize and QuerySize Monitors.

=cut

push @EXPORT_OK, qw(row_size);
sub row_size {
    my $row = shift;
    my $tally = 0;
    foreach my $v (values %$row) {
        # OK, actually characters, not bytes.  We just want a rough size anyway.  See 'perldoc -f length' for more accurate approaches
        $tally += length($v) if defined($v); # NULL/undef count as 0, I suppose
    }
    return $tally;
}

=head2 deprecated($message);


=cut

push @EXPORT_OK, 'deprecated';
sub deprecated {
    # TODO - write deprecated() util function
}

=head2 read_file($filename)

File::Slurp::read_file workalike, but far crappier.

=cut

push @EXPORT_OK, 'read_file';
sub read_file {
    my $filename = shift;
    my $out;
    {
        local( $/, *FH ) ;
        open( FH, $filename ) or die "could not open $filename: $!\n";
        $out = <FH>;
    }
    return $out;
}

=head2 write_file($filename, $content)

File::Slurp::write_file workalike, but far crappier.

=cut

push @EXPORT_OK, 'write_file';
sub write_file {
    my $filename = shift;
    my $content = shift;
    {
        my $fh;
        open( $fh, '>' . $filename ) or die "could not open $filename: $!\n";
        print $fh $content;
    }
}

=head2 $json_string = json_encode($perl_ref);

Version-blind JSON encoder.

=cut

push @EXPORT_OK, 'json_encode';
sub json_encode {
    if ($JSON::VERSION > 2) {
        goto &JSON::to_json;
    } else {
        goto &JSON::objToJson;
    }
}

=head2 $perl_ref = json_decode($json_string);

Version-blind JSON decoder.

=cut

push @EXPORT_OK, 'json_decode';
sub json_decode {
    if ($JSON::VERSION > 2) {
        goto &JSON::from_json;
    } else {
        goto &JSON::jsonToObj;
    }
}


$EXPORT_TAGS{all} = \@EXPORT_OK;

1;

