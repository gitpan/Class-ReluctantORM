=head1 NAME

Class::ReluctantORM::Exception - OO Exceptions

=head1 SYNOPSIS

  use Class::ReluctantORM::Exception;

  # In API code:

  # dies locally
  Class::ReluctantORM::Exception::Params::Missing->throw(param => 'id');

  # dies from caller's perspective
  Class::ReluctantORM::Exception::Params::Missing->croak(param => 'id');

  # dies from caller's caller's perspective
  Class::ReluctantORM::Exception::Params::Missing->croak(param => 'id', frames => 2);

  # To catch:
  eval { something_dangerous(); }
  if (my $e = Class::ReluctantORM::Exception::Params::Missing->caught()) {
    my $p = $e->param(); # Find out what was missing
    print $e; #  Stringifies nicely
  } else {
    die $@; # Pass on unrecognized exceptions
  }

  # Special handler included for working with OmniTI::DB connections....
  my $dbh = NatGeo::DB::Magazine->new();
  $dbh->set_handle_error(Class::ReluctantORM::Exception->make_db_error_handler());


=head1 DESCRIPTION

Uses Exception::Class to define a wide variety of exceptions.

=head1 STRINGIFICATION

Any extra fields defined for a particular exception 
class will be included in the stringification of 
the exception, like this:

  Error message
     field1 => value1
     field2 => value2

=head1 EXCEPTIONS

=over

=item Class::ReluctantORM::Exception::Param

Exceptions related to parameter passing.  Expect fields 'param'.

=over

=item Class::ReluctantORM::Exception::Param::Missing

=item Class::ReluctantORM::Exception::Param::Empty

An array or hash ref turned out to be present but empty.

=item Class::ReluctantORM::Exception::Param::ExpectedHash

Thrown when the method uses named parameters, but an odd number of parameters were provided.  param field is not used.

=item Class::ReluctantORM::Exception::Param::ExpectedHashRef

=item Class::ReluctantORM::Exception::Param::ExpectedArrayRef

=item Class::ReluctantORM::Exception::Param::ExpectedHashref

=item Class::ReluctantORM::Exception::Param::MutuallyExclusive

Used when two parameters cannot both be present.  Use fields 'param' and 'other_param'.

=item Class::ReluctantORM::Exception::Param::Duplicate

Thrown when the same named parameter is used more than once.

=back

=back


=head1 AUTHOR

  Clinton Wolfe

=cut

package Class::ReluctantORM::Exception;
use strict;
our $DEBUG ||= 0;
use Data::Dumper;
our $TRACE ||= 0; # Set to true to enable traces on croak();

use base 'Exception::Class::Base';

our %FIELD_RENDERERS = 
  (
   fetch_locations => sub {
       my $traces = shift || [];
       unless ($traces) { return '' }
       my $str = "\tFetched from " . (scalar @$traces) . " location(s):\n";
       foreach my $trace (@$traces) {
           $str .= "\t\tLocation Trace:\n";
           foreach my $frame (@$trace) {
               # TODO - TB2CRO - OmniTI-ism
               # Special hook for Mungo support
               if ($frame->{package} =~ /Mungo::FilePage/) {
                   my $file = $main::Response->{Mungo}->demangle_name($frame->{package} . '::__content');
                   $file =~ s{^Mungo::FilePage\(}{};
                   $file =~ s{\)$}{};
                   $str .= "\t\t\tfile: " . $file . " line:" . $frame->{line} . "\n";
               } else {
                   $str .= "\t\t\t" . $frame->{file} . " line:" . $frame->{line} . "\n";
               }
           }
       }
       return $str;
   },
   table => sub {
       my $table = shift;
       return '' unless $table;
       return $table->display_name();
   },
   join => sub {
       my $j = shift;
       return '' unless $j;
       return $j->pretty_print(prefix => "\t  ");
   },
   query_location => sub {
       my $frames = shift || [];
       unless ($frames) { return '' }
       my $str = "\tOriginal query location:\n";
       foreach my $frame (@$frames) {
           $str .= "\t  " . $frame->{file} . " line " . $frame->{line} . "\n";
       }
       return $str;
   },
  );

sub full_message {
    my $self = shift;
    my @field_names = $self->Fields();
    my $msg = $self->message . "\n"; # Automatic newline?
    $msg .= ($self->description . "\n") || '';
    foreach my $field (@field_names) {
        my $val = $self->$field;
        if (exists $FIELD_RENDERERS{$field}) {
            $msg .= $FIELD_RENDERERS{$field}->($val);
        } else {
            $msg .= "\t$field => " . (defined($val) ? $val : 'undef') . "\n";
        }
    }

    # Include filename and file
    #print STDERR "In full_message, have frame count " . $self->trace->frame_count . "\n";
    my $frame = $self->trace->frame(0);
    $msg .= " at " . $frame->filename . " line " . $frame->line . "\n";

    return $msg;
}

sub croak {
    my $class = shift;
    my %args;

    # Behave like throw: if one arg, it's the message.
    if (@_ == 1) {
        %args = (message => shift());
    } else {
        %args = @_;
    }

    my $frame_count = defined($args{frames}) ? $args{frames} : 1;
    delete $args{frames};

    my $self = $class->new(%args);

    my @frames  = $self->{trace}->frames;
    #print STDERR "In croak, have orginal frame count " . (scalar @frames) . "\n";

    # If requested frame skip is greater than actual frame count, force to be one less than actual frame count.
    if ($frame_count >= @frames) {
        $frame_count = @frames - 1;
    }

    my @dropped_frames = splice @frames, 0, $frame_count; # Delete $frame_count frames from the top (nearest) end of the stack.
    $self->{trace}->{frames} = \@frames;
    #print STDERR "In croak, have final frame count " . (scalar @frames) . "\n";

    if ($DEBUG > 1) {
        print STDERR __PACKAGE__ . ':' . __FILE__ . " - Have dropped frames:\n";
        for (@dropped_frames) {
            print "\t" . $_->filename . ':' . $_->line . "\n";
        }
    }

    if ($TRACE) {
        $self->show_trace(1);
    }

    die $self;
}

# Returns a coderef suitable for setting the DBI HandleError attribute.
sub make_db_error_handler {
    my $class = shift;
    my $code = sub {
        my $errstr = shift;
        my $dbh = shift;
        my $bind_msg = '(in PREPARE stage)';
        if ($dbh->{ParamValues}) {
            my %binds = %{$dbh->{ParamValues}};
            if ($DEBUG) { print STDERR __PACKAGE__ . ':' . __LINE__ . "In DB error handler, have binds:\n" .  Dumper(\%binds); }
            $bind_msg = join ', ', map { $_ . ':' . (defined($binds{$_}) ? $binds{$_} : 'undef') } keys %binds;
        }
        Class::ReluctantORM::Exception::SQL::ExecutionError->croak(
                                                 frames => 1,
                                                 error => $errstr,
                                                 statement => $dbh->{Statement},
                                                 bind_values => $bind_msg,
                                                );
    }
}


package main;

# Note: this should follow the definition of Class::ReluctantORM::Exception
use Exception::Class 
  (

   # Coding
   'Class::ReluctantORM::Exception::NotImplemented' =>
   {
    isa => 'Class::ReluctantORM::Exception',
    description => 'The code that is being attempted to execute has not yet been written.',
   },
   'Class::ReluctantORM::Exception::CannotLoadClass' =>
   {
    isa => 'Class::ReluctantORM::Exception',
    description => 'The requested class cannot be loaded.',
    fields => [qw(class)],
   },



   # Param handling
   'Class::ReluctantORM::Exception::Param' => 
   {
    isa => 'Class::ReluctantORM::Exception',
    description => 'A general parameter error',
    fields => [ qw(param value) ],
   },

   'Class::ReluctantORM::Exception::Param::Missing' => 
   {
    isa => 'Class::ReluctantORM::Exception::Param',
    description => 'A required parameter is missing. The required parameter is listed in the param field.',
   },

   'Class::ReluctantORM::Exception::Param::BadValue' => 
   {
    isa => 'Class::ReluctantORM::Exception::Param',
    description => 'A parameter has an invalid value.',
    fields => [ qw(expected) ],
   },

   'Class::ReluctantORM::Exception::Param::ExpectedHash' => 
   {
    isa => 'Class::ReluctantORM::Exception::Param',
    description => 'The method or subroutine expected to be called with named paramters, but an odd number of parameters were passed.',
   },

   'Class::ReluctantORM::Exception::Param::ExpectedArrayRef' => 
   {
    isa => 'Class::ReluctantORM::Exception::Param',
    description => 'A parameter was expected to be an array ref, but was not.  The parameter is listed in the param field.',
   },

   'Class::ReluctantORM::Exception::Param::ExpectedHashRef' => 
   {
    isa => 'Class::ReluctantORM::Exception::Param',
    description => 'A parameter was expected to be an hash ref, but was not.  The parameter is listed in the param field.',
   },

   'Class::ReluctantORM::Exception::Param::WrongType' => 
   {
    isa => 'Class::ReluctantORM::Exception::Param',
    description => 'A parameter is of the wrong type. ',
    fields => [ qw(param expected) ],
   },

   'Class::ReluctantORM::Exception::Param::MutuallyExclusive' => 
   {
    isa => 'Class::ReluctantORM::Exception::Param',
    description => 'You may only provide or or the other of a pair of parameters, but you provided both.  The parameters are listed in the param_set field.',
    fields => [ qw(param_set) ],
   },

   'Class::ReluctantORM::Exception::Param::Duplicate' =>
   {
    isa => 'Class::ReluctantORM::Exception::Param',
    description => 'You specified the same parameter more than once in a list, when unique values are required.',
   },
   'Class::ReluctantORM::Exception::Param::Spurious' =>
   {
    isa => 'Class::ReluctantORM::Exception::Param',
    description => 'You provided extra, unrecognized parameters.',
   },



   # Other function calling problems
   'Class::ReluctantORM::Exception::Call' => 
   {
    isa => 'Class::ReluctantORM::Exception',
    description => 'A general error in function/method calling style',
   },

   'Class::ReluctantORM::Exception::Call::NotMutator' => 
   {
    isa => 'Class::ReluctantORM::Exception::Call',
    description => 'You may not use this function to set a value.',
    fields => [ qw(attribute) ],
   },

   'Class::ReluctantORM::Exception::Call::ExpectationFailure' => 
   {
    isa => 'Class::ReluctantORM::Exception::Call',
    description => 'A precondition failed for this operation.  Hilarity ensues.',
   },

   'Class::ReluctantORM::Exception::Call::NotPermitted' => 
   {
    isa => 'Class::ReluctantORM::Exception::Call',
    description => 'You may not call this method.',
   },

   'Class::ReluctantORM::Exception::Call::NotPermitted::ClassMethodOnly' => 
   {
    isa => 'Class::ReluctantORM::Exception::Call::NotPermitted',
    description => 'You may not call this method as an instance method.  You must call it as a class method.',
    fields => [ qw(method) ],
   },

   'Class::ReluctantORM::Exception::Call::NotPermitted::InstanceMethodOnly' => 
   {
    isa => 'Class::ReluctantORM::Exception::Call::NotPermitted',
    description => 'You may not call this method as a class method.  You must call it as an instance method.',
    fields => [ qw(method) ],
   },

   'Class::ReluctantORM::Exception::Call::Deprecated' => 
   {
    isa => 'Class::ReluctantORM::Exception::Call',
    description => 'You may not call this method, because it is no longer supported.',
   },

   'Class::ReluctantORM::Exception::Call::PureVirtual' => 
   {
    isa => 'Class::ReluctantORM::Exception::Call',
    description => 'This method may not be called directly, because a subclass is supposed to provide its own implementation.',
   },

   'Class::ReluctantORM::Exception::Call::NoSuchMethod' => 
   {
    isa => 'Class::ReluctantORM::Exception::Call',
    description => 'You tried to use a method that does not exist.  This usually means the module does not know how to AUTOLOAD the requested method.',
   },


   # Database problems
   'Class::ReluctantORM::Exception::Data' =>
   {
    isa => 'Class::ReluctantORM::Exception',
    description => 'A general data-related error.',
    fields => [  ],
   },

   'Class::ReluctantORM::Exception::Data::NotFound' =>
   {
    isa => 'Class::ReluctantORM::Exception::Data',
    description => 'Required data was not found.',
    fields => [ qw(primary_key criteria) ],
   },

   'Class::ReluctantORM::Exception::Data::AlreadyInserted' =>
   {
    isa => 'Class::ReluctantORM::Exception::Data',
    description => 'A data object is marked as already existing in the database, but you just tried to insert it again.',
    fields => [ qw(primary_key) ],
   },
   'Class::ReluctantORM::Exception::Data::DependsOnInsert' =>
   {
    isa => 'Class::ReluctantORM::Exception::Data',
    description => 'This object depends on another data object, which must be inserted before you can perform this operation.',
   },

   'Class::ReluctantORM::Exception::Data::UpdateWithoutInsert' =>
   {
    isa => 'Class::ReluctantORM::Exception::Data',
    description => 'A data object is marked as not yet existing in the database, but you just tried to do an UPDATE on it.',
   },

   'Class::ReluctantORM::Exception::Data::DeleteWithoutInsert' =>
   {
    isa => 'Class::ReluctantORM::Exception::Data',
    description => 'A data object is marked as not yet existing in the database, but you just tried to do a DELETE on it.',
   },

   'Class::ReluctantORM::Exception::Data::NeedMoreKeys' =>
   {
    isa => 'Class::ReluctantORM::Exception::Data',
    description => 'An operation depends on having multiple primary or foreign keys, but you did not provide enough keys.',
   },


   'Class::ReluctantORM::Exception::Data::UnsupportedCascade' =>
   {
    isa => 'Class::ReluctantORM::Exception::Data',
    description => 'An insert or update would require performing a cascading insert or update, which is not supported.',
   },

   'Class::ReluctantORM::Exception::Data::UniquenessViolation' =>
   {
    isa => 'Class::ReluctantORM::Exception::Data',
    description => 'A proposed operation would violate a uniqueness constraint, either DB-enforced or ReluctantORM-based.',
   },

   'Class::ReluctantORM::Exception::Data::FetchRequired' =>
   {
    isa => 'Class::ReluctantORM::Exception::Data',
    description => 'You tried to access related data, but the related data has not been fetched yet.  This ORM does not support implicit lazy loading (that is what makes it Reluctant).  Please adjust the fetch call to include the related data.  If Origin Tracking is enabled, the location of the fetch call(s) will be listed.',
    # Special renderer for fetch_locations in message()
    fields => [ qw(call_instead called fetch_locations) ],
   },


   # SQL problems
   'Class::ReluctantORM::Exception::SQL' =>
   {
    isa => 'Class::ReluctantORM::Exception',
    description => 'An error related to parsing, manipulation, or execution of SQL objects.',
    fields => [ qw(sql) ],
   },

   'Class::ReluctantORM::Exception::SQL::AbortedByMonitor' =>
   {
    isa => 'Class::ReluctantORM::Exception::SQL',
    description => 'The query exceeded the fatal_limit threshold of a monitor, and was aborted.',
    fields => [ qw(monitor observed limit query_location) ],
   },


   'Class::ReluctantORM::Exception::SQL::NotInflatable' =>
   {
    isa => 'Class::ReluctantORM::Exception::SQL',
    description => 'The SQL object has insufficient metadata to inflate',
    fields => [ qw() ],
   },

   'Class::ReluctantORM::Exception::SQL::NotInflatable::MissingColumn' =>
   {
    isa => 'Class::ReluctantORM::Exception::SQL::NotInflatable',
    description => 'An essential column is missing in the output columns, which is needed to inflate',
    fields => [ qw(table column) ],
   },

   'Class::ReluctantORM::Exception::SQL::NotInflatable::ExtraTable' =>
   {
    isa => 'Class::ReluctantORM::Exception::SQL::NotInflatable',
    description => 'Could not figure out what to do with a table in the query.',
    fields => [ qw(table) ],
   },

   'Class::ReluctantORM::Exception::SQL::NotInflatable::VagueJoin' =>
   {
    isa => 'Class::ReluctantORM::Exception::SQL::NotInflatable',
    description => 'Could not figure out what to do with a join in the query.',
    fields => [ qw(join) ],
   },

   'Class::ReluctantORM::Exception::SQL::TooComplex' =>
   {
    isa => 'Class::ReluctantORM::Exception::SQL',
    description => 'The statement or SQL object was too complicated to interpret.',
    fields => [ qw() ],
   },

   'Class::ReluctantORM::Exception::SQL::ParseError' =>
   {
    isa => 'Class::ReluctantORM::Exception::SQL',
    description => 'The SQL statement contained a syntax error or other parsing problem.',
    fields => [ qw(sql) ],
   },

   'Class::ReluctantORM::Exception::SQL::AmbiguousReference' =>
   {
    isa => 'Class::ReluctantORM::Exception::SQL',
    description => 'A table or column reference could not be disambiguated.',
    fields => [ qw(statement referent) ],
   },

   'Class::ReluctantORM::Exception::SQL::ExecuteWithoutPrepare' =>
   {
    isa => 'Class::ReluctantORM::Exception::SQL',
    description => 'You must call $driver->prepare($sql) before calling $sql->execute()',
    fields => [ qw() ],
   },

   'Class::ReluctantORM::Exception::SQL::FinishWithoutPrepare' =>
   {
    isa => 'Class::ReluctantORM::Exception::SQL',
    description => 'You must call $driver->prepare($sql) before calling $sql->finish()',
    fields => [ qw() ],
   },

   'Class::ReluctantORM::Exception::SQL::ExecutionError' =>
   {
    isa => 'Class::ReluctantORM::Exception::SQL',
    description => 'An error occured within the database.',
    fields => [ qw(statement bind_values) ],
   },


  );


1;
