package Class::ReluctantORM::Static;

use strict;
use base 'Class::ReluctantORM';
our $DEBUG = 0;
use Data::Dumper;

use Class::ReluctantORM::SQL::Aliases;

# @objects = @{$OBJECTS{class}};
our %OBJECTS;

# $obj = $INDEXES{$class}{$field}{$value};
our %INDEXES;

our %STATIC_METADATA = ();

=head1 NAME

Class::ReluctantORM::Static - ORM base class for type tables

=head1 SYNOPSIS

  # In your type table class...

  package Pirate::Status;
  use base 'Class::ReluctantORM::Static';
  __PACKAGE__->build_class(
    schema => 'highseas',
    table => 'pirate_status',
    primary_key => 'pirate_status_id',
    order => 'name',   # Optional - provide this if you want to have your fetch_all sorted
    index => ['name'], # Special for static - you always get an index by primary key, FYI
  );

=head1 DESCRIPTION

Most databases contain tables that are used as enumarations, specifiying one of a liumited number of values.  These tables are sometimes called 'validation tables', 'type tables', or 'check tables'.  Common examples include status of an order, states in a country, etc.

Splitting the table out is a big win for data integrity, but a big loss for performance, as you have to go to the database every time you need to find a value.  This class eases that pain by caching all values from the table at the first query, and the always responding from the cache.

=head1 TRADEOFFS

First, you're trading space for time.  This module will use a lot of memory if there are a lot of rows in the table, but it's a lot faster than running to the database each time.

Don't use this module for tables with "a lot" of rows.  "A lot" will vary, but you certainly shouldn't use it for thousands of rows.

We also trade speed for latency.  If the table changes, the cache will never know about it.  If you're using this in mod_perl, that means you shouold restart the webserver after making a change to a type table.

Static-derived classes cannot themselves participate in relationships, though other TB classes may refer to them in relationships.

Search, fetch_deep, and search_deep are not supported, but fetch and fetch_by_FIELD are.

=head1 IN-MEMORY INDEXES

If you providing the 'index' argument to build_class, Static will build in-memory indexes of the object list, by the field you specify.

You always get an index by primary key.

If more than one object has the same value for an indexed field, it is indetermined which object will be returned.  You've been warned.

=head1 NOT PERMITTED METHODS

=over

=item has_one, has_many, has_many_many

Relationships are not supported.

=item new

It doesn't make sense to make new rows for the type table.

=item search, search_by_FIELD

As most fetches are performed from memory, there is no way to handle the WHERE clause.

=item fetch_with_FIELD, fetch_deep

As static classes can't have relations, these methods are not supported.

=back

=cut

sub build_class {
    my $class = shift;
    my %args = @_;
    @args{qw(updatable insertable deletable)} = (0,0,0);
    $args{refresh_fields} = [];
    if ($DEBUG > 1) {
        print STDERR __PACKAGE__ . ':' . __LINE__ . " - In Static build_class:\nClass: $class\nArgs:" . Dumper(\%args); 
    }

    my $order = $args{order} || '';
    delete $args{order};

    my @indexes = @{$args{index} ||$args{indexes} || []};
    delete $args{index};
    delete $args{indexes};

    $class->SUPER::build_class(%args);

    # Ensure the indexes list has primary keys on it
    my %indexes = map { $_ => 1 } (@indexes, $class->primary_key_fields);
    @indexes = keys %indexes;

    $STATIC_METADATA{$class} = {
                                order => $order,
                                indexes => \@indexes,
                               };
}

sub is_static { return 1; } # duh

# Hmmm....
sub new {
    my $class = shift;
    my $calling_package = (caller())[0];
    #print STDERR "Have class $class and calling package $calling_package\n";
    unless ($calling_package =~ /Class::ReluctantORM::/) {
        Class::ReluctantORM::Exception::Call::NotPermitted->croak('May only call Static::new() from within Class::ReluctantORM');
    }
    return $class->SUPER::new(@_);
}


sub has_one { Class::ReluctantORM::Exception::Call::NotPermitted->croak('has_one() not permitted on a Static CRO subclass'); }
sub has_many { Class::ReluctantORM::Exception::Call::NotPermitted->croak('has_many() not permitted on a Static CRO subclass'); }
sub has_many_many { Class::ReluctantORM::Exception::Call::NotPermitted->croak('has_many_many() not permitted on a Static CRO subclass'); }


# Override TB's version with our own
sub fetch {
    my $class = shift;
    unless (@_) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'primary key value'); }
    my @pks = @_;
    $class->_static_fetch_all();

    my $pk = join '_', map { defined($_) ? $_ : 'NULL' } @pks;

    my $obj = $INDEXES{$class}{$class->__pk_composite_name}{$pk};

    unless ($obj) { Class::ReluctantORM::Exception::Data::NotFound->croak(criteria => $pk); }
    return $obj;
}

sub __pk_composite_name {
    my $class = shift;
    return join '_', $class->primary_key_fields();
}

sub fetch_all {
    my $class = shift;
    $class->_static_fetch_all();
    return wantarray ? @{$OBJECTS{$class}} : $OBJECTS{$class};
}

sub _static_fetch_all {
    my $class = shift;
    if (exists $OBJECTS{$class}) { return; }

    # Init indexes
    my @indexes = @{$STATIC_METADATA{$class}{indexes}};
    $INDEXES{$class} = { map { $_ => {}} @indexes };

    my @results = $class->SUPER::search(where => Where->new(), @_);
    foreach my $obj (@results) {
        for my $i (@indexes) {
            my $idx = $INDEXES{$class}{$i};
            my $val = $obj->$i();
            if (!exists $idx->{$val}) {
                $idx->{$val} = $obj;
            } elsif (ref($idx->{$val}) eq 'ARRAY') {
                push @{$idx->{$val}}, $obj;
            } else {
                $idx->{$val} = [$idx->{$val}, $obj];
            }
        }
    }

    $OBJECTS{$class} = \@results;
}


sub _make_fetcher {
    my ($class, $field) = @_;
    my $code = sub {
        my $class2 = shift;
        my $value = shift;
        unless (defined $value) { Class::ReluctantORM::Exception::Param::Missing->croak(param => $field . ' value'); }

        $class->_static_fetch_all();

        # Indexed?
        my @results;
        if (grep { $_ eq $field } @{$STATIC_METADATA{$class}{indexes}}) {
            my $entry = $INDEXES{$class}{$field}{$value};
            if (ref($entry) eq 'ARRAY') {
                @results = @$entry;
            } else {
                @results = ($entry);
            }
        } else {
            @results = grep { $_->$field eq $value } $class->fetch_all();
        }

        unless (@results) {
            Class::ReluctantORM::Exception::Data::NotFound->croak(criteria => $value);
        }

        return wantarray ? @results : $results[0];
    };
    return $code;
}

=head2 %hash = $class->fetch_all_hash_by_id();

=head2 $hashref = $class->fetch_all_hash_by_id();

Performs a fetch_all (which is likely already cached) and returns a hash or hashref, keyed on the primary key.

=cut

sub fetch_all_hash_by_id {
    my $class = shift;
    my %hash = map { $_->id => $_ } $class->fetch_all();
    return wantarray ? %hash : \%hash;
}

=head2 %hash = $class->fetch_all_hash_by_name();

=head2 $hashref = $class->fetch_all_hash_by_name();

Performs a fetch_all (which is likely already cached) and returns a hash or hashref, keyed on the 'name' field.

Most Static classes have a 'name' field; if yours doesn't this will blow up.

=cut

sub fetch_all_hash_by_name {
    my $class = shift;
    my %hash = map { $_->name => $_ } $class->fetch_all();
    return wantarray ? %hash : \%hash;
}


=head1 AUTHOR

Clinton Wolfe

=cut

1;
