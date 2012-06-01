package Class::ReluctantORM::SQL::OrderBy;

=head1 NAME

Class::ReluctantORM::SQL::OrderBy - Represent an ORDER BY clause in a SQL statement

=head1 SYNOPSIS

  my $ob = Class::ReluctantORM::SQL::OrderBy->new();
  $ob->add($col);
  $ob->add($col, 'DESC');
  @cols = $ob->columns();

=head1 METHODS

=cut

use strict;
use warnings;

#use base 'Class::Accessor';
#__PACKAGE__->mk_accessors(qw(table alias column));

=head2 new()

Constructor.  No arguments.

=cut

sub new {
    my $class = shift;
    return bless { cols => [] }, $class;
}


=head2 @cols = $ob->columns();

Lists the Class::ReluctantORM::SQL::Columns in the order by clause, in order of occurence.  No sort direction is provided.

=cut

sub columns {
    my $self = shift;
    return map {$_->[0]} $self->columns_with_directions;
}

=head2 @tables = $ob->tables();

Returns a list of (non-unique) tables referenced in the clause.

=cut

sub tables {
    my $self = shift;
    return map { $_->table() } $self->columns;
}

=head2 @col_pairs = $ob->columns_with_directions();

Returns an array of two-element arrays.  In each subarry, the first element is the Class::ReluctantORM::SQL::Column, and the second is the sort direction (either 'ASC' or 'DESC').

=cut

sub columns_with_directions {
    my $self = shift;
    return @{$self->{cols}};
}

=head2 $ob->add($col);

=head2 $ob->add($col, $direction);

Adds a sort criteria to the clause.  $col is a Class::ReluctantORM::SQL::Column.  $direction is either of the strings 'ASC' or 'DESC', default 'ASC'.

=cut

sub add {
    my $self = shift;
    my $col = shift;
    unless ($col) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'column'); }
    unless (ref($col) && $col->isa('Class::ReluctantORM::SQL::Column')) {
        Class::ReluctantORM::Exception::Param::WrongType->croak(
                                                   param => 'column', 
                                                   expected => 'Class::ReluctantORM::SQL::Column',
                                                   value => $col,
                                                  );
    }
    my $dir = shift || 'ASC';
    $dir = uc($dir);
    my %acceptable = map {$_ => 1} qw(ASC DESC);
    unless (exists $acceptable{$dir}) {
        Class::ReluctantORM::Exception::Param::BadValue->(
                                             error => 'Driection must be one of ' . (join ',', keys %acceptable),
                                             param => 'direction',
                                             value => $dir,
                                            );
    }

    push @{$self->{cols}}, [$col, $dir];

}

=head2 $str = $ob->pretty_print();

Outputs the clause as a human-readable, driver-neutral string.  Useless for SQL execution.

=cut

sub pretty_print {
    my $self = shift;
    my %args = @_;
    my $prefix = $args{prefix} || '';
    my $str = $prefix . "ORDER BY:\n";
    foreach my $cd ($self->columns_with_directions) {
        $str .= $prefix . '  ';
        $str .= $cd->[0]->pretty_print(one_line => 1);
        $str .= ' ';
        $str .= $cd->[1];
        $str .= "\n";
    }
    return $str;
}

=head2 $clone = $ob->clone();

Deeply clones each sort expression, and copies each direction.

=cut

sub clone {
    my $self = shift;
    my $class = ref $self;
    my $other = $class->new();

    foreach my $sort_term ($self->columns_with_directions) {
        $other->add(
                    $sort_term->[0]->clone(),
                    $sort_term->[1],
                   );
    }

    return $other;

}


1;
