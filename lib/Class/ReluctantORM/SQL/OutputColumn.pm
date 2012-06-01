package Class::ReluctantORM::SQL::OutputColumn;

=head1 NAME

Class::ReluctantORM::SQL::OutputColumn - Represent an Output Column from a SQL statment

=head1 SYNOPSIS

  use Class::ReluctantORM::SQL::Aliases;

  # You get OutputColumns back from statements:
  my @ocs = $sql->output_columns();

  # You can make them implicitly
  my $oc = $sql->add_output(Column->new(column => 'foo'));
  my $oc = $sql->add_output($expression);

  # Or explcitly
  my $oc = OutputColumn->new($column);
  my $oc = OutputColumn->new($expression);
  $sql->add_output($oc);

  # Set/read column alias
  my $alias = $oc->alias();
  $oc->alias($new_alias);

  # Set/read PK flag (needed by some drivers)
  $oc->is_primary_key(1);

  # Get expression - the "payload" of the output
  # (usually just a Column)
  my $exp = $oc->expression();

  # TODO DOCS - aggregate support

  # Fetching results
  my $result = $oc->output_value();

  # Used in driver code when reading from a fetch
  $oc->output_value($value);

=head1 DESCRIPTION

Represents an output "column" in a SELECT SQL statement (or an UPDATE or INSERT when used with RETURNING).  Contains the column or expression that is the source of the data, any column alias, and provides access to the column value.

=cut

use strict;
use warnings;

use Data::Dumper;
use Class::ReluctantORM::Exception;
use Class::ReluctantORM::Utilities qw(install_method check_args);

use Class::ReluctantORM::SQL::Aliases;
use base 'Class::Accessor::Fast';

our $DEBUG = 0;

=head1 CONSTRUCTORS

=cut


=head2 $col = OutputColumn->new($column);

=head2 $col = OutputColumn->new($expression);

=head2 $col = OutputColumn->new(expression => $expression, alias => $alias, is_primary_key => 0, ...);

Makes a new OutputColumn object.

In the first form, creates an OutputColumn sourced on the given Column.

In the first form, creates an OutputColumn sourced on the given Expression.  Since a Column is a subclass of Expression, forms one and two are actually the same.

In the third form, the expression, column alias, and primary key flag are provided explicitly.  Alias and primary key flag are optional.

In the first and second forms, a column alias will be generated at render time if one is not assigned before then.

=cut

sub new {
    my $class = shift;
    my %args;
    if (@_ == 1) {
        $args{expression}  = shift;
    } elsif (@_ % 2) {
        Class::ReluctantORM::Exception::Param::ExpectedHash->croak();
    } else {
        %args = @_; 
    }
    %args = check_args(args => [ %args ], required => [qw(expression)], optional => [qw(alias is_primary_key)]);

    my $self = bless {}, $class;
    $self->expression($args{expression});
    $self->alias($args{alias});
    $self->is_primary_key($args{is_primary_key} || 0);

    # Start with a NULL output
    $self->set('output_value', undef);

    return $self;
}


=head1 ACCESSORS AND MUTATORS

=cut

=head2 $col_alias_name = $oc->alias();

=head2 $oc->alias($col_alias_name);

Reads or sets the column alias.

=cut

__PACKAGE__->mk_accessors(qw(alias));

=head2 $exp = $oc->expression();

=head2 $oc->expression($expression);

Reads or sets the Expression that acts as the source for the data.  $expression is commonly a simple Column.

=cut

__PACKAGE__->mk_accessors(qw(expression));


=head2 $value = $oc->output_value();

=head2 $oc->output_value($value);

Reads or sets the output value of the column.  An undef should interpreted as NULL.

=cut

__PACKAGE__->mk_accessors(qw(output_value));

=head2 $bool = $oc->is_primary_key();

=head2 $oc->is_primary_key($bool);

Reads or sets the primary key flag for the output column.  Set to true if the column is a member of the primary key on the base table.  Some drivers - those that don't support RETURNING clauses - require this to determine whether to do a second fetch to populate primary kyes ine memory.

=cut

__PACKAGE__->mk_accessors(qw(is_primary_key));


=head2 $str = $oc->pretty_print();

Renders a human-readable representation of the OutputColumn.

=cut

sub pretty_print {
    my $self = shift;
    my %args = @_;
    if ($args{one_line}) {
        if ($self->alias) { return $self->alias; }
        return  $self->expression->pretty_print(%args);
    } else {
        return ($args{prefix} || '' ) . 'OUTPUT ' . $self->pretty_print(one_line => 1) . "\n";
    }
}

=head2 $clone = $oc->clone()

Copies the output column, by deeply cloning the expression, and then directly copying the alias, is_primary_key flag, and output_value, if any.

=cut

sub clone {
    my $self = shift;
    my $class = ref $self;
    my $other = $class->new(
                            alias => $self->alias,
                            expression => $self->expression->clone(),
                            is_primary_key => $self->is_primary_key(),
                           );
    $other->output_value($self->output_value());
    return $other;

}



=head1 AUTHOR

Clinton Wolfe

=cut

1;
