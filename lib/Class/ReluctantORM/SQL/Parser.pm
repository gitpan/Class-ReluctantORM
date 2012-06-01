package Class::ReluctantORM::SQL::Parser;
use strict;
use warnings;

use base 'SQL::Parser';

use Scalar::Util qw(blessed);

use Class::ReluctantORM::Utilities qw(install_method);

use Class::ReluctantORM::SQL::Aliases;

use SQL::Statement::TermFactory;

our $DEBUG ||= 0;

sub new {
    my $class = shift;
    my $self = $class->SUPER::new(@_);

    $self->{RaiseError} = 1;
    $self->{PrintError} = 0;
    $self->feature('reserved_words', 'TRUE', 0);
    $self->feature('reserved_words', 'FALSE', 0);
    $self->feature('valid_data_types', 'BOOLEAN', 1);

    return $self;
}

sub LITERAL {
    my ( $self, $str ) = @_;
    return 'BOOLEAN' if ($str =~ m/^(TRUE|FALSE)$/);
    return $self->SUPER::LITERAL($str);
}


#==========================================================#
#                 parse_where Support
#==========================================================#



our %SS_LITERAL_TYPES = map { uc($_) => 1 } qw(string number null boolean);

sub __build_crit_from_parse_tree {
    my $parser = shift;
    my $stmt = shift;
    my $cro_where = shift;
    $parser->{__where_under_construction} = $cro_where;
    my $ss_where = $stmt->{where_clause}; # ENCAPSULATION VIOLATION into SQL::Parser v1.15
    if ($DEBUG > 2) { print STDERR __PACKAGE__ . ':' . __LINE__ . "Have SQL::Statement parse tree as:\n" . Dumper($ss_where); }
    return $parser->__bcfpt_recursor($ss_where);
}

sub __bcfpt_recursor {
    my $parser = shift;
    my $ss_node = shift;

    my $is_ref        = ref($ss_node);
    my $is_hash       = $is_ref && (ref($ss_node) eq 'HASH');
    my $is_operation  = ($is_hash && exists $ss_node->{op});
    my $is_param      = $is_hash && $ss_node->{type} && $ss_node->{type} eq 'placeholder';
    my $is_column     = $is_hash && $ss_node->{type} && $ss_node->{type} eq 'column';
    my $is_function   = $is_ref && blessed($ss_node) && $ss_node->isa('SQL::Statement::Util::Function');
    my $is_literal    = $is_hash && exists($SS_LITERAL_TYPES{uc($ss_node->{type} || '')});
    my $is_null       = $is_literal && $ss_node->{type} eq 'null';

    # Handle negations as a proper operation
    if ($is_hash && $ss_node->{neg}) {
        return Criterion->new('NOT', $parser->__bcfpt_recursor({%$ss_node, neg => 0}));
    }

    if ($is_operation) {
        my @args = map { $parser->__bcfpt_recursor($ss_node->{$_}) } grep { /^arg/ } sort keys %$ss_node;
        return Criterion->new($ss_node->{op}, @args);
    } elsif ($is_function) {
        my @args = map { $parser->__bcfpt_recursor($_) } @{$ss_node->args};
        my $func_name = $ss_node->{name};
        Class::ReluctantORM::Exception::NotImplemented->throw('SQL functions not yet supported');
    } elsif ($is_param) {
        return Param->new();
    } elsif ($is_column) {
        return $parser->__bcfpt_boost_to_column($ss_node);
    } elsif ($is_literal) {
        return Literal->new(($is_null ? undef : $ss_node->{value}), uc($ss_node->{type}));
    } else {
        Class::ReluctantORM::Exception::Param::BadValue->croak(error => __PACKAGE__ . '::__boost cannot handle this arg: ' . ref($ss_node));
    }
}

sub __bcfpt_boost_to_column {
    my $parser = shift;
    my $ss_node = shift;

    my ($table_name, $col_name) = split /\./, $ss_node->{value};  # DRIVER DEPENDENCY - table/column name separator
    if (!$col_name) { ($col_name, $table_name) = ($table_name, $col_name);  }
    my ($col, $table);
    if ($table_name) {
        $table = $parser->{__where_under_construction}->find_table($table_name);
        unless ($table) {
            # Must not be a previously referenced table
            $table = Table->new(table => $table_name);
        }
    }
    $col = Column->new(
                       table => $table,
                       column => lc(__trim_quotes($col_name)),
                      );
    return $col;

}

sub __trim_quotes {
    my $t = shift;
    $t =~ tr/'"[]//d;
    return $t;
}




1;
