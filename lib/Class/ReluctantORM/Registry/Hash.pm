package Class::ReluctantORM::Registry::Hash;
use strict;
use warnings;
use base 'Class::ReluctantORM::Registry';
use Scalar::Util qw(weaken);

=head1 NAME

  Class::ReluctantORM::Registry::Hash - Use Hash to store Weak refs

=head1 DESCRIPTION

Uses a simple hash structure to store weak references to objects.

=head1 AUTHOR

 Clinton Wolfe clwolfe@cpan.org January 2010

=cut

sub new {
    my $reg_class = shift;
    my $tgt_class = shift;
    my $self = $reg_class->SUPER::new($tgt_class);
    $self->{_hash_by_id} = {};
    return $self;
}

sub _hash_by_id {
    return shift->{_hash_by_id};
}

sub fetch {
    my $reg = shift;
    my $id  = shift;
    my $obj = exists($reg->_hash_by_id->{$id}) ? $reg->_hash_by_id->{$id} : undef;
    return $obj;
}

sub store {
    my $reg = shift;
    my $obj = shift;
    unless ($obj->has_all_primary_keys_defined()) {
        return;
    }

    my $id = $obj->id();
    my $hash_by_id = $reg->_hash_by_id();
    $hash_by_id->{$id} = $obj;
    weaken($hash_by_id->{$id});

    return $obj;
}

sub purge {
    my $reg = shift;
    my $thing  = shift;
    my $id;
    if (ref($thing) && $thing->isa($reg->target_class)) {
        $id = $thing->id();
    } else {
        $id = $thing;
    }

    return unless defined($id);
    delete($reg->_hash_by_id->{$id});
}

sub purge_all {
    my $reg = shift;
    $reg->{_hash_by_id} = {};
    return 1;
}

sub count {
    my $reg = shift;
    my $total = 0;
    $reg->walk(sub { $total++; });
    return $total;
}

sub walk {
    my $reg = shift;
    my $code = shift;
    while (my ($id, $obj) = each %{$reg->_hash_by_id}) {
        $code->($obj) if defined($obj);
    }
}

1;
