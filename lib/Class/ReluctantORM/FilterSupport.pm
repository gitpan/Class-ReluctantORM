package Class::ReluctantORM::FilterSupport;
use strict;
use warnings;
use Class::ReluctantORM::Exception;


=head1 NAME

Class::ReluctantORM::FilterSupport - Mix-In to TB to provide hooks for filters

=head1 SYNOPSIS

  # No user-servicable parts.

=head1 Description

Contains the guts of the Filter mechanism. For more info, see L<Class::ReluctantORM::Filter> .

=head1 AUTHOR

Clinton Wolfe clinton@omniti.com

=cut

1;

package Class::ReluctantORM;
use strict;
use warnings;
use Class::ReluctantORM::Utilities qw(conditional_load);

=for devdocs

=head2 $obj->attach_filter()

Bad method name, add an alias.

=cut

sub attach_filter {
    my $inv = shift;
    if (ref($inv)) {
        $inv->append_filter(@_);
    } else {
        $inv->attach_class_filter(@_);
    }
}

sub attach_class_filter {
    my $class = shift;
    if (ref $class) { $class = ref $class; }
    my ($filter_class, @fields) = $class->__read_attach_filter_params(@_);

    my $metadata = $class->__metadata();
    $metadata->{filters} ||= {};
    foreach my $field (@fields) {
        $metadata->{filters}{$field} ||= [];
        push @{$metadata->{filters}{$field}}, $filter_class;
    }
    return $filter_class;
}

sub __read_attach_filter_params {
    my $inv = shift;
    my $class = (ref $inv) ? ref($inv) : $inv;
    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;

    unless ($args{class}) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'class'); }
    my $filter_class = $args{class};
    delete $args{class};
    conditional_load($filter_class);

    my @fields = @{$args{fields} || []};
    delete $args{fields};
    unless (@fields) {
        # Default to all fields, excluding primary keys
        @fields = grep { !$inv->is_field_primary_key($_) } $class->field_names_including_relations();
    }

    if (keys %args) {  Class::ReluctantORM::Exception::Param::Spurious->croak(param => (join(',',keys %args))); }

    return ($filter_class, @fields);

}

sub append_filter {
    my $self = shift;
    $self->__copy_filter_list_on_write();
    my ($filter_class, @fields) = $self->__read_attach_filter_params(@_);
    my $filters = $self->get('object_filters');

    foreach my $field (@fields) {
        $filters->{$field} ||= [];
        push @{$filters->{$field}}, $filter_class;
    }
    $self->set('object_filters', $filters);

    return $filter_class;
}

sub set_filters {
    my $self = shift;

    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;
    $args{fields} ||= [];
    unless ($args{classes}) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'classes'); }
    unless (ref($args{classes}) eq 'ARRAY') { Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak(param => 'classes'); }
    unless (ref($args{fields}) eq 'ARRAY') { Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak(param => 'fields'); }
    my @classes = @{$args{classes}};
    my @fields = @{$args{fields}};
    delete @args{qw(classes fields)};
    if (keys %args) {  Class::ReluctantORM::Exception::Param::Spurious->croak(param => (join(',',keys %args))); }

    $self->__copy_filter_list_on_write();

    $self->clear_filters(fields => \@fields);
    foreach my $filter_class (@classes) {
        $self->append_filter(class => $filter_class, fields => \@fields);
    }
}

sub clear_filters {
    my $self = shift;
    my $class = ref $self;

    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;

    # Default to all fields, including primary key
    $args{fields} ||= [$self->field_names_including_relations()];
    unless (ref($args{fields}) eq 'ARRAY') { Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak(param => 'fields'); }
    my @fields = @{$args{fields}};
    delete $args{fields};
    foreach my $field (@fields) {
        unless (grep {$_ eq $field} $class->field_names_including_relations()) {
            Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'field', value => $field, error => "'$field' is not a field of $class");
        }
    }

    if (keys %args) {  Class::ReluctantORM::Exception::Param::Spurious->croak(param => (join(',',keys %args))); }

    $self->__copy_filter_list_on_write();
    my $filters = $self->get('object_filters');
    foreach my $field (@fields) {
        $filters->{$field} = [];
    }
    $self->set('object_filters', $filters);
}


sub remove_filter {
    my $self = shift;
    my $class = ref $self;

    if (@_ % 2) { Class::ReluctantORM::Exception::Param::ExpectedHash->croak(); }
    my %args = @_;

    unless ($args{class}) { Class::ReluctantORM::Exception::Param::Missing->croak(param => 'class'); }

    # Default to all fields, including primary key
    $args{fields} ||= [$self->field_names_including_relations()];
    unless (ref($args{fields}) eq 'ARRAY') { Class::ReluctantORM::Exception::Param::ExpectedArrayRef->croak(param => 'fields'); }

    my $filter_class = $args{class};
    my @fields = @{$args{fields}};
    delete @args{qw(class fields)};

    foreach my $field (@fields) {
        unless (grep {$_ eq $field} $class->field_names_including_relations()) {
            Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'field', value => $field, error => "'$field' is not a field of $class");
        }
    }

    if (keys %args) {  Class::ReluctantORM::Exception::Param::Spurious->croak(param => (join(',',keys %args))); }

    $self->__copy_filter_list_on_write();
    my $filters = $self->get('object_filters');
    foreach my $field (@fields) {
        $filters->{$field} = [grep { $_ ne $filter_class } @{ $filters->{$field} || []}];
    }
    $self->set('object_filters', $filters);
}

sub read_filters_on_field {
    my $inv = shift;
    my $field = shift;
    my $class = ref($inv) ? ref($inv) : $inv;

    unless ($field) {
        Class::ReluctantORM::Exception::Param::Missing->croak(param => 'field');
    }
    unless (grep {$_ eq $field} $class->field_names_including_relations()) {
        Class::ReluctantORM::Exception::Param::BadValue->croak(param => 'field', value => $field, error => "'$field' is not a field of $class");
    }

    my $class_meta = $class->__metadata();
    my $class_filter_metadata = $class_meta->{filters};
    my $object_filter_metadata = (ref($inv) ? $inv->get('object_filters') : {}) || undef;

    if (defined $object_filter_metadata) {
        return @{$object_filter_metadata->{$field} || []};
    } else {
        return @{$class_filter_metadata->{$field} || []};
    }
}

sub write_filters_on_field {
    my $inv = shift;

    # Note: we currently do not distinguish between write and read filters
    # (the distinction comes about in the implementation of the filter,
    # whether they implement apply_read_filter and/or apply_write_filter
    # Thus the list is the same as for read_filters_on_field, EXCEPT that 
    # the order is reversed.

    return reverse $inv->read_filters_on_field(@_);

}


sub __apply_field_read_filters {
    my $self = shift;
    my $field = shift;
    my $raw_value = shift || $self->raw_field_value($field);

    my $value = $raw_value;
    foreach my $filter ($self->read_filters_on_field($field)) {
        $value = $filter->apply_read_filter($value, $self, $field);
    }

    return $value;
}

sub __apply_field_write_filters {
    my $self = shift;
    my $field = shift;
    my $new_value = shift;

    my $value = $new_value;
    foreach my $filter ($self->write_filters_on_field($field)) {
        $value = $filter->apply_write_filter($value, $self, $field);
    }

    return $value;
}

sub __copy_filter_list_on_write {
    my $self = shift;
    my $object_filter_list = $self->get('object_filters');
    return if $object_filter_list; # Already copied

    # Start with empty object list
    $object_filter_list = {};

    my $class = ref $self;
    my $class_filters = $class->__metadata()->{filters} || {};

    # Deep copy
    foreach my $field (keys %{$class_filters}) {
        $object_filter_list->{$field} = [ @{$class_filters->{$field} || []} ];
    }

    $self->set('object_filters', $object_filter_list);
    # Self now has its own private copy of the class filter list

}

# Fetch-deep support

#    my $filter_info = $class->_extract_deep_filter_args(\%args);
sub _extract_deep_filter_args {
    my $class = shift;
    my $arg_ref = shift;
    my @filter_options = qw(append_filter remove_filter clear_filters set_filters);
    my $info;
    foreach my $option (@filter_options) {
        next unless exists($arg_ref->{$option});
        if ($info) {
            Class::ReluctantORM::Exception::Param::MutuallyExclusive->croak(param => "$option, " . $info->{method});
        }
        $info = { method => $option, args => $arg_ref->{$option} };
        delete $arg_ref->{$option};
    }
    return $info;
}


sub _apply_deep_filter_args {
    my $class = shift;
    my $info = shift;
    my $objects_ref = shift;

    return unless ($info);
    my $method = $info->{method};
    foreach my $obj (@$objects_ref) {
        $obj->$method(%{$info->{args}});
    }
}

1;
