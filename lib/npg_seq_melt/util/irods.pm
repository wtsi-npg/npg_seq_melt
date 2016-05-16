package npg_seq_melt::util::irods;

use Moose::Role;
use Carp;
use WTSI::NPG::iRODS;
use MooseX::StrictConstructor;

our $VERSION = '0';

=head1 NAME

npg_seq_melt::util::irods

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS



=head2 irods

WTSI::NPG::iRODS iRODS connection handle

=cut

has 'irods' =>
    (isa => 'WTSI::NPG::iRODS',
     is => 'rw',
     documentation => 'An iRODS connection handle',
     clearer       => 'clear_irods',
     predicate     => 'has_irods',
     writer        => 'set_irods',
);

=head2 get_irods
=cut

sub get_irods {
    my $self   = shift;
    return WTSI::NPG::iRODS->new();
}

=head2 irods_root

iRODS root directory

=cut

has 'irods_root' =>
    (isa => q[Str],
     is => q[ro],
     default => q[/seq],
     documentation => q[iRODS root directory - defaults to /seq],
);


=head2 get_irods_hostname

=cut

sub get_irods_hostname{
    my $self          = shift;
    my $irods_object  = shift; #/irods_root/id_run/rpt.cram
    my $index         = shift; #0 or 1

    my @replicates = $self->irods->replicates($irods_object);
    my $hostname   = q[//].$replicates[$index]{'location'} . q[.internal.sanger.ac.uk];
    return($hostname);
}


1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item MooseX::StrictConstructor

=item Carp

=item WTSI::NPG::iRODS

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015 Genome Research Limited

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
