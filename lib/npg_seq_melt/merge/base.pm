package npg_seq_melt::merge::base;

use Moose;
use MooseX::StrictConstructor;
use Cwd;


our $VERSION  = '0';

=head1 NAME

npg_seq_melt::merge::base

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 rpt_list

Semi-colon separated list of run:position or run:position:tag for the same sample
that define a composition for this merge. An optional attribute.

=cut

has 'rpt_list' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
);

with 'npg_tracking::glossary::composition::factory::rpt' =>
     { 'component_class' =>
       'npg_tracking::glossary::composition::component::illumina' };

=head2 composition

npg_tracking::glossary::composition object corresponding to rpt_list

=cut

has 'composition' => (
     isa           => q[npg_tracking::glossary::composition],
     is            => q[ro],
     lazy_build    => 1,
);
sub _build_composition {
  my $self = shift;
  my $composition =  $self->create_composition();
  $composition->sort();
  return $composition;
}

=head2 merge_dir

Directory where merging takes place

=cut

has 'merge_dir' => (
        is              => 'ro',
        isa             => 'Str',
        lazy_build      => 1,
);
sub _build_merge_dir{
  my($self) = shift;
  return join q[/],$self->run_dir(),$self->composition->digest();
}

=head2 run_dir

=cut

has 'run_dir'  => (
    isa           => q[Str],
    is            => q[ro],
    default       => cwd(),
    documentation => q[Parent directory where sub-directory for merging is created, default is cwd ],
    );

__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item MooseX::Getopt

=item npg_tracking::glossary::composition::factory::rpt

=item npg_tracking::glossary::composition::component::illumina

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
