package npg_seq_melt::merge::qc;

use Moose::Role;
use Carp;

requires qw/ composition
             merge_dir
             run_cmd
             log
             sample_merged_name/;

our $VERSION  = '0';



=head1 NAME

npg_seq_melt::merge::qc

=head1 SYNOPSIS

=head1 DESCRIPTION

Generate JSON files in the qc sub-directory

=head1 SUBROUTINES/METHODS

=head2 merged_qc_dir

=cut
has 'merged_qc_dir' => (isa           => q[Str],
                        is            => q[ro],
                        lazy_build    => 1,
                        documentation => q[JSON file directory],
                       );
sub _build_merged_qc_dir {
    my $self = shift;
    return $self->merge_dir.q[/outdata/qc/];
}

=head2 make_bam_flagstats_json

qc script is used to parse the markdups_metrics and flagstat file creating a JSON file of the combined results

=cut

sub make_bam_flagstats_json {
    my $self = shift;

    my $args = {};
    $args->{'check'}           = q[bam_flagstats];
    $args->{'file_type'}       = q[cram];
    $args->{'filename_root'}   = $self->sample_merged_name;
    $args->{'qc_in'}           = $self->merge_dir.q[/outdata/];
    $args->{'qc_out'}          = $self->merged_qc_dir;
    $args->{'rpt_list'}        = q['] . $self->composition->freeze2rpt . q['];
    # Not adding subset, assuming we are merging target files.
    my $command = q[];
    foreach my $arg ( sort keys %{$args} ) {
      $command .= q[ --] . $arg . q[ ] . $args->{$arg};
    }
    $command = 'qc ' . $command;
    if (!$self->run_cmd($command)) {
      croak 'QC script exited';
    }

    return;
}

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item Carp

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016 Genome Research Limited

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
