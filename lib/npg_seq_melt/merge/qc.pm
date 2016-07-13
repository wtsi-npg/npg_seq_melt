package npg_seq_melt::merge::qc;

use Moose::Role;
use Carp;
use English qw(-no_match_vars);
use npg_qc::autoqc::results::bam_flagstats;

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

npg_qc::autoqc::results::bam_flagstats is used to parse the markdups_metrics and flagstat file creating a JSON file of the combined results

=cut

sub make_bam_flagstats_json {
    my $self = shift;

    my $file_prefix = $self->merge_dir.q[/outdata/].$self->sample_merged_name;

    $self->log('Writing temporary file');
    # We do not need the content of the cram file!
    my $empty_cram = $file_prefix.q[.cram];
    $self->run_cmd(qq[touch $empty_cram]);
    my $markdup_file  = $file_prefix.q[.markdups_metrics.txt];
    my $flagstat_file = $file_prefix.q[.flagstat];

    my $r = npg_qc::autoqc::results::bam_flagstats->new(
      markdups_metrics_file  => $markdup_file,
      flagstats_metrics_file => $flagstat_file,
      sequence_file          => $empty_cram,
      composition            => $self->composition()
                                                        );
    $r->execute();
    $r->store($self->merged_qc_dir);

    my $success = unlink $empty_cram;
    my $e = $OS_ERROR;
    $success or carp "Failed to remove $empty_cram: $e";

    return $success;
}

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item Carp

=item English

=item use npg_qc::autoqc::results::bam_flagstats

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
