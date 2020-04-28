package npg_seq_melt::merge::qc;

use Moose::Role;


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

    # temporary fix for input file check 
    $args->{'input_files'}     = $self->merge_dir.q[/outdata/];

    # Not adding subset, assuming we are merging target files.
    my $command = q[];
    foreach my $arg ( sort keys %{$args} ) {
      $command .= q[ --] . $arg . q[ ] . $args->{$arg};
    }
    return $self->run_cmd('qc' . $command);
}

=head2 make_verify_bam_id

=cut

sub make_verify_bam_id { #TODO waiting for verify_bam_id 2 which uses a cram as input
    my $self = shift;

    my $args = {};
    $args->{'check'}           = q[verify_bam_id];
    $args->{'filename_root'}   = $self->sample_merged_name;
    $args->{'qc_out'}          = $self->merged_qc_dir;
    $args->{'rpt_list'}        = q['] . $self->composition->freeze2rpt . q['];
    $args->{'input_files'}     = $self->merge_dir.q[/outdata/].$self->sample_merged_name.q[.bam];
###NPG_CACHED_SAMPLESHEET_FILE $self->merge_dir.q[/outdata/].$self->composition->digest(). q[.csv];

    my $command = q[];
    foreach my $arg ( sort keys %{$args} ) {
      $command .= q[ --] . $arg . q[ ] . $args->{$arg};
    }
    #return $self->run_cmd('qc' . $command);
    return 1;
}

=head2 make_samtools_stats_targets

F0xF04_target.samtools_stats

=cut

sub make_samtools_stats_targets{
    my $self = shift;
    my $args = {};
    $args->{'ref-seq'}         = $self->reference_genome_path;
    $args->{'reference'}       = $self->reference_genome_path;
    $args->{'cov-threshold'}   = q[15];
    $args->{'filtering-flag'}  = q[0xF04];
    my $rpt_list               = q['] . $self->composition->freeze2rpt . q['];
    my $targets                = $self->target_regions_dir;
    $args->{'target-regions'}  = $targets;
    my $merge_cram             = $self->merge_dir.q[/outdata/].$self->sample_merged_name.q[.cram];
    my $merge_target_stats     = $self->merge_dir.q[/outdata/].$self->sample_merged_name.q[_F0xF04_target.stats];

     my $command = q[];
    foreach my $arg ( sort keys %{$args} ) {
      $command .= q[ --] . $arg . q[ ] . $args->{$arg};
    }
    $command .= qq[ -p $merge_cram > $merge_target_stats ];

    return $self->run_cmd( $self->samtools_executable . q[ stats ] . $command );
}

=head2 make_samtools_stats_target_autosome

F0xF04_target_autosome.samtools_stats

=cut

sub make_samtools_stats_target_autosome{
    my $self = shift;
    my $args = {};
    $args->{'ref-seq'}         = $self->reference_genome_path;
    $args->{'reference'}       = $self->reference_genome_path;
    $args->{'cov-threshold'}   = q[15];
    $args->{'filtering-flag'}  = q[0xF04];
    my $rpt_list               = q['] . $self->composition->freeze2rpt . q['];
    my $targets                = $self->target_autosome_regions_dir;
    $args->{'target-regions'}  = $targets;
    my $merge_cram             = $self->merge_dir.q[/outdata/].$self->sample_merged_name.q[.cram];
    my $merge_target_autosome_stats     = $self->merge_dir.q[/outdata/].$self->sample_merged_name.q[_F0xF04_target_autosome.stats];

     my $command = q[];
    foreach my $arg ( sort keys %{$args} ) {
      $command .= q[ --] . $arg . q[ ] . $args->{$arg};
    }
    $command .= qq[ -p $merge_cram > $merge_target_autosome_stats ];

    return $self->run_cmd( $self->samtools_executable . q[ stats ] . $command );
}

=head2 make_bcf_stats

Runs bcftools stats --collapse_snps --apply-filters PASS --samples expected_sample_name geno_refset_bcfdb_p
ath temp_bcf (filename_root.bcf)

my $bcf_stats_cmd = qq[ umask 0002 && export $npg_cached_samplesheet_file; qc --check=bcfstats --expected
_sample_name=] . $self->supplier_sample . qq[ --reference_genome=\"$ref_genome\" --geno_refset_name=\"study
5392\" --rpt_list=\"$rpt_list\" --filename_root=] . $self->composition_id . q[ --qc_out=] . $self->out_dir 
. q[/qc --input_files=] . $self->out_dir . q[/] . $self->composition_id . qq[.cram --annotation_path=$annot
ation_path  ];


=cut

sub make_bcf_stats {
    my $self = shift;

    my $args = {};
       $args->{'check'}           = q[bcfstats];
       $args->{'filename_root'}   = $self->sample_merged_name;
       $args->{'qc_out'}          = $self->merged_qc_dir;
       $args->{'annotation_path'}  = $self->geno_refset_path;
       #$args->{'geno_refset_name'} = ###e.g. study5392
       #$args->{'expected_sample_name'} = 
       #$args->{'reference_genome'} =
       $args->{'rpt_list'}        = q['] . $self->composition->freeze2rpt . q['];
       $args->{'input_files'}     = $self->merge_dir.q[/outdata/].$self->sample_merged_name.q[.cram]; ###if written locally, what if streamed to iRODS?

    my $command = q[];
    foreach my $arg ( sort keys %{$args} ) {
      $command .= q[ --] . $arg . q[ ] . $args->{$arg};
    }
    #return $self->run_cmd('qc' . $command);
    return 1;
}

=head2 make_bait_stats

=cut

sub make_bait_stats {
    my $self = shift;
    return 1;
}


=head2 make_pulldown_metrics

=cut

sub make_pulldown_metrics {
    my $self = shift;
return 1;
}


#### add to separate module e.g. gatk.pm

=head2 run_bqsr_calc

data/config_files/product_release.yml 
---
default:
  s3:
    enable: false
    url: null
    notify: false
  irods:
    enable: true
    notify: false
  data_deletion:
    staging_deletion_delay: 14

study:
  - study_id: "4112"
    markdup_method: "samtools"
    bqsr:
      enable: true
      apply: true
      known-sites:
        - dbsnp_138.hg38
        - Mills_and_1000G_gold_standard.indels.hg38
        - Homo_sapiens_assembly38.known_indels
    haplotype_caller:
      enable: true
      sample_chunking: hs38primary
      sample_chunking_number: 1


/software/npg/20200124/lib/perl5/npg_pipeline/function/bqsr_calc.pm
/software/npg/20200124/lib/perl5/npg_pipeline/function/haplotype_caller.pm

e.g.
/software/sciops/pkgg/gatk/4.1.3.0/share/gatk-4.1.3.0-0/gatk BaseRecalibrator -O /lustre/scrat
ch117/sciops/team117/npg/jillian/realign_study_4112/pipeline_realignment_test/20531_BAM_basecalls/no_cal/ar
chive/lane1/plex1/20531_1#1.bqsr_table -I /lustre/scratch117/sciops/team117/npg/jillian/realign_study_4112/
pipeline_realignment_test/20531_BAM_basecalls/no_cal/archive/lane1/plex1/20531_1#1.cram -R /lustre/scratch1
17/core/sciops_repository/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/fasta/Homo_sa
piens.GRCh38_full_analysis_set_plus_decoy_hla.fa --known-sites /lustre/scratch117/core/sciops_repository//r
esources/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/dbsnp_138.hg38.vcf.gz --known-sites /lustre/s
cratch117/core/sciops_repository//resources/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/Mills_and_
1000G_gold_standard.indels.hg38.vcf.gz --known-sites /lustre/scratch117/core/sciops_repository//resources/H
omo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/Homo_sapiens_assembly38.known_indels.vcf.gz for functio
n bqsr_calc, LSF job id 972588, array index 10001 at /lustre/scratch117/sciops/team117/npg/jillian/realign_
study_4112/npg_seq_pipeline/bin/npg_pipeline_execute_saved_command line 49.


"/software/sciops/pkgg/gatk/4.1.3.0/share/gatk-4.1.3.0-0/gatk BaseRecalibrator -O /lustre/scratch117/sciops/team117/npg/jillian/realig
n_study_4112/pipeline_realignment_test/20531_BAM_basecalls/no_cal/archive/lane1/plex1/20531_1#1.bqsr_table -I /lustre/scratch117/sciops/team117/npg/jilli
an/realign_study_4112/pipeline_realignment_test/20531_BAM_basecalls/no_cal/archive/lane1/plex1/20531_1#1.cram -R /lustre/scratch117/core/sciops_repositor
y/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/fasta/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa --known-sites /lustre
/scratch117/core/sciops_repository//resources/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/dbsnp_138.hg38.vcf.gz --known-sites /lustre/scratch117
/core/sciops_repository//resources/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz --known-sites /l
ustre/scratch117/core/sciops_repository//resources/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/Homo_sapiens_assembly38.known_indels.vcf.gz",


=cut 

sub run_bqsr_calc {
    my $self = shift;
    my $bqsr_args = shift;

    my $args = {};
       $args->{'O'}   = $self->merge_dir.q[/outdata/].$self->sample_merged_name.q[.bqsr_table];
       $args->{'I'}   = $self->merge_dir.q[/outdata/].$self->sample_merged_name.q[.cram];
       $args->{'R'}   = $self->reference_genome_path;
       $args->{'known-sites'} =  $bqsr_args->{'known-sites'};

    my $command = q[];
    foreach my $arg ( sort keys %{$args} ) {
	if (ref $args->{$arg} eq 'ARRAY'){
            foreach my $v (@{$args->{$arg}}){
                if ($arg eq 'known-sites'){
		              $command .= q[ --] . $arg . q[ ] . $self->known_sites_dir . q[/] . $v . q[.vcf.gz];
                } else { $command .= q[ --] . $arg . q[ ] . $v }
            }
        }
        else{
           $command .= q[ --] . $arg . q[ ] . $args->{$arg};
        }
    }
    return $self->run_cmd($self->gatk_executable . q[ BaseRecalibrator ] .  $command);
}

=head2 run_haplotype_caller

=cut

sub run_haplotype_caller {
my $self = shift;
my $haplotype_caller_args = shift;
my $apply = shift;

        ##no critic (ValuesAndExpressions::RequireInterpolationOfMetachars)
        # critic complains about not interpolating $TMPDIR
        my $make_temp = 'TMPDIR=`mktemp -d -t bqsr-XXXXXXXXXX`';
        my $rm_cmd = 'trap "(rm -r $TMPDIR || :)" EXIT';
        my $debug_cmd = 'echo "BQSR tempdir: $TMPDIR"';
my $command;
   $command = join ' && ', ($make_temp, $rm_cmd, $debug_cmd);

if ($apply){
my $apply_args = {};
   $apply_args->{'R'}   = $self->reference_genome_path;
   $apply_args->{'preserve-qscores-less-than'} = q[6];
   $apply_args->{'static-quantized-quals'} = ['10','20','30'];
   $apply_args->{'bqsr-recal-file'}  = $self->merge_dir.q[/outdata/].$self->sample_merged_name.q[.bqsr_table];
   $apply_args->{'I'}   = $self->merge_dir.q[/outdata/].$self->sample_merged_name.q[.cram];
   $apply_args->{'O'}   = q[$TMPDIR/].$self->sample_merged_name.q[.bqsr.cram];
   $apply_args->{'L'}   = $self->interval_lists_dir.q[/].$haplotype_caller_args->{'sample_chunking'}.q[/].$haplotype_caller_args->{'sample_chunking'}.q[.1.interval_list];
my $apply_bqsr_command = q[];
    foreach my $arg ( sort keys %{$apply_args} ) {
        if (ref $apply_args->{$arg} eq 'ARRAY'){
            foreach my $v (@{$apply_args->{$arg}}){
		$apply_bqsr_command .= q[ --] . $arg . q[ ] . $v;
            }
        }
        else{
          $apply_bqsr_command .= q[ --] . $arg . q[ ] . $apply_args->{$arg};
        }
    }

    $command .= q[ && ] . $self->gatk_executable . q[ ApplyBQSR ] .  $apply_bqsr_command;
}

my $args = {};
   $args->{'R'}   = $self->reference_genome_path;
   $args->{'emit-ref-confidence'} = q[GVCF];
   $args->{'pcr-indel-model'} = q[CONSERVATIVE];
   $args->{'I'}   = q[$TMPDIR/].$self->sample_merged_name.q[.bqsr.cram];
   #use critic;
   $args->{'O'}   = $self->merge_dir.q[/outdata/].$self->sample_merged_name.q[.g.vcf.gz];
   $args->{'L'}   = $self->interval_lists_dir.q[/].$haplotype_caller_args->{'sample_chunking'}.q[/].$haplotype_caller_args->{'sample_chunking'}.q[.1.interval_list];
   $args->{'G'}   = q[AS_StandardAnnotation]; ##  StandardAnnotation and StandardHCAnnotation are enabled by default
   if ($haplotype_caller_args->{'gvcf-gq-bands'}){ $args->{'GQB'} = $haplotype_caller_args->{'gvcf-gq-bands'} }

my $hcommand = q[];
    foreach my $arg ( sort keys %{$args} ) {
        if (ref $args->{$arg} eq 'ARRAY'){
           foreach my $v (@{$args->{$arg}}){
             $hcommand .= q[ --] . $arg . q[ ] . $v;
           }
        }
    else {
             $hcommand .= q[ --] . $arg . q[ ] . $args->{$arg};
     }
  }
   $command .= q[ && ] . $self->gatk_executable . q[ HaplotypeCaller ] .  $hcommand;

return $self->run_cmd($command);
}

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

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
