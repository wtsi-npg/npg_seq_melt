package npg_seq_melt::merge::top_up;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Readonly;
use Carp;
use English qw(-no_match_vars);
use Pod::Usage;
use IO::File;
use File::Slurp;
use npg_tracking::data::reference;
use npg_tracking::data::geno_refset;


extends qw{npg_seq_melt::query::top_up};

with qw{
     npg_common::roles::log
};

our $VERSION = '0';


Readonly::Scalar my $WR_PRIORITY  => 51;
Readonly::Scalar my $MEMORY_16G   => q[16G];
Readonly::Scalar my $MEMORY_2000M => q[2000M];
Readonly::Scalar my $MEMORY_4000M => q[4000M];
Readonly::Scalar my $TARGET_REGIONS_DIR           => q{target};
Readonly::Scalar my $TARGET_AUTOSOME_REGIONS_DIR  => q{custom_targets/autosomes_only_0419};


=head1 NAME

    npg_seq_melt::merge::top_up

=head1 SYNOPSIS

npg_seq_melt::merge::top_up->new(id_study_lims  => 5392, dry_run => 1)->run;


=head1 SUBROUTINES/METHODS

=head2 rt_ticket

Will not be needed once running as a daemon

=cut

has 'rt_ticket'     => ( isa           => 'Int',
                         is            => 'ro',
                         required      => 1,
                         documentation => q[RT ticket for batch processed],
);


=head2 wr_deployment

production or developmet

=cut

has 'wr_deployment'  => ( isa           => 'Str',
                          is            => 'ro',
                          default       => 'production',
                          documentation => q[ For use with wr --deployment option (production or development) ],
);

=head2 commands_file

File name to write wr commands to

=cut

has 'commands_file' => ( isa           => 'Str',
                         is            => 'ro',
                         default       => q[/tmp/wr_input_cmds.txt],
                         documentation => 'File name to write wr commands to',
    );


=head2 wr_env

=cut


has 'wr_env'  => (isa  => 'Str',
                  is   => 'ro',
                  documentation => 'Environment to find relevant scripts and files. NPG_REPOSITORY_ROOT=, REF_PATH=,PATH=,PERL5LIB=',
);

=head2 library

=cut

has 'library' => ( isa           => 'Int',
                   is            => 'rw',
                   documentation => 'Sequencescape legacy_library_id',
    );


=head2 supplier_sample 

=cut

has 'supplier_sample' => ( isa           => 'Str',
                           is            => 'rw',
                           documentation => 'Sequencescape supplier sample name',
    );


=head2 composition_id 

=cut

has 'composition_id' => ( isa           => 'Str',
                          is            => 'rw',
                           documentation => 'Composition id from npg_pipeline::product file_name_root',
    );


=head2 out_dir 

=cut

has 'out_dir' => ( isa           => 'Str',
                   is            => 'rw',
                   documentation => 'Results cache name derived from merge_component_cache_dir name',
    );




=head2 repository

The repository root directory.

=cut

has q{repository} => (
  isa           => q{Str},
  is            => q{ro},
  required      => 0,
  predicate     => q{has_repository},
  default       => $ENV{NPG_REPOSITORY_ROOT},
  documentation => q{The repository root directory},
);


=head2 can_run

=cut

sub can_run {
    my $self = shift;
    if (! $self->repository){ $self->log('NPG_REPOSITORY_ROOT or --repository not specified') ; return 0 };
      $self->run_query();

return 1;
}

=head2 run

=cut

sub run {
    my $self = shift;

    return 0 if ! $self->can_run();
    $self->make_commands();
    $self->run_wr();
    return 1;
}


=head2 make_commands

=cut

sub make_commands {
    my $self = shift;

    my $out_dir;
    my $tmp_dir = q[tmp];
    my $count=0;
    my $duplicates=0;
    my $command_input_fh = IO::File->new($self->commands_file,'>') or croak q[cannot open ], $self->commands_file," : $OS_ERROR\n";


    foreach my $fields (@{$self->data()}){

               $self->library($fields->{library});
            my $in_cram1 = $fields->{orig_cram};
            my $in_seqchksum1 = $in_cram1;
               $in_seqchksum1 =~ s/cram/seqchksum/smx;
            my $in_cram2 = $fields->{top_up_cram};
            my $in_seqchksum2 = $in_cram2;
               $in_seqchksum2 =~ s/cram/seqchksum/smx;
            my $rpt_list = $fields->{extended_rpt_list};

               $self->out_dir($fields->{results_cache_name});
               $self->composition_id($fields->{composition_id});
               $self->supplier_sample($fields->{supplier_sample});

	    my $ref_genome = $self->_product->lims->reference_genome();
            my $lims = st::api::lims->new(rpt_list  => $rpt_list,driver_type=>$self->lims_driver());
	    my $repository = $self->repository;
	    my $picard_reference = npg_tracking::data::reference->new(rpt_list =>$rpt_list,lims=>$lims,aligner=>q[picard],repository=>$repository)->refs->[0];
            my $fasta_reference  = npg_tracking::data::reference->new(rpt_list =>$rpt_list,lims=>$lims,aligner=>q[fasta],repository=>$repository)->refs->[0];
            my $bwa_reference    = npg_tracking::data::reference->new(rpt_list =>$rpt_list,lims=>$lims,aligner=>q[bwa0_6],repository=>$repository)->refs->[0];


=head2  p4 merge aligned crams

=cut

my $p4dir = qq[\$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib];## no critic (ValuesAndExpressions::ProhibitInterpolationOfLiterals)

my $vtfp_cmd  = q[vtfp.pl  -l ] . $self->library . q[.vtf.log -o ] . $self->library . q[.merge.json];
   $vtfp_cmd .= q[ -keys outdatadir -vals ] . $self->out_dir;
   $vtfp_cmd .= qq[ -keys incrams -vals $in_cram1 -keys incrams -vals $in_cram2 -keys incrams_seqchksum -vals $in_seqchksum1 -keys incrams_seqchksum -vals $in_seqchksum2 -keys cfgdatadir -vals $p4dir -template_path $p4dir];
   $vtfp_cmd .= q[ -keys library -vals ] . $self->library . q[ -keys fopid -vals ] . $self->composition_id ;
   $vtfp_cmd .= qq[ -keys reference_genome_fasta -vals $fasta_reference];
   $vtfp_cmd .= qq[ -keys alignment_reference_genome -vals $bwa_reference];
   $vtfp_cmd .= qq[ -keys reference_dict -vals $picard_reference];
   $vtfp_cmd .= q[ -param_vals ] . qq[$p4dir/basic_params_top_up_merge.json $p4dir/merge_aligned.json];

    my $viv_cmd = q[viv.pl -s -v 3 -x -o  ] . $self->library . q[.viv.log ] . $self->library . q[.merge.json];

    my $cmd = q[ umask 0002 &&  bash -c ' mkdir -p ] . $self->library . qq[/$tmp_dir ; cd ] . $self->library ;

       $cmd .= q[ && ]  . $vtfp_cmd . q[ && ] . $viv_cmd  . q['];

    my $merge_grp_name = q[merge_lib] . $self->library;

$self->_command_to_json({
                         cmd      => $cmd,
                         memory   => $MEMORY_16G,
                         disk     => 150,
                         rep_grp  => q[rt].$self->rt_ticket,
                         dep_grps => ["$merge_grp_name"],
                         },q[],$command_input_fh);



=head2 samtools stats targets

=cut

   my $merge_cram         = $self->out_dir . q[/] . $self->composition_id . q[.cram];
   my $merge_target_stats = $self->out_dir . q[/] . $self->composition_id . q[_F0xF04_target.stats];
   my $targets            = npg_tracking::data::reference->new(rpt_list =>$rpt_list,lims=>$lims,aligner=>$TARGET_REGIONS_DIR,repository=>$repository)->refs->[0] . q[.interval_list];
   my $stats_cmd  = qq[umask 0002 && samtools stats -r $fasta_reference --reference $fasta_reference -p -g 15 -F 0xF04 -t $targets $merge_cram >  $merge_target_stats ] ;


my $stats_dep_grp = q[stats] . $self->library;

$self->_command_to_json({
                         cmd      => $stats_cmd,
                         rep_grp  => q[rt].$self->rt_ticket,
                         dep_grps => ["$stats_dep_grp"],
                         deps     => ["$merge_grp_name"]
                         },'_F0xF04_target.stats',$command_input_fh);




=head2 samtools stats  target autosome stats

=cut

my $merge_target_autosome_stats = $self->out_dir . q[/] . $self->composition_id . q[_F0xF04_target_autosome.stats];
my $custom_targets              = npg_tracking::data::reference->new(rpt_list =>$rpt_list,lims=>$lims,aligner=>$TARGET_AUTOSOME_REGIONS_DIR,repository=>$repository)->refs->[0] . q[.interval_list];
my $autosome_stats_cmd = qq[umask 0002 && samtools stats -r $fasta_reference --reference $fasta_reference -p -g 15 -F 0xF04 -t $custom_targets $merge_cram >  $merge_target_autosome_stats ];


$self->_command_to_json({
                         cmd      => $autosome_stats_cmd,
                         rep_grp  => q[rt].$self->rt_ticket,
                         dep_grps => ["$stats_dep_grp"],
                         deps     => ["$merge_grp_name"]
                         },'_F0xF04_target_autosome.stats',$command_input_fh);



=head2 bam flagstats

=cut 

my $bam_flagstats_cmd = q[ umask 0002 && qc --check bam_flagstats --filename_root ] . $self->composition_id . q[ --qc_in ] . $self->out_dir . q[ --qc_out ] .  $self->out_dir . qq[/qc --rpt_list \"$rpt_list\" --input_files ] . $self->out_dir;

my $flag_dep_grp = q[bam_flagstats] . $self->library;

$self->_command_to_json({
                         cmd      => $bam_flagstats_cmd,
                         rep_grp  => q[rt].$self->rt_ticket,
                         dep_grps => ["$flag_dep_grp"],
                         deps     => ["$stats_dep_grp"],
                         },'bam_flagstats',$command_input_fh);



=head2 Verify bam id

=cut


my $npg_cached_samplesheet_file = q[NPG_CACHED_SAMPLESHEET_FILE=] . $self->out_dir . q[/] . $self->composition_id . q[.csv];

my $verify_bam_id_file = $self->out_dir . q[/] . $self->composition_id . q[.bam];

my $verify_bam_cmd = qq[ umask 0002 && export $npg_cached_samplesheet_file; qc --check=verify_bam_id --reference_genome \"$ref_genome\" --rpt_list=\"$rpt_list\" --filename_root=] . $self->composition_id . q[ --qc_out=] . $self->out_dir . qq[/qc --input_files=$verify_bam_id_file ];

my $vb_dep_grp = q[verify_bam_id]  . $self->library;

$self->_command_to_json({
                         cmd      => $verify_bam_cmd,
                         memory   => $MEMORY_2000M,
                         rep_grp  => q[rt].$self->rt_ticket,
                         dep_grps  => ["$vb_dep_grp"],
                         deps     => ["$merge_grp_name"],
                         },'qc_verify_bam_id',$command_input_fh);




=head2 bcf stats

Runs bcftools stats --collapse_snps --apply-filters PASS --samples expected_sample_name geno_refset_bcfdb_path temp_bcf (filename_root.bcf)

=cut

my $annotation_path = npg_tracking::data::geno_refset->new(rpt_list =>$rpt_list,lims=>$lims,repository=>$repository)->geno_refset_annotation_path;


  my $bcf_stats_cmd = qq[ umask 0002 && export $npg_cached_samplesheet_file; qc --check=bcfstats --expected_sample_name=] . $self->supplier_sample . qq[ --reference_genome=\"$ref_genome\" --geno_refset_name=\"study5392\" --rpt_list=\"$rpt_list\" --filename_root=] . $self->composition_id . q[ --qc_out=] . $self->out_dir . q[/qc --input_files=] . $self->out_dir . q[/] . $self->composition_id . qq[.cram --annotation_path=$annotation_path  ];

my $bcfstats_dep_grp = q[bcfstats]  . $self->library;

$self->_command_to_json({
                        cmd      => $bcf_stats_cmd,
                        rep_grp  => q[rt].$self->rt_ticket,
                        dep_grps =>  ["$bcfstats_dep_grp"],
                        deps     => ["$merge_grp_name"],
                        },'bcf_stats',$command_input_fh);



=head2  review

qc review check

=cut


my $qc_review_cmd = qq[ umask 0002 && export $npg_cached_samplesheet_file; qc --check=review --final_qc_outcome --rpt_list=\"$rpt_list\" --qc_in ] . $self->out_dir . q[/qc --conf_path=] .  $self->conf_path;

$self->_command_to_json({
                        cmd      => $qc_review_cmd,
                        rep_grp  => q[rt].$self->rt_ticket,
                        deps     => ["$flag_dep_grp","$vb_dep_grp","$bcfstats_dep_grp"],
                        },'review',$command_input_fh);

  }
return;
}

=head2 _command_to_json

=cut

sub _command_to_json {
    my $self     = shift;
    my $hr       = shift;
    my $analysis = shift;
    my $command_fh = shift;

       $hr->{priority} = $WR_PRIORITY;
    my $cmd = $hr->{cmd};
    my $out_dir = $self->out_dir;
    my $composition_id = $self->composition_id;
    my $out_file_path = $self->out_dir . q[/log/] . $self->composition_id;
       if ($analysis && ($analysis !~ /^_/smx)){ $out_file_path .= q[.] };
       $out_file_path .= $analysis ? $analysis : q[];
       $out_file_path .= q[.out];


       $hr->{cmd} = qq[($cmd) 2>&1 | tee -a \"$out_file_path\"];

       if (! $hr->{memory}){ $hr->{memory} = $MEMORY_4000M };
    my $json = JSON->new->allow_nonref;
    my $json_text   = $json->encode($hr);

    print {$command_fh} $json_text,"\n"  or $self->log(qq[Can't write to commands file: $ERRNO]);;
return;
}


=head2 run_wr

=cut

sub run_wr {
    my $self = shift;

    my $wr_cmd = q[wr  add --cwd /tmp --retries 0  --override 2 --disk 0 --rep_grp top_up_merge --env '];
       $wr_cmd .= $self->wr_env();
       $wr_cmd .= q[' -f ] . $self->commands_file;
       $wr_cmd .= q[ --deployment ] . $self->wr_deployment;


    $self->log("**Running $wr_cmd**");

   if (! $self->dry_run ){
     my $wr_fh = IO::File->new("$wr_cmd |") or die "cannot run cmd\n";
     while(<$wr_fh>){}
}

    return 1;
}

__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DESCRIPTION

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Moose

=item MooseX::StrictConstructor

=item Moose::Meta::Class

=item namespace::autoclean

=item Readonly

=item IO::File 

=item English

=item File::Slurp

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 Genome Research Ltd

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

