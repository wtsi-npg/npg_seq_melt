package npg_seq_melt::merge::generator;


use Moose;
use MooseX::StrictConstructor;
use DateTime;
use DateTime::Duration;
use List::MoreUtils qw/any uniq/;
use English qw(-no_match_vars);
use Readonly;
use Carp;
use Cwd qw/cwd/;
use IO::File;
use File::Basename qw/ basename /;
use st::api::lims;
use npg_tracking::data::reference;
use WTSI::DNAP::Warehouse::Schema;
use WTSI::DNAP::Warehouse::Schema::Query::LibraryDigest;
use npg_tracking::glossary::rpt;
use npg_seq_melt::merge::base;

extends q{npg_seq_melt::merge};

with qw{
  npg_seq_melt::util::irods
};

our $VERSION  = '0';

Readonly::Scalar my $MERGE_SCRIPT_NAME       => 'npg_library_merge';
Readonly::Scalar my $LOOK_BACK_NUM_DAYS      => 7;
Readonly::Scalar my $HOURS                   => 24;
Readonly::Scalar my $EIGHT                   => 8;
Readonly::Scalar my $CLUSTER                 => 'seqfarm2';
Readonly::Scalar my $RUN_NUMBER              => 20_000;


=head1 NAME

npg_seq_melt::merge::generator

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 merge_cmd

Merge command.

=cut

has 'merge_cmd'  =>  ( is            => 'ro',
                       isa           => q{NpgCommonResolvedPathExecutable},
                       coerce        => 1,
                       default       => $MERGE_SCRIPT_NAME,
                       documentation =>
 'The name of the script to call to do the merge.',
);

=head2 run_dir

=cut

has 'run_dir'  => (
    isa           => q[Str],
    is            => q[ro],
    default       => cwd(),
    documentation => q[Parent directory where sub-directory for merging is created, default is cwd ],
    );

=head2 dry_run

Boolean flag, false by default. Switches on verbose and local options and reports
what is going to de done without submitting anything for execution.

=cut
has 'dry_run'      => ( isa           => 'Bool',
                        is            => 'ro',
                        default       => 0,
                        documentation =>
  'Boolean flag, false by default. ' .
  'Switches on verbose and local options and reports ' .
  'what is going to de done without submitting anything for execution',
);

=head2 cluster

Checks that code is being run on a specific cluster, defaults to seqfarm. Should 
always run on the same farm as relies on being able to check for running jobs.

=cut 
has 'cluster' => (
    isa           => 'Str',
    is            => 'ro',
    default       => $CLUSTER,
    documentation => q[Checks that code is being run on a specified cluster],
    );

=head2 lims_id

LIMS id e.g. SQSCP, C_GCLP

=cut

has 'id_lims' => (
     isa           => q[Str],
     is            => q[ro],
     default       => q[SQSCP],
     documentation => q[LIMS id e.g. SQSCP, C_GCLP. Default SQSCP (SequenceScape)],
    );



=head2 max_jobs

Int. Limits number of jobs submitted.

=cut
has 'max_jobs'   => (isa           => 'Int',
                     is            => 'ro',
                     documentation =>'Only submit max_jobs jobs (for testing)',
);


=head2 force 

Boolean flag, false by default. If true, a merge is still run if a merged cram 
already exists for this library, if differing composition and target=library.

=cut
has 'force'  => ( isa           => 'Bool',
                  is            => 'ro',
                  default       => 0,
                  documentation =>
  'Boolean flag, false by default. ' .
  'If true, a merge is run where merge of different composition exists and target=library.',
);

=head2 use_lsf

Boolean flag, false by default, ie the commands are not submitted to LSF for
execution.

=cut
has 'use_lsf'      => ( isa           => 'Bool',
                        is            => 'ro',
                        default       => 0,
                        documentation =>
  'Boolean flag, false by default,  ' .
  'ie the commands are not submitted to LSF for execution.',
);

=head2 use_cloud

Set off commands as wr add jobs

=cut

has 'use_cloud'      => ( isa           => 'Bool',
                          is            => 'ro',
                          default       => 0,
                          documentation =>
  'Boolean flag, false by default,  ' .
  'ie the commands are not submitted to wr for execution.',
);


=head2 cloud_disk

Amount of disk space in Gb to request 

=cut

has 'cloud_disk'      => (isa           => 'Int',
                          is            => 'ro',
                          default       => 20,
                          documentation =>
                          'Default 20 G  ',
);

=head2 cloud_cleanup_false 

=cut

has 'cloud_cleanup_false'      => ( isa           => 'Bool',
                                    is            => 'ro',
                                    documentation => 'leave files from exited job on instance',
);


=head2 cloud_export_path

  --cloud_export_path /software/npg/bin 


=cut

has 'cloud_export_path'   => (  isa           => 'ArrayRef[Str]',
                                is            => 'ro',
                                default       => sub { ['/tmp/npg_seq_melt/bin','/software/npg/bin'] },
                                documentation => q[Specify alternative paths to the default. Default set to /tmp/npg_seq_melt/bin and /software/npg/bin],
);

=head2 cloud_export_perl5lib

 --cloud_export_perl5lib /tmp/npg_seq_melt/lib 

=cut

has 'cloud_export_perl5lib'   => (  isa           => 'ArrayRef[Str]',
                                    is            => 'ro',
                                    default       => sub { ['/tmp/npg_seq_melt/lib','/software/npg/lib/perl5'] },
                                    documentation => q[Specify alternative PERL5LIB to the default. Default set to /tmp/npg_seq_melt/lib and /software/npg/lib/perl5],
);

=head2 cloud_home

=cut

has 'cloud_home'  => ( isa  => 'Str',
                       is   => 'ro',
                       default => q[~ubuntu],
                       documentation => q[ HOME on the Openstack instance. default ~ubuntu ],
                     );

=head2 cloud_repository

=cut

has 'cloud_repository'  => ( isa  => 'Str',
                             is   => 'ro',
                             default => q[../../npg-repository/cram_cache/%2s/%2s/%s ],
                             documentation => q[ set REF_PATH location on the Openstack instance. default ../../npg-repository/cram_cache/%2s/%2s/%s ],
                     );


=head2 crams_in_s3

=cut

has 'crams_in_s3'      => ( isa           => 'Bool',
                            is            => 'ro',
                            default       => 0,
                            documentation => 'input cram files located on S3 rather than iRODS, for use with use_cloud',
);

=head2 num_days

Number of days to look back, defaults to seven.

=cut
has 'num_days'     => ( isa           => 'Int',
                        is            => 'ro',
                        default       => $LOOK_BACK_NUM_DAYS,
                        documentation =>
  'Number of days to look back, defaults to seven',
);


=head2 log_dir

Log directory - will be used for LSF jobs output.

=cut
has 'log_dir'      => ( isa           => 'Str',
                        is            => 'ro',
                        documentation => q[Log directory - will be used for LSF jobs output.],
);


=head2 tokens_per_job

Number of tokens per job (default 7), to limit number of jobs running simultaneously.

=cut

has 'tokens_per_job' => ( isa            => 'Int',
                           is            => 'ro',
                           default       => 7,
                           documentation => q[Number of tokens per job (default 7). See bhosts -s ],
);

=head2 token_name

LSF token name, defaults to seq_merge.

=cut
has 'token_name' => ( isa           => 'Str',
                      is            => 'ro',
                      default       => 'seq_merge',
                      documentation => q[LSF token name, defaults to seq_merge. See bhosts -s for token list.],
);

=head2 lsf_num_processors

Number of tasks for a parallel job (default 3). Used with the LSF -n option

=cut

has 'lsf_num_processors' => ( isa           => 'Int',
                              is            => 'ro',
                              default       => 3,
                              documentation => q[Number of parallel tasks per job (default 3). LSF -n],
);

=head2 lsf_runtime_limit

LSF kills the job if still found running after this time.  LSF -W option.

=cut

has 'lsf_runtime_limit' => ( isa           => 'Int',
                             is            => 'ro',
                             default       => 1440,
                             documentation => q[Job killed if running after this time length (default 1440 minutes). LSF -W],
);

=head2 lsf_group

Set the lsf group the jobs have to be submitted under

=cut

has 'lsf_group' => ( isa           => 'Str',
                     is            => 'ro',
                     default       => 'prod_users',
                     documentation => q[Set the lsf group the jobs have to be submitted under, defaults to prod_users],
);

=head2 lsf_queue

Set the lsf queue the jobs have to be submitted under

=cut

has 'lsf_queue' => ( isa           => 'Str',
                     is            => 'ro',
                     default       => 'srpipeline',
                     documentation => q[Set the lsf queue the jobs have to be submitted under, defaults to srpipeline ],
);

=head2 lane_fraction

Fraction of a whole sequencing lane required before merging should take place. e.g. 0.5 for a HiSeqX lane (e.g. usually 6 lanelets where pool size of 12) 

=cut

has 'lane_fraction' => ( isa    => 'Num',
                         is     => 'ro',
                         default => 0.5,
                         documentation => q[Fraction of a whole sequencing lane required],
);

=head2 _mlwh_schema

=cut

has '_mlwh_schema' => ( isa           => 'WTSI::DNAP::Warehouse::Schema',
                        is            => 'ro',
                        lazy_build    => 1,
);
sub _build__mlwh_schema {
  return WTSI::DNAP::Warehouse::Schema->connect();
}

=head2 _current_lsf_jobs

Hashref of LSF jobs already running and the extracted rpt strings
   
=cut

has '_current_lsf_jobs' => (
     isa          => q[Maybe[HashRef]],
     is           => q[ro],
     lazy_build   => 1,
);
sub _build__current_lsf_jobs {
    my $self = shift;
    my $job_rpt = {};
    return $job_rpt if $self->use_cloud();#temp

    my $cmd = basename($self->merge_cmd());
    my $fh = IO::File->new("bjobs -UF   | grep $cmd |") or croak "cannot check current LSF jobs: $ERRNO\n";
    while(<$fh>){
    ##no critic (RegularExpressions::ProhibitComplexRegexes)
         if (m{^Job\s\<(\d+)\>.*         #capture job id
                Status\s\<(\S+)\>.*
              --rpt_list\s\'
              (
                 (?:                     #group
                    \d+:\d:?\d*;*        #colon-separated rpt (tag optional). Optional trailing semi-colon
                 ){2,}                   #2 or more
              )
             }smx){
    ##use critic
                   my $job_id   = $1;
                   my $status   = $2;
                   my $rpt_list = $3;

		               $job_rpt->{$rpt_list}{'jobid'} = $job_id;
                   $job_rpt->{$rpt_list}{'status'} = $status;
                }
    }
    $fh->close();
return $job_rpt;
}

=head2 BUILD

=cut

sub BUILD {
  my $self = shift;
  if ($self->dry_run) {
    $self->_set_local(1);
    $self->_set_verbose(1);
  }
  if ($self->use_lsf && !$self->log_dir) {
    croak 'LSF use enabled, log directory should be defined';
  }
  if ($self->id_run_list){
      my $file = $self->id_run_list;
      my @runs;
      my $fh = IO::File->new($file,'<') or croak "cannot open $file" ;
      while(<$fh>){
           chomp;
           if (/^\d+$/smx){  push @runs,$_  }
       }
       $fh->close;
       $self->id_runs(\@runs);
   }
  return;
}

=head2 id_runs

Optional Array ref of run id's to use

=cut

has 'id_runs'               =>  ( isa        => 'ArrayRef[Int]',
                                 is         => 'rw',
                                 documentation => q[One or more run ids to restrict to],
);

=head2 id_run_list

Optional file name of list of run id's to use

=cut

has 'id_run_list'               =>  ( isa        => 'Str',
                                      is         => 'ro',
                                      documentation => q[File of run ids to restrict to],
);

=head2 only_library_ids

ArrayRef of legacy_library_ids.
Best to use in conjunction with specified --id_run_list or --id_runs unless it is known to fall within the cutoff_date.
Specifying look back --num_days is slower than supplying run ids. 

=cut

has 'only_library_ids'        =>  ( isa        => 'ArrayRef[Int]',
                                    is          => 'ro',
                                    documentation =>
q[One or more library ids to restrict to.] .
q[At least one of the associated run ids must fall in the default ] .
q[WTSI::DNAP::Warehouse::Schema::Query::LibraryDigest] .
q[ query otherwise cut off date must be increased with ] .
q[--num_days or specify runs with --id_run_list or --id_run],
);


=head2 include_rad

Boolen flag, which is false by default e.g. by default R&D libraries are not included.

=cut
has 'include_rad' => ( isa           => 'Bool',
                       is            => 'ro',
                       required      => 0,
                       default       => 0,
);

=head2 restrict_to_chemistry

Restrict to certain chemistry codes e.g. ALXX and CCXX for HiSeqX

=cut

has 'restrict_to_chemistry'  => (isa        => 'ArrayRef[Str]',
                                 is   => 'ro',
                                 predicate   => '_has_restrict_to_chemistry',
                                 documentation =>q[Restrict to certain chemistry codes e.g. ALXX and CCXX for HiSeqX],
);


=head2 id_study_lims

=cut

has 'id_study_lims'     => ( isa  => 'Str',
                             is          => 'ro',
                             documentation => q[],
                             predicate  => '_has_id_study_lims',
);


=head2 run

=cut

sub run {
  my $self = shift;
  return if ! $self->_check_host();

  if($self->_has_id_study_lims() && $self->id_runs()) {
     croak q[Aborting, study id option set so run based restrictions will be lost];
  }
  if($self->_has_id_study_lims() || $self->id_runs()) {
     carp q[Study or run restriction is set so NO date based restriction will be used];
  }

  my $ref = {};
  $ref->{'iseq_product_metrics'} = $self->_mlwh_schema->resultset('IseqProductMetric');
  $ref->{'earliest_run_status'}  = 'qc complete';
  $ref->{'filter'}               = 'mqc';

  if ($self->_has_id_study_lims()){
    $ref->{'id_study_lims'}  = $self->id_study_lims();
  } elsif ($self->id_runs()) {
    $ref->{'id_run'}  = $self->id_runs();
  } else {
    $ref->{'completed_after'}  = $self->_cutoff_date()
  }

  if ( $self->include_rad ){
    $ref->{'include_rad'} = 1;
  }

  if ( $self->only_library_ids() ) {
    $ref->{'library_id'} = $self->only_library_ids();
  }

  my $digest = WTSI::DNAP::Warehouse::Schema::Query::LibraryDigest
                 ->new($ref)->create();

  my $cmd_count=0;
  my $num_libs = scalar keys %{$digest};
  warn qq[$num_libs libraries in the digest.\n];
  my $commands = $self->_create_commands($digest);
  foreach my $command ( @{$commands} ) {
    my $job_to_kill = 0;
    if ($self->_should_run_command($command, \$job_to_kill)) {
      if ( $job_to_kill && $self->use_lsf) {
        warn qq[LSF job $job_to_kill will be killed\n];
        if ( !$self->local && !$self->dry_run) {
          $self->_lsf_job_kill($job_to_kill);
        }
      }

      $cmd_count++;
      warn qq[Will run command $command->{command}\n];
      if (!$self->dry_run) {
        $self->_call_merge($command->{'command'});
      }
      if ($self->max_jobs() && $self->max_jobs() == $cmd_count){
        return;
      }
    }
  }

  return;
}

=head2 _cutoff_date

=cut

sub _cutoff_date {
  my $self = shift;
  my $d = DateTime->now();
  $d->subtract_duration(
    DateTime::Duration->new(hours => $self->num_days * $HOURS));
  return $d;
}

=head2 _parse_chemistry

   ACXX    HiSeq V3
   ADXX    HiSeq 2500 rapid
   ALXX    HiSeqX V1
   ANXX    HiSeq V4
   BCXX    HiSeq 2500 V2 rapid
   CCXX    HiSeqX V2
   V2      MiSeq V2
   V3      MiSeq V3
   LT1     NovaSeqX Series B1
   LT3     NovaSeqX Series B3
   LT4,4LE NovaSeqX Series B4

=cut


sub _parse_chemistry{
    my $self    = shift;
    my $barcode = shift;
    my $rpt     = shift;
    my $h = npg_tracking::glossary::rpt->inflate_rpt($rpt);

    my $suffix;

    if  (($barcode =~ /(V[2|3])$/smx) || ($barcode =~ /(\S{4})$/smx)){ $suffix =  uc $1 }
    ## For v2.5 flowcells some old suffixes (ALXX) were used as the CCXX barcodes were used up,
    ## so use one code
    if ($suffix =~ /CCX[X|Y]/smx
                or
        $suffix eq q[ALXX] and $h->{'id_run'} > $RUN_NUMBER){ return ('HXV2') }


    if ($suffix =~ /\SLT1$/smx) {  return ('NXB1') };
    if ($suffix =~ /\SLT3$/smx) {  return ('NXB3') };

    if ($suffix =~ /\SLT4$/smx
                or
        $suffix =~ /\S4LE$/smx){     return ('NXB4') };
    return($suffix);
}


=head2 _validate_references

check same reference

=cut

sub _validate_references{
    my $self = shift;
    my $entities = shift;
    my $num_refs = scalar uniq map { $_->{'reference_genome'} } @{$entities};
    return $num_refs > 1 ? 0 : 1;
}

=head2 run_pos_tag_count

HashRef of the count of tags in a lane 

=cut 

has '_run_pos_tag_count' => (
    isa          => q[Maybe[HashRef]],
    is           => q[rw],
    default      => sub { {} },
);


=head2 _validate_lane_fraction

Check minimum fraction of lane has been sequenced e.g. 0.5 of HiSeqX

=cut

sub _validate_lane_fraction{
    my $self = shift;
    my $entities = shift;
    my $library  = shift;
    my $actual_lane_fraction=0;
    my @rpts = uniq map { $_->{'rpt_key'} } @{$entities};

   foreach my $rpt (sort @rpts){
         my $lanelet_fraction = 0;
         my $r = npg_tracking::glossary::rpt->inflate_rpt($rpt);
         my $id_run = $r->{'id_run'};
         my $position = $r->{'position'};
         my $rp = npg_tracking::glossary::rpt->deflate_rpt({id_run=>$id_run,position=>$position});
         if (exists $self->_run_pos_tag_count->{$rp}){
             $actual_lane_fraction += $self->_run_pos_tag_count->{$rp};
         }
         else {
              my @index_row = $self->_mlwh_schema->resultset('IseqProductMetric')->search({ ##no critic (ValuesAndExpressions::ProhibitLongChainsOfMethodCalls)
                             'id_run'     => $id_run,
                             'position'   => $position,
                              })->all();
              #remove tag 0 and phix from count
             @index_row = sort map { $_->tag_index } grep { $_->tag_index ne '888' } grep { $_->tag_index ne '0' } @index_row; #tag index 168?

             if (@index_row){
	       $lanelet_fraction = 1/ scalar @index_row;
               $actual_lane_fraction += $lanelet_fraction;
               $self->_run_pos_tag_count->{$rp} = $lanelet_fraction;
           }
        }
   }
        my $lf = $self->lane_fraction;
        if ($self->verbose){
            my $rounded = sprintf '%.3f',$actual_lane_fraction;
            $self->info(qq[Library $library total lane fraction = $rounded (required=$lf)]);
        }
        if ( $actual_lane_fraction ge  $self->lane_fraction ){ return 1 }
        return 0;
}

=head2 _validate_lims

=cut

sub _validate_lims {
  my $entities = shift;
  return (1 == scalar uniq map { $_->{'id_lims'} } @{$entities});
}


=head2 _create_commands

=cut

sub _create_commands {## no critic (Subroutines::ProhibitExcessComplexity)

  my ($self, $digest) = @_;

  my @commands = ();

  foreach my $library (keys %{$digest}) {
    foreach my $instrument_type (keys %{$digest->{$library}}) {
      foreach my $rt (keys %{$digest->{$library}->{$instrument_type}}) {
              my $expected_cycles = {};

         foreach my $e (@{$digest->{$library}->{$instrument_type}->{$rt}->{'entities'}}) {
                 push @{$expected_cycles->{$e->{'expected_cycles'}}{$e->{'study'}}}, $e;
	      }
     my $studies = {};
     foreach my $e_cycles (keys %{$expected_cycles}){
	        $studies = $expected_cycles->{$e_cycles};
             my $run_type = $rt . $e_cycles;

        foreach my $study (keys %{$studies}) {
                ## no critic (ControlStructures::ProhibitDeepNests)
                my $s_entities = $studies->{$study};

            my $fc_id_chemistry = {};
	          foreach my $e (@{$s_entities}){
  		               if ($e->{'library_type'} =~ /^Chromium/smxi){
                         carp qq[Library $library has library type $e->{'library_type'}, skipping\n];
                         next;
                      }


                     my $chem =  $self->_parse_chemistry($e->{'flowcell_barcode'},$e->{'rpt_key'});

                     if ($self->_has_restrict_to_chemistry){
                         if (! any { $chem eq $_ } @{$self->restrict_to_chemistry} ){ next }
                     }
                     push @{ $fc_id_chemistry->{$chem}}, $e;
		  }


            foreach my $chemistry_code (keys %{$fc_id_chemistry}){
                    my $entities = $fc_id_chemistry->{$chemistry_code};
          ## no critic (ControlStructures::ProhibitDeepNests)
          if ( any { exists $_->{'status'} && $_->{'status'} && $_->{'status'} =~ /archiv/smx } @{$entities} ) {
            warn qq[Will wait for other components of library $library to be archived.\n];
            next;
          }

          ## Note: if earliest_run_status is not used with LibraryDigest then status is only added to some entities
          my @completed = grep
            { (!exists $_->{'status'}) || ($_->{'status'} && $_->{'status'} eq 'qc complete') }
	                @{$entities};

          if (!@completed) {
            carp qq[No qc complete libraries - should not happen at this stage - skipping.\n];
            next;
	        }

          #if (scalar @completed < $self->minimum_component_count) {
          #  warn scalar @completed, qq[ entities for $library, skipping.\n];
          #  next;
          #}


          if (!_validate_lims(\@completed)) {
            croak 'Cannot handle multiple LIM systems';
	        }

          if($completed[0]->{'id_lims'} ne $self->id_lims){
              next;
          }

          if (!$self->_validate_references(\@completed)) {
            warn qq[Multiple reference genomes for $library, skipping.\n];
            next;
	        }

          if ($self->sample_acc_check && !$completed[0]->{'sample_accession_number'}){
              warn qq[Sample accession required but library $library not accessioned\n];
              next;
          }

	        if ($self->lane_fraction){
	            if (! $self->_validate_lane_fraction(\@completed,$library)){
                        warn qq[Lane fraction not reached for $library, skipping.\n];
	                next;
		         }
                    if (scalar @completed == 1){
                        warn qq[Lane fraction reached for $library with single cram, skipping - merging not required.\n];

                        ###Add irods meta data added with target = library
                        ##TODO check target = library not already added
                        my $singleton_obj = npg_seq_melt::merge::base->new(rpt_list => map { $_->{'rpt_key'} } @completed);
			                  my ($c) = $singleton_obj->composition->components_list();
                        my $cram_file = $self->standard_paths($c)->{irods_cram};

                        my @found = $self->irods->find_objects_by_meta($self->default_root_dir(),['target'     => 'library'],['library_id' => $library ],['study_id'   => $study ]);
                        if (! @found){
                            if ($self->dry_run() ){
                              $self->info(qq[Would be adding target = library to $cram_file]);
                            }
                            else {
                              $self->info(qq[Adding target = library to $cram_file]);
                              $self->irods->add_object_avu($cram_file,'target','library');
                            }
                        }
                        next;
                    }
	        }

          ##use critic
          push @commands,
               $self->_command(\@completed, $library, $instrument_type, $run_type, $chemistry_code);
	    }
	  }
        }
      }
    }
  }

  return \@commands;
}

=head2 _command

=cut

sub _command { ## no critic (Subroutines::ProhibitManyArgs)
  my ($self, $entities, $library, $instrument_type, $run_type, $chemistry) = @_;

  my $rpt_list = npg_tracking::glossary::rpt->join_rpts(
                   map { $_->{'rpt_key'} } @{$entities});
  my $obj = npg_seq_melt::merge::base->new(rpt_list => $rpt_list);
  $rpt_list = $obj->composition()->freeze2rpt(); # sorted list

  my $reference_genome_path = $self->_has_reference_genome_path ?
          $self->reference_genome_path : $self->_get_reference_genome_path($obj->composition);

  my @command = $self->use_cloud ? basename($self->merge_cmd) : $self->merge_cmd;
  push @command, q[--rpt_list '] . $rpt_list . q['];
  push @command, qq[--reference_genome_path $reference_genome_path];
  push @command, qq[--library_id $library];
  my $library_type = q['].$entities->[0]->{'library_type'}.q['];
  push @command, q[--library_type ], $library_type;
  push @command,  q[--sample_id], $entities->[0]->{'sample'};
  push @command,  q[--sample_name], $entities->[0]->{'sample_name'};

  if (defined $entities->[0]->{'sample_common_name'}){
    my $sample_common_name = q['].$entities->[0]->{'sample_common_name'}.q['];
    push @command,  qq[--sample_common_name $sample_common_name];
  }

  if (defined $entities->[0]->{'sample_accession_number'}){
    push @command,
      q[--sample_accession_number], $entities->[0]->{'sample_accession_number'};
  }

  push @command,  q[--study_id], $entities->[0]->{'study'};

  my $study_name = q['].$entities->[0]->{'study_name'}.q['];
  push @command,  qq[--study_name $study_name];

  my $study_title = q['].$entities->[0]->{'study_title'}.q['];

  push @command,  qq[--study_title $study_title];

  if (defined $entities->[0]->{'study_accession_number'}){
    push @command,
      q[--study_accession_number], $entities->[0]->{'study_accession_number'};
  }
  push @command,  q[--aligned],$entities->[0]->{'aligned'};
  push @command,  q[--lims_id],$entities->[0]->{'id_lims'};

  push @command, qq[--instrument_type $instrument_type];
  push @command, qq[--run_type $run_type];
  push @command, qq[--chemistry $chemistry ];
  push @command, q[--samtools_executable ], $self->samtools_executable(), q[ ];
  push @command, q[--run_dir ], $self->run_dir(), q[ ];

  if ($self->local) {
    push @command, q[--local];
  }

  if ($self->random_replicate){
    push @command, q[--random_replicate];
  }

  if ($self->default_root_dir ){
    push @command, q[--default_root_dir ] . $self->default_root_dir;
  }

  if ($self->remove_outdata){
    push @command, q[--remove_outdata ];
  }

  if (! $self->sample_acc_check){
    push @command, q[--nosample_acc_check ];
    if ( $self->reheader_rt_ticket){
       push @command, q[--reheader_rt_ticket ], $self->reheader_rt_ticket, q[ ];
    }
  }

  if ($self->use_cloud()){
    push @command, q[--use_cloud ];

    if ($self->cloud_export_perl5lib){
           my $p5 = join q[:], @{ $self->cloud_export_perl5lib }, q[\$PERL5LIB]; ## no critic (ValuesAndExpressions::RequireInterpolationOfMetachars)
           #add before script name
           unshift @command, qq[ export PERL5LIB=$p5;];
    }

   if ($self->cloud_export_path){
           my $path = join q[:], @{ $self->cloud_export_path }, q[\$PATH]; ## no critic (ValuesAndExpressions::RequireInterpolationOfMetachars)
           #add before script name
           unshift @command, qq[ export PATH=$path;];
    }

   unshift @command, q[export REF_PATH=].$self->cloud_repository().q[;];

   if ($self->crams_in_s3()){
     push @command, q[--crams_in_s3 ];
  }
  }

  if ($self->local_cram()){
      push @command, q[--local_cram ];
  }

  if ($self->new_irods_path()){
      push @command, q[--new_irods_path];
  }

  if ($self->alt_process()){
      push @command, q[--alt_process], $self->alt_process();
  }
  if ($self->markdup_method()){
      push @command, q[--markdup_method], $self->markdup_method();
  }

  return {'rpt_list'  => $rpt_list,
          'command'   => join(q[ ], @command),
          'merge_obj' => $obj,
          'entities'  => $entities,
          'library'   => $library,
          };
}

=head2 reference_genome_path

Full path to reference genome used

=cut

has 'reference_genome_path' => (
     isa           => q[Str],
     is            => q[ro],
     predicate     => '_has_reference_genome_path',
     writer        => '_set_reference_genome_path',
    );


sub _get_reference_genome_path{
    my ($self, $c) = @_;

    if (!$c) {
        croak 'Composition attribute required';
    }
     $self->info(join q[ ], 'IN reference_genome_path', $c->freeze2rpt());

    my $l=st::api::lims->new(
        driver_type=>q[ml_warehouse_fc_cache],
        rpt_list => $c->freeze2rpt());

    return npg_tracking::data::reference->new(
                            rpt_list => $c->freeze2rpt(),
                            lims => $l,
                            )->refs()->[0];
}

=head2 _should_run_command

=cut

sub _should_run_command {
  my ($self, $command_hash, $to_kill) = @_;

  my $rpt_list = $command_hash->{'rpt_list'};
  my $command  = $command_hash->{'command'};
  my $base_obj = $command_hash->{'merge_obj'};

  # if (we have already successfully run a job for this set of components and metadata) {
  # - FIXME : need DB table for submission/running/completed tracking
  my $current_lsf_jobs = $self->_current_lsf_jobs();

  if (exists $current_lsf_jobs->{$rpt_list}){
      if ($self->verbose){
          carp q[Command already queued as Job ], $current_lsf_jobs->{$rpt_list}{'jobid'},qq[ $command];
      }
     return 0;
  }

  if ($self->_check_existance($rpt_list, $base_obj, $command_hash->{'library'},$command)){
           return 0;
  }

  # check safe to run - header and irods meta data
  if($self->_check_header($base_obj,$command_hash->{'entities'}) !=  $base_obj->composition->components_list()){
      carp qq[Header check passed count doesn't match component count for $rpt_list\n];
      return 0;
  }

  if ($self->local) {
    return 1;
  }

   if ($self->use_lsf) {
   ## look for sub or super set of rpt_list and if found set for killing
    my %new_rpts = map { $_ => 1 } split/;/smx,$rpt_list;

            while (my ($old_rpt_list,$hr) = each %{ $current_lsf_jobs }){
		   my $j_id   = $hr->{'jobid'};
                   my $status = $hr->{'status'};
	           my @rpts = split/;/smx,$old_rpt_list;
                   my @found = grep { defined $new_rpts{$_} } @rpts;
                   if (@found){
                      my $desc = qq[LSF job $j_id status $status. Change in library composition,found existing @found in rpt_list $rpt_list\n];
                      if ($status eq q[PEND]){
                         carp "Scheduled for killing. $desc\n";
                         ${$to_kill} = $j_id;
                       }
                      else { ##Don't kill jobs already running
		                     carp $desc;
                      }
                   }
               }
   }

return 1;
}


=head2 _check_header

=cut

sub _check_header {
    my $self = shift;
    my $merge_obj = shift;
    my $entities  = shift;

    $self->clear_first_cram_sample_name;
    $self->clear_first_cram_ref_name;

    my $cancount=0;

    foreach my $c ($merge_obj->composition->components_list()) {
        eval{
            my $query = {'irods_cram' => $self->standard_paths($c)->{'irods_cram'},
                         'sample_id'  => $entities->[0]->{'sample'},
                         'sample_acc' => $entities->[0]->{'sample_accession_number'},
                         'library_id' => $entities->[0]->{'library'},
            };
            if ($self->crams_in_s3){ $query->{'s3_cram'} = $self->standard_paths($c)->{'s3_cram'}; $cancount++  }
            else { $cancount += $self->can_run($query) }
            1;
        }or do {
            carp qq[Failed to check header : $EVAL_ERROR];
            next;
        };
    }
    return($cancount);
}


=head2 _check_existance

Check if this library composition already exists in iRODS

=cut

sub _check_existance {
  my ($self, $rpt_list, $base_obj,$library,$command) = @_;

  my @found = $self->irods->find_objects_by_meta($self->default_root_dir(),
    ['composition' => $base_obj->composition->freeze()],
    ['target' => 'library'],
    ['type' => 'cram']);

  if(@found >= 1){
    if ($self->verbose){ carp qq[Already done this $command] }
    return 1;
  }

   #standard behaviour should now be to skip if library exists, even if component count differs
   my @found_lib = $self->irods->find_objects_by_meta($self->default_root_dir(),
    ['library_id' => $library],
    ['target' => 'library'],
    ['type' => 'cram']);

  if(@found_lib >= 1 && !$self->force()){
      if ($self->verbose){
	my $count=0;
        foreach my $path (@found_lib){
            $count++;
            carp qq[Library $library already exists (with a different composition), $count, $path];
        }
      }
      return 1;
  }

    my $merge_dir = $base_obj->merge_dir();
    if ($self->_check_merge_completed($merge_dir)){
      carp q[Merge directory for ]. $base_obj->composition->digest() .
          qq[already exists, skipping\n];
      return 1;
    }

  return 0;
}


sub _check_merge_completed {
  my ($self, $merge_dir) = @_;
  return -e $merge_dir && -d $merge_dir && -e qq[$merge_dir/status/merge_completed] ? 1 : 0;
}

=head2 _lsf_job_submit

=cut

sub _lsf_job_submit {
  my ($self, $command) = @_;
  # suspend the job straight away
  my $time = DateTime->now(time_zone => 'local');
  my $job_name = 'cram_merge_' . $time;
  my $out = join q[/], $self->log_dir, $job_name . q[_];
  my $id; # catch id;

  my $LSF_RESOURCES  = q( -q ). $self->lsf_queue . q( -G ). $self->lsf_group
                     . q( -M64000 -R 'select[mem>64000] rusage[mem=64000,) . $self->token_name .q(=)
                     . $self->tokens_per_job() . q(] span[hosts=1] order[!-slots:-maxslots]' -n )
                     . $self->lsf_num_processors();
  if ($self->lsf_runtime_limit()){ $LSF_RESOURCES .= q( -W ) . $self->lsf_runtime_limit() }

  my $cmd = qq[bsub $LSF_RESOURCES -o $out] . '%J' . qq[ -J $job_name \"$command\" ];
  warn qq[\n***COMMAND: $cmd\n];
  my $fh = IO::File->new("$cmd|") ;

  if (defined $fh){
      while(<$fh>){
        if (/^Job\s+\<(\d+)\>/xms){ $id = $1 }
      }
      $fh->close;
   }
  return $id;
}

=head2 _wr_job_submit

=cut

sub _wr_job_submit {
    my ($self, $command) = @_;

my $s3_dir = q[s3_in];

my($sample,$study,$added);
if ($command =~ /sample_name\s+(.+)\s+\-\-sample_common/smx){ $sample = $1 }
if ($command =~ /study_name\s+(.+)\s+\-\-study_title/smx){ $study = $1 } #some study names contain spaces
    $sample  =~ s/['"\\]//smxg;
    $study   =~ s/['"\\]//smxg;
    my $wr_identifier = $study . q[_library_merge]; $wr_identifier =~ s/\s+/_/smxg; $wr_identifier =~ s/[()]//smxg;

##s3_dir contains sub-dirs for each rpt   $study/$sample/$rpt/
my $s3_path = qq[npg-cloud-realign-wip/$study/$sample];

   $command =~ s/\;/\\;/smxg;
   $command =~ s/\'/\\'/smxg;
   $command =~ s/\"/\\"/smxg;
   $command =~ s/[(]/\\(/smxg;
   $command =~ s/[)]/\\)/smxg;


my $cpus = $self->lsf_num_processors();
my $disk = $self->cloud_disk();

    my $mount_json = '[{"Mount":"npg-repository","Targets":[{"Path":"npg-repository","CacheDir":"/tmp/.ref_cache"}]}';
    if ($self->crams_in_s3){
       $mount_json .= qq[,{"Mount":"$sample/$s3_dir","Targets":[{"Path":"$s3_path","Write":false}],"Verbose":true}];
    }
       $mount_json .= ']';

    my $wr_cmd  = qq[wr  add -o 2 -r 0 -m 6G --cpus $cpus --disk $disk -i $wr_identifier -t 3h -p 15  --mount_json '$mount_json' --deployment production ];

     if ($self->cloud_cleanup_false()){
         $wr_cmd .= q[ --on_exit '[{"cleanup":false}]' --on_failure '[{"cleanup":false}]'];
     }
    my $cmd = q[ export HOME=].$self->cloud_home();
       $cmd .= qq[ && echo \$HOME && mkdir -p $sample && cd $sample && ];
       $cmd .= qq[ '$command' ];

warn "**Running $cmd | $wr_cmd**\n\n";
my $wr_fh = IO::File->new("echo '$cmd' | $wr_cmd 2>&1 |") or die "cannot run cmd\n";
     while(<$wr_fh>){ print or croak q[print fails running wr command];
        ##Added 0 new commands (1 were duplicates) to the queue using default identifier 'SEQCAP_DDD_MAIN_library_merge
        if (/Added\s+(\d+)\s+new\s+commands/smx){ $added = $1 }
     }
return $added;
}


=head2 _lsf_job_kill

=cut

sub _lsf_job_kill {
  my ($self, $job_id) = @_;
  # TODO check that this is our job

  my $cmd  = qq[brequeue -p -H $job_id ];
     $self->run_cmd($cmd);
  my $cmd2 = qq[bmod -Z "/bin/true" $job_id];
     $self->run_cmd($cmd2);
  my $cmd3 = qq[bkill $job_id];
     $self->run_cmd($cmd3);
  return;
}

=head2 _call_merge

=cut

sub _call_merge {
  my ($self, $command) = @_;

  my $success = 1;

  if ($self->use_lsf) {

    my $job_id = $self->_lsf_job_submit($command);
    if (!$job_id) {
      warn qq[Failed to submit to LSF '$command'\n];
      $success = 0;
    } else {
      warn qq[\tJOBID $job_id\n\n];
    }
  }

  if ($self->use_cloud){
      if (! $self->_wr_job_submit($command)){
         warn qq[WR command not added '$command'\n];
      }

  }
  return $success;
}

=head2 _check_host

Ensure that job does not get set off on a different cluster as checks for existing jobs would not work.

=cut

sub _check_host {
    my $self = shift;

    my $cluster;
    my $check = 0;
    if ($self->use_cloud()){ $check = 1 };

    my $fh = IO::File->new('lsid|') or croak "cannot check cluster name: $ERRNO\n";
    while(<$fh>){
	      if (/^My\s+cluster\s+name\s+is\s+(\S+)/smx){ $cluster = $1 }
    }
    if ($cluster eq $self->cluster){
        $check = 1;
    }else{
        carp "Host is $cluster, should run on ". $self->cluster ."\n";
    }
    return $check;
}


__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Cwd

=item Readonly

=item DateTime

=item DateTime::Duration

=item Try::Tiny

=item Moose

=item MooseX::StrictConstructor

=item File::Basename

=item WTSI::DNAP::Warehouse::Schema

=item WTSI::DNAP::Warehouse::Schema::Query::LibraryDigest

=item npg_tracking::glossary::rpt

=item npg_seq_melt::merge::base

=item File::Basename

=item npg_seq_melt::util::irods

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015,2016,2017,2018,2019,2021 GRL.

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
