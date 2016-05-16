package npg_seq_melt::merge::generator;

use Moose;
use DateTime;
use DateTime::Duration;
use List::MoreUtils qw/any/;
use English qw(-no_match_vars);
use Readonly;
use Carp;
use Cwd qw/cwd/;
use IO::File;
use File::Basename qw/ basename /;
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

Boolean flag, false by default. If true, a merge is run despite
possible previous failures.

=cut
has 'force'        => ( isa           => 'Bool',
                        is            => 'ro',
                        default       => 0,
                        documentation =>
  'Boolean flag, false by default. ' .
  'If true, a merge is run despite possible previous failures.',
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

Number of seq_merge tokens per job (default 10), to limit number of jobs running simultaneously.

=cut

has 'tokens_per_job' => ( isa            => 'Int',
                           is            => 'ro',
                           default       => 7,
                           documentation => q[Number of seq_merge tokens per job (default 7). See bhosts -s ],
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
    my $cmd = basename($self->merge_cmd());
    my $fh = IO::File->new("bjobs -u srpipe -UF   | grep $cmd |") or croak "cannot check current LSF jobs: $ERRNO\n";
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

   ACXX   HiSeq V3
   ADXX   HiSeq 2500 rapid
   ALXX   HiSeqX V1
   ANXX   HiSeq V4
   BCXX   HiSeq 2500 V2 rapid
   CCXX   HiSeqX V2
   V2     MiSeq V2
   V3     MiSeq V3


=cut


sub _parse_chemistry{
    my $barcode = shift;

    my $suffix;
    if  (($barcode =~ /(V[2|3])$/smx) || ($barcode =~ /(\S{4})$/smx)){ $suffix = $1 }
         return(uc $suffix);
}


=head2 _validate_references

check same reference

=cut

sub _validate_references{
    my $entities = shift;
    my %ref_genomes=();
    map { $ref_genomes{$_->{'reference_genome'}}++ } @{$entities};
    if (scalar keys %ref_genomes > 1){ return 0 }
    return 1;
}

=head2 _validate_lims

=cut

sub _validate_lims {
  my $entities = shift;
  my $h = {};
  map { $h->{$_->{'id_lims'}} = 1; } @{$entities};
  return scalar keys %{$h} == 1;
}


=head2 _create_commands

=cut

sub _create_commands {## no critic (Subroutines::ProhibitExcessComplexity)

  my ($self, $digest) = @_;

  my @commands = ();

  foreach my $library (keys %{$digest}) {
    foreach my $instrument_type (keys %{$digest->{$library}}) {
      foreach my $run_type (keys %{$digest->{$library}->{$instrument_type}}) {

        my $studies = {};
        foreach my $e (@{$digest->{$library}->{$instrument_type}->{$run_type}->{'entities'}}) {
          push @{$studies->{$e->{'study'}}}, $e;
	     }

        foreach my $study (keys %{$studies}) {

          my $s_entities = $studies->{$study};

          my $fc_id_chemistry = {};
	          foreach my $e (@{$s_entities}){
                     my $chem =  _parse_chemistry($e->{'flowcell_barcode'});

                     ## no critic (ControlStructures::ProhibitDeepNests)
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

          if (scalar @completed < $self->minimum_component_count) {
            warn scalar @completed, qq[ entities for $library, skipping.\n];
            next;
          }

          if (!_validate_lims(\@completed)) {
            croak 'Cannot handle multiple LIM systems';
	        }

          if($completed[0]->{'id_lims'} ne $self->id_lims){
              next;
          }

          if (!_validate_references(\@completed)) {
            warn qq[Multiple reference genomes for $library, skipping.\n];
            next;
	        }

          if($self->sample_acc_check && !$completed[0]->{'sample_accession_number'}){
              warn qq[Sample accession required but library $library not accessioned\n];
              next;
          }

          ##use critic
          push @commands,
               $self->_command(\@completed, $library, $instrument_type, $run_type, $chemistry_code);

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

  my @command = ($self->merge_cmd);
  push @command, q[--rpt_list '] . $rpt_list . q['];
  push @command, qq[--library_id $library];
  push @command,  q[--sample_id], $entities->[0]->{'sample'};
  push @command,  q[--sample_name], $entities->[0]->{'sample_name'};

  my $sample_common_name = q['].$entities->[0]->{'sample_common_name'}.q['];
  push @command,  qq[--sample_common_name $sample_common_name];

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

  if ($self->use_irods) {
    push @command, q[--use_irods];
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
  }

  return {'rpt_list'  => $rpt_list,
          'command'   => join(q[ ], @command),
          'merge_obj' => $obj,
          'entities'  => $entities,
          };
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

  if ($self->_check_existance($rpt_list, $base_obj)){
       if (!$self->force){
           if ($self->verbose){ carp qq[Already done this $command] }
           return 0;
       }
  }

  # check safe to run - header and irods meta data
  if($self->use_irods){
      if($self->_check_header($base_obj,$command_hash->{'entities'}) !=  @{$base_obj->composition->components}){
          carp qq[Header check passed count doesn't match component count for $rpt_list\n];
          return 0;
      }
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

    foreach my $c (@{$merge_obj->composition->components}) {
        eval{
            my $paths = $self->standard_paths($c);
            my $query = {'cram'       => $paths->{'cram'},
                         'irods_cram' => $paths->{'irods_cram'},
                         'sample_id'  => $entities->[0]->{'sample'},
                         'sample_acc' => $entities->[0]->{'sample_accession_number'},
                         'library_id' => $entities->[0]->{'library'},
            };
            $cancount += $self->can_run($query);
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
  my ($self, $rpt_list, $base_obj) = @_;

  my @found = $self->irods->find_objects_by_meta($self->default_root_dir(),
    ['composition' => $base_obj->composition->freeze()],
    ['target' => 'library'],
    ['type' => 'cram']);

  if(@found >= 1){
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

  my $LSF_RESOURCES  = q(  -M6000 -R 'select[mem>6000] rusage[mem=6000,seq_merge=) . $self->tokens_per_job()
                     . q(] span[hosts=1]' -n ) . $self->lsf_num_processors();
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
  return $success;
}

=head2 _check_host

Ensure that job does not get set off on a different cluster as checks for existing jobs would not work.

=cut

sub _check_host {
    my $self = shift;
    my $cluster;
    my $fh = IO::File->new('lsid|') or croak "cannot check cluster name: $ERRNO\n";
    while(<$fh>){
	if (/^My\s+cluster\s+name\s+is\s+(\S+)/smx){ $cluster = $1 }
    }
    if ($cluster eq $CLUSTER){
        return 1;
    }
    carp "Host is $cluster, should run on $CLUSTER\n";
return 0;
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
