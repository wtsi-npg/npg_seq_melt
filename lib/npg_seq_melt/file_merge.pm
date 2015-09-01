package npg_seq_melt::file_merge;

use Moose;
use MooseX::StrictConstructor;
use DateTime;
use DateTime::Duration;
use List::MoreUtils qw/any/;
use English qw(-no_match_vars);
use Readonly;
use Carp;
use IO::File;
use WTSI::DNAP::Warehouse::Schema;
use WTSI::DNAP::Warehouse::Schema::Query::LibraryDigest;


with qw{
  MooseX::Getopt
  npg_common::roles::software_location
  npg_qc::autoqc::role::rpt_key
  npg_common::irods::iRODSCapable
  };

our $VERSION  = '0';

Readonly::Scalar my $MERGE_SCRIPT_NAME   => 'sample_merge.pl';
Readonly::Scalar my $LOOK_BACK_NUM_DAYS  => 7;
Readonly::Scalar my $HOURS  => 24;
Readonly::Scalar my $EIGHT  => 8;

Readonly::Scalar my $JOB_KILLED_BY_THE_OWNER => 'killed';
Readonly::Scalar my $JOB_SUCCEEDED           => 'succeeded';
Readonly::Scalar my $JOB_FAILED              => 'failed';

=head1 NAME

npg_seq_melt::file_merge

=head1 VERSION

$$

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

=head2 verbose

Boolean flag, switches on verbose mode, disabled by default

=cut
has 'verbose'      => ( isa           => 'Bool',
                        is            => 'ro',
                        required      => 0,
                        default       => 0,
                        writer        => '_set_verbose',
                        documentation =>
 'Boolean flag, false by default. Switches on verbose mode.',
);

=head2 local

Boolean flag. If true, no database record is created for a job,
this flag is propagated to the script that performs the merge.

=cut
has 'local'        => ( isa           => 'Bool',
                        is            => 'ro',
                        required      => 0,
                        default       => 0,
                        writer        => '_set_local',
                        documentation =>
 'Boolean flag. If true, no database record is created for a job, ' .
 'this flag is propagated to the script that performs the merge.',
);

=head2 dry_run

Boolean flag, false by default. Switches on verbose and local options and reports
what is going to de done without submitting anything for execution.

=cut
has 'dry_run'      => ( isa           => 'Bool',
                        is            => 'ro',
                        required      => 0,
                        default       => 0,
                        documentation =>
  'Boolean flag, false by default. ' .
  'Switches on verbose and local options and reports ' .
  'what is going to de done without submitting anything for execution',
);

=head2 max_jobs

Int. Limits number of jobs submitted.

=cut
has 'max_jobs'   => (isa           => 'Int',
                     is            => 'ro',
                     required      => 0,
                     documentation =>'Only submit max_jobs jobs (for testing)',
);

=head2 use_irods

=cut
has 'use_irods' => (
     isa           => q[Bool],
     is            => q[ro],
     required      => 0,
     documentation => q[Flag passed to merge script to force use of iRODS for input crams/seqchksums rather than staging],
    );


=head2 force

Boolean flag, false by default. If true, a merge is run despite
possible previous failures.

=cut
has 'force'        => ( isa           => 'Bool',
                        is            => 'ro',
                        required      => 0,
                        default       => 0,
                        documentation =>
  'Boolean flag, false by default. ' .
  'If true, a merge is run despite possible previous failures.',
);

=head2 interactive

Boolean flag, false by default. If true, the new jobs are left suspended.

=cut
has 'interactive'  => ( isa           => 'Bool',
                        is            => 'ro',
                        required      => 0,
                        default       => 0,
                        documentation =>
  'Boolean flag, false by default. ' .
  'if true the new jobs are left suspended.',
);

=head2 use_lsf

Boolean flag, false by default, ie the commands are not submitted to LSF for
execution.

=cut
has 'use_lsf'      => ( isa           => 'Bool',
                        is            => 'ro',
                        required      => 0,
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
                        required      => 0,
                        default       => $LOOK_BACK_NUM_DAYS,
                        documentation =>
  'Number of days to look back, defaults to seven',
);

=head2 default_root_dir

=cut

has 'default_root_dir' => (
    isa           => q[Str],
    is            => q[rw],
    required      => 0,
    default       => q{/seq/illumina/library_merge/},
    documentation => q[Allows alternative iRODS directory for testing],
    );

=head2 log_dir

Log directory - will be used for LSF jobs output.

=cut
has 'log_dir'      => ( isa           => 'Str',
                        is            => 'ro',
                        required      => 0,
                        documentation =>
  'Log directory - will be used for LSF jobs output.',
);


=head2 irods

irods connection (may be better to return to connection only at loading)

=cut

has 'irods' => (
     isa           => q[WTSI::NPG::iRODS],
     is            => q[ro],
     required      => 1,
     documentation => q[irods WTSI::NPG::iRODS object],
    );

=head2 _previous_jobid

for using with LSF -w 'done($jobid)'

=cut

has '_previous_jobid' => ( isa           => 'Maybe[Int]',
                           is            => 'rw',
                           required      => 0,
);

=head2 _mlwh_schema

=cut

has '_mlwh_schema' => ( isa           => 'WTSI::DNAP::Warehouse::Schema',
                        is            => 'ro',
                        required      => 0,
                        lazy_build    => 1,
);
sub _build__mlwh_schema {
  return WTSI::DNAP::Warehouse::Schema->connect();
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
                                 required   => 0,
);

=head2 id_run_list

Optional file name of list of run id's to use

=cut

has 'id_run_list'               =>  ( isa        => 'Str',
                                      is         => 'ro',
                                      required   => 0,
);

=head2 only_library_ids

Best to use in conjunction with specified --id_run_list or --id_runs unless it is known to fall within the cutoff_date.
Specifying look back --num_days is slower than supplying run ids. 

=cut

has 'only_library_ids'        =>  ( isa        => 'ArrayRef[Int]',
                                    is          => 'ro',
                                    required    => 0,
                                    documentation => q[restrict to certain library ids],
);

=head2 run

=cut

sub run {
  my $self = shift;

  $self->_update_jobs_status();
  my $digest;

  if ($self->id_runs()){
      $digest = WTSI::DNAP::Warehouse::Schema::Query::LibraryDigest->new(
      iseq_product_metrics => $self->_mlwh_schema->resultset('IseqProductMetric'),
      earliest_run_status  => 'qc complete',
      id_run => $self->id_runs(),
      library_id =>  $self->only_library_ids(),
      filter              => 'mqc',
  )->create();

  }
  else {
       $digest = WTSI::DNAP::Warehouse::Schema::Query::LibraryDigest->new(
       iseq_product_metrics => $self->_mlwh_schema->resultset('IseqProductMetric'),
       completed_after      => $self->_cutoff_date(),
       #completed_within     => [DateTime->new(year=>2015,month=>05,day=>1),DateTime->new(year=>2015,month=>12,day=>31)],
       earliest_run_status  => 'qc complete',
       library_id =>  $self->only_library_ids(),
       filter              => 'mqc',
       )->create();
  }


  my $cmd_count=0;
  my $num_libs = scalar keys %{$digest};
  warn qq[$num_libs libraries in the digest.\n];
  my $commands = $self->_create_commands($digest);
  foreach my $command ( @{$commands} ) {
    my $job_to_kill = 0;
    if ($self->max_jobs() && $self->max_jobs() == $cmd_count){ return }
    $cmd_count++;
    if ($self->_should_run_command($command->{rpt_list}, $command->{command}, \$job_to_kill)) {
      if ( $job_to_kill && $self->use_lsf) {
        warn qq[LSF job $job_to_kill will be killed\n];
        if ( !$self->local ) {
          $self->_lsf_job_kill($job_to_kill);
          $self->_update_job_status($job_to_kill, $JOB_KILLED_BY_THE_OWNER);
        }
      }

      warn qq[Will run command $command->{command}\n];
      if (!$self->dry_run) {
        $self->_call_merge($command->{command});
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

sub _create_commands {
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

          if (scalar @completed == 1) {
            warn qq[One entity for $library, skipping.\n];
            next;
          }

          if (!_validate_lims(\@completed)) {
            croak 'Cannot handle multiple LIM systems';
	        }

          if (!_validate_references(\@completed)) {
            warn qq[Multiple reference genomes for $library, skipping.\n];
            next;
	        }
          ##use critic
          push @commands, $self->_command(\@completed, $library, $instrument_type, $run_type, $chemistry_code);
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

  my @keys   = map { $_->{'rpt_key'} } @{$entities};
  my $rpt_list = join q[;], $self->sort_rpt_keys(\@keys);

  my @command = ($self->merge_cmd);
  push @command, q[--rpt_list '] . $rpt_list . q['];
  push @command, qq[--library_id $library];
  push @command,  q[--sample_id], $entities->[0]->{'sample'};
  push @command,  q[--sample_name], $entities->[0]->{'sample_name'};

  my $sample_common_name = q['].$entities->[0]->{'sample_common_name'}.q['];
  push @command,  qq[--sample_common_name $sample_common_name];

  if (defined $entities->[0]->{'sample_accession_number'}){
  push @command,  q[--sample_accession_number], $entities->[0]->{'sample_accession_number'};
   };

  push @command,  q[--study_id], $entities->[0]->{'study'};

  my $study_name = q['].$entities->[0]->{'study_name'}.q['];
  push @command,  qq[--study_name $study_name];

  my $study_title = q['].$entities->[0]->{'study_title'}.q['];

  push @command,  qq[--study_title $study_title];

  if (defined $entities->[0]->{'study_accession_number'}){
  push @command,  q[--study_accession_number], $entities->[0]->{'study_accession_number'};
   };
  push @command,  q[--aligned],$entities->[0]->{'aligned'};

  push @command, qq[--instrument_type $instrument_type];
  push @command, qq[--run_type $run_type];
  push @command, qq[--chemistry $chemistry ];

  if ($self->local) {
    push @command, q[--local];
  }

  if ($self->use_irods) {
    push @command, q[--use_irods];
  }

  return ({'rpt_list' => $rpt_list, 'command' => join q[ ], @command});
}


=head2 _should_run_command

=cut

sub _should_run_command {
  my ($self, $rpt_list, $command, $to_kill) = @_;

  # if (we have already successfully run a job for this set of components and metadata) {
  # - FIXME : need DB table for submission/running/completed tracking
  if (!$self->force && $self->_check_existance($rpt_list)){
     carp "Already done this $command";
     return 0;
  }

  if ($self->local) {
    return 1;
  }


  # if ($self->use_lsf) {
  #   if (the same or larger set is being merged) {
  # 	my $this_set_job_id;
  # 	if (metadata are the same) {
  # 	  warn "This $command is being run now";
  # 	  return 0;
  # 	} else {
  # 	  warn "Job $this_set_job_id is scheduled for killing, reason YYY";
  # 	  ${$to_kill} = $this_set_job_id;
  # 	  return 1;
  # 	}
  #   }

  #   if (a smaller set is being merged) {
  # 	my $smaller_job_id;
  # 	warn "Job $smaller_job_id is scheduled for killing, reason YYY";
  # 	${$to_kill} = $smaller_job_id;
  # 	return 1;
  #   }
  # }

  # if ( !$self->force
  #       and there were at least X(two or three) attempts to merge this set  with this metadata already
  #       and the latest X failed) {
  #   warn "This has failed X times in the past, not starting";
  #   return 0;
  # }

  return 1;
}


=head2 _check_existance

=cut

sub _check_existance {
  my ($self, $rpt_list) = @_;

  my @found = $self->irods->find_objects_by_meta($self->default_root_dir(), ['composition' => $rpt_list], ['target' => 'library'], ['type' => 'cram']);
  if(@found >= 1){
      return 1;
  }

  return 0;
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

  my $fh = IO::File->new("bsub -H -o $out" . '%J' ." -J $job_name \" $command\" |") ;
  if (defined $fh){
      while(<$fh>){
        if (/^Job\s+\<(\d+)\>/xms){ $id = $1 }
      }
      $fh->close;
   }
  return $id;
}


=head2 _lsf_job_resume

=cut

sub _lsf_job_resume {
  my ($self, $job_id) = @_;
  # check child error
  my $LSF_RESOURCES  = q[  -M6000 -R 'select[mem>6000] rusage[mem=6000,seq_irods=5]' ];
  my $lsf_wait = $self->_previous_jobid ? q[-w 'done(] . $self->_previous_jobid . q[)'] : q[];
     $LSF_RESOURCES .= $lsf_wait;
  my $cmd = qq[ bmod $LSF_RESOURCES $job_id ];
  warn qq[***COMMAND: $cmd\n];
  $self->run_cmd($cmd);
  my $cmd2 = qq[ bresume $job_id ];
  $self->run_cmd($cmd2);
  return;
}

=head2 run_cmd

=cut

sub run_cmd {
    my($self,$cmd) = @_;
    eval{
         system("$cmd") == 0 or croak qq[system command failed: $CHILD_ERROR];
     }
     or do {
     croak "Error :$EVAL_ERROR";
     };
return;
}

=head2 _lsf_job_kill

=cut

sub _lsf_job_kill {
  my ($self, $job_id) = @_;
  # TODO check that this is our job
  # TODO check child error
  system "bkill $job_id";
  return;
}

=head2 _call_merge

=cut

sub _call_merge {
  my ($self, $command) = @_;

  my $success = 1;

  if ($self->use_lsf) {
    # Might try 3 times
    my $job_id = $self->_lsf_job_submit($command);
    if (!$job_id) {
      warn qq[Failed to submit to LSF '$command'\n];
      $success = 0;
    } else {
      $self->_log_job($command, $job_id);
      if (!$self->interactive) {
        $self->_lsf_job_resume($job_id);
        $self->_previous_jobid($job_id);
      }
    }
  } else {
    $self->_log_job($command);
    my $exit_value = system $command;
    if ($exit_value) {
      $exit_value = $CHILD_ERROR >> $EIGHT;
      $success = 0;
      warn qq['$command' failed, error $exit_value\n];
    } else {
      warn qq['$command' succeeded\n];
    }
    $self->_update_job_status($command, $success ? $JOB_SUCCEEDED : $JOB_FAILED);
  }
  return $success;
}


=head2 _log_job 

=cut

sub _log_job {
  my ($self, $command, $job_id) = @_;
  if ($self->local) {
    return;
  }
  return;
}

=head2 _update_jobs_status

=cut

sub _update_jobs_status {
  my $self = shift;
  if ($self->local) {
    return;
  }
# for each job that is still running {
#   if (bhist command exists
#          and bhist knows about this job
#          and the job bhist knows about is our job) {
#      if (job status FAILED or SUCCESS) {
#        $self->_update_job_status($job_id);
#      }
#    }
# }
  return;
}


=head2 _update_job_status

=cut

sub _update_job_status {
  my ($self, $job_id_or_command, $status) = @_;
  if ($self->local || !$self->use_lsf) {
    warn qq[Not updating status of the jobs\n];
    return;
  }
  return;
}




__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Readonly

=item DateTime

=item DateTime::Duration

=item Try::Tiny

=item Moose

=item MooseX::StrictConstructor

=item WTSI::DNAP::Warehouse::Schema

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
