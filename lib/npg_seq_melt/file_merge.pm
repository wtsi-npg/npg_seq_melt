package npg_seq_melt::file_merge;

use Moose;
use MooseX::StrictConstructor;
use DateTime;
use DateTime::Duration;
use List::MoreUtils qw/any/;
use English qw(-no_match_vars);
use Readonly;
use Carp;

use npg_qc::autoqc::role::rpt_key;
use WTSI::DNAP::Warehouse::Schema;
use WTSI::DNAP::Warehouse::Schema::Query::LibraryDigest;

with qw/
  npg_common::roles::software_location
       /;

our $VERSION  = '0';

Readonly::Scalar my $MERGE_SCRIPT_NAME   => 'mmerge';
Readonly::Scalar my $LOOK_BACK_NUM_DAYS  => 7;

Readonly::Scalar my $JOB_KILLED_BY_THE_OWNER => 'killed';
Readonly::Scalar my $JOB_SUCCEEDED           => 'succeeded';
Readonly::Scalar my $JOB_FAILED              => 'failed';

=head1 NAME

npg_seq_melt::file_merge

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 merge_cmd

Merge command.

=cut

has 'merge_cmd'  =>  ( is            => 'ro',
                       isa           => 'NpgCommonResolvedPathExecutable',
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

Number of days to look back, defaults to one.

=cut
has 'num_days'     => ( isa           => 'Int',
                        is            => 'ro',
                        required      => 0,
                        default       => $LOOK_BACK_NUM_DAYS,
                        documentation =>
  'Number of days to look back, defaults to seven',
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

has '_mlwh_schema' => ( isa           => 'WTSI::DNAP::Warehouse::Schema',
                        is            => 'ro',
                        required      => 0,
                        lazy_build    => 1,
);
sub _build__mlwh_schema {
  return WTSI::DNAP::Warehouse::Schema->connect();
}

sub BUILD {
  my $self = shift;
  if ($self->dry_run) {
    $self->_set_local(1);
    $self->_set_verbose(1);
  }
  if ($self->use_lsf && !$self->log_dir) {
    croak 'LSF use enabled, log directory shoudl be defined';
  }
  return;
}

sub run {
  my $self = shift;

  $self->_update_jobs_status();

  my $digest = WTSI::DNAP::Warehouse::Schema::Query::LibraryDigest->new(
    iseq_product_metrics => $self->_mlwh_schema->resultset('IseqProductMetric'),
    completed_after      => $self->_cutoff_date(),
    #filter               => 'mqc',
  )->create();
  my $num_libs = scalar keys %{$digest};
  warn qq[$num_libs libraries in the digest.\n];

  my $commands = $self->_create_commands($digest);

  foreach my $command ( @{$commands} ) {
    my $job_to_kill = 0;
    if ($self->_should_run_command($command, \$job_to_kill)) {
      if ( $job_to_kill && $self->use_lsf) {
        warn qq[LSF job $job_to_kill will be killed\n];
        if ( !$self->local ) {
          $self->_lsf_job_kill($job_to_kill);
          $self->_update_job_status($job_to_kill, $JOB_KILLED_BY_THE_OWNER);
        }
      }
      warn qq[Will run command '$command'\n];
      if (!$self->dry_run) {
        $self->_call_merge($command);
      }
    }
  }

  return;
}

sub _cutoff_date {
  my $self = shift;
  my $d = DateTime->now();
  $d->subtract_duration(
    DateTime::Duration->new(hours => $self->num_days * 24));
  return $d;
}

sub _validate_chemistry {
  my $entities = shift;
  my @barcodes = map { $_->{'flowcell_barcode'} } @{$entities};
  # do something
  return 1;
}

sub _validate_study_and_lims {
  my $entities = shift;
  my $h = {};
  map { $h->{'study'}->{$_->{'study'}}  = 1;
        $h->{'lims'}->{$_->{'id_lims'}} = 1;
      } @{$entities};
  return (scalar keys %{$h->{'study'}} == 1) && (scalar keys %{$h->{'lims'}} == 1);
}

sub _create_commands {
  my ($self, $digest) = @_;

  my @commands = ();

  foreach my $library (keys %{$digest}) {
    foreach my $instrument_type (keys %{$digest->{$library}}) {
      foreach my $run_type (keys %{$digest->{$library}->{$instrument_type}}) {

        my $entities = $digest->{$library}->{$instrument_type}->{$run_type}->{'entities'};

        if ( any { exists $_->{'status'} && $_->{'status'} && $_->{'status'} =~ /archiv/smx } @{$entities} ) {
          warn qq[Will wait for other components of library $library to be archived.\n];
          next;
        }

        my @completed = grep
          { (!exists $_->{'status'}) || ($_->{'status'} && $_->{'status'} eq 'qc complete') }
	                @{$entities};

        if (!@completed) {
          croak qq[No qc complete libraries - should not happen at this stage.\n];
	}

        if (scalar @completed == 1) {
          warn qq[One entity for $library, skipping.\n];
          next;
        }
        
        if (!_validate_study_and_lims(\@completed)) {
          croak 'Cannot handle multiple studies or LIM systems';
	}
        if (!_validate_chemistry(\@completed)) {
          croak 'Cannot handle multiple chemistries';
	}
        
        push @commands, $self->_command(\@completed, $library, $instrument_type, $run_type);
      }
    }
  }

  return \@commands;
}

sub _command {
  my ($self, $entities, $library, $instrument_type, $run_type) = @_;

  my @keys = map { $_->{'rpt_key'} } @{$entities};

  my @command = ($self->merge_cmd);
  push @command, qq[--rpt_list '] . join(q[;], @keys) . q['];
  push @command, qq[--library $library];
  push @command,  q[--sample], $entities->[0]->{'sample'};
  push @command,  q[--study], $entities->[0]->{'study'};
  push @command, qq[--instrument_type $instrument_type];
  push @command, qq[--run_type $run_type];

  if ($self->local) {
    push @command, q[--local];
  }

  return join q[ ], @command;
}

sub _should_run_command {
  my ($self, $command, $to_kill) = @_;

  if ($self->local) {
    return 1;
  }

  # if (we have already successfully run a job for this set of components and metadata) {
  #   warn "Already done this $command";
  #   return 0;
  # }

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

sub _lsf_job_submit {
  my ($self, $command) = @_;
  # suspend the job staright away
  # need a good job name
  my $time = DateTime->now();
  my $job_name = 'cram_merge_' . $time;
  my $out = join q[/], $self->log_dir, $job_name . q[_];
  my $id; # catch id;
  system "bsub -H -o $out" . '%J' . " -J $job_name $command";
  return $id;
}

sub _lsf_job_resume {
  my ($self, $job_id) = @_;
  # check child error
  system "bresume $job_id";
  return;
}

sub _lsf_job_kill {
  my ($self, $job_id) = @_;
  # check that this is our job
  # check child error
  system "bkill $job_id";
  return;
}

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
      }
    }
  } else {
    $self->_log_job($command);
    my $exit_value = system $command;
    if ($exit_value) {
      $exit_value = $CHILD_ERROR >> 8;
      $success = 0;
      warn qq['$command' failed, error $exit_value\n];
    } else {
      warn qq['$command' succeeded\n];
    }
    $self->_update_job_status($command, $success ? $JOB_SUCCEEDED : $JOB_FAILED);
  }
  return $success;
}

sub _log_job {
  my ($self, $command, $job_id) = @_;
  if ($self->local) {
    return;
  }
  return;
}

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
