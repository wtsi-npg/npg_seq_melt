package npg_seq_melt::daemon::libmerge;

use Moose;
use MooseX::StrictConstructor;
use Carp;
use Readonly;
use English qw/-no_match_vars/;
use Try::Tiny;
use FindBin qw($Bin);
use List::MoreUtils qw/uniq/;
use Log::Log4perl;
use npg_tracking::util::abs_path qw/abs_path/;
use WTSI::DNAP::Warehouse::Schema;

with qw{ 
         MooseX::Getopt
         npg_seq_melt::util::log
};



our $VERSION = '0';

Readonly::Scalar my $GENERATOR_SCRIPT       => q{npg_run_merge_generator};
Readonly::Scalar my $MERGE_SCRIPT          => q{npg_library_merge};
Readonly::Scalar my $PATH_DELIM            => q{:};
Readonly::Scalar my $SLEEP_TIME  => 900;

has 'sleep_time' => (
  isa        => q{Int},
  is         => q{ro},
  required   => 0,
  default    => $SLEEP_TIME,
  documentation => "sleep interval, default $SLEEP_TIME seconds",
);

has 'dry_run' => (
  isa        => q{Bool},
  is         => q{ro},
  required   => 0,
  default    => 0,
  documentation => 'dry run mode flag, false by default',
);

has 'mlwh_schema' => (
  isa        => q{WTSI::DNAP::Warehouse::Schema},
  is         => q{ro},
  metaclass  => 'NoGetopt',
  lazy_build => 1,
);
sub _build_mlwh_schema {
  return WTSI::DNAP::Warehouse::Schema->connect();
}

has 'config_path' => (
 isa        => q{Str},
 is         => q{ro},
 required   => 0,
 documentation => 'Path to config file. Default is data one level up from the bin directory',
 default    => join q[/],$Bin,q[..],q[data],
);

has 'library_merge_conf' => (
  isa        => q{ArrayRef},
  is         => q{ro},
  lazy_build => 1,
  metaclass  => 'NoGetopt',
  init_arg   => undef,
);
sub _build_library_merge_conf {
  my $self = shift;

  my $config = [];
  try {
    $config = $self->_read_config(q{npg_seq_melt});
  } catch {
    $self->logger->warn(qq{Failed to retrieve library merge configuration: $_});
  };

  return $config;
}

sub _read_config{
    my $self = shift;
    my $file = shift;
my $p = $self->config_path . q[/] . $file;
my $config_file = abs_path($p);
$self->logger->warn("Looking for configuration file $config_file");
my ($config) = Config::Auto::parse($config_file,format=>q{yaml});
return $config;
}



has 'analysis_dir_prefix' => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
  metaclass  => 'NoGetopt',
  init_arg   => undef,
);
sub _build_analysis_dir_prefix {
    my ($self) = @_;

    my $config =  $self->library_merge_conf();
    my $dir;
    foreach my $c (@{$config}){
     	if ($c->{'analysis_dir'}){ $dir = $c->{'analysis_dir'} }
    }

    if (!-d $dir) {
     croak qq{Directory '$dir' does not exist};
    }
    return $dir;
}

has 'software' => (
  isa        => q{Str},
  is         => q{ro},
  required   => 0,
  lazy_build => 1,
  metaclass  => 'NoGetopt',
  init_arg   => undef,
);
sub _build_software {
  my ($self) = @_;
  my $config =  $self->library_merge_conf();
  my $software;

  foreach my $c (@{$config}){
	if ($c->{'software'}){ $software = $c->{'software'} }
  }

    if ($software && !-d $software) {
    croak qq{Directory '$software' does not exist};
  }

  return $software ? abs_path($software) : q[];
}


sub study_from_name {
    my ($self,$study_name) = @_;
    if (!$study_name) {
      croak q[Need study name];
    }
    return map {$_->id_study_lims() } $self->mlwh_schema->resultset(q[Study])->search(
         { name =>{'like' => $study_name} },
    )->all();
}


sub run {
  my $self = shift;

 my $config =  $self->library_merge_conf();

 foreach my $c (@{$config}){

    next if ! $c->{'study_name'};

    my $study_name = $c->{'study_name'};
       $study_name =~ s/\\//sxmg;

    if ($study_name =~ /\%/sxm){
       $self->logger->warn(qq{Skipping $study_name as need extra checks for wild card study name query});
       next;
    }
   foreach my $study ($self->study_from_name($study_name) ){
      my $analysis_dir = join q[/], $self->analysis_dir_prefix , qq[study_${study}_library_merging];
         $analysis_dir = abs_path($analysis_dir);

    try {
      $self->_process_one_study($study,$c,$analysis_dir);
    } catch {
      $self->logger->warn(
        sprintf 'Error processing study %i: %s', $study, $_ );
    };
   }
 }
  return;
}

sub _process_one_study {
  my ($self, $study, $config, $analysis_dir) = @_;

  if (! -e qq{$analysis_dir/log}){
     my $cmd = qq{mkdir -p $analysis_dir/log};
     my $output = qx{$cmd};
  }

  $self->logger->info(qq{Considering study $study $analysis_dir});


  my $arg_refs = {};
  $arg_refs->{'id_study_lims'} = $study;
  $arg_refs->{'generator_script'} = $GENERATOR_SCRIPT;
  $arg_refs->{'merge_script'} = $MERGE_SCRIPT;
  $arg_refs->{'analysis_dir'} = $analysis_dir;
  $arg_refs->{'minimum_component_count'} = $config->{'minimum_component_count'};
  $arg_refs->{'dry_run'} = $self->dry_run ? 1 : 0;
  $arg_refs->{'software'} = $self->software;

  $self->run_command( $study, $self->_generate_command( $arg_refs ));

  return;
}

sub run_command {
  my ( $self, $study, $cmd ) = @_;

  $self->logger->info(qq{COMMAND: $cmd});
  my ($output, $error);

  if (!$self->dry_run) {
    $output = qx($cmd);
    $error  = $CHILD_ERROR;
  }
  if ($error) {
    $self->logger->warn(
      qq{Error $error occurred. Will try $study again on next loop.});
  }

  if ($output) {
    $self->logger->info(qq{COMMAND OUTPUT: $output});
  }

  return;
}


sub local_path {
  my $self = shift;
  my $perl_path = "$EXECUTABLE_NAME";
  $perl_path =~ s/\/perl$//xms;
  return ( abs_path($Bin), abs_path($perl_path));
}

##########
# Remove from the PATH the bin the daemon is running from
#
sub _clean_path {
  my ($self, $path) = @_;
  my $bin = abs_path($Bin);
  my @path_components  = split /$PATH_DELIM/smx, $path;
  return join $PATH_DELIM, grep { abs_path($_) ne $bin} @path_components;
}

sub _generate_command {
  my ( $self, $arg_refs ) = @_;

  my $cmd = sprintf ' %s --merge_cmd %s --use_lsf --use_irods --log_dir %s --run_dir %s',
             $arg_refs->{'generator_script'},
             $arg_refs->{'merge_script'},
             $arg_refs->{'analysis_dir'} . q[/log],
             $arg_refs->{'analysis_dir'};


    if ($arg_refs->{'minimum_component_count'}){
       $cmd .= q{ --minimum_component_count } . $arg_refs->{'minimum_component_count'};
    }
    if ($arg_refs->{'dry_run'}){
       $cmd .= q{ --dry_run };
    }
     $cmd .= q{ --id_study_lims }  . $arg_refs->{'id_study_lims'};

  my $path = join $PATH_DELIM, $self->local_path(), $ENV{'PATH'};

  my $libmerge_path_root = $arg_refs->{'software'};

  if ($libmerge_path_root) {
    $path = join $PATH_DELIM, "${libmerge_path_root}/bin", $self->_clean_path($path);
  }
   $cmd = qq{export PATH=$path; $cmd};

  if ($libmerge_path_root) {
    $cmd = join q[; ],
           qq[export PERL5LIB=${libmerge_path_root}/lib/perl5],
           $cmd;
  }
  return $cmd;
}


sub loop {
  my $self = shift;

  my $sleep = $self->sleep_time;
  my $class = ref $self;
  while (1) {
    try {
      $self->logger->info(qq{$class running});
      if ($self->dry_run) {
        $self->logger->info(q{DRY RUN});
      }
      $self->run();
    } catch {
      $self->logger->warn(qq{Error in $class : $_} );
    };
    $self->logger->info(qq{Going to sleep for $sleep secs});
    sleep $sleep;
  }

  return;
}


no Moose;
__PACKAGE__->meta->make_immutable;
1;

__END__

=head1 NAME

npg_seq_melt::daemon::libmerge

=head1 SYNOPSIS

  my $runner = npg_seq_melt::daemon::libmerge->new();
  $runner->loop();

=head1 DESCRIPTION

Runner for the library merging pipeline.

=head1 SUBROUTINES/METHODS

=head2 library_merge_conf

Returns an array ref of library merge configuration details.
If the configuration file is not found or is not readable,
an empty array is returned.

=head2 analysis_dir_prefix

Taken from config file

=head2 software

Taken from config file. Optional.

=head2 study_from_name

Returns array of id_study_lims

=head2 run

Invokes the library merging generator script for studies
specified in the library_merge.yml config file

=head2 loop

An indefinite loop of calling run() method with sleep_time pauses
between the repetitions. Any errors in the run() method are
captured and printed to the log.

=head2 local_path

Returns a list with paths to bin the code is running from
and perl executable the code is running under

=head2 run_command

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Try::Tiny

=item Readonly

=item Carp

=item List::MoreUtils

=item npg_tracking::util::abs_path

=item Config::Auto

=item npg_seq_melt::util::log

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016 Genome Research Ltd.

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
