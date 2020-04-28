package t::util;

use Moose;
use Carp;
use English qw{-no_match_vars};
use File::Temp qw{ tempdir };
use Readonly;
use Cwd qw(getcwd abs_path);
use IPC::Run qw(run);
use File::Which qw(which);


Readonly::Scalar our $TEMP_DIR => q{/tmp};
Readonly::Scalar our $IENV => q(ienv);

has q{cwd} => (
  isa => q{Str},
  is => q{ro},
  lazy_build => 1,
);

sub _build_cwd {
  my ( $self ) = @_;
  return getcwd();
}

# for getting a temporary directory which will clean up itself, and should not clash with other people attempting to run the tests
has q{temp_directory} => (
  isa => q{Str},
  is => q{ro},
  lazy_build => 1,
);
sub _build_temp_directory {
  my ( $self ) = @_;

  my $tempdir = tempdir(
    DIR => $TEMP_DIR,
    CLEANUP => 1,
  );
  return $tempdir;
}

has q{home} => (
  isa => q{HashRef},
  is => q{ro},
  lazy_build => 1,
);
sub _build_home{
    my ( $self ) = @_;

    my $out = q();
    my $cmd = which $IENV;
    if (not $cmd) {
           croak(qq(Command '$IENV' not found));
    }
    run [abs_path $cmd], q(>), \$out;
    ## handle either iRODS 3 or iRODS 4 format
    my ($key, $sep, $home) = $out =~ m/(irodsHome|irods_home)(=|\s-\s)(\S+)/smx;
    my ($key1, $sep1, $zone) = $out =~ m/(irodsZone|irods_zone_name)(=|\s-\s)(\S+)/smx;
    if ($home =~ /Sanger1-dev/){
        $home =~ s/Sanger1-dev/seq-dev/;
        $home .= q[#].$zone;
    }
    return { home => $home, zone => $zone };
}

1;
