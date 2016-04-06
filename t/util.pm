package t::util;

use Moose;
use Carp;
use English qw{-no_match_vars};
use File::Temp qw{ tempdir };
use Readonly;
use Cwd qw(getcwd);

Readonly::Scalar our $TEMP_DIR => q{/tmp};

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


1;
