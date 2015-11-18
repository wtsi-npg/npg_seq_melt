package npg_seq_melt::merge;

use Moose;
use MooseX::StrictConstructor;
use English qw(-no_match_vars);
use Carp;
use npg_tracking::glossary::composition;
use npg_tracking::glossary::composition::component::illumina;
use Cwd qw/cwd/;


with qw{
  MooseX::Getopt
  npg_common::roles::log
  npg_common::roles::software_location
  npg_qc::autoqc::role::rpt_key
  npg_common::irods::iRODSCapable
  };

our $VERSION  = '0';

=head1 NAME

npg_seq_melt::merge

=head1 VERSION

$$

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS


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

Boolean flag. This flag is propagated to the script that performs the merge.
Not currently using database reporting.

=cut
has 'local'        => ( isa           => 'Bool',
                        is            => 'ro',
                        required      => 0,
                        default       => 0,
                        writer        => '_set_local',
                        documentation =>
 'Boolean flag.' .
 'This flag is propagated to the script that performs the merge',
);


=head2 load_only

Boolean flag, false by default.
Only run if existing directory & data not loaded

=cut

has 'load_only'      => (
    isa           => 'Bool',
    is            => 'ro',
    required      => 0,
    default       => 0,
    documentation => 'Boolean flag, false by default. ',
);

=head2 run_dir

=cut

has 'run_dir'  => (
    isa           => q[Str],
    is            => q[ro],
    required      => 0,
    default       => cwd(),
    documentation => q[Parent directory where sub-directory for merging is created, default is cwd ],
    );


=head2 use_irods

=cut
has 'use_irods' => (
     isa           => q[Bool],
     is            => q[ro],
     required      => 0,
     documentation => q[Flag passed to merge script to force use of iRODS for input crams/seqchksums rather than staging],
    );

=head2 random_replicate

Flag passed to merge script

=cut

has 'random_replicate' => (
    isa           => q[Bool],
    is            => q[ro],
    required      => 0,
    default       => 0,
    documentation => q[Randomly choose between first and second iRODS cram replicate. Boolean flag, false by default],
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



=head2 composition

=cut

has 'composition' => (
     isa         => q[npg_tracking::glossary::composition],
     is          => q[rw],
     required    => 0,
     documentation => q[npg_tracking::glossary::composition object],
   );


=head2 merge_dir

Directory where merging takes place

=cut
has 'merge_dir' => (
        is            => 'rw',
        isa           => 'Str',
        required      => 0,
        lazy_build      => 1,
        metaclass  => 'NoGetopt',
);
sub _build_merge_dir{
    my($self) = shift;
    return( join q[/],$self->run_dir(),$self->composition->digest() );
}


=head2 run_cmd

=cut

sub run_cmd {
    my $self = shift;
    my $start_cmd  = shift;

    my $cwd = cwd();
    $self->log("\n\nCWD=$cwd\nRunning ***$start_cmd***\n");
    eval{
         system("$start_cmd") == 0 or croak qq[system command failed: $CHILD_ERROR];
        }
        or do {
        carp "Error :$EVAL_ERROR";
        return 0;
        };
return 1;
}


=head2 remove_outdata

Remove files from outdata directory, post loading to iRODS

=cut

has 'remove_outdata' => (
     isa           => q[Bool],
     is            => q[ro],
     required      => 0,
     default       => 0,
     documentation => q[Remove generated files from outdata directory post loading to iRODS],
);

=head2 samtools_executable

Allow path to different version of samtools to be provided

=cut

has 'samtools_executable' => (
    isa           => q[Str],
    is            => q[ro],
    required      => 0,
    documentation => q[Optionally provide path to different version of samtools],
    default       => q[samtools1],
);


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

=item npg_tracking::glossary::composition

=item npg_tracking::glossary::composition::component::illumina

=item File::Basename

=item POSIX

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

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
