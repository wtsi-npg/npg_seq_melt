package npg_seq_melt::merge;

use Moose;
use MooseX::StrictConstructor;
use English qw(-no_match_vars);
use Carp;
use Cwd qw/cwd/;

with qw{
  MooseX::Getopt
  npg_common::roles::log
  npg_common::roles::software_location
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
                        default       => 0,
                        writer        => '_set_local',
                        documentation =>
 'Boolean flag.' .
 'This flag is propagated to the script that performs the merge',
);


=head2 run_dir

=cut

has 'run_dir'  => (
    isa           => q[Str],
    is            => q[ro],
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
    default       => 0,
    documentation => q[Randomly choose between first and second iRODS cram replicate. Boolean flag, false by default],
);


=head2 default_root_dir

=cut

has 'default_root_dir' => (
    isa           => q[Str],
    is            => q[rw],
    default       => q{/seq/illumina/library_merge/},
    documentation => q[Allows alternative iRODS directory for testing],
    );

=head2 sample_acc_check

=cut 

has 'sample_acc_check' => (
    isa           => q[Bool],
    is            => q[ro],
    default       => 1,
    documentation => q[Checks that sample_accession_number present and matches cram header SM: field. Boolean flag, true by default],
    );


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
     default       => 0,
     documentation => q[Remove generated files from outdata directory post loading to iRODS],
);

=head2 samtools_executable

Allow path to different version of samtools to be provided

=cut

has 'samtools_executable' => (
    isa           => q[Str],
    is            => q[ro],
    documentation => q[Optionally provide path to different version of samtools],
    default       => q[samtools1],
);


=head2

minimum_component_count

=cut

has 'minimum_component_count' => ( isa           =>  'Int',
                                   is            =>  'ro',
                                   default       =>  6,
                                   documentation => q[ A merge should not be run if less than this number to merge],
);

=head2 irods_disconnect

Delete  WTSI::NPG::iRODS object to avoid baton processes 
remaining longer than necessary (limited iCAT connections available) 

=cut 

sub irods_disconnect{
    my $self  = shift;
    my $irods = shift;

    if (! $irods->isa(q[WTSI::NPG::iRODS])){
      croak q[Object to disconnect is not a WTSI::NPG::iRODS];
    }

   foreach my $k(keys %{$irods}){
        delete $irods->{$k};
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

=item English

=item Cwd

=item Moose

=item MooseX::StrictConstructor

=item MooseX::Getopt

=item npg_common::roles::log

=item npg_common::roles::software_location

=item npg_common::irods::iRODSCapable

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
