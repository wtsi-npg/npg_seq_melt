package npg_seq_melt::merge;

use Moose;
use MooseX::StrictConstructor;
use English qw(-no_match_vars);
use Carp;
use Cwd qw/cwd/;
use File::Basename qw/ basename /;

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


=head2 use_irods

=cut
has 'use_irods' => (
     isa           => q[Bool],
     is            => q[ro],
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

    if($self->verbose){
        $self->log("Disconnecting from iRODS\n");
    }

   foreach my $k(keys %{$irods}){
        delete $irods->{$k};
    }
    return;
}


=head2 standard_paths

=cut

sub standard_paths {

    my $self = shift;
    my $c    = shift;

    if (!$c) {
        croak 'Component attribute required';
    }

    my $filename = $c->filename(q[.cram]);
    my $path     = join q[/],q[/seq], $c->id_run, $filename;
    my $paths    = {'cram' => $path, 'irods_cram' => $path};

    return $paths;

}


=head2 _first_cram_sample_name

Store the sample name from the first seen cram file

=cut

has '_first_cram_sample_name' => (
     isa           => q[Str],
     is            => q[ro],
     predicate     => '_has_first_cram_sample_name',
     writer        => '_set_first_cram_sample_name',
     clearer       => '_clear_first_cram_sample_name',
     init_arg      => undef,
  );

=head2 _first_cram_ref_name

Store the ref name from the first seen cram file

=cut

has '_first_cram_ref_name' => (
     isa           => q[Str],
     is            => q[ro],
     predicate     => '_has_first_cram_ref_name',
     writer        => '_set_first_cram_ref_name',
     clearer       => '_clear_first_cram_ref_name',
     init_arg      => undef,
);


=head2 can_run

Check headers and iRODS meta data.

=cut

sub can_run {

    my $self      = shift;
    my $query     = shift;

    if (!$query->{'irods_cram'} || ! $self->irods()){
        croak 'Not all required attributes defined';
    }

    my @irods_meta = $self->irods->get_object_meta($query->{'irods_cram'});
    $query->{'irods_meta'} = \@irods_meta;

    if(!$self->_check_cram_header($query)){
        return 0;
    }
    return 1;
}


=head2 _check_cram_header

1. Check that appropriate commands have been run in PG line (currently used for HiSeqX)
i.e. bamsort with adddupmarksupport
2. Sample name in SM field in all cram headers should be consistent 
3. Library id in cram header should match that in the imeta data 
4. UR field of SQ row should be consistent across samples and should match the that returned by npg_tracking::data::reference (s/fasta/bwa/) 

=cut

sub _check_cram_header { ##no critic (Subroutines::ProhibitExcessComplexity)
    my $self      = shift;
    my $query     = shift;

    if (!$query->{'cram'}|| !$query->{'sample_id'} || !$query->{'library_id'} || !$query->{'irods_meta'}) {
        croak 'Not all required attributes defined';
    }
    my @sample_id  = map { $_->{value} => $_ } grep { $_->{attribute} eq 'sample_id' }  @{$query->{'irods_meta'}};

    if ($sample_id[0] ne $query->{'sample_id'}) {
        croak 'Supplied sample id does not match irods ' .
            "( $query->{'sample_id'} vs $sample_id[0])\n";
    }

    my $samtools_view_cmd =  $self->samtools_executable() . qq[ view -H irods:$query->{'cram'} |];
    if ($query->{'cram'} !~ /^\/seq\//xms){ $samtools_view_cmd =~ s/irods://xms }

    my @imeta_library_id;
    my $header_info = {};
    my $adddup=0;
    my $sample_problems=0;
    my $library_problems=0;
    my $reference_problems=0;
    my $first_sq_line=1;

    my $first_sample_name = $self->_first_cram_sample_name;
    my $first_ref_name    = $self->_first_cram_ref_name;

    my $fh = IO::File->new($samtools_view_cmd) or croak "Error viewing cram header: $OS_ERROR\n";

    ##no critic (ControlStructures::ProhibitDeepNests)
    while(<$fh>){
        chomp;
        my @fields = split /\t/smx;

        if(/^\@PG/smx){
            foreach my $field (@fields){
               if ($field  =~ /^CL:(\S+)/smx){
                   if ($field =~ /bamsort.*\s+adddupmarksupport=1/xms){ $adddup=1 };
	             }
	          }
	      }

        if(/^\@RG/smx){
            foreach my $field (@fields) {
                if ($field  =~ /^SM:(\S+)/smx){
                    my $header_sample_name  = $1;
                    if($self->verbose()){$self->log("SM:$header_sample_name");}
                    if ($self->sample_acc_check()){
                        ##sample_accession_number must match header_sample_name
			                  if ($header_sample_name ne $query->{'sample_acc'}){
                            carp 'Header sample name does not match sample accession number:' .
                                "$header_sample_name ", $query->{'sample_acc'},"\n";
                            $sample_problems++;
                        }
                    }
                    # comparing against first usable cram header in list
                    if (defined $first_sample_name) {
                        if ($header_sample_name ne $first_sample_name) {
                            carp 'Header sample names are not consistent across samples: ' .
                                "$header_sample_name $first_sample_name\n";
                            $sample_problems++;
                        }
                    } else {
                        $header_info->{'sample_name'} = $header_sample_name;
                    }
                }elsif($field =~ /^LB:(\d+)/smx) {
                    my $header_library_id = $1;
                    @imeta_library_id = map { $_->{'value'} => $_ }
                    grep { $_->{'attribute'} eq 'library_id' } @{$query->{'irods_meta'}};

                    if ($self->verbose()) {
                        $self->log("LIBRARY IMETA:$imeta_library_id[0] HEADER:$header_library_id");
                    }
                    if ($imeta_library_id[0] ne $header_library_id) {
                        carp 'library id in LIMS and header do not match : ' .
                            "$imeta_library_id[0] vs $header_library_id\n";
                        $library_problems++;
                    }
                }
            }
        }

        if(/^\@SQ/smx && $first_sq_line) {
      	    $first_sq_line=0;
            foreach my $field (@fields){
                if ($field  =~ /^UR:(\S+)/smx) {
                    my $header_ref_name  = $1;
                    # comparing against first usable cram header in list
                    if (defined $first_ref_name) {
                        if (basename($header_ref_name) ne basename($first_ref_name)) {
                            carp 'Header reference paths are not consistent across samples: ' .
                                "$header_ref_name $first_ref_name\n";
                            $reference_problems++;
		                    }
		                } else {
                        $header_info->{'ref_name'} = $header_ref_name;
		                }

                    if(defined $query->{'ref_path'}){
                        if (basename($query->{'ref_path'}) ne basename($header_ref_name)) {
                            carp 'Header reference path does not match npg_tracking reference: ' .
                                $query->{'ref_path'} ." $header_ref_name\n";
                            $reference_problems++;
                        }
                    }
	              }
      	    }
        }
    }

    if (! $adddup){
        carp "Cram header checked: $query->{'cram'} has not had bamsort with " .
             "adddupmarksupport=1 run. Skipping this run\n";
        return 0;
    }

    if ($imeta_library_id[0] ne $query->{'library_id'} ) {
        carp 'Supplied library id does not match irods ' .$query->{'library_id'}.
            "( vs  $imeta_library_id[0]) \n";
        return 0;
    }


    if ($sample_problems or $library_problems or $reference_problems) {
        return 0;
    }

    ## set first_cram_sample_name and first_cram_ref_name if no problems
    if(defined $header_info->{'ref_name'} && !$self->_has_first_cram_ref_name) {
        $self->_set_first_cram_ref_name($header_info->{'ref_name'});
    }
    if(defined $header_info->{'sample_name'} && !$self->_has_first_cram_sample_name) {
        $self->_set_first_cram_sample_name($header_info->{'sample_name'});
    }

    return 1;

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

=item File::Basename

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
