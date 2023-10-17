package npg_seq_melt::merge;

use Moose;
use MooseX::StrictConstructor;
use English qw(-no_match_vars);
use Carp;
use Cwd qw/cwd/;
use Readonly;
use File::Basename qw/ basename /;
use npg_pipeline::product;

with qw{
  MooseX::Getopt
  WTSI::DNAP::Utilities::Loggable
  npg_common::roles::software_location
  npg_seq_melt::util::irods
};

our $VERSION  = '0';

Readonly::Scalar my $ERROR_VALUE_SHIFT => 8;
Readonly::Scalar my $DEFAULT_DUP_METHOD => q[samtools];
Readonly::Scalar my $ORIG_DUP_METHOD => q[biobambam];

Readonly::Array  my @DUP_METHODS => ($DEFAULT_DUP_METHOD, $ORIG_DUP_METHOD);

=head1 NAME

npg_seq_melt::merge

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
    lazy_build    => 1,
    documentation => q[Allows alternative iRODS directory for testing],
    );

sub _build_default_root_dir{
    my $self = shift;
    return $self->irods_root . q{/illumina/library_merge/};
}

=head2 sample_acc_check

=cut 

has 'sample_acc_check' => (
    isa           => q[Bool],
    is            => q[ro],
    default       => 1,
    documentation => q[Checks that sample_accession_number present and matches cram header SM: field. Boolean flag, true by default],
    );

=head2 reheader_rt_ticket

=cut

has 'reheader_rt_ticket' => (
     isa           => q[Str],
     is            => q[ro],
     documentation => q[ used where --nosample_acc_check set and imeta contains the sample accession number],
    );


=head2 local_cram

=cut

has 'local_cram' => (
    isa           => q[Bool],
    is            => q[ro],
    default       => 0,
    documentation => q[Writes the output cram locally before loading to iRODS, rather than streaming with tears. Boolean flag, false by default],
    );

=head2 run_cmd

Run the given command, return 1 if successful, 0 if an error
occurs in the child process. Log both the command and error
(if any).

=cut

sub run_cmd {
    my $self = shift;
    my $start_cmd  = shift;

    my $cwd = cwd();
    $self->info("\n\nCWD=$cwd\nRunning ***$start_cmd***\n");

    my $err = 0;
    if (system "$start_cmd") {
        $err = $CHILD_ERROR >> $ERROR_VALUE_SHIFT;
        $self->error(qq[System command ***$start_cmd*** failed with error $err]);
    }

    return $err ? 0 : 1;
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
    default       => q[samtools],
);


=head2

minimum_component_count

=cut

has 'minimum_component_count' => (
    isa           =>  'Int',
    is            =>  'ro',
    default       =>  6,
    documentation => q[ A merge should not be run if less than this number to merge],
    );

=head2 

new_irods_path

=cut

has 'new_irods_path' => (
    isa           => q[Bool],
    is            => q[ro],
    documentation => q[For paths such as /seq/illumina/runs/29/29226/lane1/plex28],
    );

=head2

alt_process

=cut

has 'alt_process'  => (
    isa           => q[Str],
    is            => q[ro],
    default       => q[],
    documentation => q[For paths such as /seq/illumina/runs/29/29226/lane1/plex28/DD2023I],
    );

=head2

markdup_method

=cut

has 'markdup_method'  => (
    isa           => q[Str],
    is            => q[ro],
    default       => q[samtools],
    documentation => q[Default duplicate marking is samtools. Alternatively biobambam can be specified and run where appropriate.],
    );

=head2 standard_paths

=cut

sub standard_paths {

    my $self = shift;
    my $c    = shift;

    if (!$c) {
        croak 'Component attribute required';
    }

    my $rpt_list = join q[:],$c->id_run,$c->position,$c->tag_index;
    my $p        = npg_pipeline::product->new(rpt_list => $rpt_list);
    my $filename = $p->file_name(ext =>'cram');
    my $path     = join q[/],$self->irods_root, $c->id_run, $self->alt_process || (), $filename;
    my $paths    = {'irods_cram' => $path};


    if ($self->new_irods_path){
      my $subpath = $p->dir_path(); #e.g. lane6/plex147 for single rpt 
      my $run = $c->id_run;
      my $index = substr $run,0,2;
      $path = join q[/],$self->irods_root,q[illumina/runs],$index,$run,$subpath, $self->alt_process || (), $filename;
      $self->info(join q[ ],q[irods_cram],$path);
      $paths = {'irods_cram' => $path};
    }

    if ($self->crams_in_s3){
      my $rpt = $filename; $rpt =~ s/[.]cram//smx;
      ###/tmp/wr_cwd/f/8/4/9861c532bf76a93c223863d07cdb6309050632/cwd/DDD_MAIN5251086/s3_in/7849_3#7/7849_3#7.cram
      my $s3_path  = join q[/],$self->run_dir(),q[s3_in],$rpt,$filename;
          $paths->{'s3_cram'} = $s3_path;
    };

    return $paths;
}


=head2 _first_cram_sample_name

Store the sample name from the first seen cram file

=cut

has 'first_cram_sample_name' => (
     isa           => q[Str],
     is            => q[rw],
     predicate     => 'has_first_cram_sample_name',
     writer        => 'set_first_cram_sample_name',
     reader        => 'get_first_cram_sample_name',
     clearer       => 'clear_first_cram_sample_name',
     init_arg      => undef,
     metaclass     => 'NoGetopt',
  );

=head2 _first_cram_ref_name

Store the ref name from the first seen cram file

=cut

has 'first_cram_ref_name' => (
     isa           => q[Str],
     is            => q[rw],
     predicate     => 'has_first_cram_ref_name',
     writer        => 'set_first_cram_ref_name',
     reader        => 'get_first_cram_ref_name',
     clearer       => 'clear_first_cram_ref_name',
     init_arg      => undef,
     metaclass     => 'NoGetopt',
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

    ###temp for remapped crams in S3 with bam only in iRODS
    if ($self->crams_in_s3){
       if (!$self->irods->is_object($query->{'irods_cram'})){
        $query->{'irods_cram'} =~ s/cram$/bam/xms;
       }
    }

    my $markdup_method = $self->markdup_method;
    if (! grep( /^$markdup_method$/, @DUP_METHODS ) ) {
       croak "Markdup method specified $markdup_method is not supported";
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
i.e. bamsort with adddupmarksupport (bamsormadup from 20161109)
2. Sample name in SM field in all cram headers should be consistent 
3. Library id in cram header should match that in the imeta data 
4. UR field of SQ row should be consistent across samples and should match the that returned by npg_tracking::data::reference (s/fasta/bwa/) 

=cut

sub _check_cram_header { ##no critic (Subroutines::ProhibitExcessComplexity)
    my $self      = shift;
    my $query     = shift;

    if (!$query->{'irods_cram'}|| !$query->{'sample_id'} || !$query->{'library_id'} || !$query->{'irods_meta'}) {
        croak 'Not all required attributes defined';
    }
    my @sample_id  = map { $_->{value} => $_ } grep { $_->{attribute} eq 'sample_id' }  @{$query->{'irods_meta'}};

    if ($sample_id[0] ne $query->{'sample_id'}) {
        croak 'Supplied sample id does not match irods ' .
            "( $query->{'sample_id'} vs $sample_id[0])\n";
    }

    my $root = $self->irods_root();

    my $cram = $query->{'s3_cram'} ? $query->{'s3_cram'} :
                ($query->{'irods_cram'} =~ /^$root/xms) ? qq[irods:$query->{'irods_cram'}] :
                $query->{'irods_cram'};

    my $samtools_view_cmd =  $self->samtools_executable() . qq[ view -H $cram |];

    my @imeta_library_id;
    my $header_info = {};
    my $adddup=0;
    my $sample_problems=0;
    my $library_problems=0;
    my $reference_problems=0;
    my $first_sq_line=1;

    my $first_sample_name = $self->get_first_cram_sample_name;
    my $first_ref_name    = $self->get_first_cram_ref_name;

    my $fh = IO::File->new($samtools_view_cmd) or croak "Error viewing cram header: $OS_ERROR\n";

    ##no critic (ControlStructures::ProhibitDeepNests)
    while(<$fh>){
        chomp;
        my @fields = split /\t/smx;

        if(/^\@PG/smx){
            foreach my $field (@fields){
               if ($field  =~ /^CL:(\S+)/smx){
                   if ($field =~ /bamsor.*\s+adddupmarksupport=1/xms){ $adddup=1 };
	             }
	          }
	      }

        if(/^\@RG/smx){
            foreach my $field (@fields) {
                if ($field  =~ /^SM:(\S+)/smx){
                    my $header_sample_name  = $1;
                    if($self->verbose()){$self->info("SM:$header_sample_name");}
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
                            #some of the later crams may already have the SM:sample_acc
                            if (! $self->reheader_rt_ticket()){ $sample_problems++ };
                        }
                    } else {
                        $header_info->{'sample_name'} = $header_sample_name;
                    }
                }elsif($field =~ /^LB:(\S+)/smx) {
                    my $header_library_id = $1;
                    @imeta_library_id = map { $_->{'value'} => $_ }
                    grep { $_->{'attribute'} eq 'library_id' } @{$query->{'irods_meta'}};

                    if ($self->verbose()) {
                        $self->info("LIBRARY IMETA:$imeta_library_id[0] HEADER:$header_library_id");
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

    if (! $adddup && ($self->markdup_method eq $ORIG_DUP_METHOD)){
        carp "Cram header checked: $cram has not had bamsormadup or bamsort with " .
             "adddupmarksupport=1 run. Skipping this run\n";
        return 0;
    }

    if ($imeta_library_id[0] ne $query->{'library_id'} ) {
        carp 'Supplied library id does not match irods ' .$query->{'library_id'}.
            "( vs  $imeta_library_id[0]) \n";
        return 0;
    }


    if ($sample_problems or $library_problems or $reference_problems) {
        if ($self->crams_in_s3){
            if ($sample_problems or $library_problems){ return 0 }  ###temp for crams located on S3 
        }
        else {
        return 0;
        }
    }

    ## set first_cram_sample_name and first_cram_ref_name if no problems
    if(defined $header_info->{'ref_name'} && !$self->has_first_cram_ref_name) {
        $self->set_first_cram_ref_name($header_info->{'ref_name'});
    }
    if(defined $header_info->{'sample_name'} && !$self->has_first_cram_sample_name) {
        $self->set_first_cram_sample_name($header_info->{'sample_name'});
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

=item Readonly

=item Moose

=item MooseX::StrictConstructor

=item MooseX::Getopt

=item WTSI::DNAP::Utilities::Loggable

=item npg_common::roles::software_location

=item File::Basename

=item npg_seq_melt::util::irods

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015,2016,2017,2018,2019,2021 Genome Research Ltd.

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
