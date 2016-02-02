######### 
# Author:        jillian
# Created:       2015-04-29
#

package npg_seq_melt::merge::library;

use Moose;
use Moose::Meta::Class;
use Carp;
use English qw(-no_match_vars);
use List::MoreUtils qw { any };
use IO::File;
use File::Path qw/ make_path /;
use File::Spec qw/ splitpath /;
use File::Copy qw/ copy move /;
use File::Basename qw/ basename /;
use File::Slurp qw( :std );
use Archive::Tar;
use srpipe::runfolder;
use npg_tracking::data::reference;
use npg_common::irods::Loader;

extends qw/npg_seq_melt::merge npg_seq_melt::merge::base/;

our $VERSION = '0';

Readonly::Scalar my $P4_MERGE_TEMPLATE   => q[merge_aligned.json];
Readonly::Scalar my $P4_COMMON_TEMPLATE  => q[alignment_common.json];
Readonly::Scalar my $VIV_SCRIPT          => q[viv.pl];
Readonly::Scalar my $VTFP_SCRIPT         => q[vtfp.pl];
Readonly::Scalar my $SUMMARY_LINK        => q{Latest_Summary};
Readonly::Scalar my $MD5_SUBSTRING_LENGTH => 10;

=head1 NAME

npg_seq_melt::merge::library

=head1 SYNOPSIS

my $sample_merge = npg_seq_melt::merge::library->new({
   rpt_list                =>  '15972:5;15733:1;15733:2',
   sample_id               =>  '1111111',
   sample_name             =>  '3185STDY1111111',
   sample_common_name      =>  'Homo Sapiens',
   sample_accession_number =>  'EGAN00000000000',
   library_id              =>  '2222222',        
   instrument_type         =>  'HiSeqX' ,        
   study_id                =>  '3185',
   study_name              =>  'The life history of colorectal cancer metastases study WGS X10',
   study_title             =>  'The life history of colorectal cancer metastases study WGS X10',
   study_accession_number  =>  'EGAS00000000000',
   aligned                 =>  1,
   run_type                =>  'paired302',
   local                   =>  1,
   chemistry               =>  'CCXX',# from flowcell id HiSeqX_V2
 });

=head1 DESCRIPTION

Commands generated from npg_seq_melt::merge::generator

=head1 SUBROUTINES/METHODS

=head2 rpt_list

Semi-colon separated list of run:position or run:position:tag for the same sample
that define a composition for this merge. Required attribute.

=cut

has '+rpt_list' => (documentation =>
                   q[Semi-colon separated list of run:position or run:position:tag ] .
		   q[for the same sample e.g. 15990:1:78;15990:2:78],);

=head2 composition

npg_tracking::glossary::composition object corresponding to rpt_list

=cut

has '+composition' => (metaclass => 'NoGetopt',);

=head2 merge_dir

Directory where merging takes place

=cut

has '+merge_dir' => (metaclass => 'NoGetopt',);

=head2 sample_id

Sample ID

=cut

has 'sample_id' => (
     isa           => q[Int],
     is            => q[ro],
     required      => 1,
     documentation => q[database Sample ID],
    );

=head2 sample_name

=cut

has 'sample_name' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[from database],
    );

=head2 sample_common_name

=cut

has 'sample_common_name' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[from database e.g. Homo sapiens],
    );

=head2 sample_accession_number

=cut

has 'sample_accession_number' => (
     isa           => q[Str | Undef],
     is            => q[ro],
     required      => 0,
     documentation => q[from database],
    );

=head2 library_id

Library ID

=cut

has 'library_id' => (
     isa           => q[Int],
     is            => q[ro],
     required      => 1,
     documentation => q[database Library ID],
    );

=head2 study_id

Study ID

=cut

has 'study_id' => (
     isa           => q[Int],
     is            => q[ro],
     required      => 1,
     documentation => q[Study ID],
    );


=head2 study_name

Study Name

=cut

has 'study_name' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[from database],
    );

=head2 study_title

Study title

=cut

has 'study_title' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[database study title],
    );


=head2 study_accession_number

Study accession number

=cut

has 'study_accession_number' => (
     isa           => q[Str | Undef],
     is            => q[ro],
     required      => 0,
     documentation => q[database study accession number],
    );


=head2 aligned 

Boolean value 

=cut

has 'aligned' => (
     isa           => q[Bool],
     is            => q[ro],
     required      => 1,
     documentation => q[database study is aligned to a reference genome],
    );

=head2 reference_genome_path

Full path to reference genome used

=cut

has 'reference_genome_path' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 0,
     predicate     => '_has_reference_genome_path',
     writer        => '_set_reference_genome_path',
    );

sub _get_reference_genome_path{
    my ($self, $c) = @_;

    if (!$c) {
        croak 'Component attribute required';
    }
    $self->log(join q[ ], 'IN reference_genome_path', $c->freeze());

    return npg_tracking::data::reference->new(
                            id_run    => $c->id_run(),
                            position  => $c->position(),
                            tag_index => $c->tag_index(),
                                              )->refs()->[0];
}

=head2 instrument_type

=cut

has 'instrument_type' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[e.g. HiSeqX],
    );

=head2 run_type

paired or single with cycle count

=cut

has 'run_type' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[ paired or single with cycle count e.g. paired302],
    );


=head2 chemistry

=cut

has 'chemistry' => (
     isa          => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[e.g. HiSeqX_V2],
    );

=head2 mkdir_flag

A boolean flag; if true the iRods directory is created

=cut

has 'mkdir_flag'   => (isa           => q[Bool],
                       is            => q[rw],
                       documentation => q[boolean flag to make the iRods directory],
                      );


=head2 collection

Subdirectory within irods to store the output of the merge

=cut
has 'collection' => (isa           => q[Str],
                     is            => q[rw],
                     lazy_build    => 1,
                     documentation => q[collection within irods to store the output of the merge],
                    );

sub _build_collection {
    my $self = shift;
    return $self->default_root_dir().$self->_sample_merged_name();
}

=head2 _runfolder_location 

Records runfolder paths which got moved from outgoing back to analysis and also those already in analysis

=cut

has '_runfolder_location' => (
     isa           => q[ArrayRef[Str]],
     is            => q[rw],
     default       => sub { return []; },,
    );

=head2 vtlib

Specify P4 vtlib to use to find template json files

=cut

has 'vtlib'   => (
    isa           => q[Str],
    is            => q[rw],
    default       => q{$}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl)))/../data/vtlib/},
    documentation => q[Location of vtlib of template json files. The default is the one in the path environment],
    );

=head2 test_cram_dir

Alternative input location of crams

=cut

has 'test_cram_dir'  => (
    isa           => q[Maybe[Str]],
    is            => q[ro],
    default       => $ENV{'TEST_CRAM_DIR'},
    documentation => q[Alternative input location of crams],
    );

has '_composition2merge' => (
     isa           => q[npg_tracking::glossary::composition],
     is            => q[ro],
     default       => sub { return npg_tracking::glossary::composition->new() },
    );

=head2 _sample_merged_name

Name for the merged cram file, representing the component rpt .

=cut 

has '_sample_merged_name' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 0,
     lazy_build    => 1,
    );
sub _build__sample_merged_name {
    my $self = shift;
    my $md5 = $self->_composition2merge()->digest('md5');
    $md5 = substr $md5, 0, $MD5_SUBSTRING_LENGTH;
    return join q{.}, $self->library_id(),
                      $self->chemistry(),
                      $self->run_type(),
                      $md5;
}

=head2 _readme_file_name

Name for the README file

=cut 

has '_readme_file_name' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 0,
     lazy_build    => 1,
);
sub _build__readme_file_name {
    my $self = shift;
    return join q{.}, q{README}, $self->_sample_merged_name();
}

=head2 _tar_log_files

Make gzip file of log files for loading to iRODS

=cut

has '_tar_log_files' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 0,
     lazy_build    => 1,
);
sub _build__tar_log_files{
    my $self = shift;
    opendir my $dh, $self->merge_dir() or q[Cannot get listing for a directory, cannot open ] ,$self->merge_dir();
    my @logs =();
    while(readdir $dh){
        if (/err$|LOG$|json$/xms){  push @logs,join q[/],$self->merge_dir(),$_; }
    }
    closedir $dh;

    my $tar = Archive::Tar->new;
    $tar->add_files(@logs);
    my $tar_file_name = q[library_merge_logs.tgz];
    my $path = join q[/],$self->merge_dir(),q[outdata],$tar_file_name;
    $tar->write($path,COMPRESS_GZIP);
    return $tar_file_name;
}

=head2 _source_cram
 
Cram files are used from the staging directory, if still available. 
e.g.

/nfs/sf47/ILorHSany_sf47/analysis/150410_HS32_15990_B_HCYFKADXX/Latest_Summary/archive/lane1/15990_1#78.cram

If staging files are not present iRODS is used.
The use_irods attribute forces the files to be used from iRODS.
The test_cram_dir attribute allows an alternative location to be used for cram and seqchksum files.

=cut

sub _source_cram {
    my ($self, $c) = @_;

    my $filename = $c->filename(q[.cram]);
    if ($self->test_cram_dir) {
        ##no critic (CodeLayout::ProhibitParensWithBuiltins)
        return {'cram' => join(q[/], $self->test_cram_dir, $filename)};
    }

    my $path = join q[/],q[/seq], $c->id_run, $filename;
    my $paths = {'cram' => $path, 'irods_cram' => $path};
    if ($self->use_irods()) {
        return $paths;
    }

    my $run_folder;
    try {
        $run_folder = srpipe::runfolder->new(id_run=>$c->id_run)->runfolder_path;
    } catch {
        carp "Using iRODS cram as $_";
    };

    ## No run folder anymore, so iRODS path should be used.
    if (! $run_folder) {
        return $paths;
    }

    ## analysis staging run folder, make npg_do_not_move dir to prevent moving to outgoing mid job 
    ## and add README file
    my $do_not_move_dir = qq[$run_folder/npg_do_not_move];

    ## if exists - risk another user has touched do_not_move file and removes beneath us
    if (! -e $do_not_move_dir){
        ## no point in continuing without as job will die 
        mkdir $do_not_move_dir or croak "Could not mkdir $do_not_move_dir error: $OS_ERROR";
    }

    my $readme_file = $do_not_move_dir .q[/]. $self->_readme_file_name();
    if (-d $do_not_move_dir) {
        my $readme_fh = IO::File->new($readme_file, '>');
        ## no critic (InputOutput::RequireCheckedSyscalls)
        print {$readme_fh} $self->_readme_file();
        $readme_fh->close();
        $self->log("Added: $readme_file");
    } else {
        $self->log("README $readme_file not added: $do_not_move_dir does not exist as a directory");
    }

    $path = q[];
    my $link = readlink qq[$run_folder/$SUMMARY_LINK];
    $path = qq[$run_folder/$link] . q[/archive];

    if ($path =~ /outgoing/msx ) {
        my $destination = $self->_destination_path($run_folder,'outgoing','analysis');
        $self->log("Destination $destination");
        return if ! $self->_move_folder($run_folder,$destination);
        ### full path
        $path = $self->_destination_path($path,'outgoing','analysis');
        $self->log("Archive path: $path\n");
    } else {
        push @{$self->_runfolder_location()},$run_folder;
    }

    if ($c->tag_index()) {
        $path .= q[/lane].$c->position ;
    }
    $path .= q[/].$filename;
    $paths->{'cram'} = $path;

    return $paths;
}

=head2 get_irods_hostname

=cut

sub get_irods_hostname{
    my $self          = shift;
    my $irods_object  = shift; #/seq/id_run/rpt.cram
    my $index         = shift; #0 or 1
    my $irods         = shift;

## {"collection": "/seq/16912", "data_object": "16912_1#57.cram", "replicates": [{"resource": "irods-seq-sr01-ddn-rd10-18-19-20", "number": 0, "location": "irods-seq-sr01", "checksum": "f22fdd90548291d01171586a56c36689", "valid": true}, {"resource": "irods-seq-i05-de", "number": 1, "location": "irods-seq-i05", "checksum": "f22fdd90548291d01171586a56c36689", "valid": true}]}

##first replicate if option random_replicate not specified  

    my @replicates = $irods->replicates($irods_object);
    my $hostname   = q[//].$replicates[$index]{'location'} . q[.internal.sanger.ac.uk];
    return($hostname);
}

sub _readme_file {
    my $self = shift;

    my $library          = $self->library_id();
    my $instrument_type  = $self->instrument_type();
    my $chemistry        = $self->chemistry();
    my $run_type         = $self->run_type();

    my $file_contents =<<"END";
    This file was added by $PROGRAM_NAME which is accessing files in this run folder.
    Library    $library
    Instrument $instrument_type
    Chemistry  $chemistry
    Run type   $run_type
END

    return($file_contents);
}

sub _move_folder {
    my ($self,$runfolder,$destination) = @_;
    if (!$runfolder || !$destination) {
        carp q[Need runfolder and destination to move folder];
        return;
    }
    ### for testing - shouldn't need this
    if (any { $_ && ($_ eq $destination) }  @{$self->_runfolder_location()}){
      	carp "runfolder $destination had already been moved\n"; return 1;
    }

    move($runfolder,$destination) or croak "Staging run folder move failed: $OS_ERROR";
    push @{$self->_runfolder_location()},$destination;

    return 1;
}

=head2 original_seqchksum_dir

=cut

has 'original_seqchksum_dir' => (
     isa           => q[Str],
     is            => q[rw],
     required      => 0,
     metaclass  => 'NoGetopt',
);

has '_paths2merge' => (
     isa           => q[ArrayRef],
     is            => q[ro],
     required      => 0,
     lazy_build    => 1,
    );

sub _build__paths2merge {
    my $self = shift;

    my @path_list = ();
    my $header_info = {};

    foreach my $c (@{$self->composition->components}) {

        my $paths = $self->_source_cram($c);

        if ($self->verbose()) {
	    $self->log($c->freeze());
        }

        my $reference_genome_path = $self->_has_reference_genome_path ?
          $self->reference_genome_path : $self->_get_reference_genome_path($c);

        eval {
	    my @irods_meta = ();

            if (!$self->test_cram_dir) {
                my $cram = $paths->{'irods_cram'} || croak 'iRODS cram file path is undefined';

                @irods_meta = $self->irods->get_object_meta($cram);
                my @sample_id = map { $_->{value} => $_ } grep { $_->{attribute} eq 'sample_id' }  @irods_meta ;

                if ($sample_id[0] ne $self->sample_id()) {
                    croak 'Supplied sample id does not match irods ' .
                          "( $self->sample_id() vs $sample_id[0])\n";
                }
            }

            my $cram = $paths->{'cram'} || croak 'cram file path is undefined';
            my $imeta_lib_id = $self->check_cram_header(
              \@irods_meta, $cram, $header_info, $reference_genome_path);
            if (! defined $imeta_lib_id) {
                croak qq[Cram header check failed, skipping $cram\n];
            }

            if ($imeta_lib_id ne $self->library_id() ) {
                croak 'Supplied library id does not match irods ' .
                      "( $self->library_id() vs  $imeta_lib_id) \n";
            }

            1;
        } or do {
            carp $EVAL_ERROR;
            next;
        };

        if (!$self->_has_reference_genome_path) { # set once if not given by the caller
          $self->_set_reference_genome_path($reference_genome_path);
        }
        push @path_list, $paths->{'cram'};

        my $num_paths = scalar @path_list;
        if ($num_paths == 0) {
	    $header_info = {}; # reset
	} elsif ($num_paths == 1) {
            if (!$header_info->{'sample_name'} || !$header_info->{'ref_name'}) {
                croak 'Failed to get complete header info for the first sample to be merged';
            }
        }
        $self->_composition2merge()->add_component($c);
    }


    $self->log("Disconnect from iRODS\n");
    $self->irods_disconnect($self->irods);

     if ($self->composition->num_components() != $self->_composition2merge->num_components()){
        my $digest1 = $self->composition->freeze();
        my $digest2 = $self->_composition2merge->freeze();
        $self->log("Original composition: $digest1\n");
        $self->log("New composition: $digest2\n");
        croak "\nComponent count to merge(" . $self->_composition2merge->num_components() . ') does not equal that in original list ('
              . $self->composition->num_components() . ")\n";
 }

    return \@path_list;
}

=head2 process 

main method to call, run the cram file merging and add meta data to merged file 

=cut

=head1

meta data from irods is an Array[HashRef]

$VAR5 = {
          'attribute' => 'library',
          'value' => '13880085'
        };
$VAR6 = {
          'attribute' => 'library_id',
          'value' => '13880085'
        };

=cut

sub process{
    my $self = shift;


    $self->log(q{PERL5LIB:},$ENV{'PERL5LIB'},qq{\n});
    $self->log(q{PATH:},$ENV{'PATH'},qq{\n});
    chdir $self->run_dir() or croak qq[cannot chdir $self->run_dir(): $CHILD_ERROR];

    if ($self->sample_acc_check() &! $self->sample_accession_number()){
        croak "sample_accession_number required (sample_acc_check set)\n";
    }

    if (scalar @{ $self->_paths2merge } > 1) {  #do merging

        my $merge_err=0;
        if ($self->do_merge()) { ### viv command successfully finished
            ###TODO with streaming to iRODS would still get cram loaded to iRODS even with --local set
            if ($self->local()) {
                carp "Merge successful, skipping iRODS loading step as local flag set\n";
            } else {
                ### upload file and meta-data to irods
                $self->load_to_irods();
            }
        } else {
           $merge_err=1;
        }

        if (defined $self->_runfolder_location()) {
            $self->_clean_up();
        }
        if ($merge_err) {
            croak "Skipping iRODS loading, problems with merge\n";
        }
    } else {
        carp "0 sample(s) passed checks, skip merging\n";
    }

    return;
}

=head2 check_cram_header

1. Check that appropriate commands have been run in PG line (currently used for HiSeqX)
i.e. bamsort with adddupmarksupport

bamsort SO=coordinate level=0 verbose=0 fixmates=1 adddupmarksupport=1

2. Sample name in SM field in all cram headers should be consistent 

3. Library id in cram header should match that in the imeta data 

4. UR field of SQ row should be consistent across samples and should match the that returned by npg_tracking::data::reference (s/fasta/bwa/) 

=cut

sub check_cram_header { ## no critic (Subroutines::ProhibitExcessComplexity)
    my $self         = shift;
    my $irods_meta   = shift;
    my $cram         = shift;
    my $header_info  = shift;
    my $ref_path     = shift;

    if (!$irods_meta || !$cram || !$header_info || !$ref_path) {
        croak 'Not all attributes defined';
    }

    my $samtools_view_cmd =  $self->samtools_executable() . qq[ view -H irods:$cram |];
    if ($cram !~ /^\/seq\//xms){ $samtools_view_cmd =~ s/irods://xms }

    my $fh = IO::File->new($samtools_view_cmd) or croak "Error viewing cram header: $OS_ERROR\n";

    my @imeta_library_id;

    my $adddup=0;
    my $sample_problems=0;
    my $library_problems=0;
    my $reference_problems=0;
    my $first_sq_line=1;
    my $first_sample_name = $header_info->{'sample_name'};
    my $first_ref_name    = $header_info->{'ref_name'};

    ##no critic (ControlStructures::ProhibitDeepNests)

    while(<$fh>){
        chomp;
        if(/^\@PG/smx){
            my @fields = split /\t/smx;
            foreach my $field (@fields){
               if ($field  =~ /^CL:(\S+)/smx){
                   if ($field =~ /bamsort.*\s+adddupmarksupport=1/xms){ $adddup=1 };
	       }
	    }
	}

        if(/^\@RG/smx){
            my @fields = split /\t/smx;
            foreach my $field (@fields) {
                if ($field  =~ /^SM:(\S+)/smx){
                    my $header_sample_name  = $1;
                    $self->log("SM:$header_sample_name");
                    if ($self->sample_acc_check()){
                        ##sample_accession_number must match header_sample_name
			                  if ($header_sample_name ne $self->sample_accession_number()){
                           carp 'Header sample name does not match sample accession number:' .
			                           "$header_sample_name ", $self->sample_accession_number(),"\n";
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

                } elsif($field =~ /^LB:(\d+)/smx) {
                    if ($self->test_cram_dir) {
                       next; # @{$irods_meta} is empty
                    }
                    my $header_library_id = $1;
                    @imeta_library_id = map { $_->{'value'} => $_ }
                                        grep { $_->{'attribute'} eq 'library_id' } @{$irods_meta};
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
            my @fields = split /\t/smx;
            foreach my $field (@fields){
                if ($field  =~ /^UR:(\S+)/smx) {
                    my $header_ref_name  = $1;
                    # comparing against first usable cram header in list
                    if (defined $first_ref_name) {
                        if ($header_ref_name ne $first_ref_name) {
                            carp 'Header reference paths are not consistent across samples: ' .
                                 "$header_ref_name $first_ref_name\n";
                            $reference_problems++;
		        }
		    } else {
                        $header_info->{'ref_name'} = $header_ref_name;
		    }

                    if (basename($ref_path) ne basename($header_ref_name)) {
                        carp 'Header reference path does not match npg_tracking reference: ' .
                             "$ref_path $header_ref_name\n";
                        $reference_problems++;
                    }
	        }
      	    }
        }
    }
    if (! $adddup){
        carp "Cram header checked: $cram has not had bamsort with " .
             "adddupmarksupport=1 run. Skipping this run\n";
        return;
    }

    if ($sample_problems or $library_problems or $reference_problems) {
        return;
    }

    return $imeta_library_id[0];
}

=head2 do_merge

=cut

sub do_merge {
    my $self    = shift;

    $self->log(q[DO MERGING name=], $self->_sample_merged_name());

    ###set up sub-directory for sample  ################################
    my $subdir = $self->merge_dir();
    return 0 if !$self->run_make_path(qq[$subdir/outdata]);

    my $original_seqchksum_dir = join q{/},$subdir,q{input};
    return 0 if !$self->run_make_path($original_seqchksum_dir);
    $self->original_seqchksum_dir($original_seqchksum_dir);

    chdir $original_seqchksum_dir or croak qq[cannot chdir $original_seqchksum_dir : $CHILD_ERROR];
    return 0 if !$self->get_seqchksum_files();

    chdir $subdir or croak qq[cannot chdir $subdir: $CHILD_ERROR];

    ## mkdir in iRODS and ichmod so directory not public 
    my $mkdir_cmd = q{imkdir -p } . $self->collection();
    $self->run_cmd($mkdir_cmd);

    my $irods = WTSI::NPG::iRODS->new();
    $irods->set_collection_permissions('null','public',$self->collection());


    return 0 if !$self->run_make_path(qq[$subdir/status]);

    my($vtfp_cmd) = $self->vtfp_job($irods);
    $self->irods_disconnect($irods);

    return 0 if !$self->run_cmd($vtfp_cmd);
    my($viv_cmd) = $self->viv_job();
    return 0 if !$self->run_cmd($viv_cmd);

    my $success =  $self->merge_dir . q[/status/merge_completed];
    $self->run_cmd(qq[touch $success]);
    return 1;
}

=head2 run_make_path

=cut 

sub run_make_path {
    my $self = shift;
    my $path = shift;
    if (! -d $path ) {
        eval {
            make_path($path) or croak qq[cannot make_path $path: $CHILD_ERROR];
            1;
        } or do {
            carp qq[cannot make_path $path: $EVAL_ERROR];
            return 0;
        };
    }
    return 1;
}


=head2 get_seqchksum_files

=cut 

sub get_seqchksum_files {
    my $self = shift;
    my $seqchksum_file;
    foreach my $cram (@{$self->_paths2merge}){
        ($seqchksum_file = $cram)  =~ s/cram$/seqchksum/xms;

        # non-iRODS, copy files (seqchksum) over
        if ($cram !~ / ^\/seq\/ /xms) {
            eval {
                copy($seqchksum_file,$self->original_seqchksum_dir()) or croak "Copy failed: $OS_ERROR";
                1;
            } or do {
                carp "Copying seqchksum failed: $EVAL_ERROR";
                return 0;
            };
        } else {
            ##next line for testing ONLY skip if file already present
            next if -e join q{/},$self->original_seqchksum_dir(),basename($seqchksum_file);
            return 0 if !$self->run_cmd(qq[iget -K $seqchksum_file]);
        }
    }
    return 1;
}

=head2 vtfp_job

vtfp.pl -l vtfp.13149764.HiSeqX.merge_aligned.LOG -o 13149764.HiSeqX.merge_aligned.json -keys library -vals 13149764.HiSeqX -keys cfgdatadir -vals $VTLIB_PATH -keys samtools_executable -vals samtools1 -keys outdatadir -vals outdata -keys basic_pipeline_params_file -vals $VTLIB_PATH/alignment_common.json -keys bmd_resetdupflag_val -vals 1 -keys incrams -vals irods:/seq/15733/15733_3.cram -keys incrams -vals irods:/seq/15972/15972_6.cram  -keys incrams_seqchksum -vals /lustre/scratch110/xx/input/15733_3.seqchksum -keys incrams_seqchksum -vals /lustre/scratch110/xx/input/15972_6.seqchksum   $VTLIB_PATH/merge_aligned.json 

=cut

sub vtfp_job {
    my $self = shift;
    my $irods = shift;

    my $vtlib = $self->vtlib();
    my $merge_sample_name = $self->_sample_merged_name();
    my $vtfp_log = join q[.],'vtfp',$merge_sample_name,$P4_MERGE_TEMPLATE;
    $vtfp_log    =~ s/json$/LOG/xms;
    my $sample_vtfp_template = join q[.],$merge_sample_name,$P4_MERGE_TEMPLATE;
    my($sample_seqchksum_input,$sample_cram_input);

    my $replicate_index = 0;
    #use same replicate version for all crams 
    if ($self->random_replicate()){
        $replicate_index = int rand 2; #0 or 1
        $self->log("Using iRODS replicate index $replicate_index\n");
    }

    foreach my $cram ( @{$self->_paths2merge}){
        ## seqchksum needs to be prior downloaded from iRODS or from the staging area
        my $sqchk;
        ($sqchk = $cram) =~ s/cram/seqchksum/xms;
        my(@path) = File::Spec->splitpath($sqchk);
        $sqchk =  $self->original_seqchksum_dir().q[/].$path[-1];

        if ($cram =~ / ^\/seq\/ /xms){
            ##irods: prefix needs adding to the cram irods path name
            my $hostname = $self->get_irods_hostname($cram,$replicate_index,$irods);
            $cram =~ s/^/irods:$hostname/xms;
        }

        $sample_cram_input      .= qq(-keys incrams -vals $cram );
        $sample_seqchksum_input .= qq(-keys incrams_seqchksum -vals $sqchk );
    }

    my $ref_path = $self->reference_genome_path();
    $ref_path =~ s/bwa/fasta/xms;

    my $cmd       = qq($VTFP_SCRIPT -l $vtfp_log -o $sample_vtfp_template ) .
                    qq(-keys library -vals $merge_sample_name ) .
                    qq(-keys cfgdatadir -vals $vtlib ) .
                     q(-keys samtools_executable -vals ) . $self->samtools_executable() . q( ).
                     q(-keys outdatadir -vals outdata ) .
                     q(-keys outirodsdir -vals  ) . $self->collection() . q( ).
                    qq(-keys basic_pipeline_params_file -vals $vtlib/$P4_COMMON_TEMPLATE ) .
                     q(-keys bmd_resetdupflag_val -vals 1 ) .
                     q(-keys bmdtmp -vals merge_bmd ) .
                    qq(-keys genome_reference_fasta -vals $ref_path ).
                    qq($sample_cram_input $sample_seqchksum_input  $vtlib/$P4_MERGE_TEMPLATE );

    $self->log("\nVTFP_CMD $cmd\n");

    return $cmd;
}


=head2 viv_job
=cut

sub viv_job {
   my $self = shift;

   my $merge_sample_name = $self->_sample_merged_name();

    my $viv_log   = join q[.],'viv',$merge_sample_name,$P4_MERGE_TEMPLATE;
       $viv_log   =~ s/json$/LOG/xms;
    my $viv_template = join q[.],$merge_sample_name,$P4_MERGE_TEMPLATE;
    my $cmd  = qq($VIV_SCRIPT -v 3 -x -s -o $viv_log ./$viv_template);
    my $job_name = 'viv_merge-%J';

    return $cmd;
}

=head2 _destination_path
=cut

sub _destination_path {
    my ($self, $runfolder_path, $src, $dest) = @_;
    if (!$src || !$dest) {
        carp 'Need two names'; return;
    }

    ## outgoing -> analysis or vice versa
    $runfolder_path =~ s{/$src/}{/$dest/}msx;

    return $runfolder_path;
}

=head2 load_to_irods

Files to load are those in $self->merge_dir().q[/outdata]   (not cram.md5, markdups_metrics)

=cut

sub load_to_irods {
    my $self = shift;

    my $data =  $self->irods_data_to_add();
    my $path_prefix = $self->merge_dir().q[/outdata/];

    my @permissions; ## TODO check study_id will always be the current one
    push @permissions,  q{read ss_}.$data->{$self->_sample_merged_name().q[.cram]}->{study_id}, q{null public};

    # initialise mkdir flag
    $self->mkdir_flag(0);
    my $irods = WTSI::NPG::iRODS->new();
    my $in_progress =  $self->merge_dir . q[/status/loading_to_irods];
    $self->run_cmd(qq[touch $in_progress]);


    foreach my $file (keys %{$data}){
        $self->log("Trying to load irods object ${path_prefix}$file to ". $self->collection());

        #####sub/super set may already exist so remove target=library 
        if ($file =~/cram$/xms){
            my @found = $irods->find_objects_by_meta($self->default_root_dir(),
                                                          ['library_id' => $self->library_id()],
                                                          ['target'     => 'library'],
                                                          ['chemistry'  => $self->chemistry()],
                                                          ['run_type'   => $self->run_type() ],
                                                          ['study_id'   => $self->study_id() ]);

            if (@found){ $self->log("Remove target=library for $found[0]");
                         $irods->remove_object_avu($found[0],'target','library') ;
             }
        }

        my $loader = npg_common::irods::Loader->new
            (file       => qq[${path_prefix}$file],
             irods      => $irods,
             collection => $self->collection(),
             meta_data  => $data->{$file},
             mkdir      => $self->mkdir_flag(),
            );

        $loader->chmod_permissions(\@permissions);

        $loader->run();

        $self->log("Added irods object $file to ". $self->collection());

        if ($file =~/cram$/xms){
            foreach my $permission(@permissions){
                my $irodsfile = File::Spec->catfile($self->collection(),$file);
                $loader->run_set_permissions_command($permission, $irodsfile);
            }
            $irods->set_collection_permissions('read','public',$self->collection());
        }

        $self->remove_outdata() && unlink qq[${path_prefix}$file];

    }

    $self->log("Removing $in_progress");
    unlink $in_progress or carp "cannot remove $in_progress : $ERRNO\n";

    return;
}

=head2 irods_data_to_add

=cut

sub irods_data_to_add {
    my $self = shift;
    my $data = {};

    my $path_prefix = $self->merge_dir().q[/outdata/].$self->_sample_merged_name();
    my $merged_name = $self->_sample_merged_name();

    my $cram_md5 = read_file($path_prefix.q[.cram.md5]);
    chomp $cram_md5;

    ## Need ArrayRef of json strings to populate multiple member attributes in iRODS
    my @members = map { $_->freeze() } @{$self->composition->components};

    ### values from _lims will be for last sample in sorted rpt list ##
    ## add tag=>$tag if tag

    $data->{$merged_name.q[.cram]} = {
                    'type'                    => 'cram',
                    'reference'               => $self->reference_genome_path(),
                    'sample_id'               => $self->sample_id(),
                    'sample'                  => $self->sample_name(),
                    'is_paired_read'          => $self->run_type =~ /^paired/msx ? 1 : 0,
                    'sample_common_name'      => $self->sample_common_name(),
                    'manual_qc'               => 1, #if filter=>mqc not used in file_merge.pm this may not be true
                    'study_id'                => $self->study_id(),
                    'study'                   => $self->study_name(),
                    'study_title'             => $self->study_title(),
                    'library_id'              => $self->library_id(),
                    'target'                  => q[library],
                    'alignment'               => $self->aligned,
                    'total_reads'             => $self->get_number_of_reads($path_prefix.q[.flagstat]),
                    'md5'                     => $cram_md5,
                    'chemistry'               => $self->chemistry(),
                    'instrument_type'         => $self->instrument_type(),
                    'run_type'                => $self->run_type(),
                    'composition_id'          => $self->_composition2merge->digest(),
                    'component'                  => \@members,
                    'composition'             => $self->_composition2merge->freeze(),
                       };

    if( $self->sample_accession_number()){
        $data->{$merged_name.q[.cram]}->{'sample_accession_number'} = $self->sample_accession_number();
    }
    if( $self->study_accession_number()){
        $data->{$merged_name.q[.cram]}->{'study_accession_number'} = $self->study_accession_number();
    }

    $data->{$merged_name.q[.cram.crai]}                    = {'type' => 'crai'};
    $data->{$merged_name.q[.flagstat]}                     = {'type' => 'flagstat'};
    $data->{$merged_name.q[_F0x900.stats]}                 = {'type' => 'stats'};
    $data->{$merged_name.q[_F0xB00.stats]}                 = {'type' => 'stats'};
    $data->{$merged_name.q[.seqchksum]}                    = {'type' => 'seqchksum'};
    $data->{$merged_name.q[.sha512primesums512.seqchksum]} = {'type' => 'sha512primesums512.seqchksum'};

    if(-f $self->merge_dir().q[/outdata/].$merged_name.q[_F0x200.stats]){
         $data->{$merged_name.q[_F0x200.stats]} = {'type' => 'stats'};
    }
    if(-f $self->merge_dir().q[/outdata/].$merged_name.q[.stats]){
         $data->{$merged_name.q[.stats]}        = {'type' => 'stats'};
    }

    my $tar_file = $self->_tar_log_files();
    if ($tar_file){
        $data->{$tar_file} = {'type' => 'tgz'};
    }

    return($data);
}

=head2 get_number_of_reads

Get number of reads from first line of flagstat file to add to cram file total_reads iRODS meta data object AVU

333140294 + 6982226 in total (QC-passed reads + QC-failed reads)
0 + 0 secondary
0 + 0 supplementary
37002769 + 0 duplicates
332928038 + 6919067 mapped (99.94%:99.10%)
333140294 + 6982226 paired in sequencing
166570147 + 3491113 read1
166570147 + 3491113 read2
314700320 + 5338104 properly paired (94.46%:76.45%)
332721074 + 6856042 with itself and mate mapped
206964 + 63025 singletons (0.06%:0.90%)
12499302 + 1258554 with mate mapped to a different chr
5395796 + 697933 with mate mapped to a different chr (mapQ>=5)

=cut

sub get_number_of_reads{
    my $self = shift;
    my $flagstat_file = shift;

    my $fh = IO::File->new($flagstat_file, '<') or croak "cannot open $flagstat_file : $OS_ERROR";

    my $total_reads = 0;

    while(<$fh>){
      if( /^(\d+)\s*[+]\s*(\d+)[ ].*in[ ]total/mxs ){
          $total_reads = $1 + $2;
          last;
      }
    }
    return $total_reads;
}


=head2 _clean_up

If readme file added, remove.  If outgoing moved to analysis move back to outgoing (if suitable).

=cut


sub _clean_up{
   my $self = shift;

   my @runfolders = @{$self->_runfolder_location};
   my $v = undef;

   foreach my $runfolder (@runfolders){
       my $do_not_move_dir =qq[$runfolder/npg_do_not_move];
       my $readme_file = $do_not_move_dir .q[/]. $self->_readme_file_name();
       $self->log("Looking for README files in $do_not_move_dir");

       ## only remove npg_do_not_move if directory and only contains the readme for this job
       if(-e $do_not_move_dir && -d $do_not_move_dir){

           $self->log("Remove $readme_file\n");
           eval{
               unlink $readme_file or carp "Could not remove file $readme_file: $OS_ERROR";
           } or do { carp "$EVAL_ERROR"; $v=1};

           my @file_list = glob $do_not_move_dir .q{/*};
           if(@file_list < 1){
               $self->log("Remove $do_not_move_dir\n");
               eval {
                   rmdir $do_not_move_dir or carp "Could not remove directory $do_not_move_dir: $OS_ERROR";
               } or do { carp "$EVAL_ERROR"; $v=1};

               ## could leave for daemon to do
               if ($runfolder =~ /analysis/msx ){
                   my $destination = $self->_destination_path($runfolder,'analysis','outgoing');
                   $self->log("move $runfolder $destination");
                   carp "Could not move from analysis to outgoing\n" if ! $self->_move_folder($runfolder,$destination);
               }
           }

       }
   }
   return($v);
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 process

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::Getopt

=item Moose::Meta::Class

=item English -no_match_vars

=item List::MoreUtils  

=item IO::File

=item Carp

=item srpipe::runfolder

=item npg_tracking::data::reference

=item npg_common::irods::Loader
 
=item Archive::Tar

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
