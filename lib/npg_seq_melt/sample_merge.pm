######### 
# Author:        jillian
# Created:       2015-04-29
#

package npg_seq_melt::sample_merge;

use strict;
use warnings;
use Carp;
use Moose;
use English qw(-no_match_vars);
use List::MoreUtils qw { any };
use Data::Dumper;
use IO::File;
use Pod::Usage;
use Cwd;
use File::Path qw/ make_path /;
use File::Spec qw/ splitpath /;
use File::Copy;
use File::Basename;
use File::Slurp;
use FindBin qw($Bin);
use srpipe::runfolder;
use WTSI::NPG::iRODS;
use npg_tracking::data::reference;

with qw{
     MooseX::Getopt
     npg_common::roles::log 
};

our $VERSION = '0';

Readonly::Scalar my $P4_MERGE_TEMPLATE   => q[merge_aligned.json];
Readonly::Scalar my $P4_COMMON_TEMPLATE => q[alignment_common.json];
Readonly::Scalar my $VIV_SCRIPT    => q[viv.pl];
Readonly::Scalar my $VTFP_SCRIPT   => q[vtfp.pl];
Readonly::Scalar my $SAMTOOLS      => q[samtools1];
Readonly::Scalar my $SUMMARY_LINK   => q{Latest_Summary};

=head1 NAME

npg_seq_melt::sample_merge

=head1 VERSION

$$

=head1 SYNOPSIS

my $sample_merge = npg_seq_melt::sample_merge->new({
   rpt_list        =>  '15972:5;15733:1;15733:2',
   sample_id          =>  '2183757',
   library_id         =>  '13149752',        
   instrument_type =>  'HiSeqX' ,        
   study_id           =>  '3185',
   run_type        =>  'paired',
   local           =>  1,
   chemistry       =>  'CCXX',# from flowcell id HiSeqX_V2
 });

=head1 DESCRIPTION

Commands generated from npg_seq_melt::file_merge

=head1 SUBROUTINES/METHODS

=head2 rpt_list

Input run:position[:tag] string

=cut

has 'rpt_list' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[Semi-colon separated list of run:position or run:position:tag for the same sample e.g. 15990:1:78;15990:2:78],
    );

has '_rpt_aref'  => (
     isa           => q[ArrayRef],
     is            => q[rw],
     required      => 0,
     lazy_build    => 1,
    );
sub _build__rpt_aref {
    my $self = shift;
    my @rpt = split/\;/smx,$self->rpt_list();
    my @sorted_rpt = sort _by_rpt(@rpt);
    return(\@sorted_rpt);
}
sub _by_rpt{
     my($a_run,$a_lane,$a_plex) = split/[:]/smx,$a;
     my($b_run,$b_lane,$b_plex) = split/[:]/smx,$b;

    my $sort_result =  (
                $a_run <=> $b_run
                       ||
                $a_lane <=> $b_lane
                       ||
                $a_plex <=> $b_plex
                );

 return $sort_result;
}


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
     isa           => q[Maybe[Str]],
     is            => q[ro],
     required      => 1,
     documentation => q[from database],
    );



=head2 _header_sample_name

For checking that sample names in cram headers match

=cut

has '_header_sample_name' => (
     isa           => q[Maybe[Str]],
     is            => q[rw],
     required      => 0,
     default       => undef,
    );


=head2 _header_ref_name

For checking that reference names in cram headers match

=cut

has '_header_ref_name' => (
     isa           => q[Maybe[Str]],
     is            => q[rw],
     required      => 0,
     default       => undef,
    );

=head2 library_id

Library ID

=cut

has 'library_id' => (
     isa           => q[Int],
     is            => q[ro],
     required      => 0,
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

=head2 study_id

Study ID

=cut

has 'study_title' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[database study title],
    );


=head2 study_id

Study ID

=cut

has 'study_accession_number' => (
     isa           => q[Maybe[Str]],
     is            => q[ro],
     required      => 1,
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

=head2 _reference_genome_path

Full path to reference genome used

=cut

has '_reference_genome_path' => (
     isa           => q[Maybe[Str]],
     is            => q[ro],
     required      => 0,
     lazy_build    => 1,
    );
sub _build__reference_genome_path{
    my $self = shift;
    if( defined $self->id_run() ){
      	my $msg = q[in _build__reference_genome_path ] . $self->id_run() . q[ ] . $self->lane();
	         $msg .= $self->tag_index() ? $self->tag_index() : q[];
           $self->log($msg);

       my $ref_gp = npg_tracking::data::reference->new(
                            id_run  => $self->id_run(),
                            position  => $self->lane(),
                            tag_index => $self->tag_index(),
                                )->refs()->[0];

     return($ref_gp);
    }
    $self->log( 'id_run, position or tag_index need to be set to get reference path' );
    return;
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


=head2 local

npg_seq_melt::file_merge does : add -local to the command line if no databases were updated

TODO Not currently used here

=cut

has 'local' => (
     isa           => q[Bool],
     is            => q[ro],
     required      => 0,
     documentation => q[*Not currently used*],
    );

=head2 verbose

=cut

has 'verbose' => (
     isa           => q[Bool],
     is            => q[ro],
     required      => 0,
     documentation => q[],
    );


=head2 devel


=cut

has 'devel' => (
     isa           => q[Bool],
     is            => q[ro],
     required      => 0,
     documentation => q[],
    );

=head2 runfolder_moved 

  Records runfolder paths which got moved from outgoing back to analysis

=cut

has '_runfolder_moved' => (
     isa           => q[ArrayRef[Str]],
     is            => q[rw],
     required      => 0,
     default       => sub { return []; },,
    );

=head2 vtlib

Specify P4 vtlib to use to find template json files

=cut

has 'vtlib'   => (
    isa           => q[Str],
    is            => q[rw],
    required      => 0,
    default       => q{$}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl)))/../data/vtlib/},
    documentation => q[Location of vtlib of template json files. The default is the one in the path environment],
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
has 'test_cram_dir'  => (
    isa           => q[Maybe[Str]],
    is            => q[ro],
    required      => 0,
    default       => $ENV{'TEST_CRAM_DIR'},
    documentation => q[Alternative input location of crams],
    );

has 'use_irods' => (
     isa           => q[Bool],
     is            => q[ro],
     required      => 0,
     documentation => q[force use of iRODS for input crams/seqchksums rather than staging],
    );

has 'id_run' => (
    is            => 'rw',
    isa           => 'Int',
    required      => 0,
    metaclass  => 'NoGetopt',
);

has 'lane' => (
    is            => 'rw',
    isa           => 'Int',
    required      => 0,
    metaclass  => 'NoGetopt',
);

has 'tag_index' => (
    is            => 'rw',
    isa           => 'Int',
    required      => 0,
    metaclass  => 'NoGetopt',
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
    return( join q[/],$self->run_dir(),$self->_sample_merged_name()  );
}

=head2 _formatted_rpt

=cut 
has '_formatted_rpt' => (
    is            => 'rw',
    isa           => 'Str',
    required      => 0,
    clearer       => 'clear__formatted_rpt',
    lazy_build      => 1,
);
sub _build__formatted_rpt{
    my($self) = shift;
    return ( defined $self->tag_index() ) ? ( $self->id_run().q{_}.$self->lane().q{#}.$self->tag_index() ) : ( $self->id_run().q{_}.$self->lane() );
}


=head2 use_rpt

Array reference of run position tags to be used for the merge

=cut

has '_use_rpt' => (
    isa        => q[ArrayRef],
    is            => q[rw],
    required      => 0,
    default => sub { [] },
);

=head2 access_groups

iRODS access group taken from cram file

=cut

has 'access_groups' => (
     isa           => q[Str],
     is            => q[rw],
     required      => 0,
    );


=head2 sample_merged_name

Name for the merged cram file, representing the component rpt .

=cut 

has '_sample_merged_name' => (
     isa           => q[Str],
     is            => q[rw],
     required      => 0,
     lazy_build    => 1,
);
sub _build__sample_merged_name{
    my $self = shift;
    return join q{.},$self->library_id(),$self->chemistry(),$self->run_type();
}

=head2 source_cram
 
Cram files are used from the staging directory, if still available. 
e.g.

/nfs/sf47/ILorHSany_sf47/analysis/150410_HS32_15990_B_HCYFKADXX/Latest_Summary/archive/lane1/15990_1#78.cram

If staging files are not present iRODS is used.
The use_irods attribute forces the files to be used from iRODS.
The test_cram_dir attribute allows an alternative location to be used for cram and seqchksum files.

=cut

has '_source_cram' => (
     isa           => q[Str],
     is            => q[rw],
     required      => 0,
     clearer       => 'clear__source_cram',
     lazy_build    => 1,
    );
sub _build__source_cram {
    my $self = shift;
    my $path;
    $path = q[/seq/].$self->id_run().q[/].$self->_formatted_rpt().q[.cram];
    $self->irods_cram($path);

   if ($self->test_cram_dir){
       $path = $self->test_cram_dir . q[/].$self->_formatted_rpt().q[.cram];
       return($path);
   }

    my $run_folder;
    eval {  $run_folder = srpipe::runfolder->new(id_run=>$self->id_run())->runfolder_path; }
    or do { carp "Using iRODS cram as $EVAL_ERROR" };

    ## no run folder anymore, so iRODS path should be used
    if (! $run_folder || $self->use_irods()){
         return ($path);
     }

     ## analysis staging run folder, make npg_do_not_move dir to prevent moving to outgoing mid job 
     ## and add README file
     my $do_not_move_dir = qq[$run_folder/npg_do_not_move];

     if (! -e $do_not_move_dir){
            mkdir $do_not_move_dir or carp "Could not mkdir $do_not_move_dir error: $OS_ERROR";
            if (! $OS_ERROR ){
               my $readme_fh = IO::File->new("$do_not_move_dir/README", '>');
               ## no critic (InputOutput::RequireCheckedSyscalls)
               print {$readme_fh} $self->_readme_file();
               $readme_fh->close();
               $self->log("Added:  $do_not_move_dir/README");
            }
     }

       $path = q[];
       my $link = readlink qq[$run_folder/$SUMMARY_LINK];
       $path = qq[$run_folder/$link] . q[/archive];

       if ($path =~ /outgoing/msx ){
        my $destination = $self->_destination_path($run_folder,'outgoing','analysis');
        $self->log("Destination $destination");
        return if ! $self->_move_folder($run_folder,$destination);
        ### full path
        $path = $self->_destination_path($path,'outgoing','analysis');
        $self->log("Archive path: $path\n");
      }


    if ($self->tag_index()){
        $path .= q[/lane].$self->lane() ;
     }
        $path .= q[/].$self->_formatted_rpt().q[.cram];

    return ($path);
}

has '_irods' => (
    isa           => 'Maybe[WTSI::NPG::iRODS]',
    is            => 'ro',
    required      => 0,
    lazy_build    => 1,
);
sub _build__irods{
    my $self = shift;
    return WTSI::NPG::iRODS->new(strict_baton_version => 0);
}

sub _readme_file{
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
    if (any { $_ && ($_ eq $destination) }  @{$self->_runfolder_moved()}){
      	carp "runfolder $destination had already been moved\n"; return 1;
    }

    eval {
          move($runfolder,$destination) or croak "Staging run folder move failed: $OS_ERROR";
          push @{$self->_runfolder_moved()},$destination;
          }
          or do { croak "Move failed: $EVAL_ERROR"; };

return 1;
}


has 'irods_cram' => (
     isa           => q[Str],
     is            => q[rw],
     required      => 0,
     metaclass  => 'NoGetopt',
    );

has 'original_seqchksum_dir' => (
     isa           => q[Str],
     is            => q[rw],
     required      => 0,
     metaclass  => 'NoGetopt',
);


sub split_fields{
    my $self = shift;
    my $rpt  = shift;

    my($run,$position,$tag) = split/:/smx,$rpt;


    $self->id_run($run);
    $self->lane($position);
    if ($tag) { $self->tag_index($tag) }
    $self->clear__formatted_rpt();
    $self->_formatted_rpt();
    return;
}

=head2 process 

main method to call, run the cram file merging and add meta data to merged file 

=cut

sub process{
    my $self = shift;

    chdir $self->run_dir() or croak qq[cannot chdir $self->run_dir(): $CHILD_ERROR];


    my $rpt = $self->_rpt_aref();
    my @use_rpt =();


   foreach my $rpt (@{$rpt}){
       $self->split_fields($rpt);
       $self->clear__source_cram();
       $self->_source_cram();
       $self->log(q{SOURCE CRAM: }, $self->_source_cram(),qq{\n});

       if ($self->verbose()){ $self->log($self->_formatted_rpt()) };
       my $irods = $self->_irods();

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
       ##Get cram ss group - TODO check if this can be more than 1
       $self->access_groups($irods->get_object_groups($self->irods_cram()));

      my @irods_meta;
      eval{
          @irods_meta = $irods->get_object_meta($self->irods_cram());
         } or do {
         carp $EVAL_ERROR;
         next;
         };

    my ($imeta_lib_id);
    eval{
       ($imeta_lib_id)  = $self->check_cram_header(\@irods_meta);
           } or do {
              carp $EVAL_ERROR;
              next;
           };

       if (! defined $imeta_lib_id){ carp q[Cram header check failed, skipping ], $self->irods_cram(),"\n" ; next }

       if ($imeta_lib_id ne $self->library_id() ){
           carp "Supplied library id does not match irods ( $self->library_id() vs  $imeta_lib_id) \n";
           next;
       }

       my @sample_id = map { $_->{value} => $_ } grep { $_->{attribute} eq 'sample_id' }  @irods_meta ;
       if ($sample_id[0] ne $self->sample_id()){
           carp "Supplied sample id does not match irods ( $self->sample_id() vs $sample_id[0])\n";
           next;
       }

       push @use_rpt,$self->_source_cram();
    }

$self->_reference_genome_path();

$self->_use_rpt(\@use_rpt);

if (scalar @{ $self->_use_rpt } > 1){  #do merging

   ### viv command successfully finished
   if ($self->do_merge()){
        ### upload file and meta-data to irods
       $self->load_to_irods();
   }
   else {
    carp "Skipping iRODS loading, problems with merge\n";
   }

   if (defined $self->_runfolder_moved()){  $self->_clean_up() };
}
else { carp scalar @{ $self->_use_rpt }, " sample(s) passed checks, skip merging\n" }

return;
}

=head1 check_cram_header

1. Check that appropriate commands have been run in PG line (currently used for HiSeqX)
i.e. bamsort with adddupmarksupport

bamsort SO=coordinate level=0 verbose=0 fixmates=1 adddupmarksupport=1

2. Sample name in SM field in all cram headers should be consistant 

3. Library id in cram header should match that in the imeta data 

4. UR field of SQ row should be consistant across samples and should match the that returned by npg_tracking::data::reference (s/fasta/bwa/) 

=cut

sub check_cram_header { ## no critic (Subroutines::ProhibitExcessComplexity)
    my $self = shift;
    my $irods_meta = shift;

    my $cram = $self->_source_cram();
    my $samtools_view_cmd =  qq[ $SAMTOOLS view -H irods:$cram |];
    if ($cram !~ /^\/seq\//xms){ $samtools_view_cmd =~ s/irods://xms }

    my $fh = IO::File->new($samtools_view_cmd) or croak "Error viewing cram header: $OS_ERROR\n";

    my @imeta_library_id;

    my $adddup=0;
    my $sample_problems=0;
    my $library_problems=0;
    my $reference_problems=0;
    my $first_sq_line=1;

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

          foreach my $field (@fields){

           if ($field  =~ /^SM:(\S+)/smx){
              my $header_sample_name  = $1;
              ##comparing against first cram header in list
              if (defined $self->_header_sample_name() && $header_sample_name ne $self->_header_sample_name()){
                  carp "Header sample names are not consistant across samples: $header_sample_name ", $self->_header_sample_name(),"\n";
                  $sample_problems++;
              }
              else { $self->_header_sample_name($header_sample_name) }
          }
          elsif($field =~ /^LB:(\d+)/smx){
               my $header_library_id = $1;
               @imeta_library_id = map { $_->{value} => $_ } grep { $_->{attribute} eq 'library_id' } @{$irods_meta};
               if ($self->verbose()){ $self->log("LIBRARY IMETA:$imeta_library_id[0] HEADER:$header_library_id") };
               if ($imeta_library_id[0] ne $header_library_id){
            	     carp "library id in LIMS and header do not match : $imeta_library_id[0] vs $header_library_id\n";
                   $library_problems++;
               }
           }
	       }
      }

      if(/^\@SQ/smx && $first_sq_line){
      	$first_sq_line=0;
        my @fields = split /\t/smx;
        foreach my $field (@fields){
           if ($field  =~ /^UR:(\S+)/smx){
              my $header_ref_name  = $1;
              ##comparing against first cram header in list
              if (defined $self->_header_ref_name()){
                  ##no critic (ControlStructures::ProhibitDeepNests)
                  if ($header_ref_name ne $self->_header_ref_name()){
                     carp "Header reference paths are not consistant across samples: $header_ref_name ",
                           $self->_header_ref_name(),"\n";
                     $reference_problems++;
                  }
                  my $ref_path = $self->_reference_genome_path();
                     $ref_path =~ s/bwa/fasta/xms;
                  if ($ref_path ne $header_ref_name){
                     carp "Header reference path does not match npg_tracking::data::reference reference: $ref_path $header_ref_name\n";
                     $reference_problems++;
                  }
              } ## use critic
              else { $self->_header_ref_name($header_ref_name) }
	        }
      	}
      }
   }
if (! $adddup){
    carp "Cram header checked: $cram has not had bamsort with adddupmarksupport=1 run. Skipping this run\n";
    return();
}

if ($sample_problems or $library_problems or $reference_problems){ return() }

return($imeta_library_id[0]);
   }

sub do_merge {
    my $self = shift;
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

    my($vtfp_cmd) = $self->vtfp_job();
    return 0 if !$self->run_cmd($vtfp_cmd);
    my($viv_cmd) = $self->viv_job();
    return 0 if !$self->run_cmd($viv_cmd);

    return 1;
}

sub run_make_path {
    my $self = shift;
    my $path = shift;
    if (! -d $path ){
	   eval { make_path($path) or croak qq[cannot make_path $path: $CHILD_ERROR] }
	   or do { carp qq[cannot make_path $path: $EVAL_ERROR] ; return 0 };
    }
return 1;
}

sub get_seqchksum_files {
    my $self = shift;
    my $seqchksum_file;
    foreach my $cram (@{$self->_use_rpt}){
             ($seqchksum_file = $cram)  =~ s/cram$/seqchksum/xms;

         # non-iRODS, copy files (seqchksum) over
         if ($cram !~ / ^\/seq\/ /xms){
	           eval {
                copy($seqchksum_file,$self->original_seqchksum_dir()) or croak "Copy failed: $OS_ERROR";
                }
		            or do { carp "Copying seqchksum failed: $EVAL_ERROR"; return 0};
              }
              else {
                  ##next line for testing ONLY skip if file already present
                  next if -e join q{/},$self->original_seqchksum_dir(),basename($seqchksum_file);
                  return 0 if !$self->run_cmd(qq[iget $seqchksum_file]);
           }
        }
return 1;
}

=head2 example vtfp.pl command

vtfp.pl -l vtfp.13149764.HiSeqX.merge_aligned.LOG -o 13149764.HiSeqX.merge_aligned.json -keys library -vals 13149764.HiSeqX -keys cfgdatadir -vals $VTLIB_PATH -keys samtools_executable -vals samtools1 -keys outdatadir -vals outdata -keys basic_pipeline_params_file -vals $VTLIB_PATH/alignment_common.json -keys bmd_resetdupflag_val -vals 1 -keys incrams -vals irods:/seq/15733/15733_3.cram -keys incrams -vals irods:/seq/15972/15972_6.cram  -keys incrams_seqchksum -vals /lustre/scratch110/xx/input/15733_3.seqchksum -keys incrams_seqchksum -vals /lustre/scratch110/xx/input/15972_6.seqchksum   $VTLIB_PATH/merge_aligned.json 

=cut

sub vtfp_job {
    my $self = shift;
    my $vtlib = $self->vtlib();
    my $rpt_aref = $self->_use_rpt();
    my $merge_sample_name = $self->_sample_merged_name();
    my $vtfp_log      = join q[.],'vtfp',$merge_sample_name,$P4_MERGE_TEMPLATE;
       $vtfp_log =~ s/json$/LOG/xms;
    my $sample_vtfp_template = join q[.],$merge_sample_name,$P4_MERGE_TEMPLATE;
    my($sample_seqchksum_input,$sample_cram_input);


   foreach my $cram (@{$rpt_aref}){
           ## seqchksum needs to be prior downloaded from iRODS or from the staging area
           my $sqchk;
           ($sqchk = $cram) =~ s/cram/seqchksum/xms;

           my(@path) = File::Spec->splitpath($sqchk);
           $sqchk =  $self->original_seqchksum_dir().q[/].$path[-1];

           if ($cram =~ / ^\/seq\/ /xms){
                ##irods: prefix needs adding to the cram irods path name
                $cram =~ s/^/irods:/xms;
            }

           $sample_cram_input      .= qq(-keys incrams -vals $cram );
           $sample_seqchksum_input .= qq(-keys incrams_seqchksum -vals $sqchk );
   }

   my $cmd           = qq($VTFP_SCRIPT -l $vtfp_log -o $sample_vtfp_template ) .
                    qq(-keys library -vals $merge_sample_name ) .
                    qq(-keys cfgdatadir -vals $vtlib ) .
                    qq(-keys samtools_executable -vals $SAMTOOLS ) .
                     q(-keys outdatadir -vals outdata ) .
                    qq(-keys basic_pipeline_params_file -vals $vtlib/$P4_COMMON_TEMPLATE ) .
                     q(-keys bmd_resetdupflag_val -vals 1 ) .
                    qq($sample_cram_input $sample_seqchksum_input  $vtlib/$P4_MERGE_TEMPLATE );

                     $self->log("\nVTFP_CMD $cmd\n");

return($cmd);
}

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

sub viv_job {
   my $self = shift;

   my $merge_sample_name = $self->_sample_merged_name();

    my $viv_log   = join q[.],'viv',$merge_sample_name,$P4_MERGE_TEMPLATE;
       $viv_log   =~ s/json$/LOG/xms;
    my $viv_template = join q[.],$merge_sample_name,$P4_MERGE_TEMPLATE;
    my $cmd  = qq($VIV_SCRIPT -v 3 -x -s -o $viv_log ./$viv_template);
    my $job_name = 'viv_merge-%J';

return($cmd);
}

sub _destination_path {
    my ($self, $runfolder_path, $src, $dest) = @_;
    if (!$src || !$dest) {
        carp 'Need two names'; return;
    }

    ## outgoing -> analysis or vice versa
    $runfolder_path =~ s{/$src/}{/$dest/}msx;

    return $runfolder_path;
}

sub load_to_irods {
    my $self = shift;

    my $IRODS_AREA = q[/seq/illumina/library_merge/].$self->_sample_merged_name();
    #my $IRODS_AREA = q[/seq/npg/test1/merged/].$self->_sample_merged_name();

    my $irods = $self->_irods();

    my $collection =  $irods->add_collection($IRODS_AREA);

    ##remove public access
       $irods->set_group_access('null','public',$collection);

    ##set group access
    my $group = $self->access_groups();

    my @group_access = $irods->set_group_access('read',$group,$collection);

    $self->log(qq[Added collection to iRODS: $collection]);

=head1

Files to load are those in $self->merge_dir().q[/outdata]   (not cram.md5, markdups_metrics)

  11869933.ALXX.paired302.bamcheck
  11869933.ALXX.paired302.cram
  11869933.ALXX.paired302.cram.crai
  11869933.ALXX.paired302.flagstat
  11869933.ALXX.paired302.seqchksum
  11869933.ALXX.paired302.sha512primesums512.seqchksum
  11869933.ALXX.paired302_F0x900.stats
  11869933.ALXX.paired302_F0xB00.stats


###objects to add

my $path = $self->merge_dir().q[/outdata/].$self->_sample_merged_name();
                        (*not id_run and lane*)
           .cram       #reference, type (cram),sample_id, is_paired_read, sample_common_name
                       # manual_qc, sample, sample_accession_number, study, study_accession_number
                       # library, study_id study_title, library_id, total_reads, md5, alignment
                       #  target =library composition(?)=$self->rpt_list() 
           .cram.crai  #md5 type (crai)
           .flagstat   #object avus: md5, type (flagstat)
           .bamcheck   #md5 type(bamcheck)
           _F0x900.stats #md5 type (stats)
           _F0xB00.stat  #md5 type (stats)
           .seqchksum    #md5 type (seqchksum)
           .sha512primesums512.seqchksum  #md5 type (sha512primesums512.seqchksum)
           
=cut

my $data =  $self->irods_data_to_add();
my $path_prefix = $self->merge_dir().q[/outdata/];

foreach my $file (keys %{$data}){
        my $irods_path  = $irods->add_object(qq[${path_prefix}$file],qq[$collection/$file]);
        $self->log("Added irods object $irods_path");

        ## Other than for the cram (already present) calculate md5sum ##
         my $merged_md5;
        $merged_md5 = ($file =~ /cram$/msx) ? $data->{$file}{'md5'} : $irods->md5sum(qq[${path_prefix}$file]);
        $irods->add_object_avu(qq{$collection/$file},'md5',$merged_md5);
        my $ok = $irods->validate_checksum_metadata(qq{$collection/$file});
        if (! $ok){ carp "${path_prefix}$file md5 does not match meta data md5\n" }

        foreach my $attrib (sort keys %{$data->{$file}}){
                my $value = $data->{$file}{$attrib};

                next if ($attrib eq 'md5');

      	        $irods->add_object_avu(qq{$collection/$file},
                                     $attrib,
                                     $value);
                $self->log("Added attribute to $file $attrib $value");
        }
}

return;
}

sub irods_data_to_add {
    my $self = shift;
    my $data = {};

     my $path_prefix = $self->merge_dir().q[/outdata/].$self->_sample_merged_name();
     my $merged_name = $self->_sample_merged_name();

    my $cram_md5 = read_file($path_prefix.q[.cram.md5]);
    chomp $cram_md5;

    ### values from _lims will be for last sample in sorted rpt list ##

   ## add tag=>$tag if tag

    $data->{$merged_name.q[.cram]} = {
                    'type'                    => 'cram',
                    'reference'               => $self->_reference_genome_path(),
                    'sample_id'               => $self->sample_id(),
                    'sample'                  => $self->sample_name(),
                    'is_paired_read'          => $self->run_type =~ /^paired/msx ? 1 : 0,
                    'sample_common_name'      => $self->sample_common_name(),
                    'sample_accession_number' => $self->sample_accession_number() || undef,
                    'manual_qc'               => 1,#if filter=>mqc not used in file_merge.pm this may not be true
                    'study'                   => $self->study_accession_number() || undef,
                    'study_id'                => $self->study_id(),
                    'study_title'             => $self->study_title(),
                    'study_accession_number'  => $self->study_accession_number() || undef,
                    'library_id'              => $self->library_id(),
                    'target'                  => q[library],
                    'alignment'               => $self->aligned,
                    'total_reads'             => $self->get_number_of_reads($path_prefix.q[.flagstat]),
                    'md5'                     => $cram_md5,
                    'composition'             => $self->rpt_list()
                       };

      $data->{$merged_name.q[.cram.crai]}                    = {'type' => 'crai'};
      $data->{$merged_name.q[.flagstat]}                     = {'type' => 'flagstat'};
      $data->{$merged_name.q[.bamcheck]}                     = {'type' => 'bamcheck'};
      $data->{$merged_name.q[_F0x900.stats]}                 = {'type' => 'stats'};
      $data->{$merged_name.q[_F0xB00.stats]}                 = {'type' => 'stats'};
      $data->{$merged_name.q[.seqchksum]}                    = {'type' => 'seqchksum'};
      $data->{$merged_name.q[.sha512primesums512.seqchksum]} = {'type' => 'sha512primesums512.seqchksum'};


return($data);
}

=head1 

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
return($total_reads);
}


=head1

If npg_do_not_move/README file added, remove.  If outgoing moved to analysis move back to outgoing.
=cut


sub _clean_up{
   my $self = shift;

  my @runfolders_moved = @{$self->_runfolder_moved};
  my $v = undef;

  foreach my $runfolder (@runfolders_moved){
          my $npg_do_not_move_dir =qq[$runfolder/npg_do_not_move];
          my $readme_file = qq[$npg_do_not_move_dir/README];
          $self->log("Remove $readme_file\n");

          eval{
          unlink $readme_file or carp "Could not remove file $readme_file: $OS_ERROR";
          } or do { carp "$EVAL_ERROR"; $v=1};
          $self->log("Remove $npg_do_not_move_dir\n");

          eval {
          rmdir $npg_do_not_move_dir or carp "Could not remove directory $npg_do_not_move_dir: $OS_ERROR";
          } or do { carp "$EVAL_ERROR"; $v=1};

          my $destination = $self->_destination_path($runfolder,'analysis','outgoing');
             $self->log("move $runfolder $destination");
          carp "Could not move from analysis to outgoing\n" if ! $self->_move_folder($runfolder,$destination);
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

=item Carp

=item npg_seq_melt::file_merge

=item WTSI::NPG::iRODS 

=item WTSI::DNAP::Utilities::Runnable

=item srpipe::runfolder

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
