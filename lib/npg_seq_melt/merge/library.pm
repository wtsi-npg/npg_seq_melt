######### 
# Author:        jillian
# Created:       2015-04-29
#

package npg_seq_melt::merge::library;

use Moose;
use MooseX::StrictConstructor;
use Moose::Meta::Class;
use Carp;
use English qw(-no_match_vars);
use List::MoreUtils qw { any };
use IO::File;
use File::Path qw/ make_path /;
use File::Spec qw/ splitpath catfile /;
use File::Copy qw/ copy move /;
use File::Basename qw/ basename /;
use File::Slurp qw( :std );
use Archive::Tar;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS::Publisher;
use Cwd;

use npg_tracking::glossary::composition::factory;

extends qw/npg_seq_melt::merge npg_seq_melt::merge::base/;

our $VERSION = '0';

Readonly::Scalar my $P4_MERGE_TEMPLATE   => q[merge_aligned.json];
Readonly::Scalar my $P4_COMMON_TEMPLATE  => q[alignment_common.json];
Readonly::Scalar my $VIV_SCRIPT          => q[viv.pl];
Readonly::Scalar my $VTFP_SCRIPT         => q[vtfp.pl];
Readonly::Scalar my $MD5_SUBSTRING_LENGTH => 10;
Readonly::Scalar my $SUMMARY_LINK        => q{Latest_Summary};
Readonly::Scalar my $SSCAPE              => q[SQSCP];
Readonly::Scalar my $SUFFIX_PATTERN      => join q[|], qw[cram crai flagstat stats txt seqchksum tgz];

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

=head2 sample_merged_name

Name for the merged cram file, representing the component rpt.

=cut 

has '_sample_merged_name' => (
     isa           => q[Str],
     is            => q[ro],
     lazy_build    => 1,
     reader        => 'sample_merged_name',
    );
sub _build__sample_merged_name {
    my $self = shift;
    my $md5 = $self->composition->digest('md5');
    $md5 = substr $md5, 0, $MD5_SUBSTRING_LENGTH;
    return join q{.}, $self->library_id(),
                      $self->chemistry(),
                      $self->run_type(),
                      $md5;
}


with qw{
  npg_seq_melt::merge::qc
  npg_seq_melt::util::irods
};


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


=head2 use_cloud

Set off commands as wr add jobs

=cut

has 'use_cloud'      => ( isa           => 'Bool',
                          is            => 'ro',
                          default       => 0,
                          documentation =>
  'Boolean flag, false by default,  ' .
  'ie the commands are not submitted to wr for execution.',
);


=head2 sample_id

Sample ID

=cut

has 'sample_id' => (
     isa           => q[Str],
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
     documentation => q[from database],
    );

=head2 library_id

Library ID

=cut

has 'library_id' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[database Library ID],
    );

=head2 study_id

Study ID

=cut

has 'study_id' => (
     isa           => q[Str],
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
     documentation => q[database study accession number],
    );


=head2 lims_id

LIMS id e.g. SQSCP, C_GCLP

=cut

has 'lims_id' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[LIMS id e.g. SQSCP, C_GCLP],
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
     required      => 1,
     documentation => q[Full path to reference genome including fasta file name],
    );


=head2 library_type 


=cut

has 'library_type' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 1,
     documentation => q[iseq_flowcell.pipeline_id_lims with alias default_library_type in WTSI::DNAP::Warehouse::Schema::Result::IseqFlowcell. Libraries with library_type Chromium genome are skipped],
    );



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
                     is            => q[ro],
                     lazy_build    => 1,
                     documentation => q[collection within irods to store the output of the merge],
                    );

sub _build_collection {
    my $self = shift;
    return $self->default_root_dir().$self->sample_merged_name();
}

=head2 vtlib

Specify P4 vtlib to use to find template json files

=cut

has 'vtlib'   => (
    isa           => q[Str],
    is            => q[rw],
    default       => q{$}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl)))/../data/vtlib/},
    documentation => q[Location of vtlib of template json files. The default is the one in the path environment],
    );

=head2 _readme_file_name

Name for the README file

=cut 

has '_readme_file_name' => (
     isa           => q[Str],
     is            => q[ro],
     lazy_build    => 1,
);
sub _build__readme_file_name {
    my $self = shift;
    return join q{.}, q{README}, $self->sample_merged_name();
}

=head2 _tar_log_files

Make gzip file of log files for loading to iRODS

=cut

has '_tar_log_files' => (
     isa           => q[Str],
     is            => q[ro],
     lazy_build    => 1,
);
sub _build__tar_log_files{
    my $self = shift;
    opendir my $dh, $self->merge_dir() or carp q[Cannot get listing for a directory, cannot open ] ,$self->merge_dir();
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
 
Cram files are only sourced from iRODS.

=cut

sub _source_cram {
    my ($self, $c) = @_;

    if (!$c) {
        croak 'Component attribute required';
    }

    my $paths = $self->standard_paths($c);
    return $paths;


}

=head2 original_seqchksum_dir

=cut

has 'original_seqchksum_dir' => (
     isa           => q[Str],
     is            => q[rw],
     metaclass  => 'NoGetopt',
);

has '_paths2merge' => (
     isa           => q[ArrayRef],
     is            => q[ro],
     lazy_build    => 1,
    );

sub _build__paths2merge {
    my $self = shift;

    my @path_list = ();

    if(! $self->has_irods){$self->set_irods($self->get_irods);}

    my $factory = npg_tracking::glossary::composition::factory->new();
    foreach my $c ($self->composition->components_list()) {

        my $paths = $self->_source_cram($c);

        eval {
            my $query = {'irods_cram' => $paths->{'irods_cram'},
                         'sample_id'  => $self->sample_id(),
                         'sample_acc' => $self->sample_accession_number(),
                         'ref_path'   => $self->reference_genome_path,
                         'library_id' => $self->library_id(),
            };
            if ($self->use_cloud()){ $query->{'s3_cram'} = $paths->{'s3_cram'} }
            #if (!$self->can_run($query)){
            #   my $cram = $paths->{'s3_cram'} ? $self->use_cloud() : $paths->{'irods_cram'};
             #  croak qq[Cram header check failed for $cram \n];
            #}
            carp qq[can_run turned off in library.pm];
            1;
        } or do {
            carp $EVAL_ERROR;
            next;
        };

        if ($self->use_cloud()){
         push @path_list, $paths->{'s3_cram'};
        }
        else {
          push @path_list, $paths->{'irods_cram'};
        }

        $factory->add_component($c);
    }

    $self->clear_irods;

    my $composition2merge = $factory->create_composition();
    if ($self->composition->num_components() != $composition2merge->num_components()){
        my $digest1 = $self->composition->freeze();
        my $digest2 = $composition2merge->freeze();
        $self->log("Original composition: $digest1\n");
        $self->log("New composition: $digest2\n");
        croak
          sprintf '%sComponent count to merge(%i) does not equal that in original list (%i)%s',
	    qq[\n],
            $composition2merge->num_components(),
            $self->composition->num_components(),
            qq[\n];
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
    if ($self->use_cloud()){ $self->run_dir(cwd()); $self->log(q{RUN_DIR:},$self->run_dir()) }
    chdir $self->run_dir() or croak q[cannot chdir ],$self->run_dir(),qq[: $OS_ERROR];

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

        if ($merge_err) {
            croak "Skipping iRODS loading, problems with merge\n";
        }
    } else {
        carp "0 sample(s) passed checks, skip merging\n";
    }

    return;
}



=head2 do_merge

=cut

sub do_merge {
    my $self    = shift;

    $self->log(q[DO MERGING name=], $self->sample_merged_name());
    $self->log(q[CWD=],cwd());
    $self->log(q[RD=],$self->run_dir());
    $self->log(q[MD=],$self->merge_dir());
    ###set up sub-directory for sample  ################################
    my $subdir = $self->merge_dir();
    return 0 if !$self->run_make_path(qq[$subdir/outdata/qc]);

    my $original_seqchksum_dir = join q{/},$subdir,q{input};
    return 0 if !$self->run_make_path($original_seqchksum_dir);
    $self->original_seqchksum_dir($original_seqchksum_dir);

    chdir $original_seqchksum_dir or croak qq[cannot chdir $original_seqchksum_dir : $OS_ERROR];
    return 0 if !$self->get_seqchksum_files();

    chdir $subdir or croak qq[cannot chdir $subdir: $OS_ERROR];

    ## mkdir in iRODS and ichmod so directory not public 
    if(! $self->has_irods){$self->set_irods($self->get_irods);}

    $self->irods->add_collection($self->collection() . q{/qc});

    foreach my $dir ($self->collection(), $self->collection() . q{/qc/}){
        $self->irods->set_collection_permissions($WTSI::NPG::iRODS::NULL_PERMISSION,
                                                 $WTSI::NPG::iRODS::PUBLIC_GROUP,
                                                 $dir);
    }

    return 0 if !$self->run_make_path(qq[$subdir/status]);

    my($vtfp_cmd) = $self->vtfp_job();
    $self->clear_irods;

    return 0 if !$self->run_cmd($vtfp_cmd);
    my($viv_cmd) = $self->viv_job();
    return 0 if !$self->run_cmd($viv_cmd);

    return 0 if !$self->make_bam_flagstats_json();

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
        next if -e join q{/},$self->original_seqchksum_dir(),basename($seqchksum_file);

        if ($self->use_cloud()){ return 0 if !$self->run_cmd(qq[cp ../../$seqchksum_file . ]); }
        else {
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

    my $vtlib = $self->vtlib();
    my $merge_sample_name = $self->sample_merged_name();
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

    my $root = $self->irods_root;
    foreach my $cram ( @{$self->_paths2merge}){
        ## seqchksum needs to be prior downloaded from iRODS or from the staging area
        my $sqchk;
        ($sqchk = $cram) =~ s/cram/seqchksum/xms;
        my(@path) = File::Spec->splitpath($sqchk);
        $sqchk =  $self->original_seqchksum_dir().q[/].$path[-1];

        if ($cram =~ / ^$root /xms){
            ##irods: prefix needs adding to the cram irods path name
            my $hostname = $self->get_irods_hostname($cram,$replicate_index);
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

   my $merge_sample_name = $self->sample_merged_name();

    my $viv_log   = join q[.],'viv',$merge_sample_name,$P4_MERGE_TEMPLATE;
       $viv_log   =~ s/json$/LOG/xms;
    my $viv_template = join q[.],$merge_sample_name,$P4_MERGE_TEMPLATE;
    my $cmd  = qq($VIV_SCRIPT -v 3 -x -s -o $viv_log ./$viv_template);
    my $job_name = 'viv_merge-%J';

    return $cmd;
}


=head2 load_to_irods

Files to load are those in $self->merge_dir().q[/outdata]  or $self->merged_qc_dir()
Relies on public read access having been removed from the collection in iRODS earlier 
in the process.

=cut

sub load_to_irods {

    my $self = shift;

    my $data = $self->irods_data_to_add();
    my $path_prefix = $self->merge_dir().q[/outdata/];

    ## modify permissions
    my $irods_group;
    if($self->lims_id() eq $SSCAPE){
        $irods_group = q{ss_}.$data->{$self->sample_merged_name().q[.cram]}->{study_id};
    }

    if(! $self->has_irods){ $self->set_irods($self->get_irods); }

    # initialise mkdir flag
    $self->mkdir_flag(0);
    my $in_progress =  $self->merge_dir . q[/status/loading_to_irods];
    $self->run_cmd(qq[touch $in_progress]);

    # sub/super set may already exist so remove target=library if present
    $self->_reset_existing_cram();

    my $publisher = WTSI::NPG::iRODS::Publisher->new(irods => $self->irods);

    foreach my $file (keys %{$data}){

        my $pp_file = ${path_prefix}.$file;
        my $collection = $self->collection();

        if ($data->{$file}{'type'} eq 'json'){
              $pp_file = ${path_prefix}.q{qc/}.$file;
              $collection = $self->collection().q{/qc};
        }

        my $remote_file = File::Spec->catfile($collection,$file);
        $self->log("Trying to load irods object $pp_file to $remote_file");

        if($file =~ /[.]cram$/mxs){
            $self->_add_cram_meta($remote_file,$data->{$file});
        } else {
            $publisher->publish_file($pp_file, $remote_file);
        }

        if($irods_group){
            $self->irods->set_object_permissions($WTSI::NPG::iRODS::READ_PERMISSION,
                                                 $irods_group, $remote_file);
        }

        $self->log("Added irods object $file to $collection");

        $self->remove_outdata() && unlink $pp_file;

    }

    ## reset collection to read access for public
    foreach my $dir ($self->collection(), $self->collection() . q{/qc/}){
        $self->irods->set_collection_permissions($WTSI::NPG::iRODS::READ_PERMISSION,
                                                 $WTSI::NPG::iRODS::PUBLIC_GROUP,
                                                 $dir);
    }

    $self->log("Removing $in_progress");
    unlink $in_progress or carp "cannot remove $in_progress : $ERRNO\n";
    $self->clear_irods;

    return;
}

=head2 _add_cram_meta

Given a file on the server and a list of meta data, add the meta data to the file.
Requires a remote file and irods connection. Needed because WTSI::NPG::HTS::Publisher 
doesn't currently handle values which are refs to arrays of json values or adding
meta data to files which already exist in iRODS.

=cut

sub _add_cram_meta {
  my ($self,$file,$meta_data) = @_;

  if(!$self->has_irods() || !$file ){
      croak 'cant add meta data';
  }

  my $obj = WTSI::NPG::iRODS::DataObject->new($self->irods, $file);
  my $md5 = $obj->checksum;

  if($md5 ne $meta_data->{'md5'}){
      croak qq{MD5 from local file doesn't match iRODS value for $file};
  }

  foreach my $attr (sort keys %{$meta_data}) {
      my $value = $meta_data->{$attr};

      if (defined $value and $value ne q{}) {
          my ($leading_whitespace, $lead_trimmed) = $value =~ m{^(\s+)(.*)$}msx;
          if ($leading_whitespace) {
              $value = $lead_trimmed;
          }
          my ($trail_trimmed, $trailing_whitespace) = $value =~ m{^(.*)(\s+)$}msx;
          if ($trailing_whitespace) {
              $value = $trail_trimmed;
          }

          if (ref $value eq q{ARRAY}) {
              $obj->supersede_multivalue_avus($attr, $value);
          } else {
              $obj->supersede_avus($attr, $value);
          }
      }
  }

  return;
}

=head2 reset_existing_cram

irods attribute must be set

=cut

sub _reset_existing_cram {

    my $self = shift;
    if(!$self->has_irods() ){
        croak 'cant reset existing cram';
    }

    my @found = $self->irods->find_objects_by_meta($self->default_root_dir(),
                                                   ['library_id' => $self->library_id()],
                                                   ['target'     => 'library'],
                                                   ['chemistry'  => $self->chemistry()],
                                                   ['run_type'   => $self->run_type() ],
                                                   ['study_id'   => $self->study_id() ]);

    if (@found){
        $self->log("Remove target=library for $found[0]");
        $self->irods->remove_object_avu($found[0],'target','library') ;
    }

    return();
}


=head2 irods_data_to_add

=cut

sub irods_data_to_add {
    my $self = shift;
    my $data = {};

    my $path_prefix = $self->merge_dir().q[/outdata/].$self->sample_merged_name();
    my $merged_name = $self->sample_merged_name();

    my $cram_md5 = read_file($path_prefix.q[.cram.md5]);
    chomp $cram_md5;

    ## Need ArrayRef of json strings to populate multiple member attributes in iRODS
    my @members = map { $_->freeze() } $self->composition->components_list();

    ## create tar.gz of log files
    my $tar_file = $self->_tar_log_files();

    ## load any suitable files in outdata directory
    opendir my $od, $self->merge_dir(). q[/outdata/] or croak q[Cannot open ].$self->merge_dir();
    my @files = readdir $od;
    closedir $od;

    foreach my $file(@files){
       my ($suffix) = $file =~ m{[.]($SUFFIX_PATTERN)$}msx;
       next if ! $suffix;
       $data->{$file} = {'type' => $suffix};
    }

    ## set meta data for cram file
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
                    'library_type'            => $self->library_type(),
                    'target'                  => q[library],
                    'alignment'               => $self->aligned,
                    'total_reads'             => $self->get_number_of_reads($path_prefix.q[.flagstat]),
                    'md5'                     => $cram_md5,
                    'chemistry'               => $self->chemistry(),
                    'instrument_type'         => $self->instrument_type(),
                    'run_type'                => $self->run_type(),
                    'composition_id'          => $self->composition->digest(),
                    'component'               => \@members,
                    'composition'             => $self->composition->freeze(),
                     };

    if( $self->sample_accession_number()){
        $data->{$merged_name.q[.cram]}->{'sample_accession_number'} = $self->sample_accession_number();
    }
    if( $self->study_accession_number()){
        $data->{$merged_name.q[.cram]}->{'study_accession_number'} = $self->study_accession_number();
    }

    ## load any json files in qc directory
    opendir my $dh, $self->merged_qc_dir() or carp q[Cannot open ].$self->merged_qc_dir();
    while (readdir $dh){
          if (/(\S+json)$/xms){  $data->{$1} = {'type' => 'json'}}
    }
    closedir $dh;

    return($data);
}

=head2 get_number_of_reads

Get number of reads from first line of flagstat file to add to cram file total_reads iRODS meta data object AVU

333140294 + 6982226 in total (QC-passed reads + QC-failed reads)

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

=item MooseX::StrictConstructor

=item MooseX::Getopt

=item Moose::Meta::Class

=item English -no_match_vars

=item List::MoreUtils

=item IO::File

=item Carp

=item npg_tracking::data::reference

=item npg_tracking::glossary::composition::factory

=item npg_common::irods::Loader

=item Archive::Tar

=item File::Path

=item File::Spec 

=item File::Copy 

=item File::Slurp 

=item File::Basename

=item st::api::lims

=item npg_seq_melt::util::irods

=item npg_seq_melt::merge::qc

=item WTSI::NPG::iRODS

=item WTSI::NPG::iRODS::DataObject

=item WTSI::NPG::iRODS::Publisher

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016 Genome Research Limited

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
