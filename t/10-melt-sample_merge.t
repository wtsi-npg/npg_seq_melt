use strict;
use warnings;
use Test::More tests => 35;
use Test::Deep;
use Test::Exception;
use File::Temp qw/ tempdir /;
use File::Path qw/make_path/;
use File::Copy;
use Data::Dumper;
use Carp;
use_ok('npg_seq_melt::sample_merge');
use_ok('srpipe::runfolder');



$ENV{TEST_DIR} = q(t/data);

my $tmp_dir = tempdir( CLEANUP => 0);
#local $ENV{NPG_WEBSERVICE_CACHE_DIR} = $tmp_dir;
my $rd = q[/nfs_sf39_ILorHSany_sf39/outgoing/150312_HX7_15733_B_H27H7CCXX];
my $do_not_move_dir = $tmp_dir.$rd.q[/npg_do_not_move];
make_path($do_not_move_dir,{verbose => 0}) or carp "make_path failed : $!\n";
system("touch $do_not_move_dir/README");
my $archive =$rd.q[/Data/Intensities/BAM_basecalls_20150315-045311/no_cal/archive];
my $tmp_path = join q[/],$tmp_dir,$archive;
make_path($tmp_path,{verbose => 0}) or carp "make_path failed : $!\n";
my $analysis_path = $tmp_dir.q[/nfs_sf39_ILorHSany_sf39/analysis];
make_path($analysis_path,{verbose => 0}) or carp "make_path failed : $!\n";

my $test_cram =  join q[/],$ENV{TEST_DIR},$archive,q[15733_1.cram];
my $copy_test_cram =  join q[/],$tmp_path,q[15733_1.cram];
copy($test_cram,$copy_test_cram) or carp "Copy failed: $!";

my $run_dir     = join q[/],'npg','run';
my $batch_dir   = join q[/],'st','batches';
my $samples_dir = join q[/],'st','samples';
my $study_dir   = join q[/],'st','studies';


{
######## run where cram header has bamsort adddupmarksupport=1 present
make_path(qq[$tmp_dir/metadata_cache_15733/$run_dir],
          qq[$tmp_dir/metadata_cache_15733/$batch_dir],
          qq[$tmp_dir/metadata_cache_15733/$samples_dir],
          qq[$tmp_dir/metadata_cache_15733/$study_dir],
          {verbose => 0});

copy("$ENV{TEST_DIR}/st_api_lims/metadata_cache_15733/st/batches/36028.xml", "$tmp_dir/metadata_cache_15733/$batch_dir/36028.xml");
copy("$ENV{TEST_DIR}/st_api_lims/metadata_cache_15733/$samples_dir/2183757.xml", "$tmp_dir/metadata_cache_15733/$samples_dir/2183757.xml");
copy("$ENV{TEST_DIR}/st_api_lims/metadata_cache_15733/$run_dir/15733.xml","$tmp_dir/metadata_cache_15733/$run_dir/15733.xml");
copy("$ENV{TEST_DIR}/st_api_lims/metadata_cache_15733/$study_dir/2245.xml","$tmp_dir/metadata_cache_15733/$study_dir/2245.xml");


local $ENV{NPG_WEBSERVICE_CACHE_DIR} = "$tmp_dir/metadata_cache_15733";

my $sample_merge = npg_seq_melt::sample_merge->new({
   rpt_list        =>  '15972:5;15733:1;15733:2',
   sample_id       =>  '2183757',
   sample_name     =>  '3185STDY6014985',
   sample_accession_number => 'EGAN00001252242',
   sample_common_name => 'Homo Sapiens',
   library_id      =>  '13149752',   
   instrument_type =>  'HiSeqX' ,  
   study_id        =>  '3185',
   study_title     =>  'The life history of colorectal cancer metastases study WGS X10',
   study_accession_number => 'EGAS00001000864',
   run_type        =>  'paired',
   chemistry       =>  'CCXX', #HiSeqX_V2
   run_dir         =>  $tmp_dir,
   aligned         =>  1,
   #reference_genome => 'Homo_sapiens (CGP_GRCh37.NCBI.allchr_MT)',
   
   local           =>  1,
   nobsub          =>  1,
   });

isa_ok($sample_merge,'npg_seq_melt::sample_merge','passed object test');

is($sample_merge->_destination_path($tmp_dir.$rd,'outgoing','analysis'),"$tmp_dir/nfs_sf39_ILorHSany_sf39/analysis/150312_HX7_15733_B_H27H7CCXX","analysis runfolder made from outgoing");
is($sample_merge->_destination_path($tmp_dir.$archive,'outgoing','analysis'),qq{$analysis_path/150312_HX7_15733_B_H27H7CCXX/Data/Intensities/BAM_basecalls_20150315-045311/no_cal/archive},"analysis runfolder made from outgoing");
is($sample_merge->_move_folder($tmp_dir.$rd,qq{$tmp_dir/nfs_sf39_ILorHSany_sf39/analysis/150312_HX7_15733_B_H27H7CCXX}),1,'folder moved to analysis');


is($sample_merge->rpt_list(),'15972:5;15733:1;15733:2','Correct rpt_list');
is($sample_merge->sample_id(),'2183757','Correct sample_id');
is($sample_merge->library_id(),'13149752','Correct library_id');


is($sample_merge->vtlib(),'$(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/','expected vtlib command');


my $expected_rpt = [
                    '15733:1',
                    '15733:2',
                    '15972:5',
                   ];

cmp_deeply($sample_merge->_rpt_aref(),$expected_rpt,'Correct run-position-tag arrayref returned');

foreach my $rpt (@{$sample_merge->_rpt_aref()}){ 
        $sample_merge->split_fields($rpt);
        print $sample_merge->_formatted_rpt(),"\n";
}

my $rpt0 = $sample_merge->_rpt_aref()->[0];
           $sample_merge->split_fields($rpt0);
is($sample_merge->id_run(),'15733','First run id = 15733');
is($sample_merge->lane(),'1','First lane = 1');
is($sample_merge->tag_index(),undef,'No tag index');
$sample_merge->_source_cram();
is($sample_merge->irods_cram(),'/seq/15733/15733_1.cram','iRODS cram name OK');
is($sample_merge->_sample_merged_name(),'13149752.CCXX.paired', '_sample_merged_name is 13149752.CCXX.paired');


my $use_rpt = ['15733:1','15972:5'];
cmp_deeply($sample_merge->_use_rpt(),[],'Empty _use_rpt');
$sample_merge->_use_rpt($use_rpt);
is($sample_merge->_use_rpt(),$use_rpt,'Arrayref of values for _use_rpt');

## Set source cram to test path
#$sample_merge->_source_cram("$ENV{TEST_DIR}/nfs/sf39/ILorHSany_sf39/analysis/150312_HX7_15733_B_H27H7CCXX/Data/Intensities/BAM_basecalls_20150315-045311/no_cal/archive/15733_1.cram");
$sample_merge->_source_cram();

###_source_cram calls new instance of srpipe::runfolder and it is looking in standard location not test one TODO
#is($sample_merge->_source_cram(),"$ENV{TEST_DIR}/nfs/sf39/ILorHSany_sf39/analysis/150312_HX7_15733_B_H27H7CCXX/Data/Intensities/BAM_basecalls_20150315-045311/no_cal/archive/15733_1.cram",'cram header only path');


my @irods_meta = ();
@irods_meta = ({'attribute' => 'library_id', 'value' => '13149752'});
is($sample_merge->check_cram_header(\@irods_meta),13149752,'cram header check passes');

is($sample_merge->_header_sample_name(),'EGAN00001252242','Header sample name');
is($sample_merge->_header_ref_name(),'/lustre/scratch109/srpipe/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa','Header ref name from first SQ row');
#print Dumper $sample_merge;
$sample_merge->_header_sample_name("XXXXXXX");
#print Dumper $sample_merge;
#isnt ($sample_merge->check_cram_header(\@irods_meta),13149752,'cram header check fails if difference between header SM fields');

##test loading to iRODS
my $dir = tempdir( CLEANUP => 1 );
my @comp = split '/', $dir;
my $dname = pop @comp;
my $IRODS_TEST_AREA1 = "/seq/npg/test1/merged/$dname";
like ($sample_merge->_irods(),qr/WTSI::NPG::iRODS/msx,q[Correct WTSI::NPG::iRODS connection]);
#my $collection =  $irods->add_collection($IRODS_TEST_AREA1);

is($sample_merge->_clean_up(),undef,'_clean_up worked');

}
###### plexed run
#mysql> select id_run,position,tag_index,asset_id,sample_id,batch_id,study_id,project_id  from npg_plex_information where sample_id = 2190607;
#+--------+----------+-----------+----------+-----------+----------+----------+------------+
#| id_run | position | tag_index | asset_id | sample_id | batch_id | study_id | project_id |
#+--------+----------+-----------+----------+-----------+----------+----------+------------+
#|  15531 |        7 |         9 | 12888653 |   2190607 |    35181 |     2245 |       1154 |
#|  15795 |        1 |         9 | 12888653 |   2190607 |    36242 |     2245 |       1154 |
#+--------+----------+-----------+----------+-----------+----------+----------+------------+

#/nfs/sf18/ILorHSany_sf18/outgoing/150320_HS2_15795_A_C6N6DACXX/Data/Intensities/BAM_basecalls_20150328-170701/no_cal/archive/lane1/15795_1#9.cram
# ls /nfs/sf18/ILorHSany_sf18/outgoing/150320_HS2_15795_A_C6N6DACXX/Data/Intensities/BAM_basecalls_20150328-170701/metadata_cache_15795/
#lane_1.taglist  lane_2./taglist  lane_3.taglist  lane_4.taglist  lane_5.taglist  lane_6.taglist  lane_7.taglist  lane_8.taglist  npg  samplesheet_15795.csv  st_original
# /nfs/sf36/ILorHSany_sf36/outgoing/150213_HS33_15531_B_C6B43ACXX/Data/Intensities/BAM_basecalls_20150331-122837/no_cal/archive/lane7/15531_7#9.cram
{
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data/st_api_lims/metadata_cache_15795';
my $tempdir = tempdir( CLEANUP => 1);

my $sample_merge = npg_seq_melt::sample_merge->new({
   rpt_list        =>  '15531:7:9;15795:1:9',
   sample_id          =>  '2190607',
   sample_name        => '2245STDY6020070',
   sample_accession_number => 'ERS627290',
   sample_common_name      => 'Streptococcus pneumoniae',
   library_id         =>  '128886531',
   instrument_type =>  'HiSeq',
   study_id           =>  '2245',
   study_title     => 'Global Pneumococcal Sequencing (GPS) study I',
   study_accession_number => 'ERP001505',
   run_type        =>  'paired',
   chemistry       =>  'ACXX', #'HiSeq_V3',
   run_dir         =>  $tempdir,
   aligned         => 1,
   #reference_genome => 'Streptococcus_pneumoniae (ATCC_700669)',
   local           =>  1,
   });


#is ($sample_merge->from_staging(),1,'default original cram dir is from_staging');
is ($sample_merge->run_dir(),$tempdir, 'Correct run_dir');
is ($sample_merge->_sample_merged_name(),q[128886531.ACXX.paired],'Correct sample merged name');
is ($sample_merge->merge_dir(),qq[$tempdir/128886531.ACXX.paired], 'Correct merge library sub-directory');
my $subdir = $sample_merge->merge_dir();
is ($sample_merge->run_make_path(qq[$subdir/outdata]),1,'outdata generated OK');

my $expected_rpt = [
                   '15531:7:9','15795:1:9'
                   ];
cmp_deeply($sample_merge->_rpt_aref(),$expected_rpt,'Correct run-position-tag arrayref returned');

foreach my $rpt (@{$sample_merge->_rpt_aref()}){
        $sample_merge->split_fields($rpt);
        print $sample_merge->_formatted_rpt(),"\n";
}

is($sample_merge->id_run(),'15795','last id_run 15795');
is($sample_merge->lane(),'1','last position 1');
is($sample_merge->tag_index(),'9','last tag_index');
#is($sample_merge->reference_genome(),'Streptococcus_pneumoniae (ATCC_700669)','Correct reference_genome');
is($sample_merge->_reference_genome_path(),'/lustre/scratch110/srpipe/references/Streptococcus_pneumoniae/ATCC_700669/all/bwa/S_pneumoniae_700669.fasta','Correct full reference path');

### no bamsort adddupmarksupport=1 present in header -> should not run

$sample_merge->_source_cram("$ENV{TEST_DIR}/nfs/sf18/ILorHSany_sf18/outgoing/150320_HS2_15795_A_C6N6DACXX/Data/Intensities/BAM_basecalls_20150328-170701/no_cal/archive/lane1/15795_1#9.cram");

is($sample_merge->_source_cram(),"$ENV{TEST_DIR}/nfs/sf18/ILorHSany_sf18/outgoing/150320_HS2_15795_A_C6N6DACXX/Data/Intensities/BAM_basecalls_20150328-170701/no_cal/archive/lane1/15795_1#9.cram",'cram header only path');

print $sample_merge->id_run(),$sample_merge->lane(),$sample_merge->tag_index(),"\n";
#$sample_merge->_lims();

my @irods_meta = ();
@irods_meta = ({'attribute' => 'library_id', 'value' => '12888653'});

is($sample_merge->check_cram_header(\@irods_meta),undef,'cram header check does not pass');

my $flagstat_file = qq[$subdir/outdata/].$sample_merge->_sample_merged_name().q[.flagstat];
my $flagstat_fh = IO::File->new("$flagstat_file",">");
print $flagstat_fh "14276 + 956 in total (QC-passed reads + QC-failed reads)\n0 + 0 secondary\n";
$flagstat_fh->close();

my $md5_file = qq[$subdir/outdata/].$sample_merge->_sample_merged_name().q[.cram.md5];
my $md5_fh = IO::File->new("$md5_file",">");
print $md5_fh "37acca0b14b09bf409cee6e84048b3f0\n";
$md5_fh->close();


my $expected = expected_irods_data($subdir);
my $received = $sample_merge->irods_data_to_add();

my $result = Test::More::is_deeply($received, $expected);
if(!$result) {
    carp "RECEIVED: ".Dumper($received);
    carp "EXPECTED: ".Dumper($expected);
  }

}

sub expected_irods_data { 

my $dir = shift; 
my $data = {};
 
   $data->{qq[128886531.ACXX.paired.cram]} = {
                                                                       'study_id' => '2245',
                                                                       'is_paired_read' => 1,
                                                                       'library_id' => '128886531',
                                                                       'study_title' => 'Global Pneumococcal Sequencing (GPS) study I',
                                                                       'target' => 'library',
                                                                       'reference' => '/lustre/scratch110/srpipe/references/Streptococcus_pneumoniae/ATCC_700669/all/bwa/S_pneumoniae_700669.fasta',
                                                                       'composition' => '15531:7:9;15795:1:9',
                                                                       'alignment' => 1,
                                                                       'sample' => '2245STDY6020070',
                                                                       'sample_accession_number' => 'ERS627290',
                                                                       'study' => 'ERP001505',
                                                                       'total_reads' => '15232',
                                                                       'sample_common_name' => 'Streptococcus pneumoniae',
                                                                       'manual_qc' => 1,
                                                                       'study_accession_number' => 'ERP001505',
                                                                       'sample_id' => '2190607',
                                                                       'type' => 'cram',
                                                                       'md5' => '37acca0b14b09bf409cee6e84048b3f0'
                                                                                        };

    $data->{qq[128886531.ACXX.paired.cram.crai]} = {'type' => 'crai' };
    $data->{qq[128886531.ACXX.paired.bamcheck]}  = { 'type' => 'bamcheck' };
    $data->{qq[128886531.ACXX.paired.flagstat]}  = { 'type' => 'flagstat' };
    $data->{qq[128886531.ACXX.paired.seqchksum]}      = { 'type' => 'seqchksum' };
    $data->{qq[128886531.ACXX.paired_F0xB00.stats]}    = { 'type' => 'stats' };
    $data->{qq[128886531.ACXX.paired_F0x900.stats]}    = { 'type' => 'stats' };
    $data->{qq[128886531.ACXX.paired.cram.crai]}      = { 'type' => 'crai' };
    $data->{qq[128886531.ACXX.paired.sha512primesums512.seqchksum]} = { 'type' => 'sha512primesums512.seqchksum' };


return($data);
}

1;
