use strict;
use warnings;
use Test::More tests => 31;
use Test::Exception;
use File::Temp qw/ tempdir /;
use File::Path qw/make_path/;
use File::Copy;
use Data::Dumper;
use Carp;
use Cwd;
use Log::Log4perl;
use IO::File;

use WTSI::NPG::iRODS;
use npg_tracking::glossary::composition::component::illumina;

use_ok('npg_seq_melt::merge::library');


Log::Log4perl::init_once('./t/log4perl_test.conf');
my $logger = Log::Log4perl->get_logger('dnap');
my $irods = WTSI::NPG::iRODS->new(logger => $logger);

$ENV{TEST_DIR} = q(t/data);
my $tmp_dir = tempdir( CLEANUP => 1);

my $rd = q[/nfs/sf39/ILorHSany_sf39/outgoing/150312_HX7_15733_B_H27H7CCXX];
my $archive =$rd.q[/Data/Intensities/BAM_basecalls_20150315-045311/no_cal/archive];
my $tmp_path = join q[/],$tmp_dir,$archive;
make_path($tmp_path,{verbose => 0}) or carp "make_path failed : $!\n";

my $test_cram =  join q[/],$ENV{TEST_DIR},$archive,q[15733_1.cram];
my $copy_test_cram =  join q[/],$tmp_path,q[15733_1.cram];
copy($test_cram,$copy_test_cram) or carp "Copy failed: $!";

my $sample_merge = npg_seq_melt::merge::library->new(
   rpt_list        =>  '15972:5;15733:1;15733:2',
   sample_id       =>  '2183757',
   sample_name     =>  '3185STDY6014985',
   sample_accession_number => 'EGAN00001252242',
   sample_common_name => 'Homo Sapiens',
   library_id      =>  '13149752',
   library_type    => 'Standard',
   instrument_type =>  'HiSeqX' ,
   study_id        =>  '3185',
   study_name      =>  'The life history of colorectal cancer metastases study WGS X10',
   study_title     =>  'The life history of colorectal cancer metastases study WGS X10',
   study_accession_number => 'EGAS00001000864',
   run_type        =>  'paired',
   chemistry       =>  'CCXX', #HiSeqX_V2
   run_dir         =>  q[/some/run/dir],
   aligned         =>  1,
   local           =>  1,
   irods           => $irods,
   default_root_dir => q[/seq/npg/test1/merged/],
   remove_outdata  => 1,
   lims_id         => 'SQSCP',
   _sample_merged_name => 'some_name',
   reference_genome_path => q[/lustre/scratch110/srpipe/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa],
   );

{
  isa_ok($sample_merge,'npg_seq_melt::merge::library','passed object test');
  is($sample_merge->run_dir(),q[/some/run/dir],"run_dir generated from npg_seq_melt::merge::base"); 
  like ($sample_merge->irods(),qr/WTSI::NPG::iRODS/msx,q[Correct WTSI::NPG::iRODS connection]);
  is ($sample_merge->default_root_dir(),q[/seq/npg/test1/merged/],q[default_root_dir set to test area]);
  is ($sample_merge->collection(),q[/seq/npg/test1/merged/some_name],q[collection o.k.]);
  is ($sample_merge->merged_qc_dir(),q[/some/run/dir/d991c213db834e3c1850587115c43b311d51819dd16faa72c94a7da8417ddbda/outdata/qc/],q[qc_dir o.k.]);
  is($sample_merge->rpt_list(),'15972:5;15733:1;15733:2','Correct rpt_list');
  is($sample_merge->sample_id(),'2183757','Correct sample_id');
  is($sample_merge->library_id(),'13149752','Correct library_id');
}

{
  is($sample_merge->composition()->freeze(),
    '{"components":[{"id_run":15733,"position":1},{"id_run":15733,"position":2},{"id_run":15972,"position":5}]}',
    'correctly built composition'
  );


  is($sample_merge->vtlib(),'$(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/','expected vtlib command');

  my $cram = "$ENV{TEST_DIR}/nfs/sf39/ILorHSany_sf39/analysis/150312_HX7_15733_B_H27H7CCXX/Data/Intensities/BAM_basecalls_20150315-045311/no_cal/archive/15733_1.cram";
  my @irods_meta = ({'attribute' => 'library_id', 'value' => '13149752'},{'attribute' => 'sample_id', 'value' => '2183757'});
  my $query = {
     'irods_cram' => $cram,
     'sample_acc' => 'EGAN00001252242',
     'sample_id'  => '2183757',
     'library_id' => '13149752',
     'irods_meta' => \@irods_meta,
     'ref_path'        => $sample_merge->reference_genome_path,
  };
  is($sample_merge->_check_cram_header($query),1,'cram header check passes');

  is($sample_merge->get_first_cram_sample_name, 'EGAN00001252242','Header sample name');
  is($sample_merge->get_first_cram_ref_name,
    '/lustre/scratch109/srpipe/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa',
    'Header ref name from first SQ row');

  $sample_merge->set_first_cram_sample_name('XXXXXXX');
  isnt ($sample_merge->_check_cram_header($query),1,
    'cram header check fails if difference between header SM fields');

}

is($sample_merge->remove_outdata(),1,"remove_outdata set");
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
#lane_1.taglist  lane_2.taglist  lane_3.taglist  lane_4.taglist  lane_5.taglist  lane_6.taglist  lane_7.taglist  lane_8.taglist  npg  samplesheet_15795.csv  st_original
# /nfs/sf36/ILorHSany_sf36/outgoing/150213_HS33_15531_B_C6B43ACXX/Data/Intensities/BAM_basecalls_20150331-122837/no_cal/archive/lane7/15531_7#9.cram

{
  my $tempdir = tempdir( CLEANUP => 1);

  my $sample_merge = npg_seq_melt::merge::library->new(
   rpt_list                => '15795:1:9;15531:7:9',
   sample_id               => '2190607',
   sample_name             => '2245STDY6020070',
   sample_common_name      => 'Streptococcus pneumoniae',
   library_id              => '128886531',
   library_type            => 'Standard',
   instrument_type         => 'HiSeq',
   study_id                => '2245',
   study_name              => 'ILB Global Pneumococcal Sequencing (GPS) study I (JP)',
   study_title             => 'Global Pneumococcal Sequencing (GPS) study I',
   study_accession_number  => 'ERP001505',
   run_type                =>  'paired',
   chemistry               =>  'ACXX', #'HiSeq_V3',
   run_dir                 =>  $tempdir,
   aligned                 =>  1,
   local                   =>  1,
   irods                   =>  $irods, 
   lims_id                 => 'SQSCP',
   sample_acc_check        =>  0,  #--nosample_acc_check
   _paths2merge            =>  ['/my/location/15531_7#9.cram',
                                '/my/location/15795_1#9.cram'],
   reference_genome_path   => '/references/Spneumoniae/ATCC_700669/all/bwa/S_pneumoniae_700669.fasta',
  );

  is ($sample_merge->run_dir(),$tempdir, 'Correct run_dir');

  my $cmps = $sample_merge->composition();
  isa_ok($cmps,'npg_tracking::glossary::composition','isa npg_tracking::glossary::composition');
  isa_ok($sample_merge->composition->get_component(0),
    'npg_tracking::glossary::composition::component::illumina',
    'component isa npg_tracking::glossary::composition::component::illumina');
  
  is($sample_merge->composition()->freeze(),
    '{"components":[{"id_run":15531,"position":7,"tag_index":9},{"id_run":15795,"position":1,"tag_index":9}]}',
    'correctly built composition');

  is ($sample_merge->sample_merged_name(),q[128886531.ACXX.paired.974845690a],'Correct sample merged name');

  is ($sample_merge->merge_dir(),
    qq[$tempdir/ea8e04061077270a470560e9f0527abe8e246e5ff70c3e161f0747373b41be92],
    'Correct merge library sub-directory');
  my $subdir = $sample_merge->merge_dir();
  is ($sample_merge->run_make_path(qq[$subdir/outdata]),1,'outdata generated OK');

  ### no bamsort adddupmarksupport=1 present in header -> should not run
  my $test_15795_1_9_cram = qq[$ENV{TEST_DIR}/nfs/sf18/ILorHSany_sf18/outgoing/150320_HS2_15795_A_C6N6DACXX/Data/Intensities/BAM_basecalls_20150328-170701/no_cal/archive/lane1/15795_1#9.cram]; 

  my @irods_meta2 = ({'attribute' => 'library_id', 'value' => '12888653'},{'attribute' => 'sample_id', 'value' => '2190607'});
  my $query2 = {
     'irods_cram' => $test_15795_1_9_cram,
     'sample_acc' => 'EGAN00001252242',
     'sample_id'  => '2190607',
     'library_id' => '12888653',
     'irods_meta' => \@irods_meta2,
     'ref_path'   => $sample_merge->reference_genome_path,
  };

  isnt($sample_merge->_check_cram_header($query2),
    1,'cram header check does not pass');


  ### some variables needed for vtfp_job
  my $original_seqchksum_dir = join q{/},$sample_merge->merge_dir(),q{input};
  $sample_merge->original_seqchksum_dir($original_seqchksum_dir);

  my $vtfp_cmd = q[vtfp.pl -l vtfp.128886531.ACXX.paired.974845690a.merge_aligned.LOG -o 128886531.ACXX.paired.974845690a.merge_aligned.json -keys library -vals 128886531.ACXX.paired.974845690a -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys samtools_executable -vals samtools1 -keys outdatadir -vals outdata -keys outirodsdir -vals  /seq/illumina/library_merge/128886531.ACXX.paired.974845690a -keys basic_pipeline_params_file -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib//alignment_common.json -keys bmd_resetdupflag_val -vals 1 -keys bmdtmp -vals merge_bmd -keys genome_reference_fasta -vals /references/Spneumoniae/ATCC_700669/all/fasta/S_pneumoniae_700669.fasta -keys incrams -vals /my/location/15531_7#9.cram -keys incrams -vals /my/location/15795_1#9.cram  -keys incrams_seqchksum -vals ] . $original_seqchksum_dir .q[/15531_7#9.seqchksum -keys incrams_seqchksum -vals ] . $original_seqchksum_dir . q[/15795_1#9.seqchksum   $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib//merge_aligned.json ];

  my $job = $sample_merge->vtfp_job();
  is_deeply ([split /-/, $job], [split /-/, $vtfp_cmd], 'vtfp.pl command o.k.');

  my $viv_cmd = q[viv.pl -v 3 -x -s -o viv.128886531.ACXX.paired.974845690a.merge_aligned.LOG ./128886531.ACXX.paired.974845690a.merge_aligned.json];
  is($sample_merge->viv_job(),$viv_cmd,'viv.pl command o.k.');

  my $flagstat_file = qq[$subdir/outdata/].$sample_merge->sample_merged_name().q[.flagstat];
  my $flagstat_fh = IO::File->new("$flagstat_file",">");
  print $flagstat_fh "864498220 + 6722760 in total (QC-passed reads + QC-failed reads)\n0 + 0 secondary\n";
  $flagstat_fh->close();

  my $md5_file = qq[$subdir/outdata/].$sample_merge->sample_merged_name().q[.cram.md5];
  my $md5_fh = IO::File->new("$md5_file",">");
  print $md5_fh "37acca0b14b09bf409cee6e84048b3f0\n";
  $md5_fh->close();

  my $logfile = $sample_merge->merge_dir . q[/123.err];
  system("touch $logfile");
  my $tar_file = q[library_merge_logs.tgz]; 
  is ($sample_merge->_tar_log_files(),$tar_file,q[Logs tar file created o.k.]);


  is ($sample_merge->run_make_path(qq[$subdir/outdata/qc]),1,'qc dir generated OK');

  my $mdm_file = qq[$subdir/outdata/].$sample_merge->sample_merged_name().q[.markdups_metrics.txt];
  my $mdm_fh   = IO::File->new($mdm_file,">");
  print $mdm_fh "\# /software/bin/bamstreamingmarkduplicates level=0 verbose=0 tmpfile=outdata/merge_bmd_";
  print $mdm_fh $sample_merge->sample_merged_name() . " M=outdata/\n";
  print $mdm_fh $sample_merge->sample_merged_name() . ".markdups_metrics.txt resetdupflag=1\n";
  print $mdm_fh "\n\#\#METRICS\n";
  print $mdm_fh "LIBRARY	UNPAIRED_READS_EXAMINED	READ_PAIRS_EXAMINED	UNMAPPED_READS	UNPAIRED_READ_DUPLICATES	READ_PAIR_DUPLICATES	READ_PAIR_OPTICAL_DUPLICATES	PERCENT_DUPLICATION	ESTIMATED_LIBRARY_SIZE
15319869	249411	431973442	301925	97576	93530355	54458316	0.216569	1695642998\n";
  print $mdm_fh "\n\#\# HISTOGRAM\n";
  print $mdm_fh "BIN\tVALUE\n";
  print $mdm_fh "1\t1.12675\n2\t2.00009\n3\t2.67703\n";
  $mdm_fh->close();

  foreach my $suffix('.cram.crai','_F0xB00.stats','_F0x900.stats','.flagstat','.markdups_metrics.txt','.seqchksum','.sha512primesums512.seqchksum'){
        my $file = qq[$subdir/outdata/].$sample_merge->sample_merged_name().$suffix; 
        system("touch $file");
  }
  $sample_merge->make_bam_flagstats_json($sample_merge->composition());
 
  my $expected = expected_irods_data($subdir);
  my $received = $sample_merge->irods_data_to_add();
  delete $received->{'128886531.ACXX.paired.974845690a_F0xB00.samtools_stats.json'};
  delete $received->{'128886531.ACXX.paired.974845690a_F0x900.samtools_stats.json'};
  delete $received->{'128886531.ACXX.paired.974845690a.sequence_summary.json'};

  my $result = is_deeply($received, $expected, 'irods data to add as expected');
  if(!$result) {
    carp "RECEIVED: ".Dumper($received);
    carp "EXPECTED: ".Dumper($expected);
  }

}

{

my $sample_merge = npg_seq_melt::merge::library->new(
   rpt_list                => '20118:1:7;20130:1:7;20131:1:7;20132:1:7;20151:1:7;20155:1:7',
   sample_id               => '2060992',
   sample_name             => 'SC_WES_INT5916088',
   sample_common_name      => 'Homo Sapiens',
   library_id              => '16735808',
   library_type            => 'Standard',
   instrument_type         => 'HiSeqX',
   study_id                => '3765',
   study_name              => 'IHTP_WGS_INTERVAL Cohort (15x)',
   study_title             => 'Whole Genome Sequencing of INTERVAL',
   study_accession_number  => 'ERP001505',
   run_type                =>  'paired310',
   chemistry               =>  'CCXX', #'HiSeq_V3',
   run_dir                 =>  q[/some/run/dir],
   aligned                 =>  1,
   local                   =>  1,
   lims_id                 => 'SQSCP',
  # sample_acc_check        =>  0, 
   _paths2merge            =>  ['/my/location/20118_1#7.cram',
                                '/my/location/20130_1#7.cram',
                                '/my/location/20131_1#7.cram',
                                '/my/location/20132_1#7.cram',
                                '/my/location/20151_1#7.cram',
                                '/my/location/20155_1#7.cram',
                               ],
   reference_genome_path    => q[/lustre/scratch110/srpipe/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/fasta/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa],
  );

 ## old style @RG ID -> should not run

 my $old_rg_id_cram;
  
 my $test_20131_1_7_cram = qq[$ENV{TEST_DIR}/cram/20131_1#7.cram];

  my @irods_meta2 = ({'attribute' => 'library_id', 'value' => '16735808'},{'attribute' => 'sample_id', 'value' => '2035998'});

  my $query = {
     'irods_cram'       => $test_20131_1_7_cram,
     'sample_acc' => 'EGAN00001409522',
     'sample_id'  => '2035998',
     'library_id' => '16735808',
     'irods_meta' => \@irods_meta2,
     'ref_path'   => $sample_merge->reference_genome_path,
  };

  # ID:1#7 rather than ID:20131_1#7
  isnt($sample_merge->_check_cram_header($query),
    1,'cram header check does not pass (old format @RG ID)'); 
  
}

sub expected_irods_data { 
  my $dir = shift; 
  my $data = {};
 
  $data->{qq[128886531.ACXX.paired.974845690a.cram]} = {
    'study_id' => '2245',
    'is_paired_read' => 1,
    'library_id' => '128886531',
    'study' => 'ILB Global Pneumococcal Sequencing (GPS) study I (JP)',
    'composition_id' => 'ea8e04061077270a470560e9f0527abe8e246e5ff70c3e161f0747373b41be92',
    'run_type' => 'paired',
    'total_reads' => '871220980',
    'component' => ['{"id_run":15531,"position":7,"tag_index":9}',
                    '{"id_run":15795,"position":1,"tag_index":9}'],
    'study_title' => 'Global Pneumococcal Sequencing (GPS) study I',
    'target' => 'library',
    'reference' => '/references/Spneumoniae/ATCC_700669/all/bwa/S_pneumoniae_700669.fasta',
    'composition' =>
      '{"components":[{"id_run":15531,"position":7,"tag_index":9},{"id_run":15795,"position":1,"tag_index":9}]}',
    'alignment' => 1,
    'sample' => '2245STDY6020070',
    'total_reads' => '871220980',
    'sample_common_name' => 'Streptococcus pneumoniae',
    'manual_qc' => 1,
    'study_accession_number' => 'ERP001505',
    'sample_id' => '2190607',
    'type' => 'cram',
    'md5' => '37acca0b14b09bf409cee6e84048b3f0',
    'chemistry' => 'ACXX',
    'instrument_type' => 'HiSeq',
    'run_type' => 'paired',
    'library_type' => 'Standard',
     };

  $data->{qq[128886531.ACXX.paired.974845690a.cram.crai]}    = {'type' => 'crai' };
  $data->{qq[128886531.ACXX.paired.974845690a.flagstat]}     = { 'type' => 'flagstat' };
  $data->{qq[128886531.ACXX.paired.974845690a.seqchksum]}    = { 'type' => 'seqchksum' };
  $data->{qq[128886531.ACXX.paired.974845690a_F0xB00.stats]} = { 'type' => 'stats' };
  $data->{qq[128886531.ACXX.paired.974845690a_F0x900.stats]} = { 'type' => 'stats' };
  $data->{qq[128886531.ACXX.paired.974845690a.sha512primesums512.seqchksum]} = { 'type' => 'seqchksum' };
  $data->{qq[128886531.ACXX.paired.974845690a.markdups_metrics.txt]} = {'type' => 'txt'};
  $data->{qq[128886531.ACXX.paired.974845690a.bam_flagstats.json]} = {'type' => 'json'};
  $data->{q[library_merge_logs.tgz]}   = { 'type' => 'tgz' };

  return($data);
}

1;
__END__
