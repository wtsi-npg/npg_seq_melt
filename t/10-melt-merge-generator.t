use strict;
use warnings;
use WTSI::NPG::iRODS;
use English qw(-no_match_vars);
use Test::More tests => 17;
use File::Temp qw/ tempfile tempdir/;
use File::Basename qw/ basename /;
use t::util;

use_ok('npg_seq_melt::merge::base');
use_ok('npg_seq_melt::merge::generator');


my $util = t::util->new();
my $h = $util->home();
my $irods_home = $h->{home};
my $irods_zone = $h->{zone};
diag("iRODS home = $irods_home, zone = $irods_zone");

my $IRODS_WRITE_PATH = qq[$irods_home/npg/merged/];

##environment variable to allow iRODS loading 
my $env_set = $ENV{'WTSI_NPG_MELT_iRODS_Test'} || q{};


my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                  strict_baton_version => 0
                                 ); 

my $tmpdir          = tempdir( CLEANUP => 1 );
my ($fh, $filename) = tempfile(DIR => $tmpdir,SUFFIX => '_npg_library_merge');
chmod 0775, $filename; 

my $chemistry = ['HXV2'];
my $r = npg_seq_melt::merge::generator->new(
    merge_cmd               => $filename,
    dry_run                 => 1,
    lsf_runtime_limit       => 720,
    restrict_to_chemistry   => $chemistry,
    minimum_component_count => 2,
    run_dir                 => q[test_dir],
    irods                   => $irods);

is ($r->verbose,1,q[verbose mode set when dry_run]);
is ($r->default_root_dir,'/seq/illumina/library_merge/',q[Default iRODS root dir ok]);
    $r->default_root_dir($IRODS_WRITE_PATH);
is ($r->minimum_component_count,'2', 'minimum_component_count is 2');
is ($r->lsf_num_processors,'3', 'lsf_num_processors is 3');
is ($r->lsf_runtime_limit,'720', 'lsf_runtime_limit set to 720 minutes');
is ($r->restrict_to_chemistry,$chemistry,'Restrict to chemistry HXV2 (HiSeqX)');
my $digest = 'b5a04fbf270d41649224463c03d228632847195786ab9e850e90b6a7c50916df';
my $base_obj = npg_seq_melt::merge::base->new(rpt_list => '14582:7;14582:8',run_dir => $r->run_dir());
isa_ok ($base_obj->composition(),q[npg_tracking::glossary::composition],"composition attribute o.k.");
my $merge_dir = $r->run_dir . q[/] . $digest; 
is ($base_obj->merge_dir,$merge_dir,'merge_dir correct');
is ($base_obj->composition->digest, $digest, 'digest correct');

is ($r->_parse_chemistry('HCGNNALXX','21433:1:1'),'HXV2', 'ALXX barcode and run > 20000 returns HXV2');
is ($r->_parse_chemistry('H0CH3ALXX','15218:2:10'),'ALXX', 'ALXX barcode and run < 20000 returns ALXX');
is ($r->_parse_chemistry('HYKWGCCXX','21202:1:1'),'HXV2', 'CCXX barcode returns HXV2');
is ($r->_parse_chemistry('HFGYLADXY','21778:2:6'),'ADXY', 'ADXY barcode returns ADXY');


SKIP: {
    my $irods_tmp_coll;

     if ($env_set){
        if ($irods_zone =~ /-dev/){
          diag("WTSI_NPG_MELT_iRODS_Test set and zone is $irods_zone");
          $irods_tmp_coll = add_irods_data($irods);
        }
       else { skip qq[Not in dev zone (zone=] . $irods_zone . q[)],1 }
       }
     else { skip qq[Environment variable WTSI_NPG_MELT_iRODS_Test not set],1  } 

is ($r->_check_existance('14582:7;14582:8', $base_obj,'1','library_id','my_cmd'),1,
    "String found as composition imeta in test iRODS");

$irods->remove_collection($irods_tmp_coll) if ($irods_zone =~ /-dev/ && $env_set);
  
}

my $commands = $r->_create_commands(library_digest_data());

my $command_string = qq[$filename --rpt_list '11111:7:9;11112:8:9' --library_id 15756535 --library_type  'HiSeqX PCR free' --sample_id 2275905 --sample_name yemcha6089636 --sample_common_name 'Homo Sapien' --sample_accession_number EGAN00001386875 --study_id 4014 --study_name 'SEQCAP_WGS_GDAP_Chad' --study_title 'Genome Diversity in Africa Project: Chad' --study_accession_number EGAS00001001719 --aligned 1 --lims_id SQSCP --instrument_type HiSeqX --run_type paired158 --chemistry HXV2  --samtools_executable  samtools1   --run_dir  test_dir   --local --default_root_dir $IRODS_WRITE_PATH];

is ($commands->[0]{'command'},$command_string,'library merge command is correct');




sub add_irods_data {
    my $irods = shift;
    my $coll_name =  q[tmp_].$PID;

my ($fh, $cram_filename) = tempfile(UNLINK => 1, SUFFIX => '.cram');
my $irods_tmp_coll = $irods->add_collection(qq[$IRODS_WRITE_PATH/$coll_name]);
my $irods_cram_path = $irods_tmp_coll.q[/].basename($cram_filename);
   $irods->add_object($cram_filename,$irods_cram_path);

##add meta data
   $irods->add_object_avu($irods_cram_path,q[type],q[cram]);
   $irods->add_object_avu($irods_cram_path,q[composition],q[{"components":[{"id_run":14582,"position":7},{"id_run":14582,"position":8}]}]);
   $irods->add_object_avu($irods_cram_path,q[target],q[library]);

return($irods_tmp_coll);
}

sub library_digest_data {
    my $data = {
          '15756535' => {
                          'HiSeqX' => {
                                        'paired' => {
                                                      'entities' => [
                                                                      {
                                                                        'status' => 'qc complete',
                                                                        'library' => '15756535',
                                                                        'study_title' => 'Genome Diversity in Africa Project: Chad',
                                                                        'id_lims' => 'SQSCP',
                                                                        'new_library_id' => 'DN431215P:E9',
                                                                        'sample_name' => 'yemcha6089636',
                                                                        'sample' => '2275905',
                                                                        'sample_accession_number' => 'EGAN00001386875',
                                                                        'study' => '4014',
                                                                        'flowcell_barcode' => 'HJCFHCCXX',
                                                                        'library_type' => 'HiSeqX PCR free',
                                                                        'expected_cycles' => 308,
                                                                        'cycles' => '310',
                                                                        'sample_common_name' => 'Homo Sapien',
                                                                        'study_accession_number' => 'EGAS00001001719',
                                                                        'aligned' => '1',
                                                                        'study_name' => 'SEQCAP_WGS_GDAP_Chad',
                                                                        'rpt_key' => '19000:5:9',
                                                                        'reference_genome' => 'Homo_sapiens (GRCh38_full_analysis_set_plus_decoy_hla)'
                                                                      },
                                                                      {
                                                                        'status' => 'qc complete',
                                                                        'library' => '15756535',
                                                                        'study_title' => 'Genome Diversity in Africa Project: Chad',
                                                                        'id_lims' => 'SQSCP',
                                                                        'new_library_id' => 'DN431215P:E9',
                                                                        'sample_name' => 'yemcha6089636',
                                                                        'sample' => '2275905',
                                                                        'sample_accession_number' => 'EGAN00001386875',
                                                                        'study' => '4014',
                                                                        'flowcell_barcode' => 'HJK22CCXX',
                                                                        'library_type' => 'HiSeqX PCR free',
                                                                        'expected_cycles' => 308,
                                                                        'cycles' => '310',
                                                                        'sample_common_name' => 'Homo Sapien',
                                                                        'study_accession_number' => 'EGAS00001001719',
                                                                        'aligned' => '1',
                                                                        'study_name' => 'SEQCAP_WGS_GDAP_Chad',
                                                                        'rpt_key' => '19264:6:9',
                                                                        'reference_genome' => 'Homo_sapiens (GRCh38_full_analysis_set_plus_decoy_hla)'
                                                                      },
{
                                                                        'status' => 'qc complete',
                                                                        'library' => '15756535',
                                                                        'study_title' => 'Genome Diversity in Africa Project: Chad',
                                                                        'id_lims' => 'SQSCP',
                                                                        'new_library_id' => 'DN431215P:E9',
                                                                        'sample_name' => 'yemcha6089636',
                                                                        'sample' => '2275905',
                                                                        'sample_accession_number' => 'EGAN00001386875',
                                                                        'study' => '4014',
                                                                        'flowcell_barcode' => 'HJK22CCXX',
                                                                        'library_type' => 'HiSeqX PCR free',
                                                                        'expected_cycles' => 158,
                                                                        'cycles' => '166',
                                                                        'sample_common_name' => 'Homo Sapien',
                                                                        'study_accession_number' => 'EGAS00001001719',
                                                                        'aligned' => '1',
                                                                        'study_name' => 'SEQCAP_WGS_GDAP_Chad',
                                                                        'rpt_key' => '11111:7:9',
                                                                        'reference_genome' => 'Homo_sapiens (GRCh38_full_analysis_set_plus_decoy_hla)'
                                                                      },
                                                                      {
                                                                        'status' => 'qc complete',
                                                                        'library' => '15756535',
                                                                        'study_title' => 'Genome Diversity in Africa Project: Chad',
                                                                        'id_lims' => 'SQSCP',
                                                                        'new_library_id' => 'DN431215P:E9',
                                                                        'sample_name' => 'yemcha6089636',
                                                                        'sample' => '2275905',
                                                                        'sample_accession_number' => 'EGAN00001386875',
                                                                        'study' => '4014',
                                                                        'flowcell_barcode' => 'HJK22CCXX',
                                                                        'library_type' => 'HiSeqX PCR free',
                                                                        'expected_cycles' => 158,
                                                                        'cycles' => '158',
                                                                        'sample_common_name' => 'Homo Sapien',
                                                                        'study_accession_number' => 'EGAS00001001719',
                                                                        'aligned' => '1',
                                                                        'study_name' => 'SEQCAP_WGS_GDAP_Chad',
                                                                        'rpt_key' => '11112:8:9',
                                                                        'reference_genome' => 'Homo_sapiens (GRCh38_full_analysis_set_plus_decoy_hla)'
                                                                      }
                                                            ]
                                                    }
                                      }
                              }
                        };

return $data;

}
 
1;
