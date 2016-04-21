use strict;
use warnings;
use WTSI::NPG::iRODS;
use English qw(-no_match_vars);
use Test::More tests => 12;
use File::Temp qw/ tempfile /;
use File::Basename qw/ basename /;

use_ok('npg_seq_melt::merge::base');
use_ok('npg_seq_melt::merge::generator');

my $IRODS_WRITE_PATH = q[/seq/npg/test1/merged];
my $dev_hostname = q[irods-sanger1-dev];
    my $seen_hostname = qx(uname -n);
       chomp($seen_hostname);
    ###currently need to be logged in to irods-sanger1-dev.internal.sanger.ac.uk to write to seq-dev
    ###Also requires version of WTSI::NPG::iRODS which can handle iRODS 4.1.8  (new format for ienv output)
    if ( $dev_hostname eq  $seen_hostname) { $IRODS_WRITE_PATH = q[/seq-dev/npg/test1/merged]; }


##set to dev iRODS
#$ENV{'WTSI_NPG_MELT_iRODS_Test_irodsEnvFile'} = ~/.irods/irods_environment.json_sanger1_dev
my $env_file = $ENV{'WTSI_NPG_MELT_iRODS_Test_irodsEnvFile'} || q{};

my %env_copy = %ENV;
   $env_copy{'irodsEnvFile'} = $env_file || 'DUMMY_VALUE';
local %ENV = %env_copy;

my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                  strict_baton_version => 0
                                 ); 

my ($fh, $filename) = tempfile();
chmod 0775, $filename; 

my $chemistry = ['ALXX','CCXX'];
my $r = npg_seq_melt::merge::generator->new(
    merge_cmd => $filename,
    dry_run   => 1,
    lsf_runtime_limit => 720,
    restrict_to_chemistry => $chemistry,
    irods     => $irods);

is ($r->verbose,1,q[verbose mode set when dry_run]);
is ($r->default_root_dir,'/seq/illumina/library_merge/',q[Default iRODS root dir ok]);
    $r->default_root_dir($IRODS_WRITE_PATH);
is ($r->minimum_component_count,'6', 'minimum_component_count is 6');
is ($r->lsf_num_processors,'3', 'lsf_num_processors is 3');
is ($r->lsf_runtime_limit,'720', 'lsf_runtime_limit set to 720 minutes');
is ($r->restrict_to_chemistry,$chemistry,'ALXX and CCXX are HiSeqX');
my $digest = 'b5a04fbf270d41649224463c03d228632847195786ab9e850e90b6a7c50916df';
my $base_obj = npg_seq_melt::merge::base->new(rpt_list => '14582:7;14582:8',run_dir => $r->run_dir());
isa_ok ($base_obj->composition(),q[npg_tracking::glossary::composition],"composition attribute o.k.");
my $merge_dir = $r->run_dir . q[/] . $digest; 
is ($base_obj->merge_dir,$merge_dir,'merge_dir correct');
is ($base_obj->composition->digest, $digest, 'digest correct');


SKIP: {
    my $irods_tmp_coll;

    if ($env_copy{'irodsEnvFile'} && $env_copy{'irodsEnvFile'} ne 'DUMMY_VALUE'){
        print "**", $env_copy{'irodsEnvFile'},"\n";
        $irods_tmp_coll = add_irods_data($irods);
      }
      else { skip q[Environment variable WTSI_NPG_MELT_iRODS_Test_irodsEnvFile not set],1  }
  
is ($r->_check_existance('14582:7;14582:8', $base_obj),1,
    "String found as composition imeta in test iRODS");

$irods->remove_collection($irods_tmp_coll) if ($env_copy{'irodsEnvFile'} && $env_copy{'irodsEnvFile'} ne 'DUMMY_VALUE');
  
}

sub add_irods_data {
    my $irods = shift;
    my $coll_name =  q[tmp_].$PID;

my ($fh, $cram_filename) = tempfile(SUFFIX => '.cram');

my $irods_tmp_coll = $irods->add_collection(qq[$IRODS_WRITE_PATH/$coll_name]);
my $irods_cram_path = $irods_tmp_coll.q[/].basename($cram_filename);
   $irods->add_object($cram_filename,$irods_cram_path);

##add meta data
   $irods->add_object_avu($irods_cram_path,q[type],q[cram]);
   $irods->add_object_avu($irods_cram_path,q[composition],q[{"components":[{"id_run":14582,"position":7},{"id_run":14582,"position":8}]}]);
   $irods->add_object_avu($irods_cram_path,q[target],q[library]);

return($irods_tmp_coll);
}


1;
