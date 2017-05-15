use strict;
use warnings;
use Test::More tests => 12;
use File::Temp qw/ tempfile tempdir/;
use File::Copy;
use IO::File;
use t::util;
use t::dbic_util;
use Data::Dumper;
use WTSI::NPG::iRODS::DataObject;

use_ok('npg_seq_melt::util::change_header');

my $util = t::util->new();
my $h = $util->home();
my $irods_home = $h->{home};
my $irods_zone = $h->{zone};
diag("iRODS home = $irods_home, zone = $irods_zone");

my $IRODS_ROOT       = qq[$irods_home/npg/];
my $IRODS_PREFIX     = q[irods-sanger1-dev];

##set to dev iRODS
my $env_set = $ENV{'WTSI_NPG_MELT_iRODS_Test'} || q{}; #temp name

$ENV{TEST_DIR} = q(t/data/crams);

Log::Log4perl::init_once('./t/log4perl_test.conf');
my $logger = Log::Log4perl->get_logger('dnap');
my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  logger               => $logger);

#to get around cached original meta data and see values added by change_header module 
my $irods2 = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                  strict_baton_version => 0,
                                  logger               => $logger);

{

  my $tempdir = tempdir( CLEANUP => 1);

  my @runs = (19900);

  my $dbic_util = t::dbic_util->new();
  my $wh_schema = $dbic_util->test_schema_mlwh('t/data/fixtures/mlwh');

  diag("test for on disk re-headering");
  my $cram  = $ENV{TEST_DIR} .qq[/19900_8#12.old.cram]; #header
  my $archive_dir = join q[/],$tempdir,qq[Latest_Summary/archive/lane8];
  qx(mkdir -p $archive_dir);
  my $copy_cram = qq[$archive_dir/19900_8#12.cram];
  copy($cram,$copy_cram);

  my $l = npg_seq_melt::util::change_header->new(
                   dry_run      => 0,
                   is_local     => 1,
                   rpt          => q[19900:8:12],
                   run_dir      => $tempdir,
                   archive_cram_dir => $archive_dir, #avoiding NPG tracking reports run 19900 no longer on staging error
          )->run();
     $l->read_header();
     $l->run_reheader();
     ok ((-e qq[$copy_cram.md5]),"re-headered cram md5 produced");

   SKIP: { 
   diag("tests for iRODS re-headering");         
       if ($env_set){
          if ($irods_zone =~ /-dev/){
             diag("WTSI_NPG_MELT_iRODS_Test set and zone is $irods_zone");
             foreach my $run (@runs){
                      my $cram  = $ENV{TEST_DIR} .qq[/${run}_8#12.old.cram]; #header
                      my $icram = qq[${run}_8#12.cram];
                      my $icram_path = qq[$IRODS_ROOT/$run/$icram];

                  ## load test cram 

                  my $Hr = {
                     'irods'      => $irods,
                     'id_run'     => $run,
                     'cram'       => $cram,
                     'icram'      => $icram,
                     'library_id' => q[16477382],
                     'md5'        => q[4fb52abe4a1d0f53ae76fe909812991a],
                     };

                 add_irods_data($Hr);

              my @original_irods_meta = $irods->get_object_meta($icram_path);
              is(scalar @original_irods_meta,q[3],q[OK 3 items on meta data prior to re-headering]);

              my $r = npg_seq_melt::util::change_header->new(
                       rt_ticket    => 12345,
                       dry_run      => 0,
                       irods_root   => $IRODS_ROOT,
                       rpt          => qq[$run:8:12],
                       irods        => $irods,
                       mlwh_schema  => $wh_schema,
                       run_dir      => $tempdir,
                      );
 
              is ($r->rpt,q[19900:8:12],q[rpt correct]);

              is ($r->run_dir,$tempdir,qq[run_dir is $tempdir]);
         
                  $r->run();

                 is ($r->sample, 'EGAN00001390989', q[sample name to use is EGAN00001390989]);
                 is ($r->library, '16477382', q[library to use is 16477382]);
                 is ($r->study, 'EGAS00001001355: Whole genome sequencing of participants from the INTERVAL study.', q[study to use is correct]);

                 $r->read_header();

                 my $new_header = $r->new_header();
                 my (@header_lines) = split/\n/, $new_header;
                 like($header_lines[1],qr/LB:16477382\s+PG:BamIndexDecoder\s+SM:EGAN00001390989\s+PL:ILLUMINA\s+DS:EGAS00001001355: Whole genome sequencing of participants from the INTERVAL study./, q[new header @RG line correct]); 

                 $r->run_reheader();

                 my @irods_meta = $irods2->get_object_meta($icram_path);
 
                 is(scalar @irods_meta,q[5],q[OK 5 items on meta data after re-headering]);
                 is($irods_meta[2]->{'attribute'},q[md5_history],q[post re-headered cram md5_history present]);
                 like($irods_meta[2]->{'value'},qr/4fb52abe4a1d0f53ae76fe909812991a/,q[post re-headered cram md5_history value correct]);

           }
      }
    else { skip qq[Not in dev zone (zone=] . $irods_zone . q[)],10 }
  }
 else { skip q[Environment variable WTSI_NPG_MELT_iRODS_Test not set],10 }
 }


foreach my $run (@runs){
    my $tmp_coll = $IRODS_ROOT.$run;
    $irods->remove_collection($tmp_coll) if ($irods_zone =~ /-dev/ && $env_set);
  }


}


sub add_irods_data {
    my $Hr = shift;
    my $irods              = $Hr->{irods};
    my $coll_name          = $Hr->{id_run};
    my $cram_filename      = $Hr->{cram};
    my $icram              = $Hr->{icram};
    my $md5                = $Hr->{md5};
    my $library_id         = $Hr->{library_id};

my $irods_tmp_coll = $irods->add_collection(qq[$IRODS_ROOT/$coll_name]);
my $irods_cram_path = $irods_tmp_coll.q[/].$icram;
   $irods->add_object($cram_filename,$irods_cram_path);

##add meta data
   $irods->add_object_avu($irods_cram_path,q[type],q[cram]);
   ##library_id, md5
   $irods->add_object_avu($irods_cram_path,q[md5],$md5);
   $irods->add_object_avu($irods_cram_path,q[library_id],$library_id);

}

1;
