use strict;
use warnings;
use t::dbic_util;
use File::Copy;
use File::Temp qw/ tempdir /;
use File::Path qw/make_path/;
use Carp;
use File::Slurp;
use Data::Dumper;


use Test::More tests => 3;

use_ok('npg_seq_melt::query::top_up');

$ENV{TEST_DIR} = q(t/data);

my $dbic_util = t::dbic_util->new();
my $wh_schema = $dbic_util->test_schema_mlwh('t/data/fixtures/mlwh_topup');

my $tempdir = tempdir( CLEANUP => 1);

{

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = qq[$tempdir/f540350a8ba6f35e630830b5dbc81c3c361ccb9e794a87a8d5288f0dfc857230.csv];

  make_path(join q[/],$tempdir,q[configs]);

  my $config_file = join q[/],$ENV{TEST_DIR},q[configs],q[product_release.yml];  
  my $config_file_copy = join q[/],$tempdir,q[configs],q[product_release.yml];
  copy($config_file,$config_file_copy) or carp "Copy failed: $!";

  my $ss = join q[/],$ENV{TEST_DIR},q[samplesheets],q[f540350a8ba6f35e630830b5dbc81c3c361ccb9e794a87a8d5288f0dfc857230.csv];
  my $ss_copy = join q[/],$tempdir,q[f540350a8ba6f35e630830b5dbc81c3c361ccb9e794a87a8d5288f0dfc857230.csv];
  copy($ss,$ss_copy) or carp "Copy failed: $!";

  chdir $tempdir;

  my $q = npg_seq_melt::query::top_up->new(id_study_lims => 5392, 
                                           conf_path => qq[$tempdir/configs],
                                           mlwh_schema => $wh_schema,
                                           lims_driver => 'samplesheet'
                                          );

 is ($q->id_study_lims,q[5392],q[Study id correct]);
 
      $q->run_query(); 

      my $expected_data = &expected_data;
      my $query_data    = $q->data;
      my $result = is_deeply($query_data,$expected_data,'Query data returned as expected');

     if (!$result){
         carp "RECEIVED: ".Dumper($query_data->[0]);
         carp "EXPECTED: ".Dumper(%$expected_data);
       }

}


sub expected_data {

return [
          {
            'supplier_sample' => '111111',
            'results_cache_name' => q[merge_component_results/5392/f5/40/f540350a8ba6f35e630830b5dbc81c3c361ccb9e794a87a8d5288f0dfc857230],
            'library' => 22768032,
            'orig_cram' => [
                           'merge_component_cache/5392/73/1b/731b6a1b769bce601564df6c165d68fd8f24d8ab20bf48a57ca1ea352154fa6d/26496#13.cram',
                           'merge_component_cache/5392/cc/56/cc56550d030d9089a23e2fbdc440eb8556f834c3735a2cafce16756217adbe16/22222#13.cram'
                         ],
            'top_up_cram' => [
                             'merge_component_cache/5392/02/ae/02aee46d66795709da097e29f13b1424f5361250570790dacc2c2c21d21a1613/28780_4#7.cram',
                             'merge_component_cache/5392/ad/f4/adf4d775f02f80393c34fc4e186c2d5141c45d621a5adbccd0947c34dc864095/98780_4#7.cram'
                           ],
            'composition_id' => 'f540350a8ba6f35e630830b5dbc81c3c361ccb9e794a87a8d5288f0dfc857230',
            'extended_rpt_list' => '22222:1:13;22222:2:13;22222:3:13;22222:4:13;26496:1:13;26496:2:13;26496:3:13;26496:4:13;28780:4:7;98780:4:7;'
          }
        ];

    
}

1;
