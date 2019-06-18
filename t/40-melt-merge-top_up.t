use strict;
use warnings;
use t::dbic_util;
use File::Copy;
use File::Temp qw/ tempdir /;
use File::Path qw/make_path/;
use Carp;
use Test::More tests => 3;

use_ok('npg_seq_melt::merge::top_up');

$ENV{TEST_DIR} = q(t/data);

my $dbic_util = t::dbic_util->new();
my $wh_schema = $dbic_util->test_schema_mlwh('t/data/fixtures/mlwh_topup');


{

my $tempdir = tempdir( CLEANUP => 0);

make_path(join q[/],$tempdir,q[configs]);

my $config_file = join q[/],$ENV{TEST_DIR},q[configs],q[product_release.yml];  
my $config_file_copy = join q[/],$tempdir,q[configs],q[product_release.yml];
copy($config_file,$config_file_copy) or carp "Copy failed: $!";
chdir $tempdir;

my $m = npg_seq_melt::merge::top_up->new(rt_ticket => q[12345],
                                         commands_file => qq[$tempdir/wr_input_cmds.txt], 
                                         id_study_lims  => 5392,
                                         conf_path => qq[$tempdir/configs],
                                         path_prefix => $tempdir,
                                         mlwh_schema => $wh_schema,
                                         );


is ($m->path_prefix,$tempdir,q[Correct path_prefix]);   

$m->run_query();

$m->make_commands();

is ($m->out_dir,qq[$tempdir/merge_component_results/5392/c0/00/c0002b941f3adc308273f994abc874d1232e285a3d5e5aa5c69cc932f509023e],q[Correct out dir]);

}

1;
