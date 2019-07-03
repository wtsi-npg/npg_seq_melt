use strict;
use warnings;
use t::dbic_util;
use File::Copy;
use File::Temp qw/ tempdir /;
use File::Path qw/make_path/;
use Carp;
use File::Slurp;
use Test::More tests => 4;
use JSON;
use IO::File;

use_ok('npg_seq_melt::merge::top_up');

$ENV{TEST_DIR} = q(t/data);
my $dbic_util = t::dbic_util->new();
my $wh_schema = $dbic_util->test_schema_mlwh('t/data/fixtures/mlwh_topup');


{

my $tempdir = tempdir( CLEANUP => 1);

# Create test reference repository

my $org = q[Homo_sapiens];
my $build = q[GRCh38_15_plus_hs38d1];
my $ref_dir = join q[/],$tempdir,'references';

my $geno_dir = join q[/],$tempdir,'geno_refset';

my $rel_dir     = join q[/],$ref_dir,$org,$build,'all';
my $bwa0_6_dir  = join q[/],$rel_dir,'bwa0_6';
my $fasta_dir   = join q[/],$rel_dir,'fasta';
my $picard_dir  = join q[/],$rel_dir,'picard';
my $target_dir  = join q[/],$rel_dir,'target';
my $targeta_dir = join q[/],$rel_dir,'custom_targets','autosomes_only_0419';
my $gen_dir     = join q[/],$geno_dir,'study5392','GRCh38_15_plus_hs38d1','bcftools';
make_path($bwa0_6_dir, $picard_dir,$fasta_dir,$target_dir, $targeta_dir,$gen_dir,{verbose => 0});
`touch $bwa0_6_dir/Homo_sapiens.GRCh38_15_plus_hs38d1.fa`;
`touch $fasta_dir/Homo_sapiens.GRCh38_15_plus_hs38d1.fa`;
`touch $picard_dir/Homo_sapiens.GRCh38_15_plus_hs38d1.fa`;
`touch $target_dir/Homo_sapiens.GRCh38_15_plus_hs38d1.fa.interval_list`;
`touch $targeta_dir/Homo_sapiens.GRCh38_15_plus_hs38d1.fa.interval_list`;
`touch $gen_dir/study5392.annotation.vcf`;



  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = qq[$tempdir/c0002b941f3adc308273f994abc874d1232e285a3d5e5aa5c69cc932f509023e.csv];

my $expected_cmd_file = join q[/],$ENV{TEST_DIR},q[wr],q[wr_input_cmds.txt];
my $expected_cmd_file_copy = join q[/],$tempdir,q[copy_wr_input_cmds.txt];
copy($expected_cmd_file,$expected_cmd_file_copy) or carp "Copy failed: $!";

my $ss = join q[/],$ENV{TEST_DIR},q[samplesheets],q[c0002b941f3adc308273f994abc874d1232e285a3d5e5aa5c69cc932f509023e.csv];
 my $ss_copy = join q[/],$tempdir,q[c0002b941f3adc308273f994abc874d1232e285a3d5e5aa5c69cc932f509023e.csv];
  copy($ss,$ss_copy) or carp "Copy failed: $!";


make_path(join q[/],$tempdir,q[configs]);

my $config_file = join q[/],$ENV{TEST_DIR},q[configs],q[product_release.yml];  
my $config_file_copy = join q[/],$tempdir,q[configs],q[product_release.yml];
copy($config_file,$config_file_copy) or carp "Copy failed: $!";
chdir $tempdir;

my $m = npg_seq_melt::merge::top_up->new(rt_ticket => q[12345],
                                         commands_file => qq[$tempdir/wr_input_cmds.txt], 
                                         id_study_lims  => 5392,
                                         conf_path => qq[$tempdir/configs],
                                         repository => $tempdir,
                                         mlwh_schema => $wh_schema,
                                         lims_driver => 'samplesheet',
                                         dry_run     => 1,
                                         wr_env      => q[NPG_REPOSITORY_ROOT=/lustre/scratch113/npg_repository,REF_PATH=/lustre/scratch113/npg_repository/cram_cache/%2s/%2s/%s,PATH=bin,PERL5LIB=lib],
                                         );

is ($m->repository,$tempdir,"Repository $tempdir");

$m->run_query();

$m->make_commands();

is ($m->out_dir,qq[merge_component_results/5392/c0/00/c0002b941f3adc308273f994abc874d1232e285a3d5e5aa5c69cc932f509023e],q[Correct out dir]);


my $expected_fh = IO::File->new("jq . -S $expected_cmd_file_copy |") or croak "Cannot open $expected_cmd_file_copy";
my @expected_wr_commands_str;
   while(<$expected_fh>){ push @expected_wr_commands_str,$_ }

my $fh = IO::File->new("jq . -S $tempdir/wr_input_cmds.txt |") or croak "Cannot open $tempdir/wr_input_cmds.txt";
my @wr_commands_str;
   while(<$fh>){ 
        s#conf_path=/tmp/\S+/configs#conf_path=/path_to/configs#; 
        s#/tmp/\S+/(references|geno_refset)#/my/$1#g;
        push @wr_commands_str,$_ ;
   }


is_deeply(\@wr_commands_str,\@expected_wr_commands_str,q[wr commands match expected]);

}

1;
