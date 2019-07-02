use strict;
use warnings;
use t::dbic_util;
use File::Copy;
use File::Temp qw/ tempdir /;
use File::Path qw/make_path/;
use Carp;
use File::Slurp;
use Test::More tests => 3;
use JSON;
use IO::File;

use_ok('npg_seq_melt::merge::top_up');

$ENV{TEST_DIR} = q(t/data);

my $dbic_util = t::dbic_util->new();
my $wh_schema = $dbic_util->test_schema_mlwh('t/data/fixtures/mlwh_topup');

{

my $tempdir = tempdir( CLEANUP => 1);


my $expected_cmd_file = join q[/],$ENV{TEST_DIR},q[wr],q[wr_input_cmds.txt];
my $expected_cmd_file_copy = join q[/],$tempdir,q[copy_wr_input_cmds.txt];
copy($expected_cmd_file,$expected_cmd_file_copy) or carp "Copy failed: $!";

make_path(join q[/],$tempdir,q[configs]);

my $config_file = join q[/],$ENV{TEST_DIR},q[configs],q[product_release.yml];  
my $config_file_copy = join q[/],$tempdir,q[configs],q[product_release.yml];
copy($config_file,$config_file_copy) or carp "Copy failed: $!";
chdir $tempdir;

my $m = npg_seq_melt::merge::top_up->new(rt_ticket => q[12345],
                                         commands_file => qq[$tempdir/wr_input_cmds.txt], 
                                         id_study_lims  => 5392,
                                         conf_path => qq[$tempdir/configs],
                                         mlwh_schema => $wh_schema,
                                         wr_env      => q[NPG_REPOSITORY_ROOT=/lustre/scratch113/npg_repository,REF_PATH=/lustre/scratch113/npg_repository/cram_cache/%2s/%2s/%s,PATH=bin,PERL5LIB=lib], 
                                         picard_genome_ref => q[/my/references/Homo_sapiens/GRCh38_15_plus_hs38d1/all/picard/Homo_sapiens.GRCh38_15_plus_hs38d1.fa],
                                         fasta_genome_ref => q[/my/references/Homo_sapiens/GRCh38_15_plus_hs38d1/all/fasta/Homo_sapiens.GRCh38_15_plus_hs38d1.fa],
                                         bwa_genome_ref => q[/my/references/Homo_sapiens/GRCh38_15_plus_hs38d1/all/bwa0_6/Homo_sapiens.GRCh38_15_plus_hs38d1.fa],
                                         targets => q[/my/references/Homo_sapiens/GRCh38_15_plus_hs38d1/all/target/Homo_sapiens.GRCh38_15_plus_hs38d1.fa.interval_list], 
                                         custom_targets => q[/my/references/Homo_sapiens/GRCh38_15_plus_hs38d1/all/custom_targets/autosomes_only_0419/Homo_sapiens.GRCh38_15_plus_hs38d1.fa.interval_list], 
                                         annotation_vcf => q[/my/geno_refset/study5392/GRCh38_15_plus_hs38d1/bcftools/study5392.annotation.vcf]
                                         );



$m->run_query();

$m->make_commands();

is ($m->out_dir,qq[merge_component_results/5392/c0/00/c0002b941f3adc308273f994abc874d1232e285a3d5e5aa5c69cc932f509023e],q[Correct out dir]);


my $expected_fh = IO::File->new("jq . -S $expected_cmd_file_copy |") or croak "Cannot open $expected_cmd_file_copy";
my @expected_wr_commands_str;
   while(<$expected_fh>){ push @expected_wr_commands_str,$_ }

my $fh = IO::File->new("jq . -S $tempdir/wr_input_cmds.txt |") or croak "Cannot open $tempdir/wr_input_cmds.txt";
my @wr_commands_str;
   while(<$fh>){ s|conf_path=/tmp/\S+/configs|conf_path=/path_to/configs|; push @wr_commands_str,$_ }


is_deeply(\@wr_commands_str,\@expected_wr_commands_str,q[wr commands match expected]);

}

1;
