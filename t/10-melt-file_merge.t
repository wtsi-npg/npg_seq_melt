use strict;
use warnings;
use WTSI::NPG::iRODS;
use Test::More tests => 2;

use_ok('npg_seq_melt::file_merge');

$ENV{TEST_DIR} = q(t/data);
my $irods = WTSI::NPG::iRODS->new();
my $r = npg_seq_melt::file_merge->new(merge_cmd =>qq[$ENV{TEST_DIR}/my_merge_cmd.pl], dry_run=>1,irods=>$irods);

is ($r->default_root_dir,'/seq/illumina/library_merge/',q[Default iRODS root dir ok]);
1;
