use strict;
use warnings;
use WTSI::NPG::iRODS;
use Test::More tests => 4;
use_ok('npg_seq_melt::merge::generator');

$ENV{TEST_DIR} = q(t/data);
my $irods = WTSI::NPG::iRODS->new();
my $r = npg_seq_melt::merge::generator->new(merge_cmd =>qq[$ENV{TEST_DIR}/my_merge_cmd.pl], dry_run=>1,irods=>$irods);

is ($r->default_root_dir,'/seq/illumina/library_merge/',q[Default iRODS root dir ok]);
    $r->default_root_dir(q[/seq/npg/test1/merged]);
is ($r->_check_existance('14582:7;14582:8'),1,"String found as composition imeta in test iRODS");

isa_ok ($r->composition(),q[npg_tracking::glossary::composition],"composition attribute o.k.");

1;
