use strict;
use warnings;
use WTSI::NPG::iRODS;
use Test::More tests => 8;
use File::Temp qw/ tempfile /;

use_ok('npg_seq_melt::merge::generator');

my $irods = WTSI::NPG::iRODS->new();
my ($fh, $filename) = tempfile();
chmod 0775, $filename; 

my $r = npg_seq_melt::merge::generator->new(
    merge_cmd => $filename,
    dry_run   => 1,
    irods     => $irods);

is ($r->default_root_dir,'/seq/illumina/library_merge/',q[Default iRODS root dir ok]);
    $r->default_root_dir(q[/seq/npg/test1/merged]);
my $digest = 'b5a04fbf270d41649224463c03d228632847195786ab9e850e90b6a7c50916df';
for ((1,2)) {
  is ($r->_check_existance('14582:7;14582:8'),1,"String found as composition imeta in test iRODS");
  isa_ok ($r->composition(),q[npg_tracking::glossary::composition],"composition attribute o.k.");
  is ($r->composition->digest, $digest, 'digest correct');
}

1;
