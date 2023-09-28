use strict;
use warnings;
use English qw{-no_match_vars};
use Test::More tests => 2;
use Test::Exception;
use Cwd;

my $script  = cwd() . q[/bin/npg_rsync_pacbio_runfolders.sh];
lives_ok { qx{bash -t $script 2>&1} } qq{ran $script -n test};
ok(!$CHILD_ERROR, qq{Return code of $CHILD_ERROR});

1;
