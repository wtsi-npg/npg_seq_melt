#########
# Author:        rmp
# Last Modified: $Date: 2010-04-07 09:42:23 +0100 (Wed, 07 Apr 2010) $ $Author: mg8 $
# Id:            $Id: 00-distribution.t 8987 2010-04-07 08:42:23Z mg8 $
# Source:        $Source: /cvsroot/Bio-DasLite/Bio-DasLite/t/00-distribution.t,v $
# $HeadURL: svn+ssh://intcvs1/repos/svn/new-pipeline-dev/data_handling/branches/prerelease-39.0_ces/t/00-distribution.t $
#
package distribution;
use strict;
use warnings;
use Test::More;
use English qw(-no_match_vars);

our $VERSION = do { my @r = (q$LastChangedRevision: 8987 $ =~ /\d+/mxg); sprintf '%d.'.'%03d' x $#r, @r };

eval {
  require Test::Distribution;
};

if($EVAL_ERROR) {
  plan skip_all => 'Test::Distribution not installed';
} else {
  my @nots = qw(prereq pod);
  Test::Distribution->import('not' => \@nots); # Having issues with Test::Dist seeing my PREREQ_PM :(
}

1;
