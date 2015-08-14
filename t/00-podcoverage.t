#########
# Author:        mg8
# Last Modified: $Date: 2010-02-24 09:27:37 +0000 (Wed, 24 Feb 2010) $ $Author: mg8 $
# Id:            $Id: 00-distribution.t 8372 2010-02-24 09:27:37Z mg8 $
# Source:        $Source: /cvsroot/Bio-DasLite/Bio-DasLite/t/00-distribution.t,v $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/data_handling/branches/prerelease-20.0/t/00-distribution.t $
#

use strict;
use warnings;
use Test::More;

eval "use Test::Pod::Coverage 1.04";
plan skip_all => 'Test::Pod::Coverage 1.04 required' if $@;

my @dirs = ('lib/srpipe');
my @modules = all_modules(@dirs);
plan tests => scalar @modules;

foreach my $module (@modules) {
  pod_coverage_ok($module);
}

1;
