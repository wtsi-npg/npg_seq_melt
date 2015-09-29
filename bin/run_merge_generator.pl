#!/usr/bin/env perl
#########
# Author:        ces
# Created:       2015-07-01
#


use strict;
use warnings;
use FindBin qw($Bin);
use lib ( -d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib" );
use Log::Log4perl;
use WTSI::NPG::iRODS;

use npg_seq_melt::file_merge;

our $VERSION = '0';

my $log4perl_config =<< 'CONFIG';
log4perl.logger.dnap.npg.irods = ERROR, A1

log4perl.appender.A1           = Log::Log4perl::Appender::Screen
log4perl.appender.A1.utf8      = 1
log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %d %-5p %c %M - %m%n
CONFIG

Log::Log4perl::init_once(\$log4perl_config);

my $logger = Log::Log4perl->get_logger('dnap.npg.irods');
my $irods = WTSI::NPG::iRODS->new(logger => $logger);

npg_seq_melt::file_merge->new_with_options(irods => $irods)->run();

exit 0;

__END__

=head1 NAME

run_merge_generator.pl

=head1  VERSION

 $VERSION

=head1 USAGE

run_merge_generator.pl 

           --merge_cmd       merge_script.pl
           --log_dir         /mylogdir/
           --only_library_id legacy_library_id(s)
           --max_jobs        int
           --use_lsf         1
           --dry_run         1

           --id_run_list     list.txt
                       OR
           --id_runs         run_id(s)
                       OR
           --num_days        int
                       OR
           --id_study_lims   study id


=head1 CONFIGURATION

=head1 SYNOPSIS

=head1 DESCRIPTION

run_merge_generator.pl creates merge jobs

=head1 SUBROUTINES/METHODS

=head1 REQUIRED ARGUMENTS

=head1 OPTIONS

=head1 EXIT STATUS

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item strict

=item warnings

=item FindBin

=item npg_seq_melt::file_merge

=item Log::Log4perl

=item WTSI::NPG::iRODS

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Carol Scott E<lt>ces@sanger.ac.ukE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015 by Genome Research Limited

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
