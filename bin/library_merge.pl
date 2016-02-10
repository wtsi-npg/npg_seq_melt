#!/usr/bin/env perl
#########
# Author:        jillian
# Created:       2015-07-01
#


use strict;
use warnings;
use FindBin qw($Bin);
use lib ( -d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib" );
use Log::Log4perl;
use WTSI::NPG::iRODS;

use npg_seq_melt::merge::library;

our $VERSION = '0';

my $log4perl_config =<< 'CONFIG';
log4perl.logger.dnap.npg.irods = ERROR, A1

log4perl.appender.A1           = Log::Log4perl::Appender::Screen
log4perl.appender.A1.utf8      = 1
log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %d %-5p %c %M - %m%n
CONFIG

Log::Log4perl::init_once(\$log4perl_config);

my $irods = WTSI::NPG::iRODS->new();

npg_seq_melt::merge::library->new_with_options(irods => $irods)->process();

exit 0;

__END__

=head1 NAME

library_merge.pl

=head1 USAGE

library_merge.pl [-?h] [long options...]

library_merge.pl 

           --rpt_list        '14582:7;14582:8' 
           --library         11869933 
           --sample          1993949
           --study           3149 
           --instrument_type HiSeqX 
           --run_type        paired 
           --chemistry       HiSeqX_V1
           --use_irods
           --local


=head1 CONFIGURATION

=head1 SYNOPSIS

=head1 DESCRIPTION

library_merge.pl jobs set off from npg_seq_melt::merge::generator

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

=item lib

=item FindBin

=item npg_seq_melt::merge::library

=item Log::Log4perl

=item WTSI::NPG::iRODS;

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham E<lt>jillian@sanger.ac.ukE<gt>

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
