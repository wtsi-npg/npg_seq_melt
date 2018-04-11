package npg_seq_melt::util::log;

use Moose::Role;
use Log::Log4perl;
use MooseX::StrictConstructor;

our $VERSION = '0';

=head1 NAME

npg_seq_melt::util::log

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS


=head2 logger

=cut

# These methods are autodelegated to instances with this role.
our @HANDLED_LOG_METHODS = qw(trace debug info warn error fatal
                              logwarn logdie
                              logcarp logcluck logconfess logcroak);


has 'logger' => (isa        => q[Log::Log4perl::Logger],
                 is         => q[ro],
                 metaclass  => q[NoGetopt],
                 handles    => [@HANDLED_LOG_METHODS],
                 default    => sub { Log::Log4perl->get_logger() },
                 documentation => q[Optional Log::Log4perl logger],
                 );

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item Log::Log4perl

=item MooseX::StrictConstructor

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR



=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015 Genome Research Limited

This program is free software: you can redistribute it and/or modify
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
