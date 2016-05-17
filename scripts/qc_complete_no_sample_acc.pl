#!/usr/bin/env perl
use strict;
use warnings;
use FindBin qw($Bin);
use lib ( -d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib" );
use Log::Log4perl qw(:easy);
use DateTime;
use DateTime::Duration;
use Getopt::Long;
use Pod::Usage;
use WTSI::DNAP::Warehouse::Schema;

our $VERSION = '0';

my $help             = q[];
my $start            = q[];
my $end              = q[];
my $days             = q[30];
my $verbose          = q[];

GetOptions ('help'    => \$help,
            'verbose' => \$verbose,
            'start=i'   => \$start,
            'end=i'     => \$end,
            'days=i'     => \$days,
           );
if ($help) { pod2usage(0); }


Log::Log4perl->easy_init($INFO);

my $logger = get_logger();

if ($start){ if ($end < $start){ $logger->logcroak('end date range must be >= start') } }

my $s=WTSI::DNAP::Warehouse::Schema->connect();
my $dbix_ipm=$s->resultset('IseqProductMetric');

my $schema = $dbix_ipm->result_source->schema;


my $dtf = $dbix_ipm->result_source->storage->datetime_parser;

my ($start_date,$end_date) = _start_end_dates();

$logger->info("DATE RANGE: $start_date to $end_date");


my $where = {};
   $where->{'iseq_run_lane_metric.qc_complete'} = {'-between', [$start_date,$end_date] };

  my $rs = _get_product_rs($dbix_ipm,$where);

  my $study_samples={};
 
  while (my $prow = $rs->next()) {
       my $fc_row = $prow->iseq_flowcell;
       if ($fc_row){
           my $ref = _get_reference($fc_row);
           next if $ref !~ /^Homo_sapiens/smx;
           ##skip if there is a sample accession_number
           next if $fc_row->sample_accession_number;
           next if $fc_row->sample_consent_withdrawn; #210416
           #$logger->info( join"\t",$prow->id_run, $fc_row->position, $fc_row->tag_index,$fc_row->id_library_lims, $fc_row->legacy_library_id, $fc_row->sample_id, $fc_row->sample_name,$fc_row->study_id, $fc_row->study_name,$ref,"\n" ); 

            $study_samples->{$fc_row->study_name}{sample_id}{$fc_row->sample_id}++;
       }
  }


foreach my $st (sort keys %{$study_samples}){
          my $sample_count = map {$_} sort keys %{$study_samples->{$st}{sample_id}};
          $logger->info("$st sample count = $sample_count");

          if ($verbose){
              my $msg;
	            foreach my $sid (sort keys %{$study_samples->{$st}{sample_id}}){
		           $msg .= "$sid ";
              }
              $logger->info("$msg");
          }
}


sub _get_product_rs {
  my ($ipm, $where_clause) = @_;
  my $join =  { 'join' => [qw/iseq_flowcell iseq_run_lane_metric/] };
  my $res = $ipm->search($where_clause, $join);
  return $res;
}


sub _get_reference {
  my $fc_row = shift;
  my $ref = $fc_row->sample_reference_genome;
  if ($ref){ $ref =~ s/^\s+//xms }
  $ref ||= $fc_row->study_reference_genome;
  $ref =~ s/^\s+//xms;
  return $ref;
}


=head1 _opt_date

returns DateTime object from string YYYYMMDD

=cut


sub _opt_date{
    my $str = shift;
    my($yyyy,$mm,$dd);

    if ($str =~ /(\d{4})-*(\d{2})-*(\d{2})/smx){
        $yyyy = $1;
        $mm   = $2;
        $dd   = $3;
	      return(DateTime->new(year=>$yyyy,month=>$mm,day=>$dd));
    }
    return;
}

=head1  _start_end_dates

returns start and end in format YYYY-MM-DD hh:mm:ss

=cut


sub _start_end_dates {
    my ($dte,$dts);   
    $dte = $end ? _opt_date($end) : DateTime->now;
    $dts = $start ?  _opt_date($start) : _subtract_days_from_end($dte->ymd());
	  return ($dtf->format_datetime($dts),$dtf->format_datetime($dte));
}

sub _subtract_days_from_end {
   my $end_ymd = shift;
   my $dts = _opt_date($end_ymd);
   my $hours = '24';
   return $dts->subtract_duration(DateTime::Duration->new(hours =>$days * $hours));
}


exit 0;

__END__
=head1 NAME

qc_complete_no_sample_acc.pl

=head1 CONFIGURATION

=head1 USAGE 

./qc_complete_no_sample_acc.pl 

=head1 DESCRIPTION

Script to report the study name and count of samples lacking accession numbers, from human libraries, where the run is at qc complete status.

=head1 REQUIRED ARGUMENTS

=head1 OPTIONS

--help     brief help message

--verbose  additionally print the sample id's , not just the count

--start    Earliest qc_complete date YYYYMMDD (default last 30 days)

--end      Latest qc_complete date YYYYMMDD (default today)

--days     If start not specified, how many days in date range (default 30)

=head1 EXIT STATUS

0

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item strict

=item warnings

=item lib

=item FindBin

=item Log::Log4perl

=item WTSI::DNAP::Warehouse::Schema

=item DateTime

=item Getopt::Long;

=item Pod::Usage;

=item DateTime

=item DateTime::Duration

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016 by Genome Research Limited

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

=cut
