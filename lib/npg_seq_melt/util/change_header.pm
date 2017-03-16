package npg_seq_melt::util::change_header;


use Moose;
use MooseX::StrictConstructor;
use Carp;
use Readonly;
use English qw(-no_match_vars);
use IO::File;
use Cwd qw/cwd/;
use Log::Log4perl qw(:easy);
use st::api::lims;
use IPC::Open3;

with qw{
        MooseX::Getopt
        npg_tracking::glossary::rpt
        npg_seq_melt::util::irods
};


our $VERSION  = '0';

Readonly::Scalar our $EXIT_CODE_SHIFT => 8;
Readonly::Scalar my $MAX_DS_LENGTH    => 500;

=head1 NAME

npg_seq_melt::util::change_header

=head1 SYNOPSIS

Functionality for updating cram header @RG sample, library and study description fields from streamed data.
Can additionally re-header in place in iRODS.


=head1 DESCRIPTION
Takes colon-delimited rpt as input, queries (default ml_warehouse_fc_cache) for sample publishable name, study publishable name and study description and library id. The current cram header is extracted by samtools and a new header is created with the updated fields. 

Re-headering in iRODS can be done along with updating the md5 imeta and rt_ticket .


=head1 SUBROUTINES/METHODS

=head2 truncate

truncate description to a length of 500 characters

=cut

has 'truncate'   => ( isa           => 'Bool',
                      is            => 'ro',
                      default       =>  0,
                     );

=head2 lims_driver

=cut 

has 'lims_driver'   => ( isa           => 'Str',
                         is            => 'ro',
                         default       =>  q[ml_warehouse_fc_cache],
                       );

=head2 mlwh_schema

=cut

has 'mlwh_schema'  => ( isa    => 'WTSI::DNAP::Warehouse::Schema',
                        is            => 'ro',
    );

=head2 dry_run 

Boolean flag, true by default. Skips iRODS updating.


=cut

has 'dry_run'      => ( isa           => 'Bool',
                        is            => 'ro',
                        default       => 1,
                        documentation =>
  'Boolean flag, true by default. ' .
  'Skips iRODS updating',
);


=head2 samtools


=cut

has 'samtools'  => ( isa           => q[Str],
                     is            => q[rw],
                     default       => q[samtools1],
                   );


=head2  rt_ticket


=cut

has 'rt_ticket'   => ( isa           => q[Int],
                       is            => q[ro],
                       documentation => q[RT ticket number to add to iRODS meta data],
                     );


=head2  run_dir


=cut

has 'run_dir'   => ( isa           => q[Str],
                     is            => q[ro],
                     default       => cwd(),
                    );


=head2 mismatch

=cut

has 'mismatch'  => (isa   => q[Int],
                    is    => q[rw],
                    metaclass  => 'NoGetopt',
                    default => 0,
                   );
=head2 sample

=cut

has 'sample'  => ( isa           => q[Str],
                   is            => q[rw],
                 );


=head2 library

Usually a numerical library id. Tag 0 cram has LB:unknown. 

=cut

has 'library'  => ( isa           => q[Str],
                    is            => q[rw],
                  );

=head2 study

=cut

has 'study'  => ( isa           => q[Str],
                  is            => q[rw],
                );

=head2 rpt

Semi-colon separated run:position or run:position:tag

=cut

has 'rpt' => (
     isa           => q[Str],
     is            => q[ro],
);


=head2 ifile

CSV file

=cut

has 'ifile'      => ( isa    => q[Str],
                      is     => q[ro],
                      documentation => q[ csv file of run,position[,tag]],
);

=head2 logger

=cut

has 'logger'     => (isa        => q[Log::Log4perl::Logger],
                     is         => q[ro],
                     default    => sub {  Log::Log4perl->get_logger(); },
                    );

=head2 cram

=cut

has 'cram'     => (isa      => q[Str],
                   is       => q[rw],
                    );

=head2 icram

=cut

has 'icram'     => (isa      => q[Str],
                   is       => q[rw],
                    );

=head2 new_header

=cut

has 'new_header'     => (isa      => q[Str],
                         is       => q[rw],
                       );

=head2 new_header_file

=cut

has 'new_header_file'     => (isa      => q[Str],
                              is       => q[rw],
                            );

=head2 _run_acmd

=cut

sub _run_acmd {
    my $self = shift;
    my $cmd  = shift;
    my $err = 0;
    my $cwd = cwd();
    $self->logger->log($INFO,qq[\n\nCWD=$cwd\nRunning ***$cmd***]);
    if ( system "$cmd" ){
       $err = $CHILD_ERROR >> $EXIT_CODE_SHIFT;
       $self->logger->logcroak($FATAL,qq[System command ***$cmd*** failed with error $err]);
    }
     return();
}

=head2 run

Get library, sample and study information from LIMS

=cut 

sub run {
    my $self = shift;

    my ($sample, $library, $study);
    my $rpt_key = $self->rpt;

    my $rpt     = npg_tracking::glossary::rpt->inflate_rpt($rpt_key);
    my $tag = $rpt->{'tag_index'};

    my $ref = {
       driver_type => $self->lims_driver,
       id_run      => $rpt->{'id_run'},
       position    => $rpt->{'position'}
    };
    if (defined $tag) {
        $ref->{'tag_index'} = $tag;
    }
    if (defined $self->mlwh_schema) {
        $ref->{'mlwh_schema'} = $self->mlwh_schema;
    }
    my $lims = st::api::lims->new($ref);

    if (defined $tag && $tag == 0) {
     ($sample, $library, $study) = $self->_get_limsm($lims);
    } else {
     ($sample, $library, $study) = $self->_get_limsi($lims);
    }

    $self->logger->log($DEBUG, qq[[DEBUG],$sample, $library, $study]);

    $self->sample($sample);
    $self->library($library);
    $self->study($study);

  return $self;
}


#--------------------------------#
# Return multiple LIMS values as
# a concatenated list.
#--------------------------------#
sub _get_limsm {
    my ($self,$lims) = @_;
    my(@samples,@studies,%s);
    foreach my $plex ($lims->children) {
        next if $plex->is_phix_spike;
        my ($sample_name,$library_id,$study) = $self->_get_limsi($plex);
        push @samples, $sample_name;
        if (! defined $s{$study}){ push @studies, $study };
        $s{$study}++;
    }
    my $sample_list = join q[,], @samples;
    my $study_list  = join q[,], @studies;
    return($sample_list, 'unknown', q[Study ]. $study_list);
}

=head2 _get_limsi

 Return individual LIMS values.

=cut 

sub _get_limsi {
    my $self = shift;
    my ($lims) = shift;
    my $sample_name       = $self->_check_lims_info($lims->sample_publishable_name());
    my $library_id        = $self->_check_lims_info($lims->library_id());
    my $study_name        = $self->_check_lims_info($lims->study_publishable_name());
    my $study_description = $self->_check_lims_info($lims->study_description());
    if($lims->is_phix_spike){
        $study_description = 'SPIKED_CONTROL'
    }
    return($sample_name, $library_id, $study_name. q[: ].$study_description);
}

=head2 _check_lims_info

 Remove '\t' and '\n' characters contained in LIMS information.

=cut

sub _check_lims_info {
    my $self = shift;
    my ($lims_info) = shift;
    $lims_info =~ s/\n/\ /gmxs;
    $lims_info =~ s/\t/\ /gmxs;
    $lims_info =~ s/\r/\ /gmxs; #Ctrl-M
    return $lims_info;
}


=head2 _compare_info

 Compare the value obtained from
 the LIMS vs the value of SM, LB
 and DS present in the header
 and prints a message if they
 are different.

=cut 

sub _compare_info {
    my ($self,$tag, $hdr_val, $lims_val, $rpt) = @_;
    my ($new_hdr_val, $new_lims_val);
    $self->logger->log($DEBUG,"$tag, $hdr_val, $lims_val, $rpt");
    if($hdr_val ne $lims_val){
	 $self->mismatch( $self->mismatch + 1 );

         if ($tag eq q[DS] && $self->truncate) {
            # Avoid very long values in warning message
            $new_hdr_val = (substr $hdr_val, 0, $MAX_DS_LENGTH) . q[... [TRUNCATED]];
            $new_lims_val = (substr $lims_val, 0, $MAX_DS_LENGTH) . q[... [TRUNCATED]];
        } else {
            $new_hdr_val = $hdr_val;
            $new_lims_val = $lims_val;
        }
        $self->logger->log($INFO, qq[[INFO] [$rpt]: There is a mismatch between tag value and LIMS metadata:]);
        $self->logger->log($INFO, qq[[INFO] [$tag tag]: $new_hdr_val]);
        $self->logger->log($INFO, qq[[INFO] [ LIMS ]: $new_lims_val]);
        carp qq[Value of tag $tag doesn't match LIMS metadata];
    }
    return;
}


#--------------------------------#
# Process header stream
#--------------------------------#

=head2 read_header

Read iRODS cram header and write new one, with any updates, locally.

=cut 

sub read_header {
    my $self = shift;
    my $irods_root = $self->irods_root;
    $self->logger->log($DEBUG,qq[irods_root $irods_root]);
    my $rpt     = npg_tracking::glossary::rpt->inflate_rpt($self->rpt);

     my $cram    =  $rpt->{'id_run'} .q[_]. $rpt->{'position'};
        if (defined $rpt->{'tag_index'}){ $cram   .=  q[#]. $rpt->{'tag_index'} };
        $cram   .=  q[.cram];
    my $icram   =  $self->irods_root .q[/]. $rpt->{'id_run'} .q[/]. $cram;
    $self->icram($icram);
    $self->cram($cram);

    $self->logger->log($INFO,qq[[input CRAM] $icram]);

    if(! $self->has_irods){$self->set_irods($self->get_irods);}

    if (! $self->irods->is_object($icram) ){ $self->logger->logcroak($FATAL,"$icram not found\n") }

    my $header_cmd = $self->samtools .q{ view -H irods:}. $icram;

    my $pid = open3( undef, my $header_fh, undef, $header_cmd);
              binmode $header_fh, ':encoding(UTF-8)';

    my $new_header;
    while (<$header_fh>) {
           $new_header .= $self->process_header();
    }

    waitpid $pid, 0;
    if( $CHILD_ERROR >> $EXIT_CODE_SHIFT){
        croak "Failed $header_cmd";
    }
    close $header_fh or croak "cannot close a handle to '$header_cmd' output: $ERRNO";
    $self->new_header($new_header);

    $self->write_header();

return;
}

=head2 write_header

Write out the updated header to a file

=cut

sub write_header {
    my $self = shift;

    my $header = $self->new_header();
    my $newheader_file = $self->run_dir . q[/] . $self->cram .q(.headernew);
    $self->new_header_file($newheader_file);

    if(open my $headout_fh, q(>), $newheader_file){
        binmode $headout_fh, ':encoding(UTF-8)';
        print {$headout_fh} $header or croak "Can't write to '$newheader_file': $ERRNO";
        close $headout_fh or croak "Can't close '$newheader_file': $ERRNO";
    }
    croak "Header file $newheader_file zero or doesn't exist\n" if ! (-f $newheader_file or -z $newheader_file);
    return();
}

=head2 process_header

Update sample, library and study fields in the header, reporting any differences

=cut

sub process_header {
    my $self = shift;

    my $sample    = $self->sample;
    my $library   = $self->library;
    my $study     = $self->study;
    my $rpt_key   = $self->rpt;

my $header;
    if(/^\@RG/xms){
       chomp;
       my @l = split /\t/xms;
        my $i;
        my ($sm, $lb, $ds);
        for my $i (0..$#l){
            if($l[$i] =~ /^SM:(.*)$/xms){
                $sm = $1;
                $l[$i] = q[SM:] . $sample;
                $self->_compare_info(q[SM],$sm, $sample, $rpt_key)
            }elsif($l[$i] =~ /^LB:(.*)$/xms){
                $lb = $1;
                $l[$i] = q[LB:] . $library;
                $self->_compare_info(q[LB], $lb, $library, $rpt_key)
            }elsif($l[$i] =~ /^DS:(.*)$/xms){
                $ds = $1;
                $l[$i] = q[DS:] . $study;
                $self->_compare_info(q[DS], $ds, $study, $rpt_key)
            }
        }
        $header .= join(qq{\t}, @l) ."\n";
    }else{
        $header .= $_;
    }

 return $header;
}

=head2 run_reheader

If the header has changed, update in iRODS, add new md5 and rt_ticket.

=cut

sub run_reheader {
    my $self = shift;

    if (! $self->dry_run){

	if ($self->mismatch){

	$self->logger->log($INFO,$self->mismatch, q[ mis-matched field(s) re-headering in iRODS]);
        my $icram = $self->icram;
        my @irods_meta = $self->irods->get_object_meta($icram);
        my @library_id  = map { $_->{value} => $_ } grep { $_->{attribute} eq 'library_id' }  @irods_meta;
        my $libid = $library_id[0];
        if ($libid && ($libid != $self->library)){ $self->logger->log($INFO, qq[[iRODS meta library_id ] $libid differs from LIMS ], $self->library) }

        my @mmd5  = map { $_->{value} => $_ } grep { $_->{attribute} eq 'md5' }  @irods_meta;
        my $mmd5 = $mmd5[0];
        my @rts   = map { $_->{value} => $_ } grep { $_->{attribute} eq 'rt_ticket' } @irods_meta;

        $self->_run_reheader_cmd();


        ## update md5 
        $self->_update_md5($mmd5);

        ## update rt_ticket
        if ($self->rt_ticket){ $self->_update_rt_ticket($self->rt_ticket,\@rts) };
	}
        else {
           $self->logger->log($INFO,q[ 0 mis-matched field(s) not re-headering in iRODS]);
        }
    }
    return;
}

sub _update_md5{
    my($self,$mmd5) = @_;

     my $obj = WTSI::NPG::iRODS::DataObject->new($self->irods,$self->icram);
     my $md5file = $obj->checksum;

     $self->logger->log($INFO,qq[old md5: $mmd5, new md5: $md5file]);

     ## this also generates a md5_history attribute
     $obj->supersede_avus(q[md5], $md5file);  #WTSI::NPG::iRODS::Path
    return;
}

sub _update_rt_ticket {
    my($self,$rt,$rts) = @_;

    my ($seen_rt);
        foreach my $r (@{$rts}){
           next if ref($r) eq 'HASH';
           if ($r == $rt){ $seen_rt = 1 ; $self->logger->log($DEBUG,qq[rt_ticket $rt already present]);}
        }

    if (! $seen_rt){
      $self->logger->log($INFO,qq[Adding rt_ticket $rt]);
      $self->irods->add_object_avu($self->icram,'rt_ticket',$rt);
    }
    return;
}



sub _run_reheader_cmd {
     my $self = shift;

     my $cmd = $self->samtools . q( reheader -i ) . $self->new_header_file . q( irods:) . $self->icram;

return $self->_run_acmd($cmd);
}

1;


__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose 

=item MooseX::StrictConstructor

=item Carp

=item Readonly

=item English

=item IO::File

=item Cwd

=item Log::Log4perl

=item st::api::lims

=item IPC::Open3

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Ruben Bautista, Jillian Durham 

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2017 Genome Research Limited

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
