package npg_seq_melt::util::change_header;


use Moose;
use MooseX::StrictConstructor;
use Readonly;
use English qw(-no_match_vars);
use IO::File;
use Cwd qw/cwd/;
use st::api::lims;
use IPC::Open3;
use WTSI::NPG::iRODS::DataObject;
use Try::Tiny;
use npg_pipeline::function::util;
use npg_tracking::illumina::runfolder;
use JSON;

with qw{
        MooseX::Getopt
        npg_tracking::glossary::rpt
        npg_seq_melt::util::irods
        WTSI::DNAP::Utilities::Loggable
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

Re-headering in iRODS can be done along with updating the md5 imeta and rt_ticket.


=head1 SUBROUTINES/METHODS

=head2 truncate

truncate description to a length of 500 characters

=cut

has 'truncate'   => ( isa           => q[Bool],
                      is            => q[ro],
                      default       =>  0,
                     );

=head2 lims_driver

=cut 

has 'lims_driver'   => ( isa           => q[Str],
                         is            => q[ro],
                         default       => q[ml_warehouse_fc_cache],
                       );

=head2 mlwh_schema

=cut

has 'mlwh_schema'  => ( isa    => q[WTSI::DNAP::Warehouse::Schema],
                        is     => q[ro],
    );

=head2 dry_run 

Boolean flag, true by default. Skips iRODS updating.

=cut

has 'dry_run'      => ( isa           => q[Bool],
                        is            => q[ro],
                        default       => 1,
                        documentation =>
  'Boolean flag, true by default. ' .
  'Skips iRODS updating',
);


=head2 local

Boolean flag, false by default. Where file is local and not in iRODS.

=cut

has 'is_local'     => ( isa           => q[Bool],
                        is            => q[ro],
                        default       => 0,
                        documentation =>
  'Boolean flag, false by default.',
);


has 'prefix'  =>
  (isa           => q[Str],
   is            => q[ro],
   default       => q[irods:],
   documentation => q[Prefix for samtools iRODS],);


=head2 samtools

=cut

has 'samtools'  => ( isa           => q[Str],
                     is            => q[rw],
                     default       => q[samtools],
                   );


=head2  rt_ticket

=cut

has 'rt_ticket'   => ( isa           => q[Maybe[Int]],
                       is            => q[ro],
                       documentation => q[RT ticket number to add to iRODS meta data],
                     );


=head2  run_dir

=cut

has 'run_dir'   => ( isa           => q[Str],
                     is            => q[ro],
                     default       => cwd(),
                    );

=head2  non_standard_cram_dir 

=cut

has 'non_standard_cram_dir'   => ( isa           => q[Str],
                                   is            => q[ro],
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
     predicate     => '_has_rpt',
     writer        => '_set_rpt',
);

sub _get_rpt{  ##for merged
    my ($self) = @_;
    if ($self->merged_cram) {
    ###populate $self->rpt with one component of composition

      if(! $self->has_irods){$self->set_irods($self->get_irods);}
        my $icram = $self->library_merged_cram_path();
        my @component_imeta =  map { $_->{'value'} => $_ } grep { $_->{'attribute'} eq 'component' }
                               $self->irods->get_object_meta($icram);
        $self->clear_irods;
        my $json = JSON->new->allow_nonref;
        return $self->_set_rpt(npg_tracking::glossary::rpt->deflate_rpt(decode_json $component_imeta[0]));
    }
}


=head2 merged_cram

e.g. 13571036.ANXX.paired158.2ca9f7e25f.cram

=cut

has 'merged_cram' => (
     isa           => q[Str],
     is            => q[ro],
     required      => 0,
);


=head2 cram

=cut

has 'cram'  =>
  (isa           => q[Str],
   is            => q[ro],
   init_arg      => undef,
   lazy          => 1,
   builder       => q[_build_cram],
   );

sub _build_cram{
    my $self    = shift;

    if ($self->merged_cram){ return $self->merged_cram }

    my $rpt     = npg_tracking::glossary::rpt->inflate_rpt($self->rpt);

    my $cram    =  $rpt->{'id_run'} .q[_]. $rpt->{'position'};
    if (defined $rpt->{'tag_index'}){ $cram   .=  q[#]. $rpt->{'tag_index'} };
    $cram   .=  q[.cram];

    return $cram;
}


=head2 icram

=cut

has 'icram'  =>
  (isa           => q[Str],
   is            => q[ro],
   init_arg      => undef,
   lazy          => 1,
   builder       => q[_build_icram],
   );

sub _build_icram{
    my $self    = shift;

    my $icram;

    if($self->is_local){
       if($self->non_standard_cram_dir){
         $icram = $self->non_standard_cram_dir . q[/] . $self->cram;
       }
       else {
         $icram = $self->archive_cram_dir . q[/] . $self->cram;
       }
       $self->_check_existance($icram);

    }else{
	    if ($self->merged_cram){
                  $icram = $self->library_merged_cram_path();
            }else{
	          $icram = $self->irods_root .q[/].
            npg_tracking::glossary::rpt->inflate_rpt($self->rpt)->{'id_run'} .
            q[/]. $self->cram;
	}

       if(! $self->has_irods){$self->set_irods($self->get_irods);}
       if(! $self->irods->is_object($icram)){ $self->logcroak(qq[$icram not found]) }
       $self->clear_irods;
    }

    $self->info(qq[[input CRAM] $icram]);

    return $icram;
}

=head2 library_merged_cram_path

Return full iRODS path from cram file name 

=cut

sub library_merged_cram_path {
    my $self = shift;
    my $collection_name  = $self->cram;
       $collection_name  =~ s/[.]cram$//xms;
    return ($self->irods_root .qq[/illumina/library_merge/$collection_name/]. $self->cram);
}

=head2 archive_cram_dir

plexed e.g. /nfs/sf34/ILorHSany_sf34/outgoing/170407_HX2_22245_B_HGTF7ALXX/Latest_Summary/archive/lane1

unplexed e.g. /nfs/sf39/ILorHSany_sf39/outgoing/170425_HX7_22345_B_HGYWMALXX/Latest_Summary/archive 

=cut

has 'archive_cram_dir' =>
  (isa           => q[Str],
   is            => q[ro],
   lazy          => 1,
   builder       => q[_build_archive_cram_dir],
   );

sub _build_archive_cram_dir{
    my $self = shift;
    my $h        = npg_tracking::glossary::rpt->inflate_rpt($self->rpt);
    my $rf       = npg_tracking::illumina::runfolder->new(id_run=>$h->{'id_run'})->runfolder_path;
    my $cram_dir =  qq[$rf/Latest_Summary/archive];
    if (defined $h->{'tag_index'}){ $cram_dir .= qq[/lane$h->{'position'}] }
    return $cram_dir;
}

=head2 new_header

=cut

has 'new_header'  => (isa      => q[Str],
                      is       => q[rw],
                      init_arg => undef,
                      );

=head2 new_header_file

=cut

has 'new_header_file' => (isa      => q[Str],
                          is       => q[rw],
                          init_arg => undef,
                          );

=head2 _run_acmd

=cut

sub _run_acmd {
    my ($self,$cmd) = @_;
    my $err = 0;
    my $cwd = cwd();
    $self->info(qq[\n\nCWD=$cwd\nRunning ***$cmd***]);
    if ( system "$cmd" ){
       $err = $CHILD_ERROR >> $EXIT_CODE_SHIFT;
       $self->logcroak(qq[System command ***$cmd*** failed with error $err]);
    }
    return();
}

=head2 run

Get library, sample and study information from LIMS

=cut 

sub run {
    my $self = shift;

    my ($sample, $library, $study);

    my $rpt_str = $self->rpt ? $self->rpt : $self->_get_rpt();
    my $rpt = npg_tracking::glossary::rpt->inflate_rpt($rpt_str);
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

    try{
        my $names = npg_pipeline::function::util->get_study_library_sample_names($lims);
          $sample  = $names->{sample}  ? join q{,}, @{$names->{sample}} : q[];
          $library = $names->{library} ? join q{,}, @{$names->{library}} : q[];
          if (defined $tag && $tag == 0) { $library = q[unknown] }
          $study   = $names->{study} ? join q{,}, @{$names->{study}} : q[];
          if ( not($sample) or not($library) or not($study) ){ $self->logwarn(q[LIMs info missing]) }

          $study   =~ s/[\t\n\r]/\ /gmxs;
          $library =~ s/[\t\n\r]/\ /gmxs;
          $sample  =~ s/[\t\n\r]/\ /gmxs;

    }catch{
       $self->logcroak(q[Failed to fetch any LIMs info for : ], $self->cram);
    };

    $self->info("$sample, $library, $study");
    $self->sample($sample);
    $self->library($library);
    $self->study($study);

    return $self;
}


=head2 _compare_info

 Compare the value obtained from the LIMS vs the value of SM, LB
 and DS present in the header and prints a message if they
 are different.

=cut 

sub _compare_info {
    my ($self,$tag, $hdr_val, $lims_val, $rpt) = @_;
    my ($new_hdr_val, $new_lims_val);
    $self->info("$tag, $hdr_val, $lims_val, $rpt");
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
        $self->info(qq[[$rpt]: There is a mismatch between tag value and LIMS metadata:]);
        $self->info(qq[[$tag tag]: $new_hdr_val]);
        $self->info(qq[[LIMS]: $new_lims_val]);
        $self->warn(qq[Value of tag $tag doesn't match LIMS metadata]);
    }
    return;
}


=head2 read_header

Read cram header and write new one, with any updates, locally.

=cut 

sub read_header {
    my $self = shift;

    my $header_cmd = $self->samtools .q{ view -H }.
          (! $self->is_local ? $self->prefix : q[ ]) . $self->icram;

    my $pid = open3( undef, my $header_fh, undef, $header_cmd);
              binmode $header_fh, ':encoding(UTF-8)';

    my $new_header;
    while (<$header_fh>) {
           $new_header .= $self->process_header();
    }

    waitpid $pid, 0;
    if( $CHILD_ERROR >> $EXIT_CODE_SHIFT){
        $self->logcroak(qq[Failed $header_cmd]);
    }
    close $header_fh or $self->logcroak(qq[cannot close a handle to '$header_cmd' output: $ERRNO]);
    $self->new_header($new_header);

    $self->_write_header();

    return;
}

=head2 _write_header

Write out the updated header to a file

=cut

sub _write_header {
    my $self = shift;

    my $header = $self->new_header();
    my $newheader_file = $self->run_dir . q[/] . $self->cram .q(.headernew);

    $self->new_header_file($newheader_file);

    if(open my $headout_fh, q(>), $newheader_file){
        binmode $headout_fh, ':encoding(UTF-8)';
        print {$headout_fh} $header or $self->logcroak(qq[Can't write to '$newheader_file': $ERRNO]);
        close $headout_fh or $self->logcroak(qq[Can't close '$newheader_file': $ERRNO]);
    }else{
        $self->logcroak(qq[Write of header file $newheader_file failed : $ERRNO]);
    }

    $self->_check_existance($newheader_file);

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
                $self->_compare_info(q[SM],$sm, $sample, $rpt_key);
            }elsif($l[$i] =~ /^LB:(.*)$/xms){
                $lb = $1;
                $l[$i] = q[LB:] . $library;
                $self->_compare_info(q[LB], $lb, $library, $rpt_key);
            }elsif($l[$i] =~ /^DS:(.*)$/xms){
                $ds = $1;
                $l[$i] = q[DS:] . $study;
                $self->_compare_info(q[DS], $ds, $study, $rpt_key);
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

    if(! $self->new_header_file){
       $self->logcroak(q[New header file not defined]);
    }

    if (! $self->dry_run){
      if ($self->mismatch){

	      $self->info($self->mismatch, q[ mis-matched field(s) : re-headering]);

        if($self->is_local){
           $self->_run_reheader_cmd;
           $self->_make_md5_cache_file;
        }else{
           if(! $self->has_irods){$self->set_irods($self->get_irods);}
           my @irods_meta = $self->irods->get_object_meta($self->icram);

           my @library_id  = map { $_->{value} => $_ } grep { $_->{attribute} eq 'library_id' }  @irods_meta;
           my $libid = $library_id[0];
           if ($libid && ($libid != $self->library)){
              $self->info(qq[[iRODS meta library_id ] $libid differs from LIMS ], $self->library);
           }

           my @mmd5  = map { $_->{value} => $_ } grep { $_->{attribute} eq 'md5' }  @irods_meta;
           my $mmd5 = $mmd5[0];
           my @rts   = map { $_->{value} => $_ } grep { $_->{attribute} eq 'rt_ticket' } @irods_meta;

           $self->_run_reheader_cmd;

           $self->_update_md5($mmd5);
           if($self->rt_ticket){ $self->_update_rt_ticket($self->rt_ticket,\@rts); }

           $self->clear_irods;
        }
      }
      else {
           $self->info(q[ 0 mis-matched field(s) : NOT re-headering]);
      }
    }
    return;
}

sub _make_md5_cache_file{
    my($self) = shift;

    if(! $self->is_local){ $self->logcroak(q[Can only make md5 for local files]); }

    my $cache_file = $self->icram .q(.md5);
    $self->info("Adding/updating MD5 cache file '$cache_file'");

    try {
      my $cmd = q{cat }. $self->icram .q{ | md5sum -b - | tr -d '\\n *\\-' > }. $cache_file;
      $self->_run_acmd($cmd);
    } catch {
      # failure to create a cache is a not hard error currently.
      $self->warn(qq[Failed to create md5 cache file : $cache_file]);
    };

    return;
}

sub _update_md5{
    my($self,$mmd5) = @_;

    my $obj = WTSI::NPG::iRODS::DataObject->new($self->irods,$self->icram);
    my $md5file = $obj->checksum;

    $self->info(qq[old md5: $mmd5, new md5: $md5file]);

    ## this also generates a md5_history attribute
    $obj->supersede_avus(q[md5], $md5file);  #WTSI::NPG::iRODS::Path
    return;
}

sub _update_rt_ticket {
    my($self,$rt,$rts) = @_;

    my ($seen_rt);
        foreach my $r (@{$rts}){
           next if ref($r) eq 'HASH';
           if ($r == $rt){ $seen_rt = 1 ; $self->info(qq[rt_ticket $rt already present]);}
        }

    if (! $seen_rt){
      $self->info(qq[Adding rt_ticket $rt]);
      $self->irods->add_object_avu($self->icram,'rt_ticket',$rt);
    }
    return;
}

sub _run_reheader_cmd {
     my $self = shift;

     my $cmd = $self->samtools . q( reheader -i ) . $self->new_header_file . q( ) .
         (! $self->is_local ? $self->prefix : q[ ]). $self->icram;

     return $self->_run_acmd($cmd);
}

sub _check_existance {
    my($self,$file) = @_;

    if (not (-f $file || -z $file)){
        $self->logcroak(qq[File $file is zero or doesn't exist]);
    }
    return;
}


1;


__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose 

=item MooseX::StrictConstructor

=item WTSI::DNAP::Utilities::Loggable

=item Readonly

=item English

=item IO::File

=item Cwd

=item Log::Log4perl

=item st::api::lims

=item IPC::Open3

=item Try::Tiny

=item npg_pipeline::function::util

=item npg_tracking::illumina::runfolder

=item JSON

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
