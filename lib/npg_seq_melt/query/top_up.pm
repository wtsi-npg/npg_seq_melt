package npg_seq_melt::query::top_up;
#BEGIN { $ENV{DBIC_TRACE} = 1 }  
use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Readonly;
use Carp;
use File::Path qw/ make_path /;
use npg_pipeline::product;
use st::api::lims;
use Moose::Meta::Class;  ###create_anon_class
use English qw(-no_match_vars);
use File::Slurp;



with qw{npg_tracking::glossary::rpt
        MooseX::Getopt
        npg_pipeline::product::cache_merge
       };



our $VERSION = '0';

=head1 NAME

npg_seq_melt::query::top_up

=head1 SYNOPSIS

 npg_set_melt::query::top_up(id_study_lims  => 5392, dry_run => 1)->run_query();

 Returns an arrayref of individual records ready for top up merging

=head1 SUBROUTINES/METHODS

=head2 id_study_lims 

Integer id_study_lims, required

=cut

has 'id_study_lims'     => ( isa  => 'Str',
                             is          => 'ro',
                             required    => 1,
                             documentation => q[],
);

=head2 mlwh_schema
 
DBIx schema class for ml_warehouse access.

=cut

has q{mlwh_schema} => (
                isa        => q{WTSI::DNAP::Warehouse::Schema},
                is         => q{ro},
                required   => 0,
                lazy_build => 1,);
sub _build_mlwh_schema {
  require WTSI::DNAP::Warehouse::Schema;
  return WTSI::DNAP::Warehouse::Schema->connect();
}

=head2 dry_run

=cut

has 'dry_run'      => ( isa           => 'Bool',
                        is            => 'ro',
                        default       => 0,
                        documentation =>
                        'Boolean flag, false by default. ' .
                        'no directories created',
);


=head2 path_prefix

=cut

has 'path_prefix' => ( isa           => 'Str',
                       is            => 'ro',
                       default       => q[/lustre/scratch113],
                       documentation => '',
    );


=head2 _cache_name

e.g.

/lustre/scratch113/merge_component_cache/5392/2e/e6/2ee6e9a843a8f65b3d569cdc522087c51010fb81bfce638514aeec15232d61f2

=cut   
                   
has '_cache_name' => ( isa           => 'Str',
                       is            => 'rw',
                       documentation => '',
                    
    );

=head2 _cram_filename

=cut

has '_cram_filename' => ( isa           => 'Str',
                          is            => 'rw',
                          documentation => '',
    );

=head2 _product

=cut

has '_product'       => (isa           => q[npg_pipeline::product],
                         is            => 'rw',
                         documentation => '',
    );

=head 2 data

=cut

has 'data' => ( isa           => 'ArrayRef',
                is            => 'rw',
                documentation => 'Various data for top up merges',
    );

sub run_query {
    my $self = shift;


    my $input_data_product     = {};
    my $single_component_dp    = {};
    my %multi_component_run_id = ();
    my %top_up_run_library     = ();
    my @data                   = ();

   ###id_iseq_flowcell_tmp, id_run,position,tag_index,iseq_composition_tmp,qc_seq,qc_lib,qc
    my $p = $self->mlwh_schema->resultset(q[IseqProductMetric]);

    ## Get id_study_tmp
    my @study_rs = $self->mlwh_schema->resultset(q[Study])->search({id_study_lims => $self->id_study_lims });
print $study_rs[0]->id_study_tmp,"\n";
    my $ipm_rs = $p->search({'iseq_flowcell.id_study_tmp' => $study_rs[0]->id_study_tmp},{'join' => [qw/iseq_run_lane_metric iseq_flowcell/]});

    ## Input data product : sequence pass and library unset and which share the same run id and are part of a related composition (via iseq_product_components table) of more than 1 component

    while (my $prow = $ipm_rs->next()) {
	
         ##skip those unless qc_lib is undefined (NULL)
         next if (defined $prow->qc_lib);
         next if ! $prow->qc ; # needs to be qc pass

          my $irlm_row = $prow->iseq_run_lane_metric;
          next if ! $irlm_row->qc_complete;

          my $fc_row = $prow->iseq_flowcell;
          next if $fc_row->is_r_and_d;
          next if $fc_row->sample_consent_withdrawn;

	 my $rpt = $self->deflate_rpt({id_run=>$prow->id_run,position=>$prow->position,tag_index=>$prow->tag_index});
	 
          my @c =  $prow->iseq_product_components();

         foreach my $c (@c){
             ### input data products
             if ($c->num_components > 1){ 
                  $input_data_product->{$c->id_iseq_pr_tmp }{rpt_list} .= $rpt.';';
                  $input_data_product->{$c->id_iseq_pr_tmp}->{library} = $fc_row->legacy_library_id;
                  $input_data_product->{$c->id_iseq_pr_tmp }{sample_supplier_name} = $fc_row->sample_supplier_name;
                  $multi_component_run_id{$prow->id_run}++;
             }
             else { 
                  $single_component_dp->{$c->id_iseq_pr_tmp }{rpt_list} .= $rpt;
                  $single_component_dp->{$c->id_iseq_pr_tmp }{id_run} = $prow->id_run;
                  $single_component_dp->{$c->id_iseq_pr_tmp }{library} = $fc_row->legacy_library_id; 
                   
              };
          }
    }

foreach my $single (keys %$single_component_dp){

    ##skip any where the id_run also exists in a multi-component data product (i.e. it is not a top-up run) 
    next if exists $multi_component_run_id{ $single_component_dp->{$single}{id_run} };
    $top_up_run_library{ $single_component_dp->{$single}{library} } = $single_component_dp->{$single}{rpt_list};
}


foreach my $comp (keys %$input_data_product){
        my $record = {};

           $record->{library} = $input_data_product->{$comp}{library};
           $record->{supplier_sample} = $input_data_product->{$comp}{sample_supplier_name};

        #### This library has a top-up run
       if (exists $top_up_run_library{ $input_data_product->{$comp}{library} }){

           my $rpt_list = $input_data_product->{$comp}{rpt_list};
              $self->make_merge_dir($rpt_list);

              $record->{orig_cram} = join q[/],$self->path_prefix,$self->_cache_name(),$self->_cram_filename();

           my $extended_rpt_list;
           my $top_up_rpt =  $top_up_run_library{ $input_data_product->{$comp}{library} };
              $extended_rpt_list = $rpt_list . $top_up_rpt;
              $self->make_merge_dir($extended_rpt_list);
           my ($results_cache_name) =join q[/],$self->_cache_name();
               $results_cache_name =~ s/cache/results/;
       
	            $record->{results_cache_name} = $results_cache_name; 
	            $record->{composition_id}     = $self->_product->file_name_root();
	            $record->{extended_rpt_list}  = $extended_rpt_list;
	

           if (! $self->dry_run){
               # write composition.json to output dir
               croak if ! $self->run_make_path(qq[$results_cache_name/qc]);
               croak if ! $self->run_make_path(qq[$results_cache_name/log]);
           
             write_file( $self->_product->file_path($results_cache_name,ext => 'composition.json'), 
                         $self->_product->composition->freeze(with_class_names => 1) );
           }


         ### top-up input cram  
             $self->make_merge_dir($top_up_rpt);
            ## /lustre/scratch113/merge_component_cache/5392/2e/e6/2ee6e9a843a8f65b3d569cdc522087c51010fb81bfce638514aeec15232d61f2
            ## 28780_2#6.cram
	          $record->{top_up_cram} = join q[/],$self->path_prefix,$self->_cache_name(),$self->_cram_filename();

	   push @data,$record;

            }
     }
    
    $self->data(\@data);

    return;
}

sub run_make_path {    ###same as in library.pm
    my $self = shift;
    my $path = shift;

    if (! -d $path ) {
       eval {
       make_path($path) or croak qq[cannot make_path $path: $CHILD_ERROR];
       1;
             } or do { 
      carp qq[cannot make_path $path: $EVAL_ERROR];
      return 0;
       };
     }
     return 1;
}


sub make_merge_dir {
    my $self = shift;
    my $rpt = shift;
    ### uses config file product_release.yml
    my $p = npg_pipeline::product->new(rpt_list => $rpt, lims => st::api::lims->new(rpt_list => $rpt));

    my $filename = $p->file_name(ext =>'cram');

    $self->_cache_name($self->merge_component_cache_dir($p));
    $self->_cram_filename($filename);
    $self->_product($p);
return 1;
}



__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DESCRIPTION

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Moose

=item MooseX::StrictConstructor

=item Moose::Meta::Class

=item namespace::autoclean

=item Readonly

=item WTSI::DNAP::Warehouse::Schema

=item npg_pipeline::product

=item st::api::lims

=item English

=item npg_tracking::glossary::rpt

=item File::Slurp

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 Genome Research Ltd

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

