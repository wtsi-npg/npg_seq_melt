use strict;
use warnings;

use WTSI::DNAP::Utilities::Build;

my $build = WTSI::DNAP::Utilities::Build->new
  (module_name        => 'npg_seq_melt',
   dist_author        => ['wtsi-npg <npg@sanger.ac.uk>'],
   dist_abstract      => 'Operations on processed sequencing data',
   dist_version       => WTSI::DNAP::Utilities::Build->report_version,
   license            => 'gpl',
   configure_requires => {
                          'Module::Build'         => '>= 0.42'
                         },
   test_requires      => {
                          'Data::Dumper'                 => '>= 0',
                          'File::Temp'                   => '>= 0',
                          'Test::More'                   => '>= 0.8',
                          'Test::Distribution'           => '>= 0',
                          'Test::Exception'              => '>= 0',
                          'Test::Pod'                    => '>= 1.0',
                          'Test::Pod::Coverage'          => '>= 1.04',
                          'Test::Perl::Critic'           => '>= 0',
                          'npg_tracking::util::abs_path' => '>= 0',
                         },
   requires =>           {
                          'Archive::Tar'          => '>=0',
                          'Carp'                  => '>= 1.04',
                          'Cwd'                   => '>=0',
                          'DateTime'              => '>= 1.18',
                          'DateTime::Duration'    => '>= 0',
                          'English'               => '>= 1.02',
                          'File::Basename'        => '>= 0',
                          'File::Copy'            => '>= 0',
                          'File::Path'            => '>=0',
                          'File::Slurp'           => '>=0',
                          'File::Spec'            => '>=0',
                          'FindBin'               => '>= 0',
                          'IO::File'              => '>=0',
                          'lib'                   => '>=0',
                          'List::MoreUtils'       => '>= 0',
                          'Log::Log4perl'         => '>= 1.46',
                          'Moose'                 => '>= 2.1',
                          'Moose::Role'           => '>= 0',
                          'Moose::Meta::Class'    => '>= 0',
                          'MooseX::StrictConstructor' => '>= 0',
                          'MooseX::Getopt'        => '0.39',
                          'npg_tracking::data::reference' => '>= 0',
                          'npg_tracking::glossary::rpt'   => '>= 0',
                          'npg_tracking::glossary::composition::factory' => '>= 0',
                          'npg_tracking::glossary::composition::component::illumina' => '>= 0',
                          'npg_common::roles::log' => '>= 0',
                          'npg_common::roles::software_location' => '>= 0',
                          'npg_pipeline::roles::business::base' => '>= 0',
                          'npg_qc::autoqc::role::rpt_key' => '>= 0',
                          'POSIX'                 => '>= 0',
                          'Readonly'              => '>= 1.04',
                          'srpipe::runfolder'     => '>= 0',
                          'strict'                => '>= 0',
                          'st::api::lims'         => '>= 0',
                          'Try::Tiny'             => '>= 0.12',
                          'warnings'              => '>= 0',
                          'WTSI::DNAP::Utilities' => '>= 0.5.2',   # perl-dnap-utilities
                          'WTSI::DNAP::Warehouse::Schema' => '>0',
                          'WTSI::DNAP::Warehouse::Schema::Query::LibraryDigest' => '>0',
                          'WTSI::NPG::iRODS' => '>=2.5.0', # perl-irods-wrap
                          'WTSI::NPG::iRODS::DataObject' => '>=2.5.0',
                          'WTSI::NPG::iRODS::Publisher'  => '>=2.5.0',
                         },
   recommends =>         {
                         });

$build->create_build_script;
