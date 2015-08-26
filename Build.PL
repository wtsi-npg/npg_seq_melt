
use strict;
use warnings;

# These paths are propagated to @INC in the build script. 

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
                          'English'               => '>= 1.02',
                          'File::Copy'            => '>= 0',
                          'File::Path'            => '>= 0',
                          'File::Temp'            => '>= 0',
                          'Log::Log4perl'         => '>= 0',
                          'Test::More'            => '>= 0.8',
                          'Test::Exception'       => '>= 0',
                          'Test::Deep'            => '>= 0',
                          'Test::Pod::Coverage'   => '>= 1.04',
                          'WTSI::NPG::iRODS'      => '>=1.6.0',
                         },
   requires =>           {
                          'Carp'                  => '>= 1.04',
                          'Cwd'                   => '>=0',
                          'DateTime'              => '>= 1.18',
                          'DateTime::Duration'    => '>= 0',
                          'Digest::MD5'           => '>= 0',
                          'Digest::SHA'           => '>= 0',
                          'English'               => '>= 1.02',
                          'File::Basename'        => '>= 0',
                          'File::Copy'            => '>= 0',
                          'File::Path'            => '>=0',
                          'File::Slurp'           => '>=0',
                          'File::Spec'            => '>=0',
                          'FindBin'               => '>= 0',
                          'IO::File'              => '>=0',
                          'List::MoreUtils'       => '>= 0',
                          'Log::Log4perl'         => '>= 1.46',
                          'Moose'                 => '>= 2.1',
                          'Moose::Meta::Class'    => '>= 0',
                          'MooseX::StrictConstructor' => '>= 0',
                          'MooseX::Getopt'        => '0.39',
                          'npg_tracking::data::reference' => '>= 0',
                          'npg_common::roles::log' => '>= 0',
                          'npg_common::roles::software_location' => '>= 0',
                          'npg_common::irods::iRODSCapable' => '>= 0',
                          'npg_qc::autoqc::role::rpt_key' => '>= 0',
                          'Readonly'              => '>= 1.04',
                          'srpipe::runfolder'     => '>= 0',
                          'WTSI::DNAP::Warehouse::Schema' => '>0',
                          'WTSI::DNAP::Warehouse::Schema::Query::LibraryDigest' => '>0',
                          'WTSI::NPG::iRODS'      => '>=1.6.0',

                         },
   recommends =>         {
                         });

$build->create_build_script;
