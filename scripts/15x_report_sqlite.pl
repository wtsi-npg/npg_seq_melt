#!/usr/bin/env perl
use strict; 
use Number::Fraction;
use Number::Format ('format_number','round');
use Digest::MD5 qw(md5_hex); 
use File::Slurp; 
use List::MoreUtils qw(uniq); 
use List::Util qw(sum); 
use Text::CSV; 
use JSON::XS;
use JSON;
use DateTime::Format::Strptime;
use Getopt::Long;
use IO::File;
use Carp;
use DBI;
use Pod::Usage;
use Log::Log4perl qw[:levels];
#use WTSI::DNAP::Warehouse::Schema;
#use npg_tracking::glossary::rpt;#
#use npg_tracking::glossary::composition::factory;
#use npg_tracking::glossary::composition::component::illumina;

my $help;
my $tsv_file;
my $mlwh_report;
my $irods_json;
my $studyid_list;
my $verbose;
my $debug;
my $dbfile = q[];
my $userid = "";
my $password = "";
my $tsv_outfile = q[15x_report.tsv];
my $irods_env = q[~/.irods/irods_environment_i4.1.12.json];
my $mlwh_ro_file = q[~/.npg/mlwh_humgen_ro.json];
my $strp = DateTime::Format::Strptime->new(pattern => q(%FT%T));
our %ah;
our $irods_strp;
    $irods_strp = DateTime::Format::Strptime->new(pattern => q(%Y-%m-%dT%T));
my %code_versions;


GetOptions(
           'mlwh_report=s'     => \$tsv_file,
           'report_outfile=s'  => \$tsv_outfile,
           'dbfile=s'          => \$dbfile,
           'irods_json=s'      => \$irods_json,
           'studyid_list=s'    => \$studyid_list,
           'irods_env=s'       => \$irods_env,
           'verbose'           => \$verbose,
           'debug'             => \$debug,
           'mlwh_ro_file=s'    => \$mlwh_ro_file, #temp
           'help'              => \$help,
           );

if ($help) { pod2usage(0); }

if (! -e $dbfile){ carp q[Sqlite db should be specified with --dbfile] ; pod2usage(0) }
if (!$irods_json){
      if (!$studyid_list){ carp q[--studyid_list required if --irods_json not supplied] ; pod2usage(0) } 
}
if (! $tsv_file){
      if (!$studyid_list){ carp q[--studyid_list required if --mlwh_report not supplied] ; pod2usage(0) } 
}

    my $level = $debug ? $DEBUG : $verbose ? $INFO : $WARN;
    Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                              level  => $level,
                              utf8   => 1});

    my $log = Log::Log4perl->get_logger('main');
       $log->level($level);


my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile",$userid,$password,{AutoCommit=>1,RaiseError=>1,PrintError=>1}) or croak $DBI::errstr;
my $out_fh = IO::File->new($tsv_outfile,'>') or croak "cannot open $tsv_outfile\n";

my $json_fh;
if ($irods_json){
    $json_fh = IO::File->new( "xzcat $irods_json | jq -c '.[]' |") or croak "cannot open json file irods_json";
}
else {
    $json_fh = irods_query($studyid_list);
}

 while ( <$json_fh> ) {
       chomp;
       ### "timestamps":[{"created":"2013-12-14T22:18:50","replicates":0},{"modified":"2013-12-14T22:18:50","replicates":0},{"created":"2013-12-14T22:19:40","replicates":1},{"modified":"2013-12-14T22:19:40","replicates":1}],
       my $h = from_json($_); 
       
       my($datestamps) = sort { $b <=> $a }
                          map { $irods_strp->parse_datetime($_) }
                          map { $_->{modified} // $_->{created} } 
                          @{ $h->{timestamps}//[] }; 

       my %t; 
        foreach my $e( @{ $h->{q(avus)} } ){ 
                push @{$t {$e->{q(attribute)} } ||= [] },$e->{q(value)};
        } 

        while (my($k,$v)=each %t){
             $h->{ q(avh) }{$k}=@$v > 1 ? [ sort @$v ] : $v->[0] } delete $h->{ q(avus) }; 
             if ($h->{avh}{type} eq q(cram) and $h->{avh}{target}){
		    warn "No md5 meta data for $h->{data_object}\n" if ! $h->{avh}{md5};
                push @{ $ah{ $h->{avh}{study_id} }->{ $h->{avh}{sample_accession_number} }{ $h->{avh}{library_id} } ||= [] }, 
               [
                $h->{avh}{manual_qc},
                $h->{avh}{total_reads},
                $h->{collection}."/".$h->{data_object},
                $h->{avh}{target},
                $datestamps,
                $h->{avh}{md5}  #gets added as last column (required to check if file changed)
               ];
	      }
        }


my $tsv = Text::CSV->new({binary=>1, sep_char=>"\t",  eol => $/, quote_space => 0}) or die "CSV fail";
my $fh;

 if (! $tsv_file){ 
      $tsv_file  = query_mlwh($studyid_list);
  } 
      $fh  = IO::File->new($tsv_file, '<') or croak "cannot open input tsv file : $!\n"; 
 

 my $codev_file = $tsv_outfile;
    $codev_file =~ s/tsv/code_ver.tsv/;
 my $cv_tsv = Text::CSV->new({binary=>1, sep_char=>"\t",  eol => $/, quote_space => 0}) or die "CSV fail";
 my $codev_fh = IO::File->new($codev_file,'>') or croak "cannot open output code ver file $codev_file";

 
 while(my $row = $tsv->getline( $fh )){
       ##print out header row
       if ("name" eq $row->[1]){
           $tsv->print($out_fh, [@$row[0..15],qw(nominal_passing_lane_fraction last_lanelet_time merge_qc merge_reads merge_location merge_level merge_time passed_rmdup_q30 merged_nominal_passing_lane_fraction merge_ready code_version bwa_permutations)]);
       } else {
		process($row);
     }
}
 

sub get_passed_rmdup_q30{
    my($file)=@_;
    $file =~ s/\.cram$/_F0xB00.stats/ or return; 

    open(my$fh,q(-|),qq(iget $file -)) or return; 

    my$s; 
    my%d; 
    my $value;

    while(<$fh>){
         my @F=split/\s+/,$_; 
         if($F[0]=~m/^[FL]FQ/){ $s+=sum @F[(1+30) .. $#F ]}
         elsif(/^SN\s+bases (duplicated|mapped):\s(\d+)/){ $d{$1}=$2} }; 
         close $fh or return; 

         return $d{mapped} ? $s * ($d{mapped}-$d{duplicated})/$d{mapped}:q();
}

sub get_header {
     my($lf)=@_; 
     return unless $lf=~m{^/seq}; 

     open(my$fh,q(-|),qq(samtools view -H irods:$lf)); 

     my @l;
     my $header;
      while(<$fh>){
           next if /^\@SQ/;
           $header .= $_;
           
      }
      $header =~ s/\"//g;

     return $header;
}

sub pg {
    my $header = shift;
    my @header = ();
       @header = split/\n/,$header;

##@PG ID:bamsormadup  PN:bamsormadup  PP:bam12split VN:2.0.76 CL:/software/solexa/pkg/biobambam/2.0.76/bin/bamsormadup threads=12 SO=queryname level=0
     
     ###restrict to @RG and @PG and strip off prefix to commands up to 'pkg'
     my @l = map{s{(CL:)/\S+/(pkg/)}{$1$2}; $_}
              grep{/^\@[RP]G/} @header; 

     my %bwav; 
     my $bwa = join"\n",uniq sort grep {$_} 
             map{ s/^ID:bwa[^_]*(?:_(\d+))?\t//?$bwav{$1}++?():($_):($_) }
             sort map{ chomp;
                       my @F = split "\t";   ##split fields in one header row on tab
                       join "\t",(/bwa /?grep {/^ID:bwa/}@F:()),
                       join " ",
                       grep{ not m{\b(tmp|out)} and not m{/nfs/}}
                       map{ split } 
                       map{ s/ -t \d+\b//;            ##remove threads
                            s{/\S+(?=/references)}{}; ##remove reference path prefix
                            $_
                       }
                       grep{ m{/bwa } }
                       grep {/^CL:/} @F
                     } 
                       grep {/\bCL:[^\t]*Homo_sapiens/} @l; 


      ##selected useful @PG fields e.g. VN:1.14.9	PN:scramble	CL:pkg/scramble/1.14.9/bin/scramble -0 -I sam -O bam
      my $cmd_info= join"\n",uniq sort 
             map{ chomp;
                 my @F=split"\t"; 
                 join "\t",(grep {/^VN:/}@F),
                           (grep {/^PN:/}@F),
                 join " ",
                 grep{ not m{\S+\.((cr|b)am|newheader)\b} and not m{\d{5}_\d} and not m{(LANE|NAME|ALIAS|UNIT)=}i}
                 grep{ not m{\b(tmp|out)} and not m{/nfs/} }
                 grep{ not /threads=/}
                 map{/CL:uk.ac.sanger.npg.illumina/ ? split /\s+(?=\S+=)/: split}
                 map{ s/ -t \d+\b//; s{/\S+(?=/references)}{}; s{="[^"]+"}{=}g; $_}
                 grep {/^CL:/} @F } @l; 

       # read group e.g. 23885_6 or 25458_1#1,25458_2#1,25458_3#1,25458_4#1,25458_5#1,25458_6#1
       my $rg = join",",sort grep{$_}
                map{/^\@RG.*\tPU:\d{6}_\S+_(\d{4,5})_\S_\S{8,9}(_\S+)/?"$1$2":undef} @l; 
      
    return $rg,$cmd_info,$bwa;
};

sub calc_passing_lane_frac {
    my $all_lane_fractions = shift;  #e.g. 1/12,1/12
#map{ Number::Fraction->new( defined ? $_ : 0) }
         my ($r) =  
         map{ ref ? $_->to_num : $_ }
         sum
         map{ Number::Fraction->new( /NULL/ ? 0 : $_) }
         split/,/,$all_lane_fractions;

return($r);
}
 
sub process {
    my ($F) = @_; 
          my @F = @$F;
          my $library_id    = $F[4];
          my $rpts          = $F[14];
          my $lane_fractions = $F[13];
          my $nominal_passing_lane_fraction = calc_passing_lane_frac($F[13]); 
          my $study_id       = $F[0];
          my $study_col      = q[study].$study_id;
          my $study_name     = $F[1];
          my $accnum         = $F[2];

          ###my $composition_rpt = composition_string($rpts);  ##12345:1:40;12346:3:40 

###############   
          $log->logwarn("No sample accession number for $library_id study $study_id") if $accnum eq q[NULL];

          my @library_cram_info = @{ $ah{ $study_id }->{ $accnum }{ $library_id } || [] };

          #sort by manual qc (1 then 0) , target (1 or library), total_reads (largest first)
          my @c = sort { $b->[0]<=>$a->[0] or 
                         $a->[3]<=>$b->[3] or 
                         $b->[1]<=>$a->[1] } 
                  grep {
                       ($_->[3] eq q(library)) 
                       or do { my $re=join q(|), map{qr(\b\Q$_\E\b)} split /,/smx,$rpts; 
                  ## e.g. $re = (?^:\b23970_8\b)   for single cram, target = 1
                  ## e.g. $re = (?^:\b19822_1\#7\b)|(?^:\b19822_2\#7\b)|(?^:\b19859_1\#7\b)|(?^:\b19859_2\#7\b)|(?^:\b19860_1\#7\b)|(?^:\b19860_2\#7\b)   where field is 19822_1#7,19822_2#7,19859_1#7,19859_2#7,19860_1#7,19860_2#7
                  $_->[2]=~m/$re/smx  #irods collection
                             } 
	                } @library_cram_info;

        ## get most recent datestamp from component crams (target = 1)
           my ($last_datestamp) = sort { $b<=>$a } 
                       map { $_->[4] } 
                       grep { $_->[0] and ($_->[3] eq q(1)) } @c;

        ## e.g. /seq/23885/23885_5.cram or /seq/illumina/library_merge/20711165.H..
           my $cram_location = $c[0]->[2];
              #push @F,$last_datestamp,$c[0]->[0],$c[0]->[1],$cram_location,$c[0]->[3],$c[0]->[4];
               push @F,$last_datestamp,@{ $c[0] || [] };
           my ($md5) = map { $_->[5] } @c;

          unless ($md5){ $log->logwarn("No md5 in input (library $library_id)!") }
              ##e.g. 2017-10-04T14:51:18 21 2017-10-04T14:51:18
              #print "last_lanelet_time and merge_time ", @F[15,20], "\n"; 
              @F[15,20] = map{$ _ ? $strp->parse_datetime($_)->epoch : $_ } @F[15,20];

              our $ki; 
              my ($passed_rmdup_q30,$rg,$cmd_info,$bwa,$header);

              my $d = db_select($library_id,$study_col);
  
              if (defined $d){
		            if ($d->{MD5} eq $md5){
                  $log->info("Found library $library_id in db and md5 matches");

                  #TODO update database if missing rmdup_q30
                  if (! $d->{PASSED_RMDUP_Q30}){ $log->logwarn("Library $library_id has empty PASSED_RMDUP_Q30 field") }
                  $passed_rmdup_q30 = $d->{PASSED_RMDUP_Q30} ? $d->{PASSED_RMDUP_Q30} : get_passed_rmdup_q30($cram_location) // undef;
                  #$passed_rmdup_q30 = $d->{PASSED_RMDUP_Q30};
                  $rg               = $d->{READ_GROUP};
                  $header           = $d->{HEADER};
                  if ($cram_location  ne $d->{CRAM} ){ croak "Cram in database does not match for $study_col $cram_location\n" }
 
                  my $rg1;
                  ($rg1,$cmd_info,$bwa)=pg($header);
	              } else {
		              $log->logwarn("Found library $library_id in db; md5 does not match *$md5 vs $d->{MD5}");
                     $passed_rmdup_q30 = get_passed_rmdup_q30($cram_location) // undef;
                     my($header) = get_header($cram_location);
                     ($rg,$cmd_info,$bwa)=pg($header);
                     
                     ##update values if already present 
                     $log->info("UPDATE $study_col library $library_id");
                     my $stmt = qq(UPDATE $study_col 
                                  set PASSED_RMDUP_Q30 = "$passed_rmdup_q30", 
                                      READ_GROUP = "$rg",
                                      MD5 = "$md5", 
                                      HEADER = "$header",
                                      CRAM = "$cram_location" 
                                  where LIBRARY_ID = $library_id;);
                     my $rv = $dbh->do($stmt) or croak $DBI::errstr;

                    if( $rv < 0 ) {
                        print $DBI::errstr;
                    } else {
                      print "Total number of rows updated : $rv\n";
                    }
                 }
              }
              else {
                   $passed_rmdup_q30 = get_passed_rmdup_q30($cram_location) // undef;
                  my($header) = get_header($cram_location);
		                ($rg,$cmd_info,$bwa)=pg($header);

                  ##insert new row if not found
		  $log->info("INSERT INTO $study_col library $library_id ....");
                  my $stmt = qq(INSERT INTO $study_col (STUDY_NAME,LIBRARY_ID,PASSED_RMDUP_Q30,CRAM,READ_GROUP,MD5,HEADER)
                                VALUES ("$study_name",$library_id,"$passed_rmdup_q30","$cram_location","$rg","$md5","$header")
                               );
                   my $rv = $dbh->do($stmt) or croak $DBI::errstr;
      
	      }

              my %pbrg;
              @pbrg{ split/,/,$rpts } = split/,/,$lane_fractions;  ##$F[14] e.g. 25458_5#1,25458_6#1 $F[13] 1/11,1/11
              
              ## calculate fraction of lane sequenced e.g. 23885_5 = 1,  
              my($mnf)= map{ ref ? $_->to_num : $_ }
                        sum 
                        map{ Number::Fraction->new( defined ? $_ : 0) }
                        @pbrg{ split/,/,$rg }; 

              my @pc_rg = map{ m{^(\d)/};$1 } split/,/,$lane_fractions; #input e.g. 1/11,1/11
   

              ##check if rpt count matches lanelet count; 
              my $rpt_count = scalar split/,/,$rpts;
              my $merge_ready_status = (scalar @pc_rg eq $rpt_count);

              my ($code_ver) = $cmd_info ? $code_versions{$cmd_info} ||= ++$ki : q();
  
              ##sort these fields for display
              my $sorted_pc_dup = join q[,], sort { $a <=> $b } split/,/,$F[10];
              my $sorted_verify_bamid = join q[,], sort { $a <=> $b } split/,/,$F[11];
              my $formatted_passed_rmdup_q30 ='';
              if ($passed_rmdup_q30){
                 $formatted_passed_rmdup_q30 = format_number($passed_rmdup_q30,0); #add comma separators, 0 decimal places
              }
              else { $log->logwarn("No passed_rmdup_q30 for $library_id") }
  
              $tsv->print($out_fh, [@F[0..8],
                                    format_number($F[9],0),
                                    $sorted_pc_dup,
                                    $sorted_verify_bamid,
                                    @F[12..14],
                                    #round(($F[15] * 100),0) . q[%],
                                    round(($nominal_passing_lane_fraction * 100),0) . q[%],
                                   # @F[16..21],
                                    @F[15..20],
                                    $formatted_passed_rmdup_q30,
                                    round(($mnf * 100),0) . q[%],
                                    $merge_ready_status,
                                    $code_ver,
                                    $bwa]);
              

return 1;
}



#### code versions
    print "key count in \%code_versions=", scalar keys %code_versions,"\n";
    my @r;
    while(my($k,$v)=each %code_versions){ 
          push @r,[$v,$k]; 
    }  

    foreach my $cv (sort { $a->[0] <=> $b->[0] } @r ){
                  $cv_tsv->print($codev_fh,[@$cv]);
    }


sub db_select{
    my $library_id = shift;
    my $study = shift;

    my $stmt = qq(SELECT * FROM $study where library_id = $library_id);
    $log->info("$stmt");
    my $sth = $dbh->prepare( $stmt );
    my $rv = $sth->execute() or croak $DBI::errstr;
  
  if($rv < 0) {
     print $DBI::errstr;
   }

    my $Hr = $sth->fetchrow_hashref();
 
    return $Hr;
}

sub check_study_table_exists{
    my $study_col = shift;
    my $stmt = qq(SELECT name FROM sqlite_master WHERE type='table' AND name=$study_col);
    my $sth = $dbh->prepare($stmt);
    my $rv = $sth->execute() or die $DBI::errstr;
    my @row_ary  = $sth->fetchrow_array;
    return $row_ary[0]; #table name if it exists  
}

sub create_table {
    my $study_col = shift;
    
    my $stmt = qq(CREATE TABLE $study_col
                   (STUDY_NAME CHAR(255)  NULL,
                    LIBRARY_ID INT PRIMARY KEY NOT NULL,
                    PASSED_RMDUP_Q30 CHAR(50) NULL,
                    CRAM CHAR(120) NOT NULL,
                    READ_GROUP CHAR(255) NOT NULL,
                    MD5 CHAR(50) NOT NULL,
                    HEADER TEXT NOT NULL);
                 ); 

     my $rv = $dbh->do($stmt);
if($rv < 0) {
   print $DBI::errstr;
   return 0;
} else {
   print "Table $study_col created successfully\n";
   return 1;
}


}



sub composition_string {
    my $rpt_string = shift;

    my $factory = npg_tracking::glossary::composition::factory->new();
    
    $rpt_string =~ s/[_#]/:/g;
    $rpt_string = npg_tracking::glossary::rpt->join_rpts(split/,/,$rpt_string);
    my $rpts = npg_tracking::glossary::rpt->inflate_rpts($rpt_string);

    foreach my $rpt (@$rpts){
            my $c = 'npg_tracking::glossary::composition::component::illumina';
            my $component = $c->new($rpt);
	    $factory->add_component($component);
    }

    my $composition = $factory->create_composition();
    return($composition->freeze2rpt);
    
}

sub irods_query{
     my $file = shift;    
     my $study_ids = read_file( $file );
        $study_ids =~ s/\n/,/g;
        $study_ids =~ s/,$//g;
        $study_ids =~ s/(\d+)/\"$1\"/g; ##need to add double quotes around each study id 
        

     my $cmd = qq[ (. /software/npg/etc/profile.npg; which baton-metaquery >&2;  export IRODS_ENVIRONMENT_FILE=$irods_env ; echo '{"avus":[{"operator":"in","attribute":"target","value":["1","library"]},{"operator":"in","attribute":"study_id","value":[$study_ids]}]}' |time baton-metaquery -z seq --timestamp --avu --unbuffered) | jq -c '.[]'];

     $log->info($cmd);
     my $json_fh = IO::File->new("$cmd |") or croak "cannot run metaquery command $!";
     return $json_fh;
}

sub query_mlwh{
    my $file = shift;
    my $study_ids = read_file( $file );
        $study_ids =~ s/\n/,/g;
        $study_ids =~ s/,$//g;    

    my $tsv_file = qq[study_query_out.$$.tsv];
    my $tsv = Text::CSV->new({binary=>1, sep_char=>"\t",  eol => $/, quote_space => 0, empty_is_undef => 1 }) or die "CSV fail";
    my $fh = IO::File->new($tsv_file, '>') or croak "cannot open output tsv file : $!\n"; 
    my $sql = multi_lims_wh_sql($study_ids);#2808,2809,2953 etc
    #my $wh=WTSI::DNAP::Warehouse::Schema->connect();

     my $Hr = from_json(read_file(glob $mlwh_ro_file));
     my $wh_dbh = DBI->connect($Hr->{'dsn'},$Hr->{dbuser},$Hr->{dbpass},{RaiseError => 1,AutoCommit => 0,});
    
    my $result = $wh_dbh->prepare($sql) or croak "Cannot prepare query :" . $wh_dbh->errstr;
       $result->execute or croak "Cannot execute query :" . $wh_dbh->errstr;

       $tsv->print( $fh , $result->{NAME_lc} );
         while (my $row = $result->fetchrow_arrayref){
                  #my @row =  map { (defined ? $_ : 0) } @$row; #Number::Fraction converts NULL to 0, so convert undefined to 0
                  my @row =  map { (defined ? $_ : q[NULL]) } @$row;
                  $tsv->print( $fh , \@row );
         }

     $wh_dbh->disconnect;

return($tsv_file);
}

sub multi_lims_wh_sql{
    my $study_ids = shift;

my $sql = qq{
SELECT   study.id_study_lims,
         study.name,
         sample.accession_number,
         pipeline_id_lims                                                                                                                                                                        library_type,
         Coalesce(legacy_library_id,id_library_lims)                                                                                                                                             library_id,
          Group_concat( DISTINCT IF(Locate(':',id_pool_lims), Concat(Substring_index(id_pool_lims,':',1),':',Substring_index(id_pool_lims,':',-1)),id_pool_lims) ORDER BY id_iseq_pr_metrics_tmp) tube,
         Group_concat( IF(Locate(':',id_pool_lims),Substring_index(Substring_index(id_pool_lims,':',2),':',-1),'') ORDER BY id_iseq_pr_metrics_tmp)                                              strip_tube_rev,
         Sum(qc)                                                                                                                                                                                 npass,
         Count(*)                                                                                                                                                                                n,
         Sum(qc *(iseq_product_metrics.q30_yield_kb_forward_read + iseq_product_metrics.q30_yield_kb_reverse_read)*(100.0 - percent_duplicate)/100.0)                                            passed_rmdup_q30,
         Group_concat(percent_duplicate ORDER BY id_iseq_pr_metrics_tmp),
         Group_concat(verify_bam_id_score ORDER BY id_iseq_pr_metrics_tmp),
         group_concat(verify_bam_id_average_depth ORDER BY id_iseq_pr_metrics_tmp),
         group_concat(concat(coalesce(qc,0),'/',lane.nplex) ORDER BY id_iseq_pr_metrics_tmp),
         group_concat(concat(iseq_product_metrics.id_run,'_',iseq_product_metrics.position,coalesce(concat('#',iseq_product_metrics.tag_index),'')) ORDER BY id_iseq_pr_metrics_tmp)
FROM     iseq_product_metrics
JOIN     iseq_flowcell
USING   (id_iseq_flowcell_tmp)
JOIN     sample
USING   (id_sample_tmp)
JOIN     study
USING   (id_study_tmp)
JOIN
         (
                  SELECT   id_run,
                           iseq_product_metrics.position,
                           count(*) nplex
                  FROM     iseq_product_metrics
                  JOIN     iseq_flowcell
                  USING   (id_iseq_flowcell_tmp)
                  WHERE    entity_type != 'library_indexed_spike'
                  GROUP BY id_run,
                           iseq_product_metrics.position) lane
ON       lane.position=iseq_product_metrics.position
AND      lane.id_run=iseq_product_metrics.id_run
JOIN     iseq_run_lane_metrics
ON       iseq_run_lane_metrics.position = iseq_product_metrics.position
AND      iseq_run_lane_metrics.id_run = iseq_product_metrics.id_run
WHERE    study.id_study_lims IN ($study_ids)
AND      sample.consent_withdrawn=0
AND      iseq_run_lane_metrics.instrument_model != 'MiSeq'
AND      pipeline_id_lims != 'Chromium genome'
GROUP BY study.name,
         sample.name,
         pipeline_id_lims,
         coalesce(legacy_library_id,id_library_lims)
ORDER BY study.name,
         sample.name,
         pipeline_id_lims,
         coalesce(legacy_library_id,id_library_lims)
};

return($sql);
}

exit 0;

=head1 NAME

15x_report_sqlite.pl

=head1 DESCRIPTION

Query mlwarehouse and iRODS to generate tsv report of information related to library merged status from a list of id_study_lims.

=head1 USAGE

./15x_report_sqlite.pl --studyid_list <file> --dbfile <file path>

=head1 REQUIRED ARGUMENTS

--studyid_list   list of id_study_lims

--dbfile         path to sqlite db   e.g. /path/headers.db

=head1 OPTIONS

--help               brief help message

--verbose

--ml_pwd             password for mlwh_humgen

--mlwh_report        e.g. From sql query, generated if not supplied

--irods_json         e.g. 15X_lib.json.xz  Generated if not supplied

--irods_env          default ~/.irods/irods_environment_i4.1.12.json

--report_outfile     default 15x_report.tsv

=head1 EXIT STATUS

0

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item strict

=item Number::Fraction

=item Number::Format

=item Digest::MD5

=item File::Slurp

=item List::MoreUtils

=item List::Util

=item Text::CSV

=item JSON::XS

=item JSON

=item DateTime::Format::Strptime

=item Getopt::Long

=item IO::File

=item Carp

=item DBI

=item Pod::Usage

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

David Jackson, Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 by Genome Research Limited

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

=cut

