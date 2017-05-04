!/usr/bin/perl

use strict;
use warnings;

use Time::Local;
use DBI;

my $date=`date +%Y%m%d`;
chomp($date);
my $report_file="Files_Transfered" . $date . ".csv";
open(my $fh, '>', $report_file);
my $driver   = "Pg";
our $DBNAME = "****";
our $HPSSTAB = "gridftp.hpss_raw";
our $IETAB   = "gridftp.ie_raw";
our $HOST = "****";
our $USER = "****";
our $PASS = "****";
my $dsn = "DBI:$driver:dbname = $DBNAME;host = $HOST";
my $dbh = DBI->connect($dsn, $USER, $PASS, { RaiseError => 1 })
   or die $DBI::errstr;
print "Opened database successfully\n";
my $stmt = qq( select day, count(*) as No_files_transfer from (select date_trunc('day' , start_time) as day from gridftp.hpss_2016 where date_trunc('day' , start_time) =  date_trunc('day' , end_time)) as foo group by day;);
my $sth = $dbh->prepare( $stmt );
my $rv = $sth->execute() or die $DBI::errstr;
if($rv < 0) {
   print $DBI::errstr;
}

print $fh "Date,File_Count";
while(my @row = $sth->fetchrow_array()) {
      print $fh "\n".$row[0] . ",".$row[1];
}
print "Report($report_file) generated successfully\n";
close $fh;
$dbh->disconnect();
