#!/usr/bin/perl

use strict;
use warnings;

use Time::Local;
use DBI;

my $num_args = $#ARGV + 1;
if ($num_args != 2) {
    print "\nUsage: test.pl bucket_start_date bucket_end_date\n";
    exit;
}

# (2) we got two command line args, so assume they are the
# bucket start and bucket end
my $bucket_start_date=$ARGV[0];
my $bucket_end_date=$ARGV[1];

print "$bucket_start_date $bucket_end_date";

my $date=`date +%Y%m%d`;
chomp($date);
my $report_file="report_" . $date . ".csv";
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

my $stmt = qq( select username, cmd, cast(sum(bytes) as decimal)/cast(sum(elapsed) as decimal) as avg_transfer_rate, count(*) as No_File_transfers from gridftp.hpss_2016 where (start_time BETWEEN to_date('$bucket_start_date','YYYY-MM-DD') AND to_date('$bucket_end_date','YYYY-MM-DD')) AND (end_time BETWEEN to_date('$bucket_start_date','YYYY-MM-DD') AND to_date('$bucket_end_date','YYYY-MM-DD')) group by username, cmd;);

print "$stmt";
my $sth = $dbh->prepare( $stmt );
my $rv = $sth->execute() or die $DBI::errstr;
if($rv < 0) {
   print $DBI::errstr;
}

print $fh "User_Name,cmd,Transfer_rate,file_count";
while(my @row = $sth->fetchrow_array()) {
      print $fh "\n".$row[0] . ",".$row[1]. ",".$row[2]. ",".$row[3];
}
print "Report($report_file) generated successfully\n";
close $fh;
$dbh->disconnect();
