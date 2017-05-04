#!/usr/bin/perl

use strict;
use warnings;

use Time::Local;
use DBI;

my $date=`date +%Y%m%d`;
chomp($date);
my $report_file="Transfer_rate" . $date . ".csv";
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
my $stmt = qq(select username, server,cast(total_bytes as decimal)/cast(total_elapsed as decimal) as transfer_rate from (select username, server, sum(bytes) as total_bytes, sum(elapsed) as total_elapsed from gridftp.hpss_2016 group by username, server) as foo order by username,server;);
my $sth = $dbh->prepare( $stmt );
my $rv = $sth->execute() or die $DBI::errstr;
if($rv < 0) {
   print $DBI::errstr;
}

print $fh "username,server,transfer_rate";
while(my @row = $sth->fetchrow_array()) {
      print $fh "\n".$row[0] . ",".$row[1];
}
print "Report($report_file) generated successfully\n";
close $fh;
$dbh->disconnect();
