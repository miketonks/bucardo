#!/usr/bin/env perl
# -*-mode:cperl; indent-tabs-mode: nil-*-

## Test bucardo_delta and bucardo_track table tasks

use 5.008003;
use strict;
use warnings;
use Data::Dumper;
use lib 't','.';
use DBD::Pg;
use Test::More;
use MIME::Base64;

use vars qw/ $bct $dbhX $dbhA $dbhB $dbhC $res $command $t $SQL %pkey %sth %sql $sth $count $row/;

use BucardoTesting;
$bct = BucardoTesting->new() or BAIL_OUT "Creation of BucardoTesting object failed\n";
$location = '';

my $numtabletypes = keys %tabletype;
my $numsequences = keys %sequences;
plan tests => 164;

pass("*** Beginning sync stats tests");

END {
    $bct and $bct->stop_bucardo($dbhX);
    $dbhX and  $dbhX->disconnect();
    $dbhA and $dbhA->disconnect();
    $dbhB and $dbhB->disconnect();
    $dbhC and $dbhC->disconnect();
}

## Get Postgres databases A, B, and C created
$dbhA = $bct->repopulate_cluster('A');
$dbhB = $bct->repopulate_cluster('B');
$dbhC = $bct->repopulate_cluster('C');

## Create a bucardo database, and install Bucardo into it
$dbhX = $bct->setup_bucardo('A');

## Tell Bucardo about these databases (one source and two targets)
for my $name (qw/ A B C /) {
    $t = "Adding database from cluster $name works";
    my ($dbuser,$dbport,$dbhost) = $bct->add_db_args($name);
    $command = "bucardo add db $name dbname=bucardo_test user=$dbuser port=$dbport host=$dbhost";
    $res = $bct->ctl($command);
    like ($res, qr/Added database "$name"/, $t);
}

## Put all pk tables into a relgroup
$t = q{Adding all PK tables on the master works};
$res = $bct->ctl(q{bucardo add tables '*bucardo*test*' '*Bucardo*test*' db=A relgroup=trelgroup pkonly});
like ($res, qr/Created the relgroup named "trelgroup".*are now part of/s, $t);

## Add all sequences, and add them to the newly created relgroup
$t = q{Adding all sequences on the master works};
$res = $bct->ctl("bucardo add all sequences relgroup=trelgroup");
like ($res, qr/New sequences added: \d/, $t);

## Create a new database group going from A to B and C
$t = q{Created a new database group};
$res = $bct->ctl(q{ bucardo add dbgroup pg A:source B:target C:target });
like ($res, qr/Created database group "pg"/, $t);

## Create a new sync
$t = q{Created a new sync};
$res = $bct->ctl(q{ bucardo add sync dtest relgroup=trelgroup dbs=pg autokick=false });
like ($res, qr/Added sync "dtest"/, $t);

## Start up Bucardo with this new sync
$bct->restart_bucardo($dbhX);

## Add a row to A
$bct->add_row_to_database('A', 1);

## Kick off the sync
$bct->ctl('bucardo kick dtest 0');

## All rows should be on A, B, and C
my $expected = [[1]];
$bct->check_for_row($expected, [qw/A B C/]);

## The sync row should now be present
$t = "The syncrun table contains 1 entry post sync";
$SQL = qq{SELECT * from bucardo.syncrun};
$count = $dbhX->do($SQL);
is ($count, 1, $t);

$SQL = qq{SELECT * from bucardo.syncrun order by started desc limit 1};
$row = $dbhX->selectrow_hashref($SQL);
is($row->{sync}, 'dtest', 'Sync name is correct');
is($row->{truncates}, 0,  'stats: truncates is correct');
is($row->{deletes},   0,  'stats: deletes is correct');
is($row->{inserts},   22, 'stats: inserts correct');
is($row->{conflicts}, 0,  'stats: conflicts is correct');

## Create a doubled up entry in the delta table (two with same timestamp and pk)
$bct->add_row_to_database('A', 22, 0);
$bct->add_row_to_database('A', 28, 0);
$dbhA->commit();

## Kick it off
$bct->ctl('bucardo kick dtest 0');

## Run the purge program
#$bct->ctl('bucardo purge');

## All rows should be on A, B, and C
$expected = [[1], [22], [28]];
$bct->check_for_row($expected, [qw/A B C/]);

## The sync row should now be present
$t = "The syncrun table contains 2 entries post sync";
$SQL = qq{SELECT * from bucardo.syncrun};
$count = $dbhX->do($SQL);
is ($count, 2, $t);

$SQL = qq{SELECT * from bucardo.syncrun order by started desc limit 1};
$row = $dbhX->selectrow_hashref($SQL);
is($row->{sync}, 'dtest', 'Sync name is correct');
is($row->{truncates}, 0,  'stats: truncates is correct');
is($row->{deletes},   0,  'stats: deletes is correct');
is($row->{inserts},   44, 'stats: inserts correct');
is($row->{conflicts}, 0,  'stats: conflicts is correct');


## Delete a row from A
$bct->remove_row_from_database('A', 1);

## Kick off the sync
$bct->ctl('bucardo kick dtest 0');

## All rows should be on A, B, and C
$expected = [[22], [28]];
$bct->check_for_row($expected, [qw/A B C/]);

## The sync row should now be present
$t = "The syncrun table contains 3 entries post sync";
$SQL = qq{SELECT * from bucardo.syncrun};
$count = $dbhX->do($SQL);
is ($count, 3, $t);

$SQL = qq{SELECT * from bucardo.syncrun order by started desc limit 1};
$row = $dbhX->selectrow_hashref($SQL);
is($row->{sync}, 'dtest', 'Sync name is correct');
is($row->{truncates}, 0,  'stats: truncates is correct');
is($row->{deletes},   22,  'stats: deletes is correct');
is($row->{inserts},   0, 'stats: inserts correct');
is($row->{conflicts}, 0,  'stats: conflicts is correct');

## Create a doubled up entry in the delta table (two with same timestamp and pk)
$bct->remove_row_from_database('A', 22, 0);
$bct->remove_row_from_database('A', 28, 0);
$dbhA->commit();

## Kick it off
$bct->ctl('bucardo kick dtest 0');

## All rows should be on A, B, and C
$expected = [];
$bct->check_for_row($expected, [qw/A B C/]);

## The sync row should now be present
$t = "The syncrun table contains 4 entries post sync";
$SQL = qq{SELECT * from bucardo.syncrun};
$count = $dbhX->do($SQL);
is ($count, 4, $t);

$SQL = qq{SELECT * from bucardo.syncrun order by started desc limit 1};
$row = $dbhX->selectrow_hashref($SQL);
is($row->{sync}, 'dtest', 'Sync name is correct');
is($row->{truncates}, 0,  'stats: truncates is correct');
is($row->{deletes},   44,  'stats: deletes is correct');
is($row->{inserts},   0, 'stats: inserts correct');
is($row->{conflicts}, 0,  'stats: conflicts is correct');

exit;
