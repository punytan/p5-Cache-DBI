use strict;
use warnings;
use Test::More;
use Test::mysqld;

use Cache::DBI;
use DBI;
use DBIx::Connector;

$ENV{PATH} .= ':/usr/sbin';

my $mysqld = Test::mysqld->new(
    my_cnf => { 'skip-networking' => '' }
) or plan skip_all => $Test::mysqld::errstr;

{
    my $cache = Cache::DBI->new(
        connector => DBIx::Connector->new($mysqld->dsn),
    );

    isa_ok $cache, 'Cache::DBI';
}

done_testing;
