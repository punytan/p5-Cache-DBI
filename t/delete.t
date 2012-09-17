use strict;
use warnings;
use Test::More;
use Test::mysqld;

use Data::MessagePack;
use Cache::DBI;
use DBIx::Connector;

$ENV{PATH} .= ':/usr/sbin';

my $mysqld = Test::mysqld->new(
    my_cnf => { 'skip-networking' => '' }
) or plan skip_all => $Test::mysqld::errstr;

my $conn = DBIx::Connector->new(
    $mysqld->dsn, undef, undef, {
        AutoCommit => 0,
        RaiseError => 1,
    }
);

$conn->txn(
    fixup => sub {
        my $dbh = shift;

        $dbh->do(<<'SQL');
CREATE TABLE IF NOT EXISTS `cache_dbi` (
    `id`        VARCHAR(64) NOT NULL PRIMARY KEY,
    `value`     BLOB NOT NULL,
    `expires_at` INT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
SQL

        $dbh->commit;
    }
);

my $cache = Cache::DBI->new(
    connector => $conn
);

subtest delete => sub {
    my ($id, $value) = ('mail', { bar => 'baz' });

    is $cache->set($id, $value, 10), 1;
    is_deeply $cache->get($id), $value;

    is $cache->delete($id), 1;
    is_deeply [ $cache->get($id) ], [];
};

subtest expired => sub {
    my ($id, $value) = ('docs', { bar => 'baz' });

    is $cache->set($id, $value, -10), 1;
    is_deeply [ $cache->get($id) ], [];

    is $cache->delete($id), 1;
    is_deeply [ $cache->get($id) ], [];
};

subtest 'not exists' => sub {
    my $id = "search";

    is_deeply [ $cache->get($id) ], [];

    is $cache->delete($id), 1;
};

done_testing;

