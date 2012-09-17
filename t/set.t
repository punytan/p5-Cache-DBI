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

subtest set => sub {
    my ($id, $value) = ('mail', { bar => 'baz' });
    is $cache->set($id, $value, 10), 1;
    is_deeply $cache->get($id), $value;
};

subtest overwrite => sub {
    my ($id, $value) = ('mail', { bar => 'baz' });

    is_deeply $cache->get($id), $value;

    my $new_value = { foo => 'bar' };

    is $cache->set($id, $new_value, 10), 1;

    is_deeply [ $cache->get($id) ], [ $new_value ];
};


subtest 'expired + overwrite' => sub {
    my ($id, $value, $expires_at) = ("search", { bar => 'baz' }, time - 10);

    $conn->txn(
        fixup => sub {
            my $dbh = shift;
            $dbh->do(
                'INSERT INTO cache_dbi (id, value, expires_at) VALUES (?, ?, ?)',
                undef,
                $id,
                Data::MessagePack->pack($value),
                $expires_at,
            );
            $dbh->commit;
        }
    );

    is_deeply [ $cache->get($id) ], [];

    is $cache->set($id, { foo => 'bar' }), 1;

    is_deeply [ $cache->get($id) ], [ { foo => 'bar' } ];
};

subtest 'too large' => sub {
    my ($id, $value, $expires_at) = ("search", { bar => 'baz' }, time - 10);
    is $cache->set("play", { key => "value" x 65536 }), 1;
    eval { $cache->get("play") };
    like $@, qr/insufficient bytes/;
};

done_testing;


