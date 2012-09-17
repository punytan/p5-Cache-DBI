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

subtest exists => sub {
    my ($id, $value, $expires_at) = ("mail", { bar => 'baz' }, time + 10);

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

    is_deeply $cache->get($id), $value;
};

subtest not_exists => sub {
    my $id = "image";
    is_deeply [ $cache->get($id) ], [];
};


subtest expired => sub {
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

    subtest 'guarantee the expired row has been deleted' => sub {
        $conn->run(
            fixup => sub {
                my $dbh = shift;
                my $row = $dbh->selectrow_arrayref('SELECT * FROM cache_dbi WHERE id = ?', undef, $id);
                is_deeply $row, undef; 
            }
        );
    };

};

done_testing;

