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

DBI->connect($mysqld->dsn)->do(q{
    CREATE TABLE IF NOT EXISTS `cache_dbi` (
        `id`        VARCHAR(64) NOT NULL PRIMARY KEY,
        `value`     TEXT,
        `expire_at` BIGINT UNSIGNED NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
});

{
    my $cache = Cache::DBI->new(
        dbh => DBI->connect($mysqld->dsn),
    );
    isa_ok $cache, 'Cache::DBI';

    {
        my $hash = {bar => "baz"};
        $cache->set("foo", $hash);

        my $val = $cache->get("foo");
        is_deeply $val, $hash;

        $cache->delete("foo");
    }

}

{
    my $cache = Cache::DBI->new(
        connector => DBIx::Connector->new($mysqld->dsn),
    );
    isa_ok $cache, 'Cache::DBI';

    {
        use utf8;
        my $hash = {bar => "いろはにほへと"};
        $cache->set("foo", $hash, 1);

        my $val = $cache->get("foo");
        is_deeply $val, $hash;
    }

    sleep 2;

    { # expire time
        my $val  = $cache->get("foo");
        is $val, undef;
    }

    { # delete on non-stored key
        $cache->delete("xxxxx");
    }

    { # upsert test
        use utf8;
        my $hash = {bar => "いろはにほへと"};
        $cache->set("foo", {i => 'j'});
        $cache->set("foo", $hash);

        my $val = $cache->get("foo");
        is_deeply $val, $hash;
    }

}

done_testing;

