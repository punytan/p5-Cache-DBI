use inc::Module::Install;
name 'Cache-DBI';
all_from 'lib/Cache/DBI.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More' => 0.98;
auto_set_repository;
auto_include;
WriteAll;
