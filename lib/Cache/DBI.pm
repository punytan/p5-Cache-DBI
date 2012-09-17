package Cache::DBI;
use strict;
use warnings;
our $VERSION = '0.02';

use Carp;
use Data::MessagePack;

sub new {
    my ($class, %args) = @_;

    my $connector = $args{connector}
        or Carp::croak "Couldn't find DBI handle";

    my $serializer = $args{serializer}
        || Data::MessagePack->new->prefer_integer->canonical(0)->utf8;


    my $table = $args{table} || 'cache_dbi';

    my $delete = "DELETE FROM $table WHERE id = ?";
    my $get    = "SELECT id, value, expires_at FROM $table WHERE id = ?";
    my $set    = <<SQL;
INSERT INTO $table
    (id, value, expires_at)
VALUES
    (?, ?, ?)
ON DUPLICATE KEY UPDATE
    value      = VALUES(value),
    expires_at = VALUES(expires_at)
SQL

    return bless {
        connector  => $connector,
        serializer => $serializer,
        table => $table,
        sql   => {
            set    => $set,
            get    => $get,
            delete => $delete,
        },
    }, $class;
}

sub connector  { shift->{connector}  }
sub serializer { shift->{serializer} }

sub get {
    my ($self, $key) = @_;

    $self->connector->txn(
        fixup => sub {
            my $dbh = shift;
            my $row = $dbh->selectrow_hashref($self->{sql}{get}, undef, $key)
                or return;

            if ($row->{expires_at} > time) {
                return $self->serializer->decode($row->{value});
            } else {
                $dbh->do($self->{sql}{delete}, undef, $key);
                $dbh->commit;
                return;
            }
        }
    );
}

sub set {
    my ($self, $key, $value, $expires_after) = @_;
    my $expires_at = time + ($expires_after || 60);
    my $serialized = $self->serializer->encode($value);

    $self->connector->txn(
        fixup => sub {
            my $dbh = shift;
            $dbh->do($self->{sql}{set}, undef, $key, $serialized, $expires_at);
            $dbh->commit;
        }
    );
}

sub delete {
    my ($self, $key) = @_;
    $self->connector->txn(
        fixup => sub {
            my $dbh = shift;
            $dbh->do($self->{sql}{delete}, undef, $key);
            $dbh->commit;
        }
    );
}

sub incr { Carp::croak "TBD" }
sub decr { Carp::croak "TBD" }

1;
__END__

=head1 NAME

Cache::DBI -

=head1 SYNOPSIS

  use Cache::DBI;

=head1 DESCRIPTION

Cache::DBI is

=head1 SCHEMA

    -- MySQL
    CREATE TABLE IF NOT EXISTS `cache_dbi` (
        `id`        VARCHAR(64) NOT NULL PRIMARY KEY,
        `value`     TEXT,
        `expires_at` BIGINT UNSIGNED NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

=head1 AUTHOR

punytan E<lt>punytan@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
