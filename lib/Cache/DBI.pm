package Cache::DBI;
use strict;
use warnings;
our $VERSION = '0.01';

use Carp;
use Storable;
use MIME::Base64 ();
use Try::Tiny;
use Time::Piece ();

sub new {
    my ($class, %args) = @_;

    my $dbh;
    if (my $conn = $args{connector}) {
        $dbh = sub { $conn->dbh };
    } elsif (my $dbi_handle = $args{dbh}) {
        $dbh = sub { $dbi_handle };
    } else {
        Carp::croak "Couldn't find DBI handle";
    }

    my $namespace = $args{namespace} || 'cache_dbi';

    my $serializer = $args{serializer} || sub {
        MIME::Base64::encode_base64( Storable::nfreeze(shift) );
    };

    my $deserializer = $args{deserializer} || sub {
        Storable::thaw( MIME::Base64::decode_base64(shift) );
    };

    return bless {
        dbh => $dbh,
        serializer   => $serializer,
        deserializer => $deserializer,
        namespace    => $namespace,
        sql => {
            set => {
                select => "SELECT 1 FROM $namespace WHERE id = ?",
                update => "UPDATE $namespace SET value = ?, expire_at = ? WHERE id = ?",
                insert => "INSERT INTO $namespace (id, value, expire_at) VALUES (?, ?, ?)",
            },
            get    => { select => "SELECT * FROM $namespace WHERE id = ?" },
            delete => { delete => "DELETE FROM $namespace WHERE id = ?" },
        },
    }, $class;
}

sub prepare_cached { shift->{dbh}->()->prepare_cached(@_) }

sub get {
    my ($self, $key) = @_;

    my $sth = $self->prepare_cached($self->{sql}{get}{select});
    $sth->execute($key);

    my ($row) = $sth->fetchrow_hashref;
    $sth->finish;

    return if not $row;

    if ($row->{expire_at} < Time::Piece->gmtime->epoch) {
        $self->delete($key);
        return;
    } else {
        return $self->{deserializer}->($row->{value});
    }
}

sub set {
    my ($self, $key, $value, $expire_after) = @_;
    $expire_after ||= 60;

    my $sth = $self->prepare_cached($self->{sql}{set}{select});
    $sth->execute($key);

    my ($exists) = $sth->fetchrow_hashref;
    $sth->finish;

    my $val = $self->{serializer}->($value);
    my $expire_at = Time::Piece->gmtime->epoch + $expire_after;

    my @bind = $exists
        ? ($val, $expire_at, $key)
        : ($key, $val, $expire_at);

    my $type = $exists ? 'update' : 'insert';
    my $sth2 = $self->prepare_cached($self->{sql}{set}{$type});
    $sth2->execute(@bind);
}

sub delete {
    my ($self, $key) = @_;

    my $sth = $self->prepare_cached($self->{sql}{delete}{delete});
    $sth->execute($key);
    $sth->finish;
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
        `expire_at` BIGINT UNSIGNED NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

=head1 AUTHOR

punytan E<lt>punytan@gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
