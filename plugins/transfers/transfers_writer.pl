use strict;
use warnings;
use DBD::Pg qw(:pg_types);


my @insert_transfers;

sub transfers_prepare
{
    my $args = shift;

    my $dbh = $main::db->{'dbh'};

    printf STDERR ("transfers_writer.pl prepared\n");
}


sub transfers_trace
{
    my $trx_seq = shift;
    my $block_num = shift;
    my $block_time = shift;
    my $trace = shift;
    my $jsptr = shift;

    foreach my $atrace (@{$trace->{'action_traces'}})
    {
        my $act = $atrace->{'act'};
        my $contract = $act->{'account'};
        my $aname = $act->{'name'};
        my $receipt = $atrace->{'receipt'};
        my $receiver = $receipt->{'receiver'};
        my $data = $act->{'data'};
        next unless ref($data) eq 'HASH';

        if( $receiver eq $contract )
        {
            if( ($aname eq 'transfer') and
                defined($data->{'quantity'}) and
                defined($data->{'to'}) and length($data->{'to'}) <= 13 and
                defined($data->{'from'}) and
                $data->{'to'} ne $data->{'from'} and
                length($data->{'from'}) <= 13 )
            {
                my ($amount, $currency) = split(/\s+/, $data->{'quantity'});
                if( defined($amount) and defined($currency) and
                    $amount =~ /^[0-9.]+$/ and $currency =~ /^[A-Z]{1,7}$/ )
                {
                    my $decimals = 0;
                    my $pos = index($amount, '.');
                    if( $pos > -1 )
                    {
                        $decimals = length($amount) - $pos - 1;
                    }
                    $amount =~ s/\.//;

                    my $dbh = $main::db->{'dbh'};
                    push(@insert_transfers,
                         [
                          $receipt->{'global_sequence'},
                          $trx_seq,
                          $block_num,
                          $dbh->quote($block_time),
                          $dbh->quote($trace->{'id'}),
                          $dbh->quote($contract),
                          $dbh->quote($currency),
                          $dbh->quote($data->{'from'}),
                          $dbh->quote($data->{'to'}),
                          $dbh->quote($amount),
                          $decimals,
                          $dbh->quote($data->{'memo'}, $main::db_binary_type)
                         ]);
                }
            }
        }
    }
}


sub transfers_ack
{
    my $block_num = shift;

    my $dbh = $main::db->{'dbh'};

    if( scalar(@insert_transfers) > 0 )
    {
        my $query = ('INSERT INTO TOKEN_TRANSFERS (' .
                     'seq, trx_seq, block_num, block_time, trx_id, contract, currency, tx_from, tx_to, amount, decimals, memo' .
                     ') VALUES ' .
                     join(',', map {'(' . join(',', @{$_}) . ')'} @insert_transfers));
        $dbh->do($query);
        @insert_transfers = ();
    }
}

sub transfers_fork
{
    my $block_num = shift;

    transfers_ack($block_num);

    my $dbh = $main::db->{'dbh'};

    my $sth = $dbh->prepare('DELETE FROM TOKEN_TRANSFERS WHERE block_num >= ?');
    $sth->execute($block_num);

    print STDERR "fork: $block_num\n";
}





push(@main::prepare_hooks, \&transfers_prepare);
push(@main::trace_hooks, \&transfers_trace);
push(@main::ack_hooks, \&transfers_ack);
push(@main::fork_hooks, \&transfers_fork);

1;
