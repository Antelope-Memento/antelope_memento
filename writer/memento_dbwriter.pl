use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;
use Net::WebSocket::Server;
use Protocol::WebSocket::Frame;
use Time::HiRes qw (time);
use Time::Local 'timegm_nocheck';

$Protocol::WebSocket::Frame::MAX_PAYLOAD_SIZE = 100*1024*1024;
$Protocol::WebSocket::Frame::MAX_FRAGMENTS_AMOUNT = 102400;

$| = 1;

my $port = 8001;
my $ack_every = 100;

my $sourceid;

my $dsn;
my $db_user = 'memento_rw';
my $db_password = 'LKpoiinjdscudfc';
my $keep_days;
my @plugins;
my %pluginargs;
my $no_traces;

my $ok = GetOptions
    (
     'id=i'      => \$sourceid,
     'port=i'    => \$port,
     'ack=i'     => \$ack_every,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
     'keepdays=i' => \$keep_days,
     'plugin=s'  => \@plugins,
     'parg=s%'   => \%pluginargs,
     'notraces'  => \$no_traces,
    );


if( not $ok or not $sourceid or not defined($dsn) or scalar(@ARGV) > 0)
{
    print STDERR "Usage: $0 --id=N --dsn=DBSTRING [options...]\n",
        "The utility opens a WS port for Chronicle to send data to.\n",
        "Options:\n",
        "  --id=N             source instance identifier (1 or 2)\n",
        "  --port=N           \[$port\] TCP port to listen to websocket connection\n",
        "  --ack=N            \[$ack_every\] Send acknowledgements every N blocks\n",
        "  --dsn=DBSTRING     database connection string\n",
        "  --dbuser=USER      \[$db_user\]\n",
        "  --dbpw=PASSWORD    \[$db_password\]\n",
        "  --keepdays=N       delete the history older tnan N days\n",
        "  --plugin=FILE.pl   plugin program for custom processing\n",
        "  --parg KEY=VAL     plugin configuration options\n",
        "  --notraces         skip writing TRANSACTIONS, RECEIPTS tables\n";
    exit 1;
}

our $db_binary_type = DBI::SQL_BINARY;
our $db_is_postgres = 0;
if( index($dsn, 'dbi:Pg:') == 0 )
{
    require DBD::Pg;
    $db_binary_type = { pg_type => DBD::Pg->PG_BYTEA };
    $db_is_postgres = 1;
}


our @prepare_hooks;
our @trace_hooks;
our @block_hooks;
our @ack_hooks;
our @rollback_hooks;
our @fork_hooks;
our @lib_hooks;
our @prune_hooks;

foreach my $plugin (@plugins)
{
    require($plugin);
}


our $json = JSON->new->canonical;
our $db;

my $confirmed_block = 0;
my $unconfirmed_block = 0;
my $irreversible = 0;

my $i_am_master;
my $retired_on = 0; # timestamp of last time losing master status

my $just_committed = 1;

my $blocks_counter = 0;
my $trx_counter = 0;
my $counter_start = time();

my $keep_blocks;
if( defined($keep_days) )
{
    $keep_blocks = $keep_days * 24 * 7200;
}

my @insert_transactions;
my @insert_receipts;
my %upsert_recv_seq_max;
my @insert_bkp_traces;

getdb();

{
    # sanity check, there should be only one master
    my $sth = $db->{'dbh'}->prepare
        ('SELECT sourceid, block_num, irreversible FROM SYNC WHERE is_master=1');
    $sth->execute();
    my $masters = $sth->fetchall_arrayref();
    if( scalar(@{$masters}) == 0 )
    {
        die("no master is defined in SYNC table\n");
    }
    elsif( scalar(@{$masters}) > 1 )
    {
        die("more than one master is defined in SYNC table\n");
    }

    # sanity check, there cannot be more than one slave
    $sth = $db->{'dbh'}->prepare
        ('SELECT sourceid, block_num, irreversible FROM SYNC WHERE is_master=0');
    $sth->execute();
    my $slaves = $sth->fetchall_arrayref();
    if( scalar(@{$masters}) > 1 )
    {
        die("SYNC table contains more than one slave\n");
    }

    # fetch last sync status
    $sth = $db->{'dbh'}->prepare
        ('SELECT block_num, irreversible, is_master FROM SYNC WHERE sourceid=?');
    $sth->execute($sourceid);
    my $r = $sth->fetchall_arrayref();
    if( scalar(@{$r}) == 0 )
    {
        die("sourceid=$sourceid is not defined in SYNC table\n");
    }

    $confirmed_block = $r->[0][0];
    $unconfirmed_block = $confirmed_block;
    $irreversible = $r->[0][1];
    $i_am_master = $r->[0][2];
    printf STDERR ("Starting from confirmed_block=%d, irreversible=%d, sourceid=%d, is_master=%d\n",
                   $confirmed_block, $irreversible, $sourceid, $i_am_master);

    if( $no_traces )
    {
        printf STDERR ("Skipping the updates for TRANSACTIONS, RECEIPTS, RECV_SEQUENCE_MAX tables\n");
    }

    if( not $i_am_master )
    {
        # make sure the master is running
        if( $masters->[0][1] == 0 or $masters->[0][2] == 0 )
        {
            die("sourceid=" . $masters->[0][0] . " is defined as master, but it has not started yet\n");
        }

        if( defined($keep_blocks) )
        {
            printf STDERR ("Automatically pruning the history older than %d blocks\n", $keep_blocks);
        }
    }
}


foreach my $hook (@prepare_hooks)
{
    &{$hook}(\%pluginargs);
}



Net::WebSocket::Server->new(
    listen => $port,
    on_connect => sub {
        my ($serv, $conn) = @_;
        $conn->on(
            'binary' => sub {
                my ($conn, $msg) = @_;
                my ($msgtype, $opts, $js) = unpack('VVa*', $msg);
                my $data = eval {$json->decode($js)};
                if( $@ )
                {
                    print STDERR $@, "\n\n";
                    print STDERR $js, "\n";
                    exit;
                }

                if( $i_am_master and $just_committed )
                {
                    # verify that I am still the master
                    $db->{'sth_am_i_master'}->execute($sourceid);
                    my $r = $db->{'sth_am_i_master'}->fetchall_arrayref();
                    if( not $r->[0][0] )
                    {
                        printf STDERR ("I am no longer the master (sourceid=%d)\n", $sourceid);
                        $i_am_master = 0;
                        $retired_on = time();
                    }
                    $just_committed = 0;
                }

                my $ack = process_data($msgtype, $data, \$js);
                if( $ack >= 0 )
                {
                    $conn->send_binary(sprintf("%d", $ack));
                    print STDERR "ack $ack\n";
                }
            },
            'disconnect' => sub {
                print STDERR "Disconnected\n";
                $db->{'dbh'}->rollback();
            },

            );
    },
    )->start;


sub process_data
{
    my $msgtype = shift;
    my $data = shift;
    my $jsptr = shift;

    if( $msgtype == 1001 ) # CHRONICLE_MSGTYPE_FORK
    {
        my $block_num = $data->{'block_num'};
        print STDERR "fork at $block_num\n";

        getdb();
        $db->{'dbh'}->commit();
        $just_committed = 1;

        if( $confirmed_block <= $irreversible )
        {
            return ($block_num-1);
        }

        $confirmed_block = $block_num-1;
        $unconfirmed_block = $confirmed_block;

        @insert_transactions = ();
        @insert_receipts = ();
        %upsert_recv_seq_max = ();

        if( $i_am_master )
        {
            fork_traces($block_num);
        }
        else
        {
            $db->{'sth_fork_bkp'}->execute($block_num);
        }

        $db->{'sth_upd_sync_fork'}->execute($confirmed_block, $sourceid);
        $db->{'dbh'}->commit();
        $just_committed = 1;

        return $confirmed_block;
    }
    elsif( $msgtype == 1003 ) # CHRONICLE_MSGTYPE_TX_TRACE
    {
        my $block_num = $data->{'block_num'};
        if( $block_num > $confirmed_block ) {
            my $trace = $data->{'trace'};
            if( $trace->{'status'} eq 'executed' and scalar(@{$trace->{'action_traces'}}) > 0 )
            {
                my $block_time = $data->{'block_timestamp'};
                $block_time =~ s/T/ /;

                my $trx_seq = $trace->{'action_traces'}[0]->{'receipt'}{'global_sequence'};

                if( $i_am_master )
                {
                    save_trace($trx_seq, $block_num, $block_time, $trace, $jsptr);
                }
                else
                {
                    my $dbh = $db->{'dbh'};
                    push(@insert_bkp_traces,
                         [$trx_seq, $block_num, $dbh->quote($block_time), $dbh->quote($trace->{'id'}),
                          $dbh->quote(${$jsptr}, $db_binary_type)]);
                }

                $trx_counter++;
            }
        }
    }
    elsif( $msgtype == 1010 ) # CHRONICLE_MSGTYPE_BLOCK_COMPLETED
    {
        $blocks_counter++;
        my $block_num = $data->{'block_num'};
        my $block_time = $data->{'block_timestamp'};
        $block_time =~ s/T/ /;
        my $last_irreversible = $data->{'last_irreversible'};

        if( $block_num > $unconfirmed_block+1 )
        {
            printf STDERR ("WARNING: missing blocks %d to %d\n", $unconfirmed_block+1, $block_num-1);
        }

        if( $block_num > $last_irreversible )
        {
            $ack_every = 1;
        }

        if( $last_irreversible > $irreversible )
        {
            # LIB has moved
            $irreversible = $last_irreversible;
            foreach my $hook (@lib_hooks)
            {
                &{$hook}($irreversible);
            }

            if( not $i_am_master )
            {
                $db->{'sth_clean_bkp'}->execute();
                if( defined($keep_blocks) )
                {
                    my $upto = $irreversible - $keep_blocks;
                    $db->{'sth_prune_receipts'}->execute($upto);
                    $db->{'sth_prune_transactions'}->execute($upto);
                    foreach my $hook (@prune_hooks)
                    {
                        &{$hook}($upto);
                    }
                }
            }
        }

        if( $i_am_master )
        {
            send_traces_batch();
            foreach my $hook (@block_hooks)
            {
                &{$hook}($block_num, $last_irreversible, $data->{'block_id'});
            }
        }
        elsif( scalar(@insert_bkp_traces) > 0 )
        {
            my $query = 'INSERT INTO BKP_TRACES (seq, block_num, block_time, trx_id, trace) VALUES ' .
            join(',', map {'(' . join(',', @{$_}) . ')'} @insert_bkp_traces);
            $db->{'dbh'}->do($query);
            @insert_bkp_traces = ();
        }

        $unconfirmed_block = $block_num;

        if( $unconfirmed_block <= $confirmed_block )
        {
            # we are catching up through irreversible data, and this block was already stored in DB
            return $unconfirmed_block;
        }

        if( $unconfirmed_block - $confirmed_block >= $ack_every )
        {
            if( $i_am_master )
            {
                foreach my $hook (@ack_hooks)
                {
                    &{$hook}($block_num);
                }
            }

            $db->{'sth_upd_sync_head'}->execute($block_num, $block_time, $data->{'block_id'}, $last_irreversible, $sourceid);
            $db->{'dbh'}->commit();
            $just_committed = 1;
            $confirmed_block = $unconfirmed_block;

            if( not $i_am_master and $block_num > $last_irreversible and time() > $retired_on + 60 )
            {
                # check if the master is still alive

                $db->{'sth_check_sync_health'}->execute();
                my $my_upd;
                my $my_irrev;
                my $master_upd;
                my $master_irrev;
                my $old_master;

                while( my $r = $db->{'sth_check_sync_health'}->fetchrow_hashref('NAME_lc') )
                {
                    if( $r->{'sourceid'} == $sourceid )
                    {
                        $my_upd = $r->{'upd'};
                        $my_irrev = $r->{'irreversible'};
                    }
                    elsif( $r->{'is_master'} )
                    {
                        $master_upd = $r->{'upd'};
                        $master_irrev = $r->{'irreversible'};
                        $old_master = $r->{'sourceid'};
                    }
                }

                if( not defined($my_upd) or not defined($my_irrev) or
                    not defined($master_upd) or not defined($master_irrev) or
                    not defined($old_master) )
                {
                    die('SYNC corrupted');
                }

                if( $master_irrev < $my_irrev - 120 and $master_upd > $my_upd + 120 and $my_upd < 10 )
                {
                    printf STDERR ("Master process (sourceid=%i) stopped, taking over the master role\n", $old_master);
                    printf STDERR ("my_upd=%d, my_irrev=%d, master_upd=%d, master_irrev=%d\n",
                                   $my_upd, $my_irrev, $master_upd, $master_irrev);

                    $db->{'dbh'}->do('UPDATE SYNC SET is_master=0 WHERE sourceid != ?', undef, $sourceid);
                    $db->{'dbh'}->do('UPDATE SYNC SET is_master=1 WHERE sourceid = ?', undef, $sourceid);
                    $db->{'dbh'}->commit();

                    $i_am_master = 1;

                    printf STDERR ("Sleeping 5 seconds\n");
                    sleep(5);

                    # delete all reversible traces written by old master
                    my $start_block = $master_irrev + 1;
                    fork_traces($start_block);

                    # copy data from BKP_TRACES
                    my $copied_rows = 0;
                    my $sth = $db->{'dbh'}->prepare
                        ('SELECT seq, block_num, block_time, trx_id, trace ' .
                         'FROM BKP_TRACES WHERE block_num >= ? ORDER BY seq');
                    $sth->execute($start_block);
                    while( my $r = $sth->fetchrow_arrayref() )
                    {
                        my $js = $r->[4];
                        my $data = eval {$json->decode($js)};

                        save_trace($r->[0], $r->[1], $r->[2], $data->{'trace'}, \$js);
                        $copied_rows++;
                    }

                    send_traces_batch();
                    $db->{'dbh'}->commit();
                    $just_committed = 1;
                    printf STDERR ("Copied %d rows from backup\n", $copied_rows);

                    $db->{'sth_fork_bkp'}->execute($start_block);
                    $db->{'dbh'}->commit();
                    $just_committed = 1;
                }
            }

            my $gap = 0;
            {
                my ($year, $mon, $mday, $hour, $min, $sec, $msec) =
                    split(/[-:.T]/, $data->{'block_timestamp'});
                my $epoch = timegm_nocheck($sec, $min, $hour, $mday, $mon-1, $year);
                $gap = (time() - $epoch)/3600.0;
            }

            my $period = time() - $counter_start;
            printf STDERR ("%s - blocks/s: %8.2f, trx/block: %8.2f, trx/s: %8.2f, gap: %8.4fh, ",
                           ($i_am_master?'M':'S'),
                           $blocks_counter/$period, $trx_counter/$blocks_counter, $trx_counter/$period,
                           $gap);
            $counter_start = time();
            $blocks_counter = 0;
            $trx_counter = 0;

            return $confirmed_block;
        }
    }
    return -1;
}



sub getdb
{
    if( defined($db) and defined($db->{'dbh'}) and $db->{'dbh'}->ping() )
    {
        return;
    }

    my $dbh = $db->{'dbh'} = DBI->connect($dsn, $db_user, $db_password,
                                          {'RaiseError' => 1, AutoCommit => 0});
    die($DBI::errstr) unless $dbh;

    $db->{'sth_upd_sync_head'} = $dbh->prepare
        ('UPDATE SYNC SET block_num=?, block_time=?, block_id=?, irreversible=?, last_updated=NOW() WHERE sourceid=?');

    $db->{'sth_fork_transactions'} = $dbh->prepare('DELETE FROM TRANSACTIONS WHERE block_num>=?');
    $db->{'sth_fork_receipts'} = $dbh->prepare('DELETE FROM RECEIPTS WHERE block_num>=?');

    $db->{'sth_fork_bkp'} = $dbh->prepare('DELETE FROM BKP_TRACES WHERE block_num>=?');

    $db->{'sth_upd_sync_fork'} = $dbh->prepare('UPDATE SYNC SET block_num=? WHERE sourceid=?');

    if( $db_is_postgres )
    {
        $db->{'sth_check_sync_health'} =
            $dbh->prepare('SELECT sourceid, irreversible, is_master, EXTRACT(EPOCH FROM (NOW() - last_updated)) AS upd ' .
                          'FROM SYNC');
    }
    else
    {
        $db->{'sth_check_sync_health'} =
            $dbh->prepare('SELECT sourceid, irreversible, is_master, TIME_TO_SEC(TIMEDIFF(NOW(), last_updated)) AS upd ' .
                          'FROM SYNC');
    }

    $db->{'sth_am_i_master'} = $dbh->prepare('SELECT is_master FROM SYNC WHERE sourceid=?');

    $db->{'sth_clean_bkp'} = $dbh->prepare('DELETE FROM BKP_TRACES WHERE block_num < (SELECT MIN(irreversible) FROM SYNC)');

    $db->{'sth_prune_transactions'} = $dbh->prepare('DELETE FROM TRANSACTIONS WHERE block_num < ?');
    $db->{'sth_prune_receipts'} = $dbh->prepare('DELETE FROM RECEIPTS WHERE block_num < ?');

    $db->{'sth_fetch_forking_traces'} =
        $dbh->prepare('SELECT seq, block_num, block_time, trx_id, trace FROM TRANSACTIONS WHERRE block_num >= ? ORDER BY seq DESC');
}


sub save_trace
{
    my $trx_seq = shift;
    my $block_num = shift;
    my $block_time = shift;
    my $trace = shift;
    my $jsptr = shift;

    if( not $no_traces )
    {
        my $dbh = $db->{'dbh'};
        my $qtime = $dbh->quote($block_time);

        push(@insert_transactions,
             [$trx_seq, $block_num, $qtime, $dbh->quote($trace->{'id'}), $dbh->quote(${$jsptr}, $db_binary_type)]);

        foreach my $atrace (@{$trace->{'action_traces'}})
        {
            my $act = $atrace->{'act'};
            my $receipt = $atrace->{'receipt'};
            my $receiver = $receipt->{'receiver'};
            my $recv_sequence = $receipt->{'recv_sequence'};

            push(@insert_receipts, [$trx_seq, $block_num, $qtime, $dbh->quote($act->{'account'}),
                                    $dbh->quote($act->{'name'}), $dbh->quote($receiver),
                                    $recv_sequence]);

            $upsert_recv_seq_max{$receiver} = $recv_sequence;
        }
    }

    foreach my $hook (@trace_hooks)
    {
        &{$hook}($trx_seq, $block_num, $block_time, $trace, $jsptr);
    }
}


sub fork_traces
{
    my $start_block = shift;

    foreach my $hook (@fork_hooks)
    {
        &{$hook}($start_block);
    }

    if( scalar(@rollback_hooks) > 0 )
    {
        $db->{'sth_fetch_forking_traces'}->execute($start_block);
        while( my $r = $db->{'sth_fetch_forking_traces'}->fetchrow_arrayref() )
        {
            foreach my $hook (@rollback_hooks)
            {
                &{$hook}(@{$r});
            }
        }
    }

    $db->{'sth_fork_receipts'}->execute($start_block);
    $db->{'sth_fork_transactions'}->execute($start_block);
}


sub send_traces_batch
{
    if( scalar(@insert_transactions) > 0 )
    {
        my $dbh = $db->{'dbh'};

        my $query = 'INSERT INTO TRANSACTIONS (seq, block_num, block_time, trx_id, trace) VALUES ' .
            join(',', map {'(' . join(',', @{$_}) . ')'} @insert_transactions);

        $dbh->do($query);

        if( scalar(@insert_receipts) > 0 )
        {
            $query = 'INSERT INTO RECEIPTS (seq, block_num, block_time, contract, action, receiver, recv_sequence) VALUES ' .
                join(',', map {'(' . join(',', @{$_}) . ')'} @insert_receipts);
            $dbh->do($query);
        }

        if( scalar(keys %upsert_recv_seq_max) > 0 )
        {
            if( $db_is_postgres )
            {
                $query = 'INSERT INTO RECV_SEQUENCE_MAX (account_name, recv_sequence_max) VALUES ' .
                    join(',', map {'(' . $dbh->quote($_) . ',' . $upsert_recv_seq_max{$_} . ')'} keys %upsert_recv_seq_max) .
                    ' ON CONFLICT (account_name) DO UPDATE SET recv_sequence_max = EXCLUDED.recv_sequence_max';
            }
            else
            {
                $query = 'INSERT INTO RECV_SEQUENCE_MAX (account_name, recv_sequence_max) VALUES ' .
                    join(',', map {'(' . $dbh->quote($_) . ',' . $upsert_recv_seq_max{$_} . ')'} keys %upsert_recv_seq_max) .
                    ' ON DUPLICATE KEY UPDATE recv_sequence_max = VALUES(recv_sequence_max)';
            }
            $dbh->do($query);
        }

        @insert_transactions = ();
        @insert_receipts = ();
        %upsert_recv_seq_max = ();
    }
}
