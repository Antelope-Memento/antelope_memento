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
my $ack_every = 200;

my $sourceid;

my $db_name;
my $db_host = 'localhost';
my $db_user = 'memento_rw';
my $db_password = 'LKpoiinjdscudfc';


my $ok = GetOptions
    (
     'id=i'      => \$sourceid,
     'port=i'    => \$port,
     'ack=i'     => \$ack_every,
     'database=s' => \$db_name,
     'dbhost=s'  => \$db_host,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
    );


if( not $ok or not $sourceid  or scalar(@ARGV) > 0)
{
    print STDERR "Usage: $0 --id=N [options...]\n",
        "The utility opens a WS port for Chronicle to send data to.\n",
        "Options:\n",
        "  --id=N             source instance identifier (1 or 2)\n",
        "  --port=N           \[$port\] TCP port to listen to websocket connection\n",
        "  --ack=N            \[$ack_every\] Send acknowledgements every N blocks\n",
        "  --database=DBNAME  \[$db_name\]\n",
        "  --dbhost=HOST      \[$db_host\]\n",
        "  --dbuser=USER      \[$db_user\]\n",
        "  --dbpw=PASSWORD    \[$db_password\]\n";
    exit 1;
}


my $dsn = 'dbi:MariaDB:database=' . $db_name . ';host=' . $db_host;

my $json = JSON->new->canonical;


my $db;

my $confirmed_block = 0;
my $unconfirmed_block = 0;
my $irreversible = 0;

my $i_am_master;
my $retired_on = 0; # timestamp of last time losing master status

my $just_committed = 1;

my $blocks_counter = 0;
my $trx_counter = 0;
my $counter_start = time();

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

    if( not $i_am_master )
    {
        # make sure the master is running
        if( $masters->[0][1] == 0 or $masters->[0][2] == 0 )
        {
            die("sourceid=" . $masters->[0][0] . " is defined as master, but it has not started yet\n");
        }
    }
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

        $db->{'sth_fork_transactions'}->execute($block_num);
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
                    $db->{'sth_ins_bkp'}->execute($trx_seq, $block_num, $block_time, $trace->{'id'}, ${$jsptr});
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
            $irreversible = $last_irreversible;
            # LIB has moved
            if( not $i_am_master )
            {
                $db->{'sth_clean_bkp'}->execute();
            }
        }

        $unconfirmed_block = $block_num;

        if( $unconfirmed_block <= $confirmed_block )
        {
            # we are catching up through irreversible data, and this block was already stored in DB
            return $unconfirmed_block;
        }

        if( $unconfirmed_block - $confirmed_block >= $ack_every )
        {
            $db->{'sth_upd_sync_head'}->execute($block_num, $block_time, $last_irreversible, $sourceid);
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
                    $db->{'sth_fork_transactions'}->execute($start_block);

                    # copy data from BKP_TRACES
                    my $sth = $db->{'dbh'}->prepare
                        ('SELECT seq, block_num, block_time, trx_id, trace ' .
                         'FROM BKP_TRACES WHERE block_num >= ? ORDER BY seq');
                    $sth->execute($start_block);
                    while( my $r = $sth->fetchrow_arrayref() )
                    {
                        my $js = $r->[4];
                        my $data = eval {$json->decode($js)};

                        save_trace($r->[0], $r->[1], $r->[2], $data->{'trace'}, \$js);
                    }

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
        ('UPDATE SYNC SET block_num=?, block_time=?, irreversible=?, last_updated=NOW() WHERE sourceid=?');

    $db->{'sth_fork_transactions'} = $dbh->prepare('DELETE FROM TRANSACTIONS WHERE block_num>=?');

    $db->{'sth_upd_sync_fork'} = $dbh->prepare('UPDATE SYNC SET block_num=? WHERE sourceid=?');

    $db->{'sth_check_sync_health'} =
        $dbh->prepare('SELECT sourceid, irreversible, is_master, TIME_TO_SEC(TIMEDIFF(NOW(), last_updated)) AS upd ' .
                      'FROM SYNC');

    $db->{'sth_am_i_master'} = $dbh->prepare('SELECT is_master FROM SYNC WHERE sourceid=?');

    $db->{'sth_ins_tx'} = $db->{'dbh'}->prepare
        ('INSERT INTO TRANSACTIONS (seq, block_num, block_time, trx_id) VALUES(?,?,?,?)');

    $db->{'sth_ins_trace'} = $db->{'dbh'}->prepare('INSERT INTO TRACES (seq, trace) VALUES(?,?)');

    $db->{'sth_ins_receipt'} = $db->{'dbh'}->prepare('INSERT INTO RECEIPTS (seq, account_name) VALUES(?,?)');

    $db->{'sth_ins_action'} = $db->{'dbh'}->prepare('INSERT INTO ACTIONS (seq, contract, action) VALUES(?,?,?)');

    $db->{'sth_ins_bkp'} = $db->{'dbh'}->prepare
        ('INSERT INTO BKP_TRACES (seq, block_num, block_time, trx_id, trace) VALUES(?,?,?,?,?)');

    $db->{'sth_clean_bkp'} = $dbh->prepare('DELETE FROM BKP_TRACES WHERE block_num < (SELECT MIN(irreversible) FROM SYNC)');
}


sub save_trace
{
    my $trx_seq = shift;
    my $block_num = shift;
    my $block_time = shift;
    my $trace = shift;
    my $jsptr = shift;

    my %receivers_seen;
    my %actions_seen;

    foreach my $atrace (@{$trace->{'action_traces'}})
    {
        my $act = $atrace->{'act'};
        my $contract = $act->{'account'};
        my $aname = $act->{'name'};
        my $receipt = $atrace->{'receipt'};
        my $receiver = $receipt->{'receiver'};

        $receivers_seen{$receiver} = 1;

        if( $receiver eq $contract )
        {
            $actions_seen{$contract}{$aname} = 1;
        }
    }

    $db->{'sth_ins_tx'}->execute($trx_seq, $block_num, $block_time, $trace->{'id'});
    $db->{'sth_ins_trace'}->execute($trx_seq, ${$jsptr});

    foreach my $rcpt (keys %receivers_seen)
    {
        $db->{'sth_ins_receipt'}->execute($trx_seq, $rcpt);
    }

    foreach my $contract (keys %actions_seen)
    {
        foreach my $aname (keys %{$actions_seen{$contract}})
        {
            $db->{'sth_ins_action'}->execute($trx_seq, $contract, $aname);
        }
    }
}
