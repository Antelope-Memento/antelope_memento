use strict;
use warnings;
use Getopt::Long;
use DBI;

$| = 1;

my $dsn;
my $db_user = 'memento_rw';
my $db_password = 'LKpoiinjdscudfc';

my $keep_days;
my @plugins;
my %pluginargs;

my $ok = GetOptions
    (
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
     'keepdays=i' => \$keep_days,
     'plugin=s'  => \@plugins,
     'parg=s%'   => \%pluginargs,
    );


if( not $ok or not defined($keep_days) or not defined($dsn) or scalar(@ARGV) > 0)
{
    print STDERR "Usage: $0 --keepdays=N --database=DBNAME [options...]\n",
        "The utility opens a WS port for Chronicle to send data to.\n",
        "Options:\n",
        "  --dsn=DBSTRING     database connection string\n",
        "  --dbuser=USER      \[$db_user\]\n",
        "  --dbpw=PASSWORD    \[$db_password\]\n",
        "  --keepdays=N       delete the history older tnan N days\n",
        "  --plugin=FILE.pl   plugin program for custom processing\n",
        "  --parg KEY=VAL     plugin configuration options\n";
    exit 1;
}


our @prepare_hooks;
our @prune_hooks;

foreach my $plugin (@plugins)
{
    require($plugin);
}


our $db;

my $keep_blocks = $keep_days * 24 * 7200;

getdb();

{
    # sanity check
    my $sth = $db->{'dbh'}->prepare('SELECT count(*) FROM SYNC');
    $sth->execute();
    my $writers = $sth->fetchall_arrayref();
    if( $writers->[0][0] > 1 )
    {
        die("memento is running in dual-writer mode, no need to launch the dataguard\n");
    }
}

printf STDERR ("Automatically pruning the history older than %d blocks\n", $keep_blocks);


foreach my $hook (@prepare_hooks)
{
    &{$hook}(\%pluginargs);
}


my $last_irrev = 0;

while(1)
{
    $db->{'sth_get_min_irrev'}->execute();
    my $r = $db->{'sth_get_min_irrev'}->fetchall_arrayref();
    $db->{'dbh'}->commit();
    if( $r->[0][0] > $last_irrev )
    {
        $last_irrev = $r->[0][0];
        my $upto_block = $last_irrev - $keep_blocks;

        $db->{'sth_get_min_tx_block'}->execute();
        $r = $db->{'sth_get_min_tx_block'}->fetchall_arrayref();
        my $min_block = $r->[0][0];
        if( $min_block < $upto_block )
        {
            printf STDERR ("pruning %d blocks\n", $upto_block - $min_block);
        }

        while( $min_block < $upto_block )
        {
            my $delete_upto = $min_block + 10;
            if( $delete_upto > $upto_block )
            {
                $delete_upto = $upto_block;
            }

            printf STDERR ("deleting blocks < %d\n", $delete_upto);
            $db->{'sth_prune_receipts'}->execute($delete_upto);
            $db->{'sth_prune_transactions'}->execute($delete_upto);
            foreach my $hook (@prune_hooks)
            {
                &{$hook}($delete_upto);
            }

            $db->{'dbh'}->commit();
            $min_block = $delete_upto;
        }
    }
    else
    {
        printf STDERR ("nothing to delete\n");
    }

    sleep(10);
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

    $db->{'sth_get_min_irrev'} = $dbh->prepare('SELECT MIN(irreversible) FROM SYNC');

    $db->{'sth_get_min_tx_block'} = $dbh->prepare('SELECT MIN(block_num) FROM TRANSACTIONS');

    $db->{'sth_prune_transactions'} = $dbh->prepare('DELETE FROM TRANSACTIONS WHERE block_num < ?');
    $db->{'sth_prune_receipts'} = $dbh->prepare('DELETE FROM RECEIPTS WHERE block_num < ?');
}
