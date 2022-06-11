Memento: lightweight EOSIO history solution
===========================================

In essence, Memento is a MariaDB database containing transaction
traces from an EOSIO blockchain (such as EOS, Telos or WAX), and
providing convenient indexes for quick searching.

A typical use case for Memento is tracking incoming and outgoing
payments for a specific account, and checking their reversibility.

The database may contain traces for all contracts on the blockchain,
or it may be limited to a specific subset of accounts. Dapps can set
up their own instances, while public infrastructure providers may
offer the service to a wider audience.

The tool supports active-standby redundancy with two writer processes:
the standby process records its data in BKP_TRACES table, and monitors
the master activity. Once it notices that the master stopped updating
the database, it takes over the active role and continues writing the
blockchain events. The old master switches to standby mode in this
case.

The standby writer is also cleaning up transactions that are older
than a specified retention period. If you are running Memento in a
single-writer mode, you can launch the `memento_dataguard` service
that will clean the old transactions.

Memento needs one or two EOSIO state history feeds. It uses
[Chronicle](https://github.com/EOSChronicleProject/eos-chronicle)
software for decoding the state history.

Installation
------------

Installation instructions for an Ubuntu 20.04 host. In this example, a
single writer is taking a WAX state history feed.

```
## Build Chronicle

apt install -y gnupg software-properties-common
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | apt-key add -
apt-add-repository 'deb https://apt.kitware.com/ubuntu/ focal main'
add-apt-repository -y ppa:ubuntu-toolchain-r/test
apt update && apt install -y git g++-8 cmake libssl-dev libgmp-dev zlib1g-dev
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8

mkdir /opt/src
cd /opt/src

wget https://boostorg.jfrog.io/artifactory/main/release/1.67.0/source/boost_1_67_0.tar.gz
tar -xvzf boost_1_67_0.tar.gz
cd boost_1_67_0
./bootstrap.sh
nice ./b2 install -j10


cd /opt/src
git clone https://github.com/EOSChronicleProject/eos-chronicle.git
cd eos-chronicle
git submodule update --init --recursive
mkdir build
cd build
cmake ..
nice make -j 10 install

cp /opt/src/eos-chronicle/systemd/chronicle_receiver\@.service /etc/systemd/system/
systemctl daemon-reload


## Database engine

apt-get update && apt-get install -y mariadb-server mariadb-client

## Perl modules for the writer

apt install -y cpanminus libjson-xs-perl libjson-perl libmysqlclient-dev libdbi-perl libwww-perl make gcc
cpanm --notest Net::WebSocket::Server
cpanm --notest DBD::MariaDB

## Memento writer

git clone https://github.com/cc32d9/eosio_memento.git /opt/eosio_memento
cd /opt/eosio_memento
cp systemd/*.service /etc/systemd/system/
systemctl daemon-reload

## initialize the MariaDB users
sh sql/create_db_users.sh

## create the database for WAX history
sh sql/create_memento_db.sh memento_wax

## writer service. Here 8806 is the TCP port is where Chronicle will send its output.
## You need to pick the port number that is free on your host.
## The first writer has always id=1, and the second writer should have id=2 if you
## run it in dual-writer mode.
echo 'DBWRITER_OPTS="--id=1 --port=8806 --database=memento_wax --keepdays=1"' >/etc/default/memento_wax1
systemctl enable memento_dbwriter@wax1
systemctl start memento_dbwriter@wax1


## Chronicle initialization. Here host and port point to the EOSIO state history source
mkdir -p /srv/memento_wax1/chronicle-config
cat >/srv/memento_wax1/chronicle-config/config.ini <<'EOT'
host = 10.0.3.1
port = 8080
mode = scan
skip-block-events = true
plugin = exp_ws_plugin
exp-ws-host = 127.0.0.1
exp-ws-port = 8806
exp-ws-bin-header = true
skip-table-deltas = true
skip-account-info = true
EOT

# You need to initialize the Chronicle database from the first block
# in the state history archive. See the Chronicle Tutorial for more
# details. You may point it to some other state history source during
# the initialization. Here we launch it in scan-noexport mode for faster initialization.
/usr/local/sbin/chronicle-receiver --config-dir=/srv/memento_wax1/chronicle-config \
 --data-dir=/srv/memento_wax1/chronicle-data \
 --host=my.ship.host.domain.com --port=8080 \
 --start-block=186332760 --mode=scan-noexport --end-block=186332800

# Once it stops at the end block, launch the service
systemctl enable chronicle_receiver@memento_wax1
systemctl start chronicle_receiver@memento_wax1

# the database will catch up to the head block, and will be ready fort use.
journalctl -u memento_dbwriter@wax1 -f
```

If you want to launch Memento in dual-writer mode, you need to add a
line into SYNC table and start another writer with `--id=2` option. It
makes sense to run the second writer on an independent host, with an
independent state history source.

```
# adding the second writer
mysql memento_wax --execute="INSERT INTO SYNC (sourceid, block_num, block_time, irreversible, is_master, last_updated) values (2,0, '2000-01-01',0, 0, '2000-01-01')"
```

If you run the service in single-writer mode, use the dataguard to delete old traces:

```
# enabling dataguard for data pruning
echo 'DBWRITER_OPTS="--database=memento_wax --keepdays=7"' >/etc/default/memento_dataguard_wax
systemctl enable memento_dataguard@wax
systemctl start memento_dataguard@wax
```


Example queries
---------------

```
# get the current Last irreversible block number (LIR)
SELECT MIN(irreversible) FROM SYNC;


# get all system token transfers related to the account,
# starting from specific block number, but not newer than LIR
SELECT seq, ACTIONS.block_num, block_time, trx_id, trace from TRANSACTIONS
JOIN ACTIONS USING(seq) JOIN RECEIPTS USING (seq) JOIN TRACES using (seq)
WHERE ACTIONS.contract='eosio.token' AND ACTIONS.action='transfer' AND
  RECEIPTS.block_num>186201662 AND
  RECEIPTS.block_num < (SELECT MIN(irreversible) FROM SYNC) AND
  RECEIPTS.account_name='arenaofglory';


# get all transactions related to a specific account, starting from
# specified sequence number (including reversible transactions which might
# get dropped or reordered afterwards
SELECT seq, TRANSACTIONS.block_num, block_time, trx_id, trace from TRANSACTIONS
JOIN RECEIPTS USING (seq) JOIN TRACES using (seq)
WHERE 
  RECEIPTS.account_name='ysjki.c.wam' AND
  RECEIPTS.seq > 50662607733;  

```


Public access
-------------

See [MEMENTO_PUBLIC_ACCESS.md](MEMENTO_PUBLIC_ACCESS.md).


Acknowledgments
----------------

This work was sponsored by [EOS Amsterdam](https://www.apache.org/licenses/LICENSE-2.0.txt) block producer.

Copyright 2022 cc32d9@gmail.com
