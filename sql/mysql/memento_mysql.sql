
/* one or two writers are updating their current status here */
CREATE TABLE SYNC
(
 sourceid          INT PRIMARY KEY,
 block_num         BIGINT NOT NULL,
 block_time        DATETIME NOT NULL,
 irreversible      BIGINT NOT NULL,
 is_master         SMALLINT NOT NULL,
 last_updated      DATETIME NOT NULL
) ENGINE=InnoDB;

INSERT INTO SYNC (sourceid, block_num, block_time, irreversible, is_master, last_updated) values (1,0, '2000-01-01',0, 1, '2000-01-01');

/*
   parent table for transactions.
   "seq"  is a unique sequence number supplied by the blockchain.
   It is the global sequence number of the first receipt in the transaction.
   It may be reassigned to a different transaction after a microfork.
*/
CREATE TABLE TRANSACTIONS
(
 seq            BIGINT UNSIGNED PRIMARY KEY,
 block_num      BIGINT NOT NULL,
 block_time     DATETIME NOT NULL,
 trx_id         VARCHAR(64) NOT NULL,
 trace          MEDIUMBLOB NOT NULL
)  ENGINE=InnoDB;

CREATE INDEX TRANSACTIONS_I01 ON TRANSACTIONS (block_num);
CREATE INDEX TRANSACTIONS_I01 ON TRANSACTIONS (trx_id(8));


/* all receipt recipients for each transaction */
CREATE TABLE RECEIPTS
(
 seq                    BIGINT UNSIGNED NOT NULL,
 block_num              BIGINT NOT NULL,
 block_time             DATETIME NOT NULL,
 contract               VARCHAR(13) NOT NULL,
 action                 VARCHAR(13) NOT NULL,
 receiver               VARCHAR(13) NOT NULL,
 recv_sequence          BIGINT NOT NULL,
 PRIMARY KEY (receiver, contract, action, recv_sequence, seq)
)  ENGINE=InnoDB;


CREATE INDEX RECEIPTS_I01 ON RECEIPTS (block_num);
CREATE INDEX RECEIPTS_I02 ON RECEIPTS (receiver, block_time, seq);
CREATE INDEX RECEIPTS_I03 ON RECEIPTS (receiver, recv_sequence, seq);
CREATE INDEX RECEIPTS_I04 ON RECEIPTS (receiver, contract, action, block_time, seq);


/* latest recorded recv_sequence for each account */
CREATE TABLE RECV_SEQUENCE_MAX
(
 account_name           VARCHAR(13) PRIMARY KEY,
 recv_sequence_max      BIGINT NOT NULL
)  ENGINE=InnoDB;



/* this table is used internally for dual-writer setup. Not for user access */
CREATE TABLE BKP_TRACES
(
 seq            BIGINT UNSIGNED PRIMARY KEY,
 block_num      BIGINT NOT NULL,
 block_time     DATETIME NOT NULL,
 trx_id         VARCHAR(64) NOT NULL,
 trace          MEDIUMBLOB NOT NULL
)  ENGINE=InnoDB;

CREATE INDEX BKP_TRACES_I01 ON BKP_TRACES (block_num);
