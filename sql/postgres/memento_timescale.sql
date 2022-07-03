
/* one or two writers are updating their current status here */
CREATE TABLE SYNC
(
 sourceid          INT PRIMARY KEY,
 block_num         BIGINT NOT NULL,
 block_time        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
 irreversible      BIGINT NOT NULL,
 is_master         SMALLINT NOT NULL,
 last_updated      TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

INSERT INTO SYNC (sourceid, block_num, block_time, irreversible, is_master, last_updated)
VALUES (1,0, '2000-01-01 00:00',0, 1, '2000-01-01 00:00');

INSERT INTO SYNC (sourceid, block_num, block_time, irreversible, is_master, last_updated)
VALUES (2,0, '2000-01-01 00:00',0, 0, '2000-01-01 00:00');


/*
   parent table for transactions.
   "seq"  is a unique sequence number supplied by the blockchain.
   It may be reassigned to a different transaction after a microfork.
*/ 
CREATE TABLE TRANSACTIONS
(
 seq            BIGINT NOT NULL,
 block_num      BIGINT NOT NULL,
 block_time     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
 trx_id         VARCHAR(64) NOT NULL,
 trace          BYTEA NOT NULL
);

SELECT create_hypertable('TRANSACTIONS','block_time');

CREATE INDEX TRANSACTIONS_I01 ON TRANSACTIONS (seq);
CREATE INDEX TRANSACTIONS_I02 ON TRANSACTIONS (block_num);
CREATE INDEX TRANSACTIONS_I03 ON TRANSACTIONS (trx_id);



/* all receipt recipients for each transaction */
CREATE TABLE RECEIPTS
(
 seq            BIGINT NOT NULL,
 block_num      BIGINT NOT NULL,
 block_time     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
 account_name   VARCHAR(13) NOT NULL
);

SELECT create_hypertable('RECEIPTS','block_time');

CREATE INDEX RECEIPTS_I01 ON RECEIPTS (block_num);
CREATE INDEX RECEIPTS_I02 ON RECEIPTS (account_name, seq);
CREATE INDEX RECEIPTS_I03 ON RECEIPTS (account_name, block_num);
CREATE INDEX RECEIPTS_I04 ON RECEIPTS (seq);

/* smart contracts and ACTIONS in each transaction */
CREATE TABLE ACTIONS
(
 seq            BIGINT NOT NULL,
 block_num      BIGINT NOT NULL,
 block_time     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
 contract       VARCHAR(13) NOT NULL,
 action         VARCHAR(13) NOT NULL
);

SELECT create_hypertable('ACTIONS','block_time');

CREATE INDEX ACTIONS_I01 ON ACTIONS (block_num);
CREATE INDEX ACTIONS_I02 ON ACTIONS (contract, action, seq);
CREATE INDEX ACTIONS_I03 ON ACTIONS (contract, action, block_num);
CREATE INDEX ACTIONS_I04 ON ACTIONS (contract, block_num);
CREATE INDEX ACTIONS_I05 ON ACTIONS (seq);


/* this table is used internally for dual-writer setup. Not for user access */
CREATE TABLE BKP_TRACES
(
 seq            BIGINT PRIMARY KEY,
 block_num      BIGINT NOT NULL,
 block_time     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
 trx_id         VARCHAR(64) NOT NULL,
 trace          BYTEA NOT NULL
);

CREATE INDEX BKP_TRACES_I01 ON BKP_TRACES (block_num);
