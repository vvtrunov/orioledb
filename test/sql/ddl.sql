CREATE SCHEMA ddl;
SET SESSION search_path = 'ddl';
CREATE EXTENSION orioledb;

CREATE TABLE o_ddl_check
(
	f1 text,
	f2 varchar,
	f3 integer,
	PRIMARY KEY(f1)
) USING orioledb;

SELECT * FROM o_ddl_check;
INSERT INTO o_ddl_check VALUES ('1', NULL, NULL);
-- Fails because of NULL values
ALTER TABLE o_ddl_check ALTER f2 SET NOT NULL;
TRUNCATE o_ddl_check;
INSERT INTO o_ddl_check VALUES ('1', '2', NULL);
-- OK
ALTER TABLE o_ddl_check ALTER f2 SET NOT NULL;

DROP TABLE o_ddl_check;
SELECT orioledb_parallel_debug_start();
CREATE TABLE o_ddl_check
(
	f1 text NOT NULL COLLATE "C",
	f2 varchar NOT NULL,
	f3 integer,
	PRIMARY KEY (f1)
) USING orioledb;
INSERT INTO o_ddl_check VALUES ('ABC1', 'ABC2', NULL);
-- Fails, because of NOT NULL constraint
INSERT INTO o_ddl_check VALUES ('2', NULL, '3');
-- Fails, because of unique constraint
INSERT INTO o_ddl_check VALUES ('ABC1', '2', '3');

INSERT INTO o_ddl_check VALUES ('ABC2', 'ABC4', NULL);
INSERT INTO o_ddl_check VALUES ('ABC3', 'ABC6', NULL);
SELECT orioledb_parallel_debug_stop();

SELECT * FROM o_ddl_check;
SELECT orioledb_table_description('o_ddl_check'::regclass);

-- Fails because can't drop NOT NULL contraint on PK
ALTER TABLE o_ddl_check ALTER f1 DROP NOT NULL;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- Fails on unknown option
ALTER TABLE o_ddl_check OPTIONS (SET hello 'world');

ALTER TABLE o_ddl_check ALTER f2 DROP NOT NULL;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
ALTER TABLE o_ddl_check ALTER f2 SET NOT NULL;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

ALTER TABLE o_ddl_check DROP f2;
ALTER TABLE o_ddl_check DROP f1;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

DROP TABLE o_ddl_check;
CREATE TABLE o_ddl_check
(
	f1 varchar COLLATE "C",
	f2 text NOT NULL,
	PRIMARY KEY(f1)
) USING orioledb;

INSERT INTO o_ddl_check VALUES ('a', NULL);
INSERT INTO o_ddl_check VALUES (NULL, 'b');
INSERT INTO o_ddl_check VALUES ('a', 'b');
UPDATE o_ddl_check SET f1 = NULL WHERE f1 = 'a';
SELECT * FROM o_ddl_check;
ALTER TABLE o_ddl_check ADD CHECK (f2 < 'f');
INSERT INTO o_ddl_check VALUES ('b', 'ddd');
INSERT INTO o_ddl_check VALUES ('c', 'ffff');

CREATE UNIQUE INDEX o_ddl_check_f2_idx ON o_ddl_check(f2);
ALTER TABLE o_ddl_check ALTER f2 DROP NOT NULL;
ALTER TABLE o_ddl_check ALTER f2 SET NOT NULL;

-- Check partition consraint.
CREATE TABLE o_ddl_parted
(
	f1 varchar COLLATE "C",
	f2 text NOT NULL
) PARTITION BY RANGE (f1);
ALTER TABLE o_ddl_parted ATTACH PARTITION o_ddl_check FOR VALUES FROM ('a') TO ('d');
SELECT * FROM o_ddl_parted;

INSERT INTO o_ddl_parted VALUES ('abc', 'def');
-- OK
UPDATE o_ddl_parted SET f1 = 'bcd' WHERE f1 = 'abc';
-- Partition constraint failure
UPDATE o_ddl_parted SET f1 = 'efg' WHERE f1 = 'bcd';
SELECT * FROM o_ddl_parted;

CREATE TABLE o_ddl_check_2
(
	f1 varchar COLLATE "C",
	f2 text NOT NULL,
	PRIMARY KEY(f1)
) USING orioledb;

ALTER TABLE o_ddl_parted ATTACH PARTITION o_ddl_check_2 FOR VALUES FROM ('e') TO ('h');
-- Move row between partitions
UPDATE o_ddl_parted SET f1 = 'efg' WHERE f1 = 'bcd';
SELECT * FROM o_ddl_parted;
ALTER TABLE o_ddl_parted DETACH PARTITION o_ddl_check;
ALTER TABLE o_ddl_parted DETACH PARTITION o_ddl_check_2;
DROP TABLE o_ddl_parted;

DROP TABLE o_ddl_check;
DROP TABLE o_ddl_check_2;

CREATE TABLE o_ddl_check
(
	f1 int NOT NULL,
	f2 int,
	f3 int,
	f4 int,
	PRIMARY KEY(f1)
) USING orioledb;
CREATE UNIQUE INDEX o_ddl_check_unique ON o_test24 (f2, f3, f4);
CREATE INDEX o_ddl_check_regular ON o_test24 (f2, f3, f4);

INSERT INTO o_ddl_check VALUES (1, 2, NULL, 5);
INSERT INTO o_ddl_check VALUES (2, 2, NULL, 3);
INSERT INTO o_ddl_check VALUES (3, 2, NULL, 2);
INSERT INTO o_ddl_check VALUES (4, 1, NULL, 4);
INSERT INTO o_ddl_check VALUES (5, 2, NULL, 3);
INSERT INTO o_ddl_check VALUES (6, 2, NULL, NULL);
INSERT INTO o_ddl_check VALUES (7, 2, NULL, NULL);

SELECT * FROM o_ddl_check;
SELECT orioledb_tbl_structure('o_ddl_check'::regclass, 'nue');

DROP TABLE o_ddl_check;

CREATE TABLE o_ddl_missing (
	i int4 NOT NULL
) USING orioledb;
INSERT INTO o_ddl_missing SELECT * FROM generate_series(1, 10);
ALTER TABLE o_ddl_missing ADD COLUMN l int4;
SELECT * FROM o_ddl_missing;
ALTER TABLE o_ddl_missing ADD COLUMN m int4 DEFAULT 2;
SELECT * FROM o_ddl_missing;
ALTER TABLE o_ddl_missing ADD COLUMN n int4, ADD COLUMN o int4[];
SELECT * FROM o_ddl_missing;
UPDATE o_ddl_missing SET l = 5, n = 6, o = '{1, 5, 2}' WHERE i BETWEEN 3 AND 7;
SELECT * FROM o_ddl_missing;
ALTER TABLE o_ddl_missing
	DROP COLUMN m,
	ADD COLUMN p int4[] DEFAULT '{2, 4, 8}',
	ADD COLUMN r int4[];
SELECT * FROM o_ddl_missing;

CREATE FUNCTION pseudo_random(seed bigint, i bigint) RETURNS float8 AS
$$
	SELECT substr(sha256(($1::text || ' ' || $2::text)::bytea)::text,2,16)::bit(52)::bigint::float8 / pow(2.0, 52.0);
$$ LANGUAGE sql;

CREATE SEQUENCE o_test_add_column_id_seq2;
CREATE TABLE o_test_add_column
(
	id serial primary key,
	i int4,
	v int4 default nextval('o_test_add_column_id_seq2'::regclass)
) USING orioledb;
\d o_test_add_column
SELECT orioledb_tbl_indices('o_test_add_column'::regclass);
SELECT orioledb_tbl_structure('o_test_add_column'::regclass, 'ne');

INSERT INTO o_test_add_column VALUES (0, 15, NULL);
INSERT INTO o_test_add_column (i)
	SELECT pseudo_random(1, v) * 20000 FROM generate_series(1,10) v;

-- test new null column
ALTER TABLE o_test_add_column ADD COLUMN y int4;
\d o_test_add_column
SELECT orioledb_tbl_indices('o_test_add_column'::regclass);
SELECT orioledb_tbl_structure('o_test_add_column'::regclass, 'ne');

-- test new column with volatile default
ALTER TABLE o_test_add_column ADD COLUMN z int4 default 5;
\d o_test_add_column
SELECT orioledb_tbl_indices('o_test_add_column'::regclass);
SELECT orioledb_tbl_structure('o_test_add_column'::regclass, 'ne');

CREATE SEQUENCE o_test_j_seq;

-- test new column with non-volatile default
ALTER TABLE o_test_add_column
	ADD COLUMN j int4 not null default pseudo_random(2, nextval('o_test_j_seq')) * 20000;
\d o_test_add_column
SELECT orioledb_tbl_indices('o_test_add_column'::regclass);
SELECT orioledb_tbl_structure('o_test_add_column'::regclass, 'ne');

INSERT INTO o_test_add_column (i)
	SELECT pseudo_random(3, v) * 20000 FROM generate_series(1,5) v;
SELECT orioledb_tbl_structure('o_test_add_column'::regclass, 'ne');
EXPLAIN (COSTS OFF) SELECT * FROM o_test_add_column;
SELECT * FROM o_test_add_column;
-- Test that default fields not recalculated
SELECT * FROM o_test_add_column;

-- Test primary key usage after rewrite
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_add_column ORDER BY id;
SELECT * FROM o_test_add_column ORDER BY id;
COMMIT;

CREATE TABLE o_test_multiple_analyzes (
    aid integer NOT NULL PRIMARY KEY
) USING orioledb;


-- Wrapper function, which converts result of SQL query to the text
CREATE OR REPLACE FUNCTION query_to_text(sql TEXT) RETURNS SETOF TEXT AS $$
	BEGIN
		RETURN QUERY EXECUTE sql;
	END $$
LANGUAGE plpgsql;

INSERT INTO o_test_multiple_analyzes
	SELECT aid FROM generate_series(1, 10) aid;
BEGIN;
select count(1) from o_test_multiple_analyzes;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('explain (analyze, buffers)
	select * from o_test_multiple_analyzes ORDER BY aid DESC LIMIT 10;') as t;
SELECT regexp_replace(t, '[\d\.]+', 'x', 'g')
FROM query_to_text('explain (analyze, buffers)
	select count(1) from o_test_multiple_analyzes;') as t;
ROLLBACK;

CREATE FOREIGN DATA WRAPPER dummy;
CREATE SERVER s0 FOREIGN DATA WRAPPER dummy;
CREATE FOREIGN TABLE ft1 (
	c1 integer OPTIONS ("param 1" 'val1') NOT NULL,
	c2 text OPTIONS (param2 'val2', param3 'val3') CHECK (c2 <> ''),
	c3 date,
	CHECK (c3 BETWEEN '1994-01-01'::date AND '1994-01-31'::date)
) SERVER s0 OPTIONS (delimiter ',', quote '"', "be quoted" 'value');

DROP FOREIGN DATA WRAPPER dummy CASCADE;

CREATE TABLE o_unexisting_column
(
	key int4,
	PRIMARY KEY(key)
) USING orioledb;

ALTER TABLE o_unexisting_column ALTER COLUMN key_2 SET DEFAULT 5;
ALTER TABLE o_unexisting_column ALTER COLUMN key_2 DROP DEFAULT;
ALTER TABLE o_unexisting_column RENAME COLUMN key_2 TO key_3;
ALTER TABLE o_unexisting_column DROP COLUMN key_2;
ALTER TABLE o_unexisting_column ALTER COLUMN key_2 SET NOT NULL;
ALTER TABLE o_unexisting_column ALTER COLUMN key_2 DROP NOT NULL;
ALTER TABLE o_unexisting_column ALTER key_2 TYPE int;
ALTER TABLE o_unexisting_column ALTER key_2 TYPE int USING key_2::integer;
ALTER TABLE o_unexisting_column ALTER COLUMN key_2
	ADD GENERATED ALWAYS AS IDENTITY;
ALTER TABLE o_unexisting_column ALTER COLUMN key
	ADD GENERATED ALWAYS AS IDENTITY;

UPDATE o_unexisting_column SET key_2 = 4 WHERE key = 2;

CREATE TABLE o_test_unique_on_conflict (
	key int
) USING orioledb;

CREATE UNIQUE INDEX ON o_test_unique_on_conflict(key);

INSERT INTO o_test_unique_on_conflict(key)
	(SELECT key FROM generate_series (1, 1) key);
INSERT INTO o_test_unique_on_conflict (key)
	SELECT * FROM generate_series(1, 1)
	ON CONFLICT (key) DO UPDATE
		SET key = o_test_unique_on_conflict.key + 100;
SELECT * FROM o_test_unique_on_conflict;

CREATE TABLE o_test_update_set_renamed_column(
	val_1 int PRIMARY KEY,
	val_2 int
) USING orioledb;

INSERT INTO o_test_update_set_renamed_column(val_1, val_2)
	(SELECT val_1, val_1 FROM generate_series (1, 1) val_1);
SELECT * FROM o_test_update_set_renamed_column;

ALTER TABLE o_test_update_set_renamed_column RENAME COLUMN val_2 to val_3;

UPDATE o_test_update_set_renamed_column SET val_3 = 5;

SELECT * FROM o_test_update_set_renamed_column;

CREATE TABLE o_test_inherits_1 (
  val_1 int PRIMARY KEY
) USING orioledb;

CREATE TABLE o_test_inherits_2 (
	val_2 int
) INHERITS (o_test_inherits_1) USING orioledb;

BEGIN;
CREATE TABLE o_test(
	id integer NOT NULL,
	val text NOT NULL,
	PRIMARY KEY(id),
	UNIQUE(id, val)
) USING orioledb;
CREATE TABLE o_test_child(
	id integer NOT NULL,
	o_test_ID integer NOT NULL REFERENCES o_test (id),
	PRIMARY KEY(id)
) USING orioledb;
INSERT INTO o_test(id, val) VALUES (1, 'hello');
INSERT INTO o_test(id, val) VALUES (2, 'hey');
DELETE FROM o_test where id = 1;
COMMIT;

CREATE TABLE o_test_opcoptions_reset (
	val_1 int NOT NULL,
	val_3 text DEFAULT 'abc'
) USING orioledb;

INSERT INTO o_test_opcoptions_reset (val_1) VALUES (1);

BEGIN;
CREATE INDEX o_test_opcoptions_reset_idx1 ON o_test_opcoptions_reset (val_3);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_opcoptions_reset;
SELECT * FROM o_test_opcoptions_reset;
ALTER TABLE o_test_opcoptions_reset ADD PRIMARY KEY (val_1);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_opcoptions_reset;
SELECT * FROM o_test_opcoptions_reset;
COMMIT;

CREATE TABLE o_test_null_hasdef (
	val_1	int DEFAULT 1,
	val_2	text,
	val_3	text DEFAULT 'a'
) USING orioledb;

INSERT INTO o_test_null_hasdef VALUES (3);
INSERT INTO o_test_null_hasdef VALUES (4, NULL);
INSERT INTO o_test_null_hasdef VALUES (5, 'b', NULL);
INSERT INTO o_test_null_hasdef VALUES (6, NULL, NULL);
SELECT orioledb_tbl_structure('o_test_null_hasdef'::regclass, 'nue');
SELECT * FROM o_test_null_hasdef;

CREATE VIEW test_view_1 AS SELECT * FROM o_test_null_hasdef;

CREATE rule test_view_1 AS
	ON INSERT TO test_view_1
	  DO INSTEAD INSERT INTO o_test_null_hasdef SELECT new.*;

INSERT INTO test_view_1 VALUES (7);

SELECT orioledb_tbl_structure('o_test_null_hasdef'::regclass, 'nue');
SELECT * FROM test_view_1;
SELECT * FROM o_test_null_hasdef;

CREATE TABLE o_test_float_default (
  val_1 int DEFAULT 1,
  val_2 text DEFAULT 'a',
  val_3 float8 DEFAULT 1.1
)USING orioledb;
INSERT INTO o_test_float_default VALUES (2, null, 2.0);
SELECT * FROM o_test_float_default;

CREATE TABLE o_test_duplicate_key_fields (
	val_2 int,
	val_1 int
) USING orioledb;

CREATE INDEX o_test_duplicate_key_fields_ix1
	ON o_test_duplicate_key_fields (val_1, val_2, val_1) INCLUDE (val_1);

INSERT INTO o_test_duplicate_key_fields SELECT v, v * 10 FROM generate_series(1, 5) v;

SELECT orioledb_tbl_indices('o_test_duplicate_key_fields'::regclass);
SELECT orioledb_tbl_structure('o_test_duplicate_key_fields'::regclass, 'nue');

SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT val_1 FROM o_test_duplicate_key_fields ORDER BY val_1;
SELECT val_1 FROM o_test_duplicate_key_fields ORDER BY val_1;
RESET enable_seqscan;

CREATE TABLE o_test_pkey_fields_same_as_index (
	val_1 int,
	val_2 int,
	val_3 int,
	UNIQUE (val_1, val_3)
) USING orioledb;
SELECT orioledb_tbl_indices('o_test_pkey_fields_same_as_index'::regclass);

SET enable_seqscan = off;

INSERT INTO o_test_pkey_fields_same_as_index
	SELECT 1 * 10 ^ v, 2 * 10 ^ v, 3 * 10 ^ v FROM generate_series(0, 2) v;

EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;
SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;;

ALTER TABLE o_test_pkey_fields_same_as_index ADD PRIMARY KEY (val_1, val_3);
SELECT orioledb_tbl_indices('o_test_pkey_fields_same_as_index'::regclass);
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;
SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;

ALTER TABLE o_test_pkey_fields_same_as_index
	DROP CONSTRAINT o_test_pkey_fields_same_as_index_pkey;
SELECT orioledb_tbl_indices('o_test_pkey_fields_same_as_index'::regclass);
EXPLAIN (COSTS OFF)
	SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;
SELECT * FROM o_test_pkey_fields_same_as_index ORDER BY val_1;

RESET enable_seqscan;

CREATE TABLE o_test_null_pkey_field (
	val_1 text,
	val_2 text,
	val_3 text
) USING orioledb;

ALTER TABLE o_test_null_pkey_field ADD COLUMN val_10 text;

INSERT INTO o_test_null_pkey_field
	SELECT 1 * 10 ^ v, 2 * 10 ^ v, 3 * 10 ^ v
		FROM generate_series(0, 2) v;

ALTER TABLE o_test_null_pkey_field ADD PRIMARY KEY (val_1, val_3, val_10);
SELECT orioledb_tbl_indices('o_test_null_pkey_field'::regclass);
SELECT orioledb_tbl_structure('o_test_null_pkey_field'::regclass, 'nue');
SELECT * FROM o_test_null_pkey_field;

CREATE TABLE o_test_included_ix_name (
	a int,
	b int,
	c int,
	d int
) USING orioledb;
ALTER TABLE o_test_included_ix_name ADD PRIMARY KEY (d);
\d o_test_included_ix_name
CREATE INDEX ON o_test_included_ix_name (a, b) INCLUDE (a, c);
\d o_test_included_ix_name

CREATE TABLE o_test_add_pkey_empty_index (
	a int,
	b int,
	c int,
	d int8
) USING orioledb;
CREATE INDEX ON o_test_add_pkey_empty_index (a, b);
\d o_test_add_pkey_empty_index
SELECT orioledb_tbl_indices('o_test_add_pkey_empty_index'::regclass);
ALTER TABLE o_test_add_pkey_empty_index ADD PRIMARY KEY (d);
\d o_test_add_pkey_empty_index
SELECT orioledb_tbl_indices('o_test_add_pkey_empty_index'::regclass);
INSERT INTO o_test_add_pkey_empty_index
	SELECT v, v*10, v*100, v*1000 FROM generate_series(1, 5) v;
EXPLAIN (COSTS OFF) SELECT a, b, d FROM o_test_add_pkey_empty_index ORDER BY a;
SELECT a, b, d FROM o_test_add_pkey_empty_index ORDER BY a;
SELECT orioledb_tbl_structure('o_test_add_pkey_empty_index'::regclass, 'nue');
\d o_test_add_pkey_empty_index
SELECT orioledb_tbl_indices('o_test_add_pkey_empty_index'::regclass);

CREATE TABLE o_test_empty() USING orioledb;
\d o_test_empty
SELECT orioledb_table_description('o_test_empty'::regclass);
SELECT * FROM o_test_empty;
SELECT orioledb_tbl_structure('o_test_empty'::regclass, 'nue');
TRUNCATE o_test_empty;
SELECT * FROM o_test_empty;

CREATE FUNCTION o_test_plpgsql_default_func(a int)
RETURNS TEXT
AS $$
    BEGIN
		RETURN 'WOW' || a;
    END;
$$ LANGUAGE plpgsql;
CREATE TABLE o_test_plpgsql_default (
    val_1 int DEFAULT LENGTH(o_test_plpgsql_default_func(6))
) USING orioledb;

CREATE TABLE test_35_columns (
  gid serial,
  col1 varchar(1),
  col2 varchar(1),
  col3 varchar(1),
  col4 varchar(1),
  col5 varchar(1),
  col6 varchar(1),
  col7 varchar(1),
  col8 varchar(1),
  col9 varchar(1),
  col10 varchar(1),
  col11 varchar(1),
  col12 varchar(1),
  col13 varchar(1),
  col14 varchar(1),
  col15 varchar(1),
  col16 varchar(1),
  col17 varchar(1),
  col18 varchar(1),
  col19 varchar(1),
  col20 varchar(1),
  col21 varchar(1),
  col22 varchar(1),
  col23 varchar(1),
  col24 varchar(1),
  col25 varchar(1),
  col26 varchar(1),
  col27 varchar(1),
  col28 varchar(1),
  col29 varchar(1),
  col30 varchar(1),
  col31 varchar(1),
  col32 varchar(1),
  col33 varchar(1),
  col34 varchar(1)
) using orioledb;

INSERT INTO test_35_columns (col27, col10) VALUES ('A', 'J');
SELECT gid, col10, col15, col27, col33, col34 FROM test_35_columns;

CREATE TABLE test_replica_identity_set (i int PRIMARY KEY, t text) USING orioledb;
INSERT INTO test_replica_identity_set VALUES(1, 'foofoo');
INSERT INTO test_replica_identity_set VALUES(2, 'barbar');
ALTER TABLE test_replica_identity_set REPLICA IDENTITY FULL;
INSERT INTO test_replica_identity_set VALUES(3, 'aaaaaa');
SELECT * FROM test_replica_identity_set;
\d+ test_replica_identity_set

CREATE TABLE test_replica_identity_fail (i int PRIMARY KEY, t text) USING orioledb;
INSERT INTO test_replica_identity_fail VALUES(1, 'foofoo');
INSERT INTO test_replica_identity_fail VALUES(2, 'barbar');
ALTER TABLE test_replica_identity_fail REPLICA IDENTITY NOTHING;
INSERT INTO test_replica_identity_fail VALUES(3, 'aaaaaa');
SELECT * FROM test_replica_identity_fail;
\d+ test_replica_identity_fail

CREATE TABLE test_set_access_method_fail (i int PRIMARY KEY, t text) USING orioledb;
ALTER TABLE test_set_access_method_fail SET ACCESS METHOD heap;

-- Test AT_SetStatistics
CREATE TABLE test_set_statistics (
	i int PRIMARY KEY,
	t text,
	v varchar
) USING orioledb;

INSERT INTO test_set_statistics VALUES (1, 'test', 'data');

-- Set statistics target for columns
ALTER TABLE test_set_statistics ALTER COLUMN t SET STATISTICS 100;
ALTER TABLE test_set_statistics ALTER COLUMN v SET STATISTICS 1000;

-- Verify the changes
SELECT attname, attstattarget
FROM pg_attribute
WHERE attrelid = 'test_set_statistics'::regclass
  AND attnum > 0
ORDER BY attnum;

-- Reset statistics to default
ALTER TABLE test_set_statistics ALTER COLUMN t SET STATISTICS DEFAULT;

SELECT attname, attstattarget
FROM pg_attribute
WHERE attrelid = 'test_set_statistics'::regclass
  AND attname = 't';

-- Test AT_SetLogged / AT_SetUnLogged
CREATE UNLOGGED TABLE test_logged_changes (
	i int PRIMARY KEY,
	t text
) USING orioledb;

-- Check initial unlogged state
SELECT relname, relpersistence
FROM pg_class
WHERE relname = 'test_logged_changes';

-- Change to logged
ALTER TABLE test_logged_changes SET LOGGED;

SELECT relname, relpersistence
FROM pg_class
WHERE relname = 'test_logged_changes';

-- Change back to unlogged
ALTER TABLE test_logged_changes SET UNLOGGED;

SELECT relname, relpersistence
FROM pg_class
WHERE relname = 'test_logged_changes';

-- Test with data
INSERT INTO test_logged_changes VALUES (1, 'test data');
ALTER TABLE test_logged_changes SET LOGGED;

SELECT * FROM test_logged_changes;

-- Test AT_SetOptions / AT_ResetOptions (column-level options)
CREATE TABLE test_column_options (
	i int PRIMARY KEY,
	t text
) USING orioledb;

-- Set column-level storage options
ALTER TABLE test_column_options ALTER COLUMN t SET (n_distinct = 100, n_distinct_inherited = 50);

-- Verify options are set
SELECT attname, attoptions
FROM pg_attribute
WHERE attrelid = 'test_column_options'::regclass
  AND attnum > 0
  AND attoptions IS NOT NULL;

-- Reset specific option
ALTER TABLE test_column_options ALTER COLUMN t RESET (n_distinct);

-- Verify reset
SELECT attname, attoptions
FROM pg_attribute
WHERE attrelid = 'test_column_options'::regclass
  AND attname = 't';

-- Reset all options
ALTER TABLE test_column_options ALTER COLUMN t RESET (n_distinct_inherited);

SELECT attname, attoptions
FROM pg_attribute
WHERE attrelid = 'test_column_options'::regclass
  AND attname = 't';

-- Test AT_ResetRelOptions / AT_SetRelOptions (table-level options)
CREATE TABLE test_table_options (
	i int PRIMARY KEY,
	t text
) USING orioledb;

-- Set table-level options
ALTER TABLE test_table_options SET (fillfactor = 70, autovacuum_enabled = false);

-- Verify table options
SELECT relname, reloptions
FROM pg_class
WHERE relname = 'test_table_options';

-- Reset specific option
ALTER TABLE test_table_options RESET (autovacuum_enabled);

SELECT relname, reloptions
FROM pg_class
WHERE relname = 'test_table_options';

-- Reset all options
ALTER TABLE test_table_options RESET (fillfactor);

SELECT relname, reloptions
FROM pg_class
WHERE relname = 'test_table_options';

-- Test AT_ClusterOn / AT_DropCluster
CREATE TABLE test_cluster (
	i int,
	t text,
	v varchar,
	PRIMARY KEY (i)
) USING orioledb;

CREATE INDEX test_cluster_idx ON test_cluster(t);

-- Set cluster index
ALTER TABLE test_cluster CLUSTER ON test_cluster_idx;

-- Verify cluster setting
SELECT indexrelid::regclass AS index_name, indisclustered
FROM pg_index
WHERE indrelid = 'test_cluster'::regclass
ORDER BY indexrelid::regclass::text;

-- Drop cluster setting
ALTER TABLE test_cluster SET WITHOUT CLUSTER;

-- Verify cluster removed
SELECT indexrelid::regclass AS index_name, indisclustered
FROM pg_index
WHERE indrelid = 'test_cluster'::regclass
ORDER BY indexrelid::regclass::text;

-- Test AT_EnableRule / AT_DisableRule (on tables)
-- ENABLE/DISABLE RULE commands only work on tables, not views
CREATE TABLE test_rule_table (
	i int PRIMARY KEY,
	t text
) USING orioledb;

-- Create a rule on the table that filters certain inserts
CREATE RULE test_insert_rule AS
	ON INSERT TO test_rule_table
	WHERE t = 'skip'
	DO INSTEAD NOTHING;

-- Verify rule is enabled (ev_enabled = 'O' means origin)
SELECT rulename, ev_enabled
FROM pg_rewrite
WHERE rulename = 'test_insert_rule';

-- Test that rule works: insert with 'skip' should be ignored
INSERT INTO test_rule_table VALUES (1, 'skip');
INSERT INTO test_rule_table VALUES (2, 'normal');
SELECT * FROM test_rule_table ORDER BY i;

-- Disable the rule
ALTER TABLE test_rule_table DISABLE RULE test_insert_rule;

-- Verify rule is disabled (ev_enabled = 'D')
SELECT rulename, ev_enabled
FROM pg_rewrite
WHERE rulename = 'test_insert_rule';

-- Now the 'skip' insert should work since rule is disabled
INSERT INTO test_rule_table VALUES (1, 'skip');
SELECT * FROM test_rule_table ORDER BY i;

-- Enable the rule back (origin mode)
ALTER TABLE test_rule_table ENABLE RULE test_insert_rule;

SELECT rulename, ev_enabled
FROM pg_rewrite
WHERE rulename = 'test_insert_rule';

-- Enable rule for replica (ev_enabled = 'R')
ALTER TABLE test_rule_table ENABLE REPLICA RULE test_insert_rule;

SELECT rulename, ev_enabled
FROM pg_rewrite
WHERE rulename = 'test_insert_rule';

-- Enable rule always (ev_enabled = 'A')
ALTER TABLE test_rule_table ENABLE ALWAYS RULE test_insert_rule;

SELECT rulename, ev_enabled
FROM pg_rewrite
WHERE rulename = 'test_insert_rule';

-- Cleanup
DROP TABLE test_rule_table CASCADE;

-- Test AT_CheckNotNull (internally generated for partitioned tables)
-- AT_CheckNotNull is generated when you use ALTER TABLE ONLY ... SET NOT NULL
-- on a partitioned table. It checks that child partitions already have NOT NULL.

-- Create a partitioned table
CREATE TABLE test_check_not_null (
	i int,
	val text NOT NULL
) PARTITION BY RANGE (i) USING orioledb;

-- Create partitions with NOT NULL already set
CREATE TABLE test_check_not_null_p1 PARTITION OF test_check_not_null
	FOR VALUES FROM (1) TO (100) USING orioledb;

CREATE TABLE test_check_not_null_p2 PARTITION OF test_check_not_null
	FOR VALUES FROM (100) TO (200) USING orioledb;

-- Insert test data
INSERT INTO test_check_not_null VALUES (1, 'abc'), (50, 'def'), (150, 'ghi');

-- Verify partitions exist
SELECT tablename FROM pg_tables
WHERE schemaname = 'ddl' AND tablename LIKE 'test_check_not_null%'
ORDER BY tablename;

-- Verify val column is already NOT NULL in all partitions
SELECT c.relname, a.attname, a.attnotnull
FROM pg_class c
JOIN pg_attribute a ON a.attrelid = c.oid
WHERE c.relname LIKE 'test_check_not_null%'
  AND a.attname = 'val'
  AND c.relnamespace = 'ddl'::regnamespace
ORDER BY c.relname;

-- Now use ALTER TABLE ONLY ... SET NOT NULL on parent
-- This internally generates AT_CheckNotNull for each partition
-- to verify they already have NOT NULL (which they do)
ALTER TABLE ONLY test_check_not_null ALTER COLUMN val SET NOT NULL;

-- Verify the operation succeeded
SELECT c.relname, a.attname, a.attnotnull
FROM pg_class c
JOIN pg_attribute a ON a.attrelid = c.oid
WHERE c.relname = 'test_check_not_null'
  AND a.attname = 'val'
  AND c.relnamespace = 'ddl'::regnamespace;

-- Test that NOT NULL is enforced
INSERT INTO test_check_not_null VALUES (75, NULL);

-- Verify data is still correct
SELECT * FROM test_check_not_null ORDER BY i;

-- Test AT_CheckNotNull failure case: partition without NOT NULL
CREATE TABLE test_check_not_null_fail (
	i int,
	val text  -- Note: no NOT NULL here!
) PARTITION BY RANGE (i) USING orioledb;

CREATE TABLE test_check_not_null_fail_p1 PARTITION OF test_check_not_null_fail
	FOR VALUES FROM (1) TO (100) USING orioledb;

-- Try to set NOT NULL on parent ONLY (should fail because partition doesn't have NOT NULL)
ALTER TABLE ONLY test_check_not_null_fail ALTER COLUMN val SET NOT NULL;

-- Test AT_ValidateConstraint (validate a NOT VALID constraint)
CREATE TABLE test_validate_constraint (
	i int PRIMARY KEY,
	t text
) USING orioledb;

-- Insert some data
INSERT INTO test_validate_constraint VALUES (1, 'test'), (2, 'data');

-- Add a check constraint without validation
ALTER TABLE test_validate_constraint ADD CONSTRAINT check_t_length CHECK (length(t) > 2) NOT VALID;

-- Verify constraint exists but not validated
SELECT conname, convalidated
FROM pg_constraint
WHERE conrelid = 'test_validate_constraint'::regclass
  AND conname = 'check_t_length';

-- Now validate the constraint
ALTER TABLE test_validate_constraint VALIDATE CONSTRAINT check_t_length;

-- Verify constraint is now validated
SELECT conname, convalidated
FROM pg_constraint
WHERE conrelid = 'test_validate_constraint'::regclass
  AND conname = 'check_t_length';

-- Test AT_SetTableSpace (change tablespace)
-- Note: This test assumes default tablespace exists
CREATE TABLE test_tablespace (
	i int PRIMARY KEY,
	t text
) USING orioledb;

-- Try to set tablespace (may be no-op if no custom tablespace)
-- This tests that the subcommand doesn't error out
ALTER TABLE test_tablespace SET TABLESPACE pg_default;

-- Test AT_GenericOptions (for foreign tables, but we test the subcommand handling)
-- This is mainly to ensure the subcommand is accepted for OrioleDB tables
-- even though it may not do anything meaningful

-- Test constraint operations with existing ddl test patterns
CREATE TABLE test_constraint_ops (
	i int PRIMARY KEY,
	val int,
	CHECK (val > 0)
) USING orioledb;

INSERT INTO test_constraint_ops VALUES (1, 10), (2, 20);

-- Verify data
SELECT * FROM test_constraint_ops ORDER BY i;

-- Test dropping constraint
ALTER TABLE test_constraint_ops DROP CONSTRAINT test_constraint_ops_val_check;

-- Now we can insert negative values
INSERT INTO test_constraint_ops VALUES (3, -5);

SELECT * FROM test_constraint_ops ORDER BY i;

-- Test ReAdd* subcommands (triggered during table rewrites)
-- These subcommands are used internally when ALTER TABLE causes a table rewrite

-- Test AT_ReAddConstraint (triggered by ALTER TYPE with constraints)
CREATE TABLE test_readd_constraint (
	i int PRIMARY KEY,
	val int CHECK (val > 0)
) USING orioledb;

INSERT INTO test_readd_constraint VALUES (1, 100), (2, 200);

-- Verify constraint exists
SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'test_readd_constraint'::regclass
  AND contype = 'c'
ORDER BY conname;

-- Change column type - this causes table rewrite and ReAddConstraint
ALTER TABLE test_readd_constraint ALTER COLUMN val TYPE bigint;

-- Verify constraint still exists after rewrite
SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'test_readd_constraint'::regclass
  AND contype = 'c'
ORDER BY conname;

-- Verify constraint still works
INSERT INTO test_readd_constraint VALUES (3, -5);

SELECT * FROM test_readd_constraint ORDER BY i;

-- Test AT_ReAddIndex (triggered by ALTER TYPE on indexed columns)
CREATE TABLE test_readd_index (
	i int PRIMARY KEY,
	code int,
	name text
) USING orioledb;

CREATE INDEX test_readd_index_code_idx ON test_readd_index(code);
CREATE INDEX test_readd_index_name_idx ON test_readd_index(name);

INSERT INTO test_readd_index VALUES (1, 100, 'alice'), (2, 200, 'bob');

-- Verify indexes exist
SELECT indexname
FROM pg_indexes
WHERE tablename = 'test_readd_index'
  AND schemaname = 'ddl'
ORDER BY indexname;

-- Change non-indexed column type - causes table rewrite, indexes are preserved
ALTER TABLE test_readd_index ALTER COLUMN name TYPE varchar(100);

-- Verify indexes still exist after rewrite
SELECT indexname
FROM pg_indexes
WHERE tablename = 'test_readd_index'
  AND schemaname = 'ddl'
ORDER BY indexname;

-- Verify indexes still work
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM test_readd_index WHERE code = 100;
SELECT * FROM test_readd_index WHERE code = 100;
RESET enable_seqscan;

-- Test AT_ReAddStatistics (triggered by table rewrite with statistics)
CREATE TABLE test_readd_statistics (
	i int PRIMARY KEY,
	val int,
	txt text
) USING orioledb;

-- Set custom statistics targets
ALTER TABLE test_readd_statistics ALTER COLUMN val SET STATISTICS 500;
ALTER TABLE test_readd_statistics ALTER COLUMN txt SET STATISTICS 1000;

-- Verify statistics targets are set
SELECT attname, attstattarget
FROM pg_attribute
WHERE attrelid = 'test_readd_statistics'::regclass
  AND attnum > 0
ORDER BY attnum;

-- Cause a table rewrite by changing a column type
ALTER TABLE test_readd_statistics ALTER COLUMN i TYPE bigint;

-- Verify statistics targets are preserved after rewrite
SELECT attname, attstattarget
FROM pg_attribute
WHERE attrelid = 'test_readd_statistics'::regclass
  AND attnum > 0
ORDER BY attnum;

-- Test AT_ReAddComment (triggered by table rewrite with column comments)
CREATE TABLE test_readd_comment (
	i int PRIMARY KEY,
	val int
) USING orioledb;

-- Add comments to columns
COMMENT ON COLUMN test_readd_comment.i IS 'Primary key column';
COMMENT ON COLUMN test_readd_comment.val IS 'Value column';

-- Verify comments exist
SELECT a.attname, d.description
FROM pg_attribute a
LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum
WHERE a.attrelid = 'test_readd_comment'::regclass
  AND a.attnum > 0
ORDER BY a.attnum;

-- Cause table rewrite
ALTER TABLE test_readd_comment ALTER COLUMN val TYPE bigint;

-- Verify comments are preserved after rewrite
SELECT a.attname, d.description
FROM pg_attribute a
LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum
WHERE a.attrelid = 'test_readd_comment'::regclass
  AND a.attnum > 0
ORDER BY a.attnum;

-- Test AT_ReplaceRelOptions (used by CREATE OR REPLACE VIEW with options)
-- AT_ReplaceRelOptions is triggered internally when CREATE OR REPLACE VIEW
-- changes the view's options (security_barrier, security_invoker, check_option)
CREATE TABLE test_view_base (
	i int PRIMARY KEY,
	t text,
	val int
) USING orioledb;

INSERT INTO test_view_base VALUES (1, 'alice', 100), (2, 'bob', 200), (3, 'charlie', 300);

-- Create view without options
CREATE VIEW test_replace_view AS SELECT * FROM test_view_base WHERE val > 0;

-- Check initial view options (should be NULL or empty)
SELECT relname, relkind, reloptions
FROM pg_class
WHERE relname = 'test_replace_view';

-- Use CREATE OR REPLACE VIEW to add security_barrier option
-- This triggers AT_ReplaceRelOptions internally
CREATE OR REPLACE VIEW test_replace_view WITH (security_barrier=true)
AS SELECT * FROM test_view_base WHERE val > 100;

-- Verify security_barrier option is set
SELECT relname, relkind, reloptions
FROM pg_class
WHERE relname = 'test_replace_view';

-- Test the view still works
SELECT * FROM test_replace_view ORDER BY i;

-- Replace view again with different options (security_invoker)
-- This replaces the entire options list with new one
CREATE OR REPLACE VIEW test_replace_view WITH (security_invoker=true)
AS SELECT * FROM test_view_base WHERE val > 50;

-- Verify options replaced (should now have security_invoker, not security_barrier)
SELECT relname, relkind, reloptions
FROM pg_class
WHERE relname = 'test_replace_view';

SELECT * FROM test_replace_view ORDER BY i;

-- Replace view with multiple options
CREATE OR REPLACE VIEW test_replace_view
WITH (security_barrier=true, security_invoker=true, check_option=local)
AS SELECT * FROM test_view_base WHERE val > 0;

-- Verify multiple options set
SELECT relname, relkind, reloptions
FROM pg_class
WHERE relname = 'test_replace_view';

-- Replace view with no options (clears all options)
CREATE OR REPLACE VIEW test_replace_view
AS SELECT * FROM test_view_base WHERE val >= 100;

-- Verify options cleared
SELECT relname, relkind, reloptions
FROM pg_class
WHERE relname = 'test_replace_view';

SELECT * FROM test_replace_view ORDER BY i;

DROP VIEW test_replace_view;
DROP TABLE test_view_base CASCADE;

-- Test AT_ReAddDomainConstraint (domain constraints during table rewrite)
-- Domain constraints need to be re-verified when table is rewritten
-- Create a domain with CHECK constraint
CREATE DOMAIN positive_int AS int CHECK (VALUE > 0);
CREATE DOMAIN email_type AS varchar(100) CHECK (VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

CREATE TABLE test_readd_domain_constraint (
	i int PRIMARY KEY,
	quantity positive_int,
	contact_email email_type
) USING orioledb;

-- Insert valid data
INSERT INTO test_readd_domain_constraint VALUES (1, 100, 'user@example.com');
INSERT INTO test_readd_domain_constraint VALUES (2, 50, 'admin@test.org');

-- Verify domain constraints work before rewrite
INSERT INTO test_readd_domain_constraint VALUES (3, -5, 'valid@email.com');  -- Should fail: negative quantity
INSERT INTO test_readd_domain_constraint VALUES (4, 10, 'invalid-email');    -- Should fail: invalid email format

SELECT * FROM test_readd_domain_constraint ORDER BY i;

-- Verify domain constraints exist in catalog
SELECT t.typname, c.conname, c.consrc
FROM pg_constraint c
JOIN pg_type t ON t.oid = c.contypid
WHERE t.typname IN ('positive_int', 'email_type')
ORDER BY t.typname, c.conname;

-- Cause table rewrite by changing a different column
-- This should trigger AT_ReAddDomainConstraint for the domain columns
ALTER TABLE test_readd_domain_constraint ADD COLUMN extra_data text;
ALTER TABLE test_readd_domain_constraint ALTER COLUMN extra_data TYPE varchar(50);

-- Verify domain constraints still work after rewrite
INSERT INTO test_readd_domain_constraint (i, quantity, contact_email) VALUES (5, -10, 'test@example.com');  -- Should fail
INSERT INTO test_readd_domain_constraint (i, quantity, contact_email) VALUES (6, 20, 'bad-email');          -- Should fail
INSERT INTO test_readd_domain_constraint (i, quantity, contact_email) VALUES (7, 75, 'good@email.com');     -- Should succeed

SELECT * FROM test_readd_domain_constraint ORDER BY i;

-- Verify domain constraints still exist after rewrite
SELECT t.typname, c.conname, c.consrc
FROM pg_constraint c
JOIN pg_type t ON t.oid = c.contypid
WHERE t.typname IN ('positive_int', 'email_type')
ORDER BY t.typname, c.conname;

-- Test with domain that has NOT NULL constraint
CREATE DOMAIN nonempty_text AS text NOT NULL CHECK (length(VALUE) > 0);

CREATE TABLE test_domain_not_null (
	i int PRIMARY KEY,
	description nonempty_text
) USING orioledb;

INSERT INTO test_domain_not_null VALUES (1, 'Valid description');
INSERT INTO test_domain_not_null VALUES (2, NULL);  -- Should fail: NOT NULL
INSERT INTO test_domain_not_null VALUES (3, '');    -- Should fail: length check

SELECT * FROM test_domain_not_null ORDER BY i;

-- Cause rewrite
ALTER TABLE test_domain_not_null ALTER COLUMN i TYPE bigint;

-- Verify constraints still enforced after rewrite
INSERT INTO test_domain_not_null VALUES (4, NULL);  -- Should fail
INSERT INTO test_domain_not_null VALUES (5, '');    -- Should fail
INSERT INTO test_domain_not_null VALUES (6, 'Another valid description');  -- Should succeed

SELECT * FROM test_domain_not_null ORDER BY i;

-- Cleanup domains
DROP TABLE test_domain_not_null CASCADE;
DROP TABLE test_readd_domain_constraint CASCADE;
DROP DOMAIN nonempty_text;
DROP DOMAIN email_type;
DROP DOMAIN positive_int;

-- Test AT_SetCompression (column compression method)
-- PostgreSQL supports compression methods: pglz (default), lz4
CREATE TABLE test_set_compression (
	i int PRIMARY KEY,
	data1 text
) USING orioledb;

-- Check initial compression settings (should be DEFAULT or empty)
SELECT attname, attcompression
FROM pg_attribute
WHERE attrelid = 'test_set_compression'::regclass
  AND attname = 'data1'
ORDER BY attname;

-- Set compression method for data1 column
ALTER TABLE test_set_compression ALTER COLUMN data1 SET COMPRESSION pglz;

SELECT attname, attcompression
FROM pg_attribute
WHERE attrelid = 'test_set_compression'::regclass
  AND attname = 'data1'
ORDER BY attname;

DROP TABLE test_set_compression CASCADE;

-- Test AT_SetExpression (ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION)
-- AT_SetExpression allows changing the generation expression for a STORED generated column
-- This feature is available in PostgreSQL 17+
CREATE TABLE test_set_expression (
	i int PRIMARY KEY,
	price numeric(10,2),
	quantity int,
	total numeric(10,2) GENERATED ALWAYS AS (price * quantity) STORED
) USING orioledb;

-- Insert test data
INSERT INTO test_set_expression (i, price, quantity) VALUES (1, 10.50, 5);
INSERT INTO test_set_expression (i, price, quantity) VALUES (2, 25.00, 3);
INSERT INTO test_set_expression (i, price, quantity) VALUES (3, 7.99, 10);

-- Verify initial generated column values (price * quantity)
SELECT i, price, quantity, total FROM test_set_expression ORDER BY i;

-- Check the initial generated column expression in catalog
SELECT a.attname, a.attgenerated, pg_get_expr(d.adbin, d.adrelid) as generation_expr
FROM pg_attribute a
JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = 'test_set_expression'::regclass
  AND a.attname = 'total';

-- Use AT_SetExpression to change the generation formula
-- Change from (price * quantity) to (price * quantity * 1.1) to add 10% markup
ALTER TABLE test_set_expression
  ALTER COLUMN total SET EXPRESSION AS (price * quantity * 1.1);

-- Verify the expression was updated in catalog
SELECT a.attname, a.attgenerated, pg_get_expr(d.adbin, d.adrelid) as generation_expr
FROM pg_attribute a
JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = 'test_set_expression'::regclass
  AND a.attname = 'total';

-- Verify all existing values were recalculated with new expression
SELECT i, price, quantity, total,
	(price * quantity * 1.1) as expected_total
FROM test_set_expression
ORDER BY i;

UPDATE test_set_expression SET price = 15.00 WHERE i = 1;

-- Verify recalculation after update
SELECT i, price, quantity, total,
	(price * quantity * 1.1) as expected_total
FROM test_set_expression
ORDER BY i;

-- Insert new row to verify new expression is used for new data
INSERT INTO test_set_expression (i, price, quantity) VALUES (4, 20.00, 2);

SELECT i, price, quantity, total,
	(price * quantity * 1.1) as expected_total
FROM test_set_expression
ORDER BY i;

DROP TABLE test_set_expression CASCADE;

-- Test trigger enable/disable commands
-- AT_EnableTrig, AT_DisableTrig, AT_EnableAlwaysTrig, AT_EnableReplicaTrig
-- AT_EnableTrigAll, AT_DisableTrigAll, AT_EnableTrigUser, AT_DisableTrigUser

-- Create a table for trigger testing
CREATE TABLE test_trigger_table (
	i int PRIMARY KEY,
	val text,
	modified_count int DEFAULT 0
) USING orioledb;

-- Create a log table to track trigger executions
CREATE TABLE test_trigger_log (
	log_id serial PRIMARY KEY,
	trigger_name text,
	operation text,
	old_val text,
	new_val text,
	fired_at timestamp DEFAULT now()
) USING orioledb;

-- Create trigger function that logs operations
CREATE OR REPLACE FUNCTION test_trigger_func() RETURNS trigger AS $$
BEGIN
	INSERT INTO test_trigger_log (trigger_name, operation, old_val, new_val)
	VALUES (TG_NAME, TG_OP,
		CASE WHEN TG_OP = 'DELETE' THEN OLD.val ELSE NULL END,
		CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN NEW.val ELSE NULL END);

	IF TG_OP = 'UPDATE' THEN
		NEW.modified_count := OLD.modified_count + 1;
	END IF;

	RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql;

-- Create multiple triggers with different types
CREATE TRIGGER trigger_before_insert
	BEFORE INSERT ON test_trigger_table
	FOR EACH ROW EXECUTE FUNCTION test_trigger_func();

CREATE TRIGGER trigger_after_insert
	AFTER INSERT ON test_trigger_table
	FOR EACH ROW EXECUTE FUNCTION test_trigger_func();

CREATE TRIGGER trigger_before_update
	BEFORE UPDATE ON test_trigger_table
	FOR EACH ROW EXECUTE FUNCTION test_trigger_func();

CREATE TRIGGER trigger_after_delete
	AFTER DELETE ON test_trigger_table
	FOR EACH ROW EXECUTE FUNCTION test_trigger_func();

-- Check initial trigger states (all should be enabled: tgenabled = 'O' for origin)
SELECT tgname, tgenabled, tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgname;

-- Insert data and verify triggers fire
INSERT INTO test_trigger_table (i, val) VALUES (1, 'first');
INSERT INTO test_trigger_table (i, val) VALUES (2, 'second');

-- Check trigger log (should have 4 entries: 2 before_insert + 2 after_insert)
SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

-- Clear log
TRUNCATE test_trigger_log;

-- Test AT_DisableTrig - disable specific trigger
ALTER TABLE test_trigger_table DISABLE TRIGGER trigger_before_insert;

-- Verify trigger is disabled (tgenabled = 'D')
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgname;

-- Insert should only fire after_insert trigger (not before_insert)
INSERT INTO test_trigger_table (i, val) VALUES (3, 'third');

SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

TRUNCATE test_trigger_log;

-- Test AT_EnableTrig - re-enable the trigger (origin mode)
ALTER TABLE test_trigger_table ENABLE TRIGGER trigger_before_insert;

-- Verify trigger is enabled again (tgenabled = 'O')
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
  AND tgname = 'trigger_before_insert';

-- Insert should fire both triggers again
INSERT INTO test_trigger_table (i, val) VALUES (4, 'fourth');

SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

TRUNCATE test_trigger_log;

-- Test AT_EnableReplicaTrig - enable for replica mode (tgenabled = 'R')
ALTER TABLE test_trigger_table ENABLE REPLICA TRIGGER trigger_before_update;

-- Verify trigger mode changed to replica
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
  AND tgname = 'trigger_before_update';

-- Test AT_EnableAlwaysTrig - enable to always fire (tgenabled = 'A')
ALTER TABLE test_trigger_table ENABLE ALWAYS TRIGGER trigger_after_delete;

-- Verify trigger mode changed to always
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
  AND tgname = 'trigger_after_delete';

-- Test AT_DisableTrigAll - disable all triggers on the table
ALTER TABLE test_trigger_table DISABLE TRIGGER ALL;

-- Verify all triggers are disabled (tgenabled = 'D')
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgname;

-- Operations should not fire any triggers
INSERT INTO test_trigger_table (i, val) VALUES (5, 'fifth');
UPDATE test_trigger_table SET val = 'updated' WHERE i = 1;
DELETE FROM test_trigger_table WHERE i = 5;

-- Log should be empty (no triggers fired)
SELECT COUNT(*) as trigger_fire_count FROM test_trigger_log;

-- Test AT_EnableTrigAll - enable all triggers
ALTER TABLE test_trigger_table ENABLE TRIGGER ALL;

-- Verify all triggers are enabled (tgenabled = 'O')
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgname;

-- Operations should fire triggers again
INSERT INTO test_trigger_table (i, val) VALUES (6, 'sixth');

SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

TRUNCATE test_trigger_log;

-- Test AT_DisableTrigUser - disable user triggers only
-- First, let's create a constraint trigger to differentiate
CREATE TABLE test_trigger_ref (
	ref_id int PRIMARY KEY
) USING orioledb;

INSERT INTO test_trigger_ref VALUES (1), (2), (3), (4), (6), (7), (8);

-- Add foreign key which creates a constraint trigger (internal trigger)
ALTER TABLE test_trigger_table
	ADD CONSTRAINT fk_test_ref FOREIGN KEY (i) REFERENCES test_trigger_ref(ref_id);

-- Check triggers now (should have user triggers + internal FK triggers)
SELECT tgname, tgenabled, tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgisinternal, tgname;

-- Disable only user triggers (not internal FK triggers)
ALTER TABLE test_trigger_table DISABLE TRIGGER USER;

-- Verify: user triggers disabled, internal triggers still enabled
SELECT tgname, tgenabled, tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgisinternal, tgname;

-- Insert should not fire user triggers but FK constraint should still work
INSERT INTO test_trigger_table (i, val) VALUES (7, 'seventh');

-- Log should be empty (user triggers disabled)
SELECT COUNT(*) as trigger_fire_count FROM test_trigger_log;

-- Try to violate FK constraint (should still fail - internal trigger works)
INSERT INTO test_trigger_table (i, val) VALUES (99, 'invalid');  -- Should fail FK

-- Test AT_EnableTrigUser - enable user triggers only
ALTER TABLE test_trigger_table ENABLE TRIGGER USER;

-- Verify: user triggers enabled back to origin mode
SELECT tgname, tgenabled, tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgisinternal, tgname;

-- Insert should fire user triggers again
INSERT INTO test_trigger_table (i, val) VALUES (8, 'eighth');

SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

-- Test combination: DISABLE ALL then ENABLE USER
ALTER TABLE test_trigger_table DISABLE TRIGGER ALL;

SELECT tgname, tgenabled, tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgisinternal, tgname;

ALTER TABLE test_trigger_table ENABLE TRIGGER USER;

-- User triggers should be enabled, internal triggers still disabled
SELECT tgname, tgenabled, tgisinternal
FROM pg_trigger
WHERE tgrelid = 'test_trigger_table'::regclass
ORDER BY tgisinternal, tgname;

TRUNCATE test_trigger_log;

INSERT INTO test_trigger_table (i, val) VALUES (99, 'invalid');  -- Now succeed

SELECT trigger_name, operation, new_val
FROM test_trigger_log
ORDER BY log_id;

-- Cleanup
DROP TABLE test_trigger_table CASCADE;
DROP TABLE test_trigger_ref CASCADE;
DROP TABLE test_trigger_log CASCADE;
DROP FUNCTION test_trigger_func();

-- Test AT_ForceRowSecurity and AT_NoForceRowSecurity
-- Row-Level Security (RLS) allows table owners to bypass RLS policies by default
-- FORCE ROW SECURITY makes RLS policies apply even to table owner

-- Create test users for RLS testing
CREATE ROLE rls_test_owner;
CREATE ROLE rls_test_user;

-- Grant necessary permissions
GRANT USAGE ON SCHEMA ddl TO rls_test_owner, rls_test_user;
GRANT CREATE ON SCHEMA ddl TO rls_test_owner;

-- Create table as the owner
SET ROLE rls_test_owner;

CREATE TABLE test_rls_table (
	i int PRIMARY KEY,
	department text,
	employee_name text,
	salary numeric(10,2)
) USING orioledb;

-- Insert test data
INSERT INTO test_rls_table VALUES
	(1, 'HR', 'Alice', 70000),
	(2, 'HR', 'Bob', 65000),
	(3, 'IT', 'Charlie', 80000),
	(4, 'IT', 'Diana', 85000),
	(5, 'Sales', 'Eve', 60000);

-- Enable row level security on the table
ALTER TABLE test_rls_table ENABLE ROW LEVEL SECURITY;

-- Check initial RLS settings (relrowsecurity should be true, relforcerowsecurity should be false)
SELECT relname, relrowsecurity, relforcerowsecurity
FROM pg_class
WHERE relname = 'test_rls_table';

-- Create a policy that only shows HR department records
CREATE POLICY hr_policy ON test_rls_table
	FOR SELECT
	USING (department = 'HR');

-- Grant select permission to test user
GRANT SELECT ON test_rls_table TO rls_test_user;

-- As owner, we can see ALL rows (owner bypasses RLS by default)
SELECT i, department, employee_name, salary
FROM test_rls_table
ORDER BY i;

-- Switch to regular user - should only see HR department due to policy
SET ROLE rls_test_user;

SELECT i, department, employee_name, salary
FROM test_rls_table
ORDER BY i;

-- Switch back to owner
SET ROLE rls_test_owner;

-- Test AT_ForceRowSecurity - force RLS policies to apply even to owner
ALTER TABLE test_rls_table FORCE ROW LEVEL SECURITY;

-- Verify setting changed (relforcerowsecurity = true)
SELECT relname, relrowsecurity, relforcerowsecurity
FROM pg_class
WHERE relname = 'test_rls_table';

-- Now even as owner, we should only see HR department rows
SELECT i, department, employee_name, salary
FROM test_rls_table
ORDER BY i;

-- Verify the policy is actually being enforced for owner now
SELECT COUNT(*) as visible_rows FROM test_rls_table;  -- Should be 2 (only HR)

-- Test AT_NoForceRowSecurity - allow owner to bypass RLS again
ALTER TABLE test_rls_table NO FORCE ROW LEVEL SECURITY;

-- Verify setting changed back (relforcerowsecurity = false)
SELECT relname, relrowsecurity, relforcerowsecurity
FROM pg_class
WHERE relname = 'test_rls_table';

-- Owner should now see all rows again (bypassing RLS)
SELECT i, department, employee_name, salary
FROM test_rls_table
ORDER BY i;

SELECT COUNT(*) as visible_rows FROM test_rls_table;  -- Should be 5 (all rows)

-- Regular user still affected by policy
SET ROLE rls_test_user;

SELECT COUNT(*) as visible_rows FROM test_rls_table;  -- Should be 2 (only HR)

-- Cleanup
RESET ROLE;
DROP TABLE test_rls_table CASCADE;
DROP ROLE rls_test_owner;
DROP ROLE rls_test_user;

-- Test AT_AddOf and AT_DropOf (typed tables)
-- Typed tables are tables that are bound to a composite type
-- AT_AddOf converts a regular table to a typed table
-- AT_DropOf converts a typed table back to a regular table

-- Create a composite type for employee data
CREATE TYPE employee_type AS (
	emp_id int,
	emp_name text,
	emp_salary numeric(10,2)
);

-- Create a regular table (not typed)
CREATE TABLE test_regular_table (
	emp_id int PRIMARY KEY,
	emp_name text,
	emp_salary numeric(10,2)
) USING orioledb;

-- Check initial state (reloftype should be 0 for regular table)
SELECT relname, reloftype, relkind
FROM pg_class
WHERE relname = 'test_regular_table';

-- Insert test data
INSERT INTO test_regular_table VALUES (1, 'Alice', 70000);
INSERT INTO test_regular_table VALUES (2, 'Bob', 65000);

SELECT * FROM test_regular_table ORDER BY emp_id;

-- Test AT_AddOf - convert regular table to typed table
ALTER TABLE test_regular_table OF employee_type;

-- Verify table is now typed (reloftype should be OID of employee_type)
SELECT c.relname, c.reloftype, t.typname
FROM pg_class c
LEFT JOIN pg_type t ON c.reloftype = t.oid
WHERE c.relname = 'test_regular_table';

-- Verify data is preserved
SELECT * FROM test_regular_table ORDER BY emp_id;

-- Typed tables still work normally for DML
INSERT INTO test_regular_table VALUES (3, 'Charlie', 80000);
UPDATE test_regular_table SET emp_salary = 72000 WHERE emp_id = 1;
DELETE FROM test_regular_table WHERE emp_id = 2;

SELECT * FROM test_regular_table ORDER BY emp_id;

-- Test that ADD COLUMN fails on typed table (must modify type instead)
ALTER TABLE test_regular_table ADD COLUMN emp_department text;  -- Should fail

-- Test that DROP COLUMN fails on typed table
ALTER TABLE test_regular_table DROP COLUMN emp_salary;  -- Should fail

-- Test AT_DropOf - convert typed table back to regular table
ALTER TABLE test_regular_table NOT OF;

-- Verify table is no longer typed (reloftype should be 0)
SELECT c.relname, c.reloftype
FROM pg_class c
WHERE c.relname = 'test_regular_table';

-- Verify data is still preserved
SELECT * FROM test_regular_table ORDER BY emp_id;

-- Regular table operations still work, including ADD COLUMN
INSERT INTO test_regular_table VALUES (4, 'Diana', 85000);

-- Now that it's a regular table, ADD COLUMN should succeed
ALTER TABLE test_regular_table ADD COLUMN emp_department text;

-- Verify new column exists
SELECT * FROM test_regular_table ORDER BY emp_id;

-- Create a typed table directly using OF syntax
CREATE TABLE test_typed_table OF employee_type (
	PRIMARY KEY (emp_id)
) USING orioledb;

-- Verify it's typed from creation
SELECT c.relname, c.reloftype, t.typname
FROM pg_class c
LEFT JOIN pg_type t ON c.reloftype = t.oid
WHERE c.relname = 'test_typed_table';

-- Insert data into typed table
INSERT INTO test_typed_table VALUES (10, 'Eve', 60000);
INSERT INTO test_typed_table VALUES (11, 'Frank', 62000);

SELECT * FROM test_typed_table ORDER BY emp_id;

-- Convert it to regular table using AT_DropOf
ALTER TABLE test_typed_table NOT OF;

-- Verify it's no longer typed
SELECT c.relname, c.reloftype
FROM pg_class c
WHERE c.relname = 'test_typed_table';

-- Data still accessible
SELECT * FROM test_typed_table ORDER BY emp_id;

ALTER TABLE test_typed_table ADD COLUMN emp_department text;
SELECT * FROM test_typed_table ORDER BY emp_id;

-- Cleanup
DROP TABLE test_typed_table CASCADE;
DROP TABLE test_regular_table CASCADE;
DROP TYPE employee_type;

-- Test AT_AlterConstraint
-- Tests altering constraint attributes (DEFERRABLE, INITIALLY DEFERRED/IMMEDIATE)

CREATE TABLE test_alter_constraint_ref (
	i int PRIMARY KEY
) USING orioledb;

CREATE TABLE test_alter_constraint (
	i int PRIMARY KEY,
	ref_id int,
	val text,
	CONSTRAINT fk_test FOREIGN KEY (ref_id) REFERENCES test_alter_constraint_ref(i)
) USING orioledb;

-- Insert reference data
INSERT INTO test_alter_constraint_ref VALUES (1), (2), (3);

-- Check initial constraint properties (NOT DEFERRABLE by default)
SELECT conname, condeferrable, condeferred
FROM pg_constraint
WHERE conname = 'fk_test';

-- Test AT_AlterConstraint: Make constraint DEFERRABLE INITIALLY DEFERRED
ALTER TABLE test_alter_constraint
	ALTER CONSTRAINT fk_test DEFERRABLE INITIALLY DEFERRED;

-- Verify constraint properties changed
SELECT conname, condeferrable, condeferred
FROM pg_constraint
WHERE conname = 'fk_test';

-- Test deferred constraint behavior
BEGIN;
-- This should succeed because constraint is deferred
INSERT INTO test_alter_constraint VALUES (1, 99, 'test');
-- Check that invalid reference exists temporarily
SELECT * FROM test_alter_constraint WHERE ref_id = 99;
-- This should fail on commit
COMMIT;

-- Test AT_AlterConstraint: Change to DEFERRABLE INITIALLY IMMEDIATE
ALTER TABLE test_alter_constraint
	ALTER CONSTRAINT fk_test DEFERRABLE INITIALLY IMMEDIATE;

-- Verify constraint properties changed
SELECT conname, condeferrable, condeferred
FROM pg_constraint
WHERE conname = 'fk_test';

-- Even though constraint is INITIALLY IMMEDIATE, we can defer it in a transaction
BEGIN;
SET CONSTRAINTS fk_test DEFERRED;
INSERT INTO test_alter_constraint VALUES (2, 88, 'test2');
SELECT * FROM test_alter_constraint WHERE ref_id = 88;
COMMIT;

-- Test AT_AlterConstraint: Make constraint NOT DEFERRABLE
ALTER TABLE test_alter_constraint
	ALTER CONSTRAINT fk_test NOT DEFERRABLE;

-- Verify constraint properties changed back
SELECT conname, condeferrable, condeferred
FROM pg_constraint
WHERE conname = 'fk_test';

-- Now constraint cannot be deferred
BEGIN;
-- This should fail immediately (constraint not deferrable)
INSERT INTO test_alter_constraint VALUES (3, 77, 'test3');
ROLLBACK;

-- Cleanup
DROP TABLE test_alter_constraint CASCADE;
DROP TABLE test_alter_constraint_ref CASCADE;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA ddl CASCADE;
RESET search_path;
