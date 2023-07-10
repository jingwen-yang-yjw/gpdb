-- This file is used to test the feature that there are multiple remote postgres servers.

-- ===================================================================
-- create FDW objects
-- ===================================================================
SET timezone = 'PST8PDT';
SET optimizer_trace_fallback = on;
SET optimizer = off;
-- If gp_enable_minmax_optimization is on, it won't generate aggregate functions pushdown plan.
SET gp_enable_minmax_optimization = off;

-- Clean
-- start_ignore
DROP EXTENSION IF EXISTS postgres_fdw CASCADE;
-- end_ignore

CREATE EXTENSION postgres_fdw;

CREATE SERVER pgserver FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (dbname 'contrib_regression', multi_hosts 'localhost localhost',
           multi_ports '5432 5555', num_segments '2', mpp_execute 'multi servers');

CREATE USER MAPPING FOR CURRENT_USER SERVER pgserver;

-- ===================================================================
-- create objects used through FDW pgserver server
-- ===================================================================
-- remote postgres server 1 -- listening port 5432
\! env PGOPTIONS='' psql -p 5432 contrib_regression -f sql/postgres_sql/mpp_gp2pg_postgres_init_1.sql
-- remote postgres server 2 -- listening port 5555
\! env PGOPTIONS='' psql -p 5555 contrib_regression -f sql/postgres_sql/mpp_gp2pg_postgres_init_2.sql

-- ===================================================================
-- create foreign tables
-- ===================================================================
CREATE FOREIGN TABLE mpp_ft1 (
	c1 int,
	c2 int
) SERVER pgserver OPTIONS (schema_name 'MPP_S 1', table_name 'T 1');

-- ===================================================================
-- tests for validator
-- ===================================================================
CREATE SERVER testserver FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (dbname 'contrib_regression', multi_hosts 'localhost localhost',
           multi_ports '5432 5432', num_segments '2', mpp_execute 'all segments');

CREATE SERVER testserver FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (dbname 'contrib_regression', multi_hosts 'localhost localhost',
           multi_ports '5432', num_segments '2', mpp_execute 'multi servers');

CREATE SERVER testserver FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (dbname 'contrib_regression', multi_hosts 'localhost localhost',
           multi_ports '5432 5432', num_segments '1', mpp_execute 'multi servers');

CREATE FOREIGN TABLE mpp_test (
	c1 int,
	c2 int
) SERVER pgserver OPTIONS (mpp_execute 'multi servers');

-- ===================================================================
-- Simple queries
-- ===================================================================
EXPLAIN VERBOSE SELECT * FROM mpp_ft1 ORDER BY c1;
SELECT * FROM mpp_ft1 ORDER BY c1;

ALTER FOREIGN TABLE mpp_ft1 OPTIONS (add use_remote_estimate 'true');
EXPLAIN VERBOSE SELECT * FROM mpp_ft1 ORDER BY c1;
SELECT * FROM mpp_ft1 ORDER BY c1;
ALTER FOREIGN TABLE mpp_ft1 OPTIONS (drop use_remote_estimate);

-- ===================================================================
-- When mpp_execute = 'multi servers', we don't support IMPORT FOREIGN SCHEMA
-- ===================================================================
CREATE SCHEMA mpp_import_dest;
IMPORT FOREIGN SCHEMA import_source FROM SERVER pgserver INTO mpp_import_dest;

-- ===================================================================
-- Test two-stage transaction commit for multi-server foreign table
-- ===================================================================
-- remote postgres server 2 -- listening port 5555 drop column
\! env PGOPTIONS='' psql -p 5555 contrib_regression -c 'alter table "MPP_S 1"."T 1" drop column c2'
insert into mpp_ft1 select i,i from generate_series(1,100) i;
\! env PGOPTIONS='' psql -p 5555 contrib_regression -c 'alter table "MPP_S 1"."T 1" add column c2 int'
create table test_count(c1 int, c2 int);
insert into test_count select * from mpp_ft1;
select count(*) from test_count;
-- =====================================================================================
-- Test two-stage insert multi-server foreign table when num_segments is larger then 3
-- =====================================================================================
\! env PGOPTIONS='' psql -p 5432 contrib_regression -c 'truncate "MPP_S 1"."T 1"'
\! env PGOPTIONS='' psql -p 5555 contrib_regression -c 'truncate "MPP_S 1"."T 1"'
alter server pgserver options(set num_segments '4', set multi_hosts 'localhost localhost localhost localhost', set multi_ports '5432 5432 5555 5555');
\c
truncate test_count;
insert into mpp_ft1 select i, i from generate_series(1,100) as i;
insert into test_count select * from mpp_ft1;
select count(*) from test_count;
alter server pgserver options(set num_segments '2', set multi_hosts 'localhost localhost', set multi_ports '5432 5555');
\c

-- =====================================================================================
-- Test DISTRIBUTED BY clause for postgres_fdw foreign table
-- =====================================================================================
\! env PGOPTIONS='' psql -p 5432 contrib_regression -c 'truncate "MPP_S 1"."T 1"'
\! env PGOPTIONS='' psql -p 5555 contrib_regression -c 'truncate "MPP_S 1"."T 1"'

CREATE FOREIGN TABLE mpp_dist_1 (
	c1 int,
	c2 int
) SERVER pgserver OPTIONS (mpp_execute 'all segments', schema_name 'MPP_S 1', table_name 'T 1') DISTRIBUTED BY (c1);

CREATE FOREIGN TABLE mpp_dist (
	c1 int,
	c2 int
) SERVER pgserver OPTIONS (schema_name 'MPP_S 1', table_name 'T 1') DISTRIBUTED BY (c1);

INSERT INTO mpp_dist SELECT id, id % 5 FROM generate_series(1, 20) as id;
SELECT * FROM mpp_dist ORDER BY c1;

EXPLAIN VERBOSE SELECT * FROM mpp_dist t1 JOIN mpp_dist t2 ON t1.c1 = t2.c1 ORDER BY t1.c1;
SELECT * FROM mpp_dist t1 JOIN mpp_dist t2 ON t1.c1 = t2.c1 ORDER BY t1.c1;
