-- This file is used to test mpp pusdown.

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
	c2 int,
	c3 smallint,
	c4 bigint,
	c5 real,
	c6 double precision,
	c7 numeric
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
EXPLAIN VERBOSE SELECT * FROM mpp_ft1;
ALTER FOREIGN TABLE mpp_ft1 OPTIONS (add use_remote_estimate 'true');
EXPLAIN VERBOSE SELECT * FROM mpp_ft1;
ALTER FOREIGN TABLE mpp_ft1 OPTIONS (drop use_remote_estimate);

-- ===================================================================
-- When mpp_execute = 'multi servers', we don't support IMPORT FOREIGN SCHEMA
-- ===================================================================
CREATE SCHEMA mpp_import_dest;
IMPORT FOREIGN SCHEMA import_source FROM SERVER pgserver INTO mpp_import_dest;
