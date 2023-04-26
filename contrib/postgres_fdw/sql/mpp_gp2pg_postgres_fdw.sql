-- This file is used to test mpp pusdown.

-- ===================================================================
-- create FDW objects
-- ===================================================================
SET timezone = 'PST8PDT';
set optimizer_trace_fallback = on;

CREATE EXTENSION postgres_fdw;

CREATE SERVER pgserver FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (dbname 'contrib_regression', host 'localhost', port '5432', num_segments '4');

CREATE USER MAPPING FOR CURRENT_USER SERVER pgserver;

-- ===================================================================
-- create objects used through FDW pgserver server
-- ===================================================================
\! env PGOPTIONS='' psql -p ${PG_PORT} contrib_regression -f sql/postgres_sql/mpp_gp2pg_postgres_init.sql

-- ===================================================================
-- create foreign tables
-- ===================================================================
CREATE FOREIGN TABLE mpp_ft1 (
	c1 int,
	c2 int
) SERVER pgserver OPTIONS (schema_name 'MPP_S 1', table_name 'T 1', mpp_execute 'all segments');

CREATE FOREIGN TABLE ft1 (
	c1 int,
	c2 int
) SERVER pgserver OPTIONS (schema_name 'MPP_S 1', table_name 'T 1');

-- ===================================================================
-- test simple query
-- ===================================================================
EXPLAIN VERBOSE
SELECT COUNT(*) FROM mpp_ft1;
SELECT COUNT(*) FROM mpp_ft1;
SELECT COUNT(*) FROM ft1;

EXPLAIN VERBOSE
SELECT SUM(c1) FROM mpp_ft1;
SELECT SUM(c1) FROM mpp_ft1;
SELECT SUM(c1) FROM ft1;

EXPLAIN VERBOSE
SELECT avg(c1) FROM mpp_ft1;
SELECT avg(c1) FROM mpp_ft1;
SELECT avg(c1) FROM ft1;