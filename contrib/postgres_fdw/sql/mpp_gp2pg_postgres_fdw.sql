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

CREATE FOREIGN TABLE mpp_ft2 (
	c1 int,
	c2 int
) SERVER pgserver OPTIONS (schema_name 'MPP_S 1', table_name 'T 2');

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
-- Aggregate and grouping queries
-- ===================================================================
-- Simple aggregates with different data types
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(c1), count(c3), count(c4), count(c5), count(c6), count(c7) FROM mpp_ft1;
SELECT count(c1), count(c3), count(c4), count(c5), count(c6), count(c7) FROM mpp_ft1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1), sum(c3), sum(c4), sum(c5), sum(c6), sum(c7) FROM mpp_ft1;
SELECT sum(c1), sum(c3), sum(c4), sum(c5), sum(c6), sum(c7) FROM mpp_ft1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT avg(c1), avg(c3), avg(c4), avg(c5), avg(c6), avg(c7) FROM mpp_ft1;
SELECT avg(c1), avg(c3), avg(c4), avg(c5), avg(c6), avg(c7) FROM mpp_ft1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT min(c1), min(c3), min(c4), min(c5), min(c6), min(c7) FROM mpp_ft1;
SELECT min(c1), min(c3), min(c4), min(c5), min(c6), min(c7) FROM mpp_ft1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT max(c1), max(c3), max(c4), max(c5), max(c6), max(c7) FROM mpp_ft1;
SELECT max(c1), max(c3), max(c4), max(c5), max(c6), max(c7) FROM mpp_ft1;

-- Simple Aggregates with GROUP BY clause
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(c1), sum(c3), avg(c4), min(c5), max(c6), count(c1) * (random() <= 1)::int as count2 FROM mpp_ft1 GROUP BY c2 ORDER BY c2;
SELECT count(c1), sum(c3), avg(c4), min(c5), max(c6), count(c1) * (random() <= 1)::int as count2 FROM mpp_ft1 GROUP BY c2 ORDER BY c2;

-- Aggregate is not pushed down as aggregation contains random()
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1 * (random() <= 1)::int) as sum, avg(c1) FROM mpp_ft1;
SELECT sum(c1 * (random() <= 1)::int) as sum, avg(c1) FROM mpp_ft1;

-- GROUP BY clause having expressions
/* FIXME: Aggregates are not pushed down.
          Because for Remote SQL of partial agg, non-grouping columns 
		  might neither appear in the GROUP BY clause nor be used in 
		  an aggregate function.
		  This is unsafe to make foreign grouping.
*/
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2/2, sum(c2) * (c2/2) FROM mpp_ft1 GROUP BY c2/2 ORDER BY c2/2;
SELECT c2/2, sum(c2) * (c2/2) FROM mpp_ft1 GROUP BY c2/2 ORDER BY c2/2;

-- Aggregates in subquery are pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(x.a), sum(x.a) FROM (SELECT c2 a, sum(c1) b FROM mpp_ft1 GROUP BY c2, sqrt(c1) ORDER BY 1, 2) x;
SELECT count(x.a), sum(x.a) FROM (SELECT c2 a, sum(c1) b FROM mpp_ft1 GROUP BY c2, sqrt(c1) ORDER BY 1, 2) x;

-- Aggregate is still pushed down by taking unshippable expression out
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 FROM mpp_ft1 GROUP BY c2 ORDER BY 1, 2;
SELECT c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 FROM mpp_ft1 GROUP BY c2 ORDER BY 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2 * (random() <= 1)::int as c2 FROM mpp_ft1 GROUP BY c2 * (random() <= 1)::int ORDER BY 1;
SELECT c2 * (random() <= 1)::int as c2 FROM mpp_ft1 GROUP BY c2 * (random() <= 1)::int ORDER BY 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(c2) w, c2 x, 5 y, 7.0 z FROM mpp_ft1 GROUP BY 2, y, 9.0::int ORDER BY 2;
SELECT count(c2) w, c2 x, 5 y, 7.0 z FROM mpp_ft1 GROUP BY 2, y, 9.0::int ORDER BY 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, c2 FROM mpp_ft1 WHERE c2 > 6 GROUP BY 1, 2 ORDER BY sum(c1);
SELECT c2, c2 FROM mpp_ft1 WHERE c2 > 6 GROUP BY 1, 2 ORDER BY sum(c1);

-- Testing HAVING clause
-- It's unsafe for partial agg to push down HAVING clause.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, sum(c1) FROM mpp_ft1 GROUP BY c2 HAVING avg(c1) < 500 AND sum(c1) < 49800 ORDER BY c2;
SELECT c2, sum(c1) FROM mpp_ft1 GROUP BY c2 HAVING avg(c1) < 500 AND sum(c1) < 49800 ORDER BY c2;

-- Remote aggregate in combination with a local Param (for the output
-- of an initplan) can be trouble, per bug #15781
EXPLAIN (VERBOSE, COSTS OFF)
SELECT exists(SELECT 1 FROM pg_aggregate), sum(c1) FROM mpp_ft1;
SELECT exists(SELECT 1 FROM pg_aggregate), sum(c1) FROM mpp_ft1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT exists(SELECT 1 FROM pg_aggregate), sum(c1) FROM mpp_ft1 group by 1;
SELECT exists(SELECT 1 FROM pg_aggregate), sum(c1) FROM mpp_ft1 group by 1;

-- Testing ORDER BY, DISTINCT, FILTER within aggregates
-- ORDER BY within aggregate, same column used to order
-- TODO: Now we don't support array_agg mpp pushdown.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT array_agg(c1 ORDER BY c1) FROM mpp_ft1 WHERE c1 < 100 GROUP BY c2 ORDER BY 1;
SELECT array_agg(c1 ORDER BY c1) FROM mpp_ft1 WHERE c1 < 100 GROUP BY c2 ORDER BY 1;

-- FILTER within aggregate
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1) FILTER (WHERE c1 < 100 AND c2 > 5) FROM mpp_ft1 GROUP BY c2 ORDER BY 1 nulls last;
SELECT sum(c1) FILTER (WHERE c1 < 100 AND c2 > 5) FROM mpp_ft1 GROUP BY c2 ORDER BY 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate
-- It's unsafe to push down DISTINCT within aggregates for mpp_execute = 'all segments'.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1%3), sum(DISTINCT c1%3 ORDER BY c1%3) FILTER (WHERE c1%3 < 2), c2 FROM mpp_ft1 WHERE c2 = 6 GROUP BY c2;
SELECT sum(c1%3), sum(DISTINCT c1%3 ORDER BY c1%3) FILTER (WHERE c1%3 < 2), c2 FROM mpp_ft1 WHERE c2 = 6 GROUP BY c2;

-- Aggregate not pushed down as FILTER condition is not pushable
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1) FILTER (WHERE (c1 / c1) * random() <= 1) FROM mpp_ft1 GROUP BY c2 ORDER BY 1;
SELECT sum(c1) FILTER (WHERE (c1 / c1) * random() <= 1) FROM mpp_ft1 GROUP BY c2 ORDER BY 1;

-- Set use_remote_estimate to true
ALTER FOREIGN TABLE mpp_ft1 OPTIONS(add use_remote_estimate 'true');

EXPLAIN (VERBOSE, COSTS OFF)
SELECT min(c5) FROM mpp_ft1;
SELECT min(c5) FROM mpp_ft1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(c1), max(c6) FROM mpp_ft1 GROUP BY c2;
SELECT count(c1), max(c6) FROM mpp_ft1 GROUP BY c2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(c1), sum(c3), avg(c4), min(c5), max(c6), count(c1) * (random() <= 1)::int as count2 FROM mpp_ft1 GROUP BY c2 ORDER BY c2;
SELECT count(c1), sum(c3), avg(c4), min(c5), max(c6), count(c1) * (random() <= 1)::int as count2 FROM mpp_ft1 GROUP BY c2 ORDER BY c2;

ALTER FOREIGN TABLE mpp_ft1 OPTIONS(set use_remote_estimate 'false');

-- limit is not pushed down when mpp_execute is set to 'all segments'
-- limit with agg functions
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(c1), max(c6) FROM mpp_ft1 GROUP BY c2 order by c2 limit 3;
SELECT count(c1), max(c6) FROM mpp_ft1 GROUP BY c2 order by c2 limit 3;
-- limit with normal scan without agg functions
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, c2 FROM mpp_ft1 order by c1 limit 3;
SELECT c1, c2 FROM mpp_ft1 order by c1 limit 3;
-- join is not safe to pushed down when mpp_execute is set to 'all segments'
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*), sum(t1.c1), avg(t2.c2) FROM mpp_ft1 t1 inner join mpp_ft1 t2 on (t1.c1 = t2.c1) where t1.c1 = 2;
SELECT count(*), sum(t1.c1), avg(t2.c2) FROM mpp_ft1 t1 inner join mpp_ft1 t2 on (t1.c1 = t2.c1) where t1.c1 = 2;

-- ===================================================================
-- Insert, update and delete
-- ===================================================================
INSERT INTO mpp_ft2 SELECT id, id % 5 FROM generate_series(1, 10) as id;
SELECT * FROM mpp_ft2 ORDER BY c1;
UPDATE mpp_ft2 SET c1 = 0 WHERE c2 = 0;
SELECT * FROM mpp_ft2 ORDER BY c1;
DELETE FROM mpp_ft2;
SELECT * FROM mpp_ft2 ORDER BY c1;

-- ===================================================================
-- When mpp_execute = 'multi servers', we don't support IMPORT FOREIGN SCHEMA
-- ===================================================================
CREATE SCHEMA mpp_import_dest;
IMPORT FOREIGN SCHEMA import_source FROM SERVER pgserver INTO mpp_import_dest;
