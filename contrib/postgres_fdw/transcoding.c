/*-------------------------------------------------------------------------
 *
 * transcoding.c
 *		  Transcoding implementation for postgres.
 * When "mpp_execute" is set to "all segments", postgres_fdw will try to
 * run on MPP mode, the server option "number_of_segments" will set the
 * executing QE numbers of the MPP mode.
 * Postgres FDW is not designed to be executed on multi-QEs, however it
 * is the first FDW extension which can give examples of other following
 * FDW extensions.
 * 
 * So we want to add support MPP AGG push down for postgres FDW to give 
 * an example to other FDWs which need to run on multiple QEs.
 * 
 * In order to push down MPP AGG functions to multiple remote PGs, we 
 * have to deparse the partial agg sqls to form the result of partial 
 * AGG. However, if the transtype of the AGG function is internal, we
 * have to do the transcoding from the result of the Partial Agg push 
 * down which is a final result into an internal result which is what 
 * combine function of the 2-staged AGG function wants.
 * 
 * Take AVG(column) function for example, we have to deparse 
 * array[count(column), sum(column)] sql to remote, then the returned 
 * value is a final result string, but the combine AGG function wants
 * an internal struct like PolyNumAggState. So we need to do a transcoding
 * to meet the needs for input format of combine AGG function.
 * 
 * Portions Copyright (c) 2012-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *			contrib/postgres_fdw/transcoding.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "postgres_fdw.h"

#include "fmgr.h"
#include "utils/array.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"
#include "nodes/execnodes.h"

/* Initialize fake AggState to call agg functions */
static void Init_AggState(AggState *aggstate)
{
	Node *node = (Node *)aggstate;
	memset(aggstate, 0, sizeof(AggState));
	node->type = T_AggState;
}
static Datum CallAggfunction1(FmgrInfo *flinfo, Datum arg1, fmNodePtr *context)
{
	LOCAL_FCINFO(fcinfo, 1);

	InitFunctionCallInfoData(*fcinfo, flinfo, 1, InvalidOid, (Node *)context, NULL);
	fcinfo->args[0].value = arg1;
	fcinfo->args[0].isnull = false;

	return FunctionCallInvoke(fcinfo);
}

/* 
 * Handle trancoding for agg function 2100 and 2107
 * The input str is formatted like {$count, $sum} 
 * 1. Transcode the input str into ArrayType whose data type is int64
 * 2. Construct internal type PolyNumAggState
 * 3. Serialize PolyNumAggState into bytea
 */
static Datum transfn_to_polynumaggstate(PG_FUNCTION_ARGS)
{
	ArrayType	*internal_array = NULL;
	char		*str = PG_GETARG_CSTRING(0);
	Oid			element_type = INT8OID;
	int32		typmod = PG_GETARG_INT32(2);
	Int8TransTypeData *transdata = NULL;
	PolyNumAggState *internal_polynum;
	FmgrInfo flinfo;
	AggState aggstate;


	memset(&flinfo, 0, sizeof(FmgrInfo));
	fmgr_info_cxt(fmgr_internal_function("array_in"), &flinfo, CurrentMemoryContext);
	internal_array = (ArrayType *)InputFunctionCall(&flinfo, str, element_type, typmod);

	transdata = (Int8TransTypeData *) ARR_DATA_PTR (internal_array);
	internal_polynum = makePolyNumAggStateCurrentContext(false);
	internal_polynum->N = transdata->count;
#ifdef HAVE_INT128
	internal_polynum->sumX = (int128)transdata->sum;
#else
	Datum sumd;
	sumd = DirectFunctionCall1(int8_numeric,
							   Int64GetDatumFast(transdata->sum));
	do_numeric_accum(internal_polynum, (Numeric)sumd);
	internal_polynum->N--;
#endif
    
	memset(&flinfo, 0, sizeof(FmgrInfo));
	fmgr_info_cxt(fmgr_internal_function("int8_avg_serialize"), &flinfo, CurrentMemoryContext);
	Init_AggState(&aggstate);
	return CallAggfunction1(&flinfo, (Datum)internal_polynum, (fmNodePtr *)&aggstate);
}

/* 
 * Handle trancoding for agg function 2103 and 2114
 * The input str is formatted like {$count, $sum} 
 * 1. Transcode the input str into ArrayType whose data type is numeric
 * 2. Construct internal type NumericAggState
 * 3. Serialize NumericAggState into bytea
 */
static Datum transfn_to_numericaggstate(PG_FUNCTION_ARGS)
{
	ArrayType	*internal_array = NULL;
	char		*str = PG_GETARG_CSTRING(0);
	Oid			element_type = NUMERICOID;
	int32		typmod = PG_GETARG_INT32(2);
	int64		countd;
	Datum		sumd;
	Datum		*internal_numeric;
	int			num_numeric;
	NumericAggState *target_state;
	AggState aggstate;

	FmgrInfo flinfo;
	memset(&flinfo, 0, sizeof(FmgrInfo));
	fmgr_info_cxt(fmgr_internal_function("array_in"), &flinfo, CurrentMemoryContext);
	internal_array = (ArrayType *)InputFunctionCall(&flinfo, str, element_type, typmod);

	deconstruct_array(internal_array, NUMERICOID, -1, false, 'i',
					  &internal_numeric, NULL, &num_numeric);

	countd = (int64)DirectFunctionCall1(numeric_int8, internal_numeric[0]);
	sumd = internal_numeric[1];
	target_state = makeNumericAggStateCurrentContext(false);
	target_state->N = countd;
	do_numeric_accum(target_state, (Numeric)sumd);
	target_state->N--;

	memset(&flinfo, 0, sizeof(FmgrInfo));
	fmgr_info_cxt(fmgr_internal_function("numeric_avg_serialize"), &flinfo, CurrentMemoryContext);
	Init_AggState(&aggstate);
	return CallAggfunction1(&flinfo, (Datum)target_state, (fmNodePtr *)&aggstate);
}

PGFunction GetTranscodingFnFromOid(Oid aggfnoid) {
	PGFunction refnaddr = NULL;
	if (aggfnoid == InvalidOid) 
	{
		return NULL;
	}
	switch (aggfnoid) 
	{
		case 2100:
		case 2107:
			/* Those aggregate functions have same type of aggstate -- PolyNumAggState.
			 * - 2100 pg_catalog.avg int8|bigint
			 * - 2107 pg_catalog.sum int8|bigint
			 */
			refnaddr = transfn_to_polynumaggstate;
			break;
		case 2103:
		case 2114:
			/* Those aggregate functions have same type of aggstate -- NumericAggState.
			 * - 2103 pg_catalog.avg numeric
			 * - 2114 pg_catalog.sum numeric
			 */
			refnaddr = transfn_to_numericaggstate;
			break;
		default:
			break;
	}
	return refnaddr;
}