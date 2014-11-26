#include "approxCount.h"
#include <cmath>
#include <sstream>
#include <cassert>

using namespace impala_udf;
using namespace std;
template <typename T>
StringVal ToStringVal(FunctionContext* context, const T& val) {
  stringstream ss;
  ss << val;
  string str = ss.str();
  StringVal string_val(context, str.size());
  memcpy(string_val.ptr, str.c_str(), str.size());
  return string_val;
}


void ApproxCountInit(FunctionContext *context, StringVal *val)
{
    val->is_null = false;
    val->len = sizeof(CountAgg);
    val->ptr = context->Allocate(val->len);
    memset(val->ptr, 0, val->len);
    CountAgg *countAgg = reinterpret_cast<CountAgg *>(val->ptr);
    countAgg->count = 0;
    countAgg->sampleRows = 0;
    countAgg->totalRows = 0;
}

void ApproxCountUpdate(FunctionContext *context, const IntVal &input, const DoubleVal &sampleWeight, StringVal *val)
{
    if (input.is_null) return;
    assert(!val->is_null);
    assert(val->len == sizeof(CountAgg));

    CountAgg *countAgg = reinterpret_cast<CountAgg *>(val->ptr);
    countAgg->sampleRows ++;
    countAgg->totalRows += sampleWeight.val;
    countAgg->count ++;
}

void ApproxCountMerge(FunctionContext *context, const StringVal &src, StringVal *dst)
{
    if (src.is_null) return;

    const CountAgg* src_struct = reinterpret_cast<const CountAgg*>(src.ptr);
    CountAgg* dst_struct = reinterpret_cast<CountAgg*>(dst->ptr);
    dst_struct->totalRows += src_struct->totalRows;
    dst_struct->sampleRows += src_struct->sampleRows;
    dst_struct->count += src_struct->count;
}


const StringVal ApproxCountSerialize(FunctionContext *context, const StringVal &val)
{
    assert(!val.is_null);
    StringVal result(context, val.len);
    memcpy(result.ptr, val.ptr, val.len);
    context->Free(val.ptr);
    return result;
}


StringVal ApproxCountFinalize(FunctionContext *context, const StringVal &val)
{
    assert(!val.is_null);
    assert(val.len == sizeof(CountAgg));
    const CountAgg *myagg = reinterpret_cast<const CountAgg*>(val.ptr);
    StringVal result;
    long approx_count = (long) ((double) (myagg->count * myagg->totalRows) / myagg->sampleRows);
    double probability = ((double) myagg->count) / ((double) myagg->sampleRows);

    std::stringstream ss;
    ss << approx_count;
    ss << " +/- ";
    ss << ceil(2.575 * (1.0 * myagg->totalRows / myagg->sampleRows) * sqrt(myagg->count * (1 - probability)));
    ss << " (99% Confidence) ";
    std::string str = ss.str();
    result = ToStringVal(context, str);
    return result;
}
