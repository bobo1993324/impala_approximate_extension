#ifndef APPROXCOUNT_H
#define APPROXCOUNT_H

#include <impala_udf/udf.h>

#include <cassert>
#include <cmath>
#include <sstream>

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

template <typename T>
struct AvgAgg {
    long count;
    T sum;
    double variance;
};

template <typename T>
void ApproxAvgInit(FunctionContext* context, StringVal* val) {
    val->is_null = false;
    val->len = sizeof(AvgAgg<T>);
    val->ptr = context->Allocate(val->len);
    memset(val->ptr, 0, val->len);
    AvgAgg<T> *agg = reinterpret_cast<AvgAgg<T> *>(val->ptr);
    agg->count = 0;
    agg->sum = 0;
    agg->variance = 0;
}

template <typename T1, typename T2>
void ApproxAvgUpdate(FunctionContext *context, const T1 &input, StringVal *val)
{
    if (input.is_null) return;
    assert(!val->is_null);
    assert(val->len == sizeof(AvgAgg<T2>));

    AvgAgg<T2> *agg = reinterpret_cast<AvgAgg<T2> *>(val->ptr);
    double v = input.val;
    agg->count ++;
    agg->sum += input.val;
    if (agg->count > 1) {
        double t = agg->count * v - agg->sum;
        agg->variance += (t * t) / ((double) agg->count * (agg->count - 1));
    }
}

template <typename T>
void ApproxAvgMerge(FunctionContext *context, const StringVal &src, StringVal *dst)
{
    if (src.is_null) return;

    const AvgAgg<T>* src_struct = reinterpret_cast<const AvgAgg<T>*>(src.ptr);
    AvgAgg<T>* dst_struct = reinterpret_cast<AvgAgg<T>*>(dst->ptr);
    if (dst_struct->count == 0) {
        dst_struct->count = src_struct->count;
        dst_struct->sum = src_struct->sum;
        dst_struct->variance = src_struct->variance;
        return;
    }
    if (dst_struct->count != 0 && src_struct->count != 0) {
        dst_struct->count += src_struct->count;
        dst_struct->sum += src_struct->sum;
        double t = (src_struct->count / (double) dst_struct->count) * dst_struct->sum - src_struct->sum;
        dst_struct->variance += src_struct->variance +
                (dst_struct->count / (double) src_struct->count)
                    / (src_struct->count + dst_struct->count) * t * t;
    }
}

const StringVal ApproxAvgSerialize(FunctionContext* context, const StringVal& val) {
    assert(!val.is_null);
    StringVal result(context, val.len);
    memcpy(result.ptr, val.ptr, val.len);
    context->Free(val.ptr);
    return result;
}

template <typename T>
StringVal ApproxAvgFinalize(FunctionContext *context, const StringVal &val)
{
    assert(!val.is_null);
    assert(val.len == sizeof(AvgAgg<T>));
    const AvgAgg<T> *myagg = reinterpret_cast<const AvgAgg<T> *>(val.ptr);
    StringVal result;
    if (myagg->count == 0) {
        result = StringVal::null();
    } else {
        std::stringstream ss;
        ss << myagg->sum / myagg->count;
        ss << " +/- ";
        ss << 2.575 * sqrt(myagg->variance / (myagg->count * myagg->count));
        ss << " (99% Confidence) ";
        std::string str = ss.str();
        result = ToStringVal(context, str);
    }
    return result;
}
StringVal ApproxAvgDoubleFinalize(FunctionContext* context, const StringVal& val);

void ApproxAvgDoubleInit(FunctionContext* context, StringVal* val);
void ApproxAvgDoubleUpdate(FunctionContext *context, const DoubleVal &input, StringVal *val);
void ApproxAvgDoubleMerge(FunctionContext *context, const StringVal &src, StringVal *dst);
const StringVal ApproxAvgDoubleSerialize(FunctionContext *context, const StringVal &val);
StringVal ApproxAvgDoubleFinalize(FunctionContext *context, const StringVal &val);

void ApproxAvgIntInit(FunctionContext* context, StringVal* val);
void ApproxAvgIntUpdate(FunctionContext *context, const IntVal &input, StringVal *val);
void ApproxAvgIntMerge(FunctionContext *context, const StringVal &src, StringVal *dst);
const StringVal ApproxAvgIntSerialize(FunctionContext *context, const StringVal &val);
StringVal ApproxAvgIntFinalize(FunctionContext *context, const StringVal &val);

#endif // APPROXCOUNT_H

