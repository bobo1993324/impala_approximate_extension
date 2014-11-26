#ifndef APPROXSUM_H
#define APPROXSUM_H

#include <impala_udf/udf.h>

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

template <typename T>
struct SumAgg {
    bool empty;
    long count;
    T sum;
    double variance;
    double totalRows;
    long sampleRows;
};

template <typename T>
void ApproxSumInit(FunctionContext* context, StringVal* val) {
    val->is_null = false;
    val->len = sizeof(SumAgg<T>);
    val->ptr = context->Allocate(val->len);
    memset(val->ptr, 0, val->len);
    SumAgg<T> *sumAgg = reinterpret_cast<SumAgg<T> *>(val->ptr);
    sumAgg->empty = true;
    sumAgg->count = 0;
    sumAgg->sampleRows = 0;
    sumAgg->sum = 0;
    sumAgg->totalRows = 0;
    sumAgg->variance = 0;
}

template <typename T1, typename T2>
void ApproxSumUpdate(FunctionContext *context, const T1 &input, const DoubleVal &sampleWeight, StringVal *val)
{
    if (input.is_null) return;
    assert(!val->is_null);
    assert(val->len == sizeof(SumAgg<T2>));
    SumAgg<T2> *sumAgg = reinterpret_cast<SumAgg<T2> *>(val->ptr);
    sumAgg->sampleRows ++;
    sumAgg->totalRows += sampleWeight.val;
    sumAgg->count ++;
    sumAgg->sum += input.val;
    if (sumAgg->count > 1) {
        double t = sumAgg->count * input.val - sumAgg->sum;
        sumAgg->variance += (t * t) / ((double) sumAgg->count * (sumAgg->count - 1));
    }
}

template <typename T>
void ApproxSumMerge(FunctionContext *context, const StringVal &src, StringVal *dst)
{
    if (src.is_null) return;
    const SumAgg<T>* src_struct = reinterpret_cast<const SumAgg<T> *>(src.ptr);
    SumAgg<T>* dst_struct = reinterpret_cast<SumAgg<T> *>(dst->ptr);
    dst_struct->empty = false;
    dst_struct->totalRows += src_struct->totalRows;
    dst_struct->sampleRows += src_struct->sampleRows;
    if (dst_struct->count == 0) {
       dst_struct->variance = src_struct->variance;
       dst_struct->count = src_struct->count;
       dst_struct->sum = src_struct->sum;
       return;
    }
    if (src_struct->count != 0 && dst_struct->count != 0) {
        long n = dst_struct->count;
        long m = src_struct->count;
        long a = dst_struct->sum;
        long b = src_struct->sum;

        dst_struct->empty = false;
        dst_struct->count += src_struct->count;
        dst_struct->sum += src_struct->sum;
        double t = (m / (double) n) * a - b;
        dst_struct->variance += src_struct->variance
                      + ((n / (double) m) / ((double) n + m)) * t * t;
    }
}

template <typename T>
StringVal ApproxSumFinalize(FunctionContext *context, const StringVal &val)
{
    assert(!val.is_null);
    assert(val.len == sizeof(SumAgg<T>));
    const SumAgg<T> *myagg = reinterpret_cast<const SumAgg<T> *>(val.ptr);
    StringVal result;
    if (myagg->empty) {
        result = StringVal::null();
    } else {
        double approx_sum = ((double) myagg->sum * myagg->totalRows) / myagg->sampleRows;
        double probability = ((double) myagg->count) / ((double) myagg->totalRows);
        double mean = ((double) myagg->sum) / myagg->count;

        std::stringstream ss;
        ss << approx_sum;
        ss << " +/- ";
        ss << ceil((2.575 * (1.0 * myagg->totalRows / myagg->sampleRows) * sqrt(probability
              * ((myagg->variance) + ((1 - probability) * myagg->totalRows * mean * mean)))));
        ss << " (99% Confidence) ";
        std::string str = ss.str();
        result = ToStringVal(context, str);
    }
    return result;

}

const StringVal ApproxSumSerialize(FunctionContext *context, const StringVal &val)
{
    assert(!val.is_null);
    StringVal result(context, val.len);
    memcpy(result.ptr, val.ptr, val.len);
    context->Free(val.ptr);
    return result;
}

void ApproxSumIntInit(FunctionContext* context, StringVal* val);
void ApproxSumIntUpdate(FunctionContext* context, const IntVal& input, const DoubleVal& sampleWeight, StringVal* val);
void ApproxSumIntMerge(FunctionContext* context, const StringVal& src, StringVal* dst);
const StringVal ApproxSumIntSerialize(FunctionContext* context, const StringVal& val);
StringVal ApproxSumIntFinalize(FunctionContext* context, const StringVal& val);

void ApproxSumDoubleInit(FunctionContext* context, StringVal* val);
void ApproxSumDoubleUpdate(FunctionContext* context, const DoubleVal& input, const DoubleVal& sampleWeight, StringVal* val);
void ApproxSumDoubleMerge(FunctionContext* context, const StringVal& src, StringVal* dst);
const StringVal ApproxSumDoubleSerialize(FunctionContext* context, const StringVal& val);
StringVal ApproxSumDoubleFinalize(FunctionContext* context, const StringVal& val);

#endif // APPROXSUM_H
