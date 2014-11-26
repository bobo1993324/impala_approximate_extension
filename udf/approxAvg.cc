#include "approxAvg.h"

using namespace impala_udf;

void ApproxAvgDoubleInit(FunctionContext* context, StringVal* val) {
    ApproxAvgInit<double>(context, val);
}
void ApproxAvgDoubleUpdate(FunctionContext *context, const DoubleVal &input, StringVal *val) {
    ApproxAvgUpdate<DoubleVal, double>(context, input, val);
}
void ApproxAvgDoubleMerge(FunctionContext *context, const StringVal &src, StringVal *dst) {
    ApproxAvgMerge<double>(context, src, dst);
}
const StringVal ApproxAvgDoubleSerialize(FunctionContext *context, const StringVal &val) {
    return ApproxAvgSerialize(context, val);
}
StringVal ApproxAvgDoubleFinalize(FunctionContext *context, const StringVal &val) {
    return ApproxAvgFinalize<double>(context, val);
}

void ApproxAvgIntInit(FunctionContext* context, StringVal* val) {
    ApproxAvgInit<int>(context, val);
}
void ApproxAvgIntUpdate(FunctionContext *context, const IntVal &input, StringVal *val) {
    ApproxAvgUpdate<IntVal, int>(context, input, val);
}
void ApproxAvgIntMerge(FunctionContext *context, const StringVal &src, StringVal *dst) {
    ApproxAvgMerge<int>(context, src, dst);
}
const StringVal ApproxAvgIntSerialize(FunctionContext *context, const StringVal &val) {
    return ApproxAvgSerialize(context, val);
}
StringVal ApproxAvgIntFinalize(FunctionContext *context, const StringVal &val) {
    return ApproxAvgFinalize<int>(context, val);
}
