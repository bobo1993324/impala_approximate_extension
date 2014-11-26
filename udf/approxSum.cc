#include "approxSum.h"

void ApproxSumIntInit(FunctionContext* context, StringVal* val) {
    ApproxSumInit<int>(context, val);
}

void ApproxSumIntUpdate(FunctionContext* context, const IntVal& input, const DoubleVal& sampleWeight, StringVal* val) {
    ApproxSumUpdate<IntVal, int>(context, input, sampleWeight, val);
}

void ApproxSumIntMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
    ApproxSumMerge<int>(context, src, dst);
}

const StringVal ApproxSumIntSerialize(FunctionContext* context, const StringVal& val) {
    return ApproxSumSerialize(context, val);
}

StringVal ApproxSumIntFinalize(FunctionContext* context, const StringVal& val) {
    return ApproxSumFinalize<int>(context, val);
}

void ApproxSumDoubleInit(FunctionContext* context, StringVal* val) {
    ApproxSumInit<double>(context, val);
}

void ApproxSumDoubleUpdate(FunctionContext* context, const DoubleVal& input, const DoubleVal& sampleWeight, StringVal* val) {
    ApproxSumUpdate<DoubleVal, double>(context, input, sampleWeight, val);
}

void ApproxSumDoubleMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
    ApproxSumMerge<double>(context, src, dst);
}

const StringVal ApproxSumDoubleSerialize(FunctionContext* context, const StringVal& val) {
    return ApproxSumSerialize(context, val);
}

StringVal ApproxSumDoubleFinalize(FunctionContext* context, const StringVal& val) {
    return ApproxSumFinalize<double>(context, val);
}
