#ifndef APPROXCOUNT_H
#define APPROXCOUNT_H

#include <impala_udf/udf.h>

using namespace impala_udf;

struct CountAgg {
    long count;
    double totalRows;
    long sampleRows;
};

void ApproxCountInit(FunctionContext* context, StringVal* val);
void ApproxCountUpdate(FunctionContext* context, const IntVal& input, const DoubleVal& sampleWeight,
                     StringVal* val);
void ApproxCountMerge(FunctionContext* context, const StringVal &src, StringVal* dst);
const StringVal ApproxCountSerialize(FunctionContext* context, const StringVal& val);
StringVal ApproxCountFinalize(FunctionContext* context, const StringVal& val);
#endif // APPROXCOUNT_H
